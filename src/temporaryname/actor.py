from __future__ import annotations

import asyncio
import inspect
import logging
from enum import Enum
from typing import TYPE_CHECKING, Any

from .core import Envelope, StopReason
from .proxy import ActorAddr

if TYPE_CHECKING:
    from collections.abc import Coroutine, Generator

    from .core import Request, StopReasonType, T_Response
    from .hive import ActorHive


logger = logging.getLogger(__package__)


class LinkAction(Enum):
    Continue = 0
    Stop = 1


class BaseActor:
    def __init__(self, hive: ActorHive, actor_id: str):
        self._hive = hive
        self._id = actor_id
        self._mailbox: asyncio.Queue[Envelope] = asyncio.Queue()
        self._startup_future: asyncio.Future[bool] = asyncio.Future()
        self._running = False
        self._event_loop: asyncio.Task[None] | None = None
        self._bg_task: asyncio.Task[None] | None = None
        self._stop_event: asyncio.Event = asyncio.Event()
        self._stop_reason: StopReasonType | None = None

    @property
    def hive(self) -> ActorHive:
        return self._hive

    @property
    def id(self) -> str:
        return self._id

    def as_ref(self) -> ActorAddr:
        return ActorAddr(self._hive, self._id)

    def start(self) -> None:
        if self._event_loop:
            logger.warning(f"Actor {self.id} already running")
        else:
            self._event_loop = asyncio.create_task(self._loop())

    def stop(self, reason: StopReasonType = StopReason.Stop) -> None:
        if not self._event_loop:
            raise RuntimeError(f"Actor {self.id} not started")
        if self._stop_reason is not None:
            return
        self._stop_reason = reason
        self._event_loop.cancel()

    def stop_self(self) -> None:
        async def stop_deffered(hive: ActorHive, actor_id: str):
            hive.request_actor_stop(actor_id, StopReason.Stop)

        asyncio.create_task(stop_deffered(self._hive, self.id))

    def ask(self, request: Request[T_Response]) -> asyncio.Future[T_Response]:
        if self._is_called_from_self():
            raise RuntimeError("ask() called from the actor's message loop")
        future = asyncio.get_running_loop().create_future()
        self._mailbox.put_nowait(Envelope(payload=request, reply_to=future))
        return future

    def tell(self, event: Any):
        self._mailbox.put_nowait(Envelope(payload=event, reply_to=None))

    async def wait_for_startup(self) -> None:
        await self._startup_future

    async def wait_for_stop(self) -> None:
        await self._stop_event.wait()

    # --------------------- Lifecycle callbacks ----------------------------- #

    async def on_link_death(self, actor_id: str, reason: StopReasonType) -> LinkAction:
        logger.warning(f"Link '{actor_id}' died: {reason} - stopping {self.id}")
        return LinkAction.Stop

    async def on_message(self, message: Any) -> Any:
        type_name = type(message).__name__
        method_name = f"on_{snake_case(type_name)}"
        if (method := getattr(self, method_name, None)) is None:
            raise NotImplementedError(
                f"{type(self).__name__} does not implement {method_name}"
            )
        if not callable(method):
            raise TypeError(f"{type_name}.{method_name} must be callable")
        result = method(message)
        if inspect.isawaitable(result):
            return await result
        return result

    async def on_start(self) -> None:
        logger.debug(f"Actor {self.id} is starting")

    async def on_stop(self) -> None:
        logger.debug(f"Actor {self.id} is stopping")

    def on_task_create(self) -> Coroutine[Any, Any, None] | None:
        return None

    # --------------------- Internals --------------------------------------- #

    async def _loop(self) -> None:
        try:
            # Handle start lifecycle event
            await self.on_start()
        except Exception as e:
            logger.error(f"Actor {self.id} not started", exc_info=e)
            self._startup_future.set_exception(e)
            return

        if task := self.on_task_create():
            logger.debug(f"Starting background task {task}")
            self._bg_task = asyncio.create_task(task)
            self._bg_task.add_done_callback(self._on_bg_task_done)

        # Notify we started
        self._startup_future.set_result(True)
        self._running = True

        reason: StopReasonType = StopReason.Stop
        try:
            # Start the main loop
            while self._running:
                envelope = await self._mailbox.get()
                if envelope.payload is None and envelope.reply_to is None:
                    # Skip dummy envelopes
                    continue
                exception_to_set = None
                result_to_set = None
                try:
                    # Handle message lifecycle event
                    result_to_set = await self.on_message(envelope.payload)
                except Exception as e:
                    exception_to_set = e
                finally:
                    if envelope.reply_to and not envelope.reply_to.done():
                        if exception_to_set:
                            envelope.reply_to.set_exception(exception_to_set)
                        else:
                            envelope.reply_to.set_result(result_to_set)
                    self._mailbox.task_done()

        except asyncio.CancelledError:
            if self._stop_reason:
                reason = self._stop_reason

        except Exception as e:
            logger.error("Unhandled exception in main loop!", exc_info=e)
            reason = e

        finally:
            if self._bg_task:
                logger.debug("Cancelling background task")
                self._bg_task.cancel()

            self._drain_mailbox()

            # Handle stop lifecycle event
            try:
                await self.on_stop()
            except Exception as e:
                logger.error(f"on_stop() raised in actor {self.id}", exc_info=e)

            try:
                await self.hive.on_actor_stopped(self.id, reason)
            except Exception as e:
                logger.error(
                    f"on_actor_stopped() raised for actor {self.id}",
                    exc_info=e,
                )

            self._stop_event.set()

    def _drain_mailbox(self) -> None:
        while not self._mailbox.empty():
            try:
                envelope = self._mailbox.get_nowait()
                if envelope.reply_to and not envelope.reply_to.done():
                    envelope.reply_to.set_exception(
                        RuntimeError(
                            f"Actor {self.id} stopped while request was pending"
                        )
                    )
            except asyncio.QueueEmpty:
                break

    def _on_bg_task_done(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        if exc := task.exception():
            logger.error(f"Background task failed for actor {self.id}", exc_info=exc)
            self.stop(exc)

    def _is_called_from_self(self) -> bool:
        current_task = asyncio.current_task()
        return current_task is not None and current_task is self._event_loop


def snake_case(name: str) -> str:
    """Convert a CamelCase name to snake_case."""

    def sliding_window(x: str) -> Generator[tuple[str, str, str], Any, Any]:
        x = "_" + x + "_"
        for i in range(1, len(x) - 1):
            yield (x[i - 1], x[i], x[i + 1])

    def convert(prev: str, cur: str, nxt: str) -> str:
        if cur == "_" or cur.isdigit():
            return cur
        if cur.islower() and not prev.isdigit():
            return cur
        if cur.isupper() and prev.isupper() and not nxt.islower():
            return cur.lower()
        return "_" + cur.lower()

    return "".join(
        convert(prev, cur, nxt) for prev, cur, nxt in sliding_window(name)
    ).lstrip("_")
