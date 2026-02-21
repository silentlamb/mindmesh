import asyncio
import inspect
import logging
from typing import TYPE_CHECKING, Any

from .core import Envelope
from .proxy import ActorRef

if TYPE_CHECKING:
    from .core import Request, T_Response
    from .hive import ActorHive


logger = logging.getLogger(__package__)


class BaseActor:
    def __init__(self, hive: "ActorHive", id: str):
        self._hive = hive
        self._id = id
        self._mailbox: asyncio.Queue[Envelope] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None
        self._startup_future: asyncio.Future[bool] = asyncio.Future()
        self._running = False

    @property
    def hive(self) -> "ActorHive":
        return self._hive

    @property
    def id(self) -> str:
        return self._id

    def as_ref(self) -> "ActorRef":
        return ActorRef(self._hive, self._id)

    def start(self):
        if self._task:
            logger.warning(f"Actor {self.id} already running")
        else:
            self._task = asyncio.create_task(self._loop())

    def stop(self):
        if self._task:
            self._task.cancel()
        else:
            logger.warning(f"Actor {self.id} not started")

    def ask(self, request: "Request[T_Response]") -> asyncio.Future["T_Response"]:
        if self._is_called_from_self():
            raise RuntimeError("ask() called from the actor's message loop")
        future = asyncio.get_running_loop().create_future()
        self._mailbox.put_nowait(Envelope(payload=request, reply_to=future))
        return future

    def tell(self, event: Any):
        self._mailbox.put_nowait(Envelope(payload=event, reply_to=None))

    async def wait_for_startup(self):
        await self._startup_future

    # --------------------- Lifecycle callbacks ----------------------------- #

    async def on_start(self) -> None:
        logger.debug(f"Actor {self.id} is starting")

    async def on_stop(self) -> None:
        logger.debug(f"Actor {self.id} is stopping")

    async def on_message(self, message: Any) -> Any:
        type_name = type(message).__name__
        method_name = f"on_{snake_case(type_name)}"
        if (method := getattr(self, method_name, None)) is None:
            raise NotImplementedError(f"{type_name} does not implement {method_name}")
        if not callable(method):
            raise TypeError(f"{type_name}.{method_name} must be callable")
        result = method(message)
        if inspect.isawaitable(result):
            return await result
        return result

    # --------------------- Internals --------------------------------------- #

    async def _loop(self) -> None:
        try:
            # Handle start lifecycle event
            await self.on_start()
        except Exception as e:
            logger.error(f"Actor {self.id} not started", exc_info=e)
            return

        # Notify we started
        self._startup_future.set_result(True)
        self._running = True

        try:
            # Start the main loop
            while self._running:
                envelope = await self._mailbox.get()
                # FIXME: ....
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
                    break
                finally:
                    if envelope.reply_to and not envelope.reply_to.done():
                        if exception_to_set:
                            envelope.reply_to.set_exception(exception_to_set)
                        else:
                            envelope.reply_to.set_result(result_to_set)
                    self._mailbox.task_done()
        finally:
            # Handle stop lifecycle event
            await self.on_stop()

    async def _stop_internal(self):
        self._running = False
        self._mailbox.put_nowait(Envelope(payload=None, reply_to=None))

    def _is_called_from_self(self) -> bool:
        current_task = asyncio.current_task()
        return current_task is not None and current_task is self._task


def snake_case(name: str) -> str:
    """Convert a CamelCase name to snake_case."""
    return "".join(c if c.islower() else "_" + c.lower() for c in name).lstrip("_")
