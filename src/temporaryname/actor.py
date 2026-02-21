import asyncio
import inspect
import logging
from typing import TYPE_CHECKING, Any

from .core import Envelope

if TYPE_CHECKING:
    from .hive import ActorHive, ActorRef


logger = logging.getLogger(__package__)


class BaseActor:
    """Abstract base for actors.

    Attributes
    - `_hive`: reference to the `ActorHive` that manages this actor.
    - `_id`: string identifier for the actor instance.

    Subclasses should be lightweight and focus on behaviour; this base
    class provides common storage and the canonical constructor.
    """

    def __init__(self, hive: "ActorHive", id: str):
        self._hive = hive
        self._id = id
        self._mailbox: asyncio.Queue[Envelope] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None
        self._startup_future: asyncio.Future[bool] = asyncio.Future()
        self._running = False

    @property
    def hive(self) -> "ActorHive":
        """Return the actor's managing hive."""
        return self._hive

    @property
    def id(self) -> str:
        """Return the actor's identifier."""
        return self._id

    def as_ref(self) -> "ActorRef":
        """Create actor reference"""
        from .hive import ActorRef

        return ActorRef(self._hive, self._id)

    def start(self):
        """Ask this actor to start"""
        if self._task:
            logger.warning(f"Actor {self.id} already running")
        else:
            self._task = asyncio.create_task(self._loop())

    def stop(self):
        """Ask this actor to stop"""
        if self._task:
            self._task.cancel()
        else:
            logger.warning(f"Actor {self.id} not started")

    async def ask_self(self, payload: Any) -> Any:
        """Ask self for a response"""
        if not self._is_called_from_self():
            logger.warning(f"ask_self() called from outside of actor {self.id} task")
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self._mailbox.put_nowait(Envelope(payload=payload, reply_to=future))
        return await asyncio.wait_for(future, timeout=None)

    def tell_self(self, payload: Any) -> None:
        """Tell self to handle an event"""
        if not self._is_called_from_self():
            logger.warning(f"tell_self() called from outside of actor {self.id} task")
        self._mailbox.put_nowait(Envelope(payload=payload, reply_to=None))

    # --------------------- Lifecycle callbacks ----------------------------- #

    async def on_start(self) -> None:
        """Hook method called when the actor is registered with the hive."""
        logger.debug(f"Actor {self.id} is starting")

    async def on_stop(self) -> None:
        """Hook method called when the actor is unregistered from the hive."""
        logger.debug(f"Actor {self.id} is stopping")

    async def on_message(self, message: Any) -> Any:
        """Handle any message delivered to this actor.

        Default implementation uses a convention-based dispatch:
        Convert the message's runtime type name to snake_case and look
        for a handler method named `on_<type_name>` on this actor
        (e.g. ``UserJoined`` -> ``on_user_joined``). Both synchronous
        and asynchronous handlers are supported.

        Raises
        - ``NotImplementedError`` if no handler method exists.
        - ``TypeError`` if the attribute exists but is not callable.
        """
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
