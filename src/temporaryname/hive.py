from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, TypeVar

from .actor import BaseActor, LinkAction
from .core import StopReason
from .registry import ActorRegistry

if TYPE_CHECKING:
    from .core import Request, StopReasonType, T_Response
    from .proxy import ActorAddr
    from .registry import ActorContext

logger = logging.getLogger(__package__)
T = TypeVar("T", bound=BaseActor)


class ActorFactory:
    """Responsible for instantiating actors.

    Override to customise actor creation.
    """

    def create(
        self,
        hive: ActorHive,
        actor_class: type[T],
        actor_id: str,
        *args: Any,
        **kwargs: Any,
    ) -> T:
        return actor_class(hive, actor_id, *args, **kwargs)


class ActorHive:
    """Central runtime that owns and coordinates all actors.

    Typical usage:

        hive = ActorHive()
        addr = hive.start_actor(MyActor)
        await addr.ask(MyRequest())
        await hive.shutdown()

    Args:
        factory: Optional custom :class:`ActorFactory` for actor instantiation.
        registry: Optional custom :class:`ActorRegistry`.
    """

    def __init__(
        self, factory: ActorFactory | None = None, registry: ActorRegistry | None = None
    ) -> None:
        self._next_id = 0
        self._actor_factory = factory if factory else ActorFactory()
        self._registry = registry if registry else ActorRegistry()

    # ------------------------------- Public API ---------------------------------------

    @property
    def actor_ids(self) -> list[str]:
        """IDs of all currently registered actors."""
        return self._registry.actor_ids

    async def ask_actor(
        self,
        actor_id: str,
        request: Request[T_Response],
    ) -> T_Response:
        """Send a request to an actor by ID and await its typed response."""
        if actor := self._registry.get_actor(actor_id):
            return await actor.ask(request)
        raise ValueError(f"Actor {actor_id} not found")

    def create_actor(
        self, actor_class: type[T], *args: Any, **kwargs: Any
    ) -> ActorAddr:
        """Instantiate and register an actor without starting it.

        Returns proxy object.
        """
        actor_id = f"{actor_class.__name__}-{self._next_id}"
        self._next_id += 1
        actor = self._actor_factory.create(self, actor_class, actor_id, *args, **kwargs)
        return self._registry.register(actor_id, actor).actor_ref

    def create_named_actor(
        self, name: str, actor_class: type[T], *args: Any, **kwargs: Any
    ) -> ActorAddr:
        """Instantiate and register an actor under a given name without starting it.

        Returns proxy object.
        """
        actor_id = f"{actor_class.__name__}-{self._next_id}"
        self._next_id += 1
        actor = self._actor_factory.create(self, actor_class, actor_id, *args, **kwargs)
        return self._registry.register(actor_id, actor, name=name).actor_ref

    def link_actors(self, source_id: str, monitor_id: str) -> None:
        """Make *monitor_id* watch *source_id*

        When *source_id* stops, *monitor_id* is notified."""
        self._registry.add_monitor(source_id, monitor_id)

    def link_actors_both(self, actor1: str, actor2: str) -> None:
        """Bidirectional link: each actor monitors the other."""
        self._registry.add_monitor(actor1, actor2, both=True)

    def lookup(self, actor_name: str) -> ActorAddr | None:
        """Return the address of a named actor, or ``None`` if not found."""
        if context := self._registry.get_by_name(actor_name):
            return context.actor_ref
        return None

    def register(self, actor_name: str, addr: ActorAddr) -> None:
        """Associate *name* with an existing actor address for later :meth:`lookup`."""
        self._registry.register_name(actor_name, addr.id())

    def request_actor_start(self, actor_id: str) -> None:
        """Trigger the start sequence of an already-registered actor."""
        if not (actor := self._registry.get_actor(actor_id)):
            raise ValueError(f"Actor {actor_id} not found")
        try:
            actor.start()
        except Exception as e:
            logger.error(
                f"Unhandled exception while starting actor {actor_id}", exc_info=e
            )
            raise

    def request_actor_stop(self, actor_id: str, reason: StopReasonType) -> None:
        """Request an actor to stop with the given reason.

        No-op if already stopped.
        """
        if not (actor := self._registry.get_actor(actor_id)):
            return  # Actor already stopped and removed
        actor.stop(reason)

    async def shutdown(self) -> None:
        """Stop all actors and wait for them to finish.

        Call this to cleanly tear down the hive.
        """
        logger.debug("Shutting down the hive")
        for actor_id in self._registry.actor_ids:
            try:
                self.request_actor_stop(actor_id, StopReason.Shutdown)
            except RuntimeError:
                self._registry.unregister(actor_id)
        await asyncio.gather(
            *(
                self.wait_for_actor_stop(actor_id)
                for actor_id in self._registry.actor_ids
            )
        )

    def start_actor(self, actor_class: type[T], *args: Any, **kwargs: Any) -> ActorAddr:
        """Create and immediately start an actor.

        Equivalent to :meth:`create_actor` + :meth:`request_actor_start`.
        """
        actor_ref = self.create_actor(actor_class, *args, **kwargs)
        self.request_actor_start(actor_ref.id())
        return actor_ref

    def start_named_actor(
        self, name: str, actor_class: type[T], *args: Any, **kwargs: Any
    ) -> ActorAddr:
        """Create and immediately start an actor.

        Actor is registered under the name for later :meth:`lookup`.

        Like :meth:`start_actor` but also registers the actor under *name*.
        """
        actor_ref = self.create_named_actor(name, actor_class, *args, **kwargs)
        self.request_actor_start(actor_ref.id())
        return actor_ref

    def tell(self, actor_id: str, event: Any) -> None:
        """Send a fire-and-forget message to an actor by ID."""
        if actor := self._registry.get_actor(actor_id):
            actor.tell(event)
        else:
            raise ValueError(f"Tell: actor {actor_id} not found")

    async def wait_for_actor_start(self, actor_id: str) -> None:
        """Block until the actor has finished its startup sequence."""
        if actor := self._registry.get_actor(actor_id):
            await actor.wait_for_startup()

    async def wait_for_actor_stop(self, actor_id: str) -> None:
        """Block until the actor has fully stopped."""
        if actor := self._registry.get_actor(actor_id):
            await actor.wait_for_stop()

    # ------------------------------- Callbacks ----------------------------------------

    async def on_actor_stopped(self, actor_id: str, reason: StopReasonType) -> None:
        """Called by the framework when an actor stops.

        Notifies monitors and cleans up the registry.
        """
        logger.debug(f"on_actor_stopped: {actor_id}: {reason}")
        try:
            actors_to_stop: list[ActorContext] = []
            monitor_reason = (
                reason if reason != StopReason.Stop else StopReason.LinkDeath
            )

            for monitor_context in self._registry.monitors(actor_id):
                try:
                    action = await monitor_context.actor.on_link_death(
                        actor_id, monitor_reason
                    )
                    if action == LinkAction.Stop:
                        actors_to_stop.append(monitor_context)
                except ValueError:
                    logger.error(
                        f"Monitor {monitor_context.actor.id} of source {actor_id}"
                        " - not found"
                    )
                    pass

            for actor_context in actors_to_stop:
                actor_context.actor_ref.stop()

            self._registry.unregister(actor_id)

        except ValueError:
            logger.error(f"on_actor_stopped: actor {actor_id} no longer exists")
            pass
