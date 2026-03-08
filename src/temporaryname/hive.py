from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeVar

from .actor import BaseActor, LinkAction
from .core import StopReason

if TYPE_CHECKING:
    from .core import Request, StopReasonType, T_Response
    from .proxy import ActorAddr

logger = logging.getLogger(__package__)
T = TypeVar("T", bound=BaseActor)


class ActorFactory:
    def create(
        self,
        hive: ActorHive,
        actor_class: type[T],
        actor_id: str,
        *args: Any,
        **kwargs: Any,
    ) -> T:
        return actor_class(hive, actor_id, *args, **kwargs)


@dataclass
class ActorContext:
    actor: BaseActor
    actor_ref: ActorAddr
    monitors: set[str]
    monitored_by: set[str]


class ActorHive:
    def __init__(self, factory: ActorFactory | None = None) -> None:
        self._next_id = 0
        self._actor_factory = factory if factory else ActorFactory()
        self._actor_contexts: dict[str, ActorContext] = {}

    @property
    def actor_ids(self) -> list[str]:
        return list(self._actor_contexts.keys())

    def create_actor(
        self, actor_class: type[T], *args: Any, **kwargs: Any
    ) -> ActorAddr:
        actor_id = f"{actor_class.__name__}-{self._next_id}"
        self._next_id += 1
        actor = self._actor_factory.create(self, actor_class, actor_id, *args, **kwargs)
        actor_ref = actor.as_ref()
        self._actor_contexts[actor_id] = ActorContext(
            actor=actor, actor_ref=actor_ref, monitors=set(), monitored_by=set()
        )
        return actor_ref

    def start_actor(self, actor_class: type[T], *args: Any, **kwargs: Any) -> ActorAddr:
        actor_ref = self.create_actor(actor_class, *args, **kwargs)
        self.request_actor_start(actor_ref.id())
        return actor_ref

    async def ask_actor(
        self,
        actor_id: str,
        request: Request[T_Response],
    ) -> T_Response:
        context = self._find_actor_context(actor_id)
        return await context.actor.ask(request)

    def link_actors(self, source_id: str, monitor_id: str) -> None:
        logger.debug(f"link_actors: source={source_id} -> monitor={monitor_id}")
        if source_id == monitor_id:
            logger.warning(f"Cannot listen to self: {source_id}!")
            return
        try:
            src_context = self._find_actor_context(source_id)
        except ValueError as e:
            raise ValueError(
                f"link_actors failed: source actor '{source_id}' not found"
            ) from e
        try:
            mon_context = self._find_actor_context(monitor_id)
        except ValueError as e:
            raise ValueError(
                f"link_actors failed: monitor actor '{monitor_id}' not found"
            ) from e
        src_context.monitored_by.add(monitor_id)
        mon_context.monitors.add(source_id)

    def link_actors_both(self, actor1: str, actor2: str) -> None:
        if actor1 == actor2:
            logger.warning(f"Cannot listen to self: {actor1}!")
            return
        self.link_actors(actor1, actor2)
        self.link_actors(actor2, actor1)

    def tell(self, actor_id: str, event: Any) -> None:
        context = self._find_actor_context(actor_id)
        context.actor.tell(event)

    async def on_actor_stopped(self, actor_id: str, reason: StopReasonType) -> None:
        logger.debug(f"on_actor_stopped: {actor_id}: {reason}")
        try:
            context = self._find_actor_context(actor_id)
            actors_to_stop: set[str] = set()

            monitor_reason = (
                reason if reason != StopReason.Stop else StopReason.LinkDeath
            )
            for monitor_id in list(context.monitored_by):
                try:
                    monitor_context = self._find_actor_context(monitor_id)
                    action = await monitor_context.actor.on_link_death(
                        actor_id, monitor_reason
                    )
                    if action == LinkAction.Stop:
                        actors_to_stop.add(monitor_context.actor_ref.id())
                except ValueError:
                    logger.error(
                        f"Monitor actor {monitor_id} (of actor {actor_id}) - not found"
                    )
                    pass

            for stop_id in actors_to_stop:
                try:
                    stop_context = self._find_actor_context(stop_id)
                except ValueError:
                    continue  # Actor already stopped and removed
                stop_context.actor_ref.stop()

            # Unregister this actor from monitoring other actors
            for monitored_id in context.monitors:
                try:
                    mon_context = self._find_actor_context(monitored_id)
                    mon_context.monitored_by.discard(actor_id)
                except ValueError:
                    pass  # Actor already removed

            del self._actor_contexts[actor_id]

        except ValueError:
            logger.error(f"on_actor_stopped: actor {actor_id} no longer exists")
            pass

    def request_actor_start(self, actor_id: str) -> None:
        context = self._find_actor_context(actor_id)
        try:
            context.actor.start()
        except Exception as e:
            logger.error(
                f"Unhandled exception while starting actor {actor_id}", exc_info=e
            )
            raise

    async def wait_for_actor_start(self, actor_id: str) -> None:
        try:
            context = self._find_actor_context(actor_id)
        except ValueError:
            return  # Actor already stopped and removed
        await context.actor.wait_for_startup()

    def request_actor_stop(self, actor_id: str, reason: StopReasonType) -> None:
        try:
            context = self._find_actor_context(actor_id)
        except ValueError:
            return  # Actor already stopped and removed
        context.actor.stop(reason)

    async def wait_for_actor_stop(self, actor_id: str) -> None:
        try:
            context = self._find_actor_context(actor_id)
        except ValueError:
            return  # Actor already stopped and removed
        await context.actor.wait_for_stop()

    async def shutdown(self) -> None:
        logger.debug("Shutting down the hive")
        actor_ids = list(self._actor_contexts.keys())
        for actor_id in actor_ids:
            try:
                self.request_actor_stop(actor_id, StopReason.Shutdown)
            except RuntimeError:
                del self._actor_contexts[actor_id]
        await asyncio.gather(
            *(self.wait_for_actor_stop(actor_id) for actor_id in actor_ids)
        )

    def _find_actor_context(self, actor_id: str) -> ActorContext:
        if (context := self._actor_contexts.get(actor_id)) is None:
            raise ValueError(f"Actor {actor_id} not found")
        return context
