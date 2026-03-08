from __future__ import annotations

import logging
from collections.abc import Generator
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .actor import BaseActor

if TYPE_CHECKING:
    from .proxy import ActorAddr

logger = logging.getLogger(__package__)


@dataclass
class ActorContext:
    actor: BaseActor
    actor_ref: ActorAddr
    monitors: set[str] = field(default_factory=set[str])
    monitored_by: set[str] = field(default_factory=set[str])


class ActorRegistry:
    def __init__(self):
        self._contexts: dict[str, ActorContext] = {}

    @property
    def actor_ids(self) -> list[str]:
        return list(self._contexts.keys())

    def add_monitor(self, source_id: str, monitor_id: str, both: bool = False) -> None:
        if source_id == monitor_id:
            logger.warning(
                f"Source {source_id} and Monitor {monitor_id} are the same actors"
            )
            return
        if not (source := self.get(source_id)):
            logger.debug(f"Source actor {source_id} not found")
            return
        if not (monitor := self.get(monitor_id)):
            logger.debug(f"Monitor actor {monitor_id} not found")
            return
        source.monitored_by.add(monitor_id)
        monitor.monitors.add(source_id)
        if both:
            monitor.monitored_by.add(source_id)
            source.monitors.add(monitor_id)

    def drop_monitor(self, monitor_id: str) -> None:
        if not (monitor := self.get(monitor_id)):
            return
        for source_id in monitor.monitors:
            if source := self.get(source_id):
                source.monitored_by.discard(monitor_id)
        monitor.monitors.clear()

    def get(self, actor_id: str) -> ActorContext | None:
        return self._contexts.get(actor_id)

    def get_actor(self, actor_id: str) -> BaseActor | None:
        if context := self.get(actor_id):
            return context.actor
        return None

    def monitors(self, source_id: str) -> Generator[ActorContext, None, None]:
        if not (source := self.get(source_id)):
            logger.debug(f"Source {source_id} not found")
            return
        for monitor_id in list(source.monitored_by):
            if monitor := self.get(monitor_id):
                yield monitor
            else:
                logging.debug(f"Monitor {monitor_id} of source {source_id} not found")

    def register(self, actor_id: str, actor: BaseActor) -> ActorContext:
        if actor_id in self._contexts:
            raise ValueError(f"Actor {actor_id} already in the registry")
        context = ActorContext(actor=actor, actor_ref=actor.as_ref())
        self._contexts[actor_id] = context
        return context

    def unregister(self, actor_id: str) -> None:
        self.drop_monitor(actor_id)
        if actor_id in self._contexts:
            del self._contexts[actor_id]
        else:
            logger.warning(f"Actor {actor_id} not registered")
