import logging
from typing import TYPE_CHECKING, Any, TypeVar

from .actor import BaseActor
from .core import ActorContext

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__package__)
T = TypeVar("T", bound=BaseActor)


class ActorFactory:
    def create(
        self,
        hive: "ActorHive",
        actor_class: type[T],
        actor_id: str,
        *args: Any,
        **kwargs: Any,
    ) -> BaseActor:
        """Factory to instantiate BaseActor implementations"""
        return actor_class(hive, actor_id, *args, **kwargs)


class ActorHive:
    def __init__(self, factory: ActorFactory) -> None:
        self._next_id = 0
        self._actor_factory = factory if factory else ActorFactory()
        self._actor_contexts: dict[str, ActorContext] = {}

    def create_actor(
        self, actor_class: type[T], *args: Any, **kwargs: Any
    ) -> "ActorRef":
        actor_id = f"{actor_class.__name__}-{self._next_id}"
        self._next_id += 1
        actor = self._actor_factory.create(self, actor_class, *args, **kwargs)
        actor_ref = actor.as_ref()
        self._actor_contexts[actor_id] = ActorContext(
            actor=actor,
            actor_ref=actor_ref,
            monitored_by=set(),
        )
        return actor_ref

    def start_actor(
        self, actor_class: type[T], *args: Any, **kwargs: Any
    ) -> "ActorRef":
        actor_ref = self.create_actor(actor_class, *args, **kwargs)
        context = self._actor_contexts[actor_ref.id()]
        actor = context.actor
        try:
            actor.start()
        except Exception as e:
            logger.error(
                f"Unhandled exception while starting actor {actor_class}", exc_info=e
            )
            raise
        return actor_ref


class ActorRef:
    def __init__(self, hive: "ActorHive", actor_id: str):
        self._hive = hive
        self._id = actor_id

    def id(self) -> str:
        return self._id
