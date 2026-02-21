import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeVar

from .actor import BaseActor

if TYPE_CHECKING:
    from .core import Request, T_Response
    from .proxy import ActorRef

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
        return actor_class(hive, actor_id, *args, **kwargs)


@dataclass
class ActorContext:
    actor: "BaseActor"
    actor_ref: "ActorRef"


class ActorHive:
    def __init__(self, factory: ActorFactory | None = None) -> None:
        self._next_id = 0
        self._actor_factory = factory if factory else ActorFactory()
        self._actor_contexts: dict[str, ActorContext] = {}

    def create_actor(
        self, actor_class: type[T], *args: Any, **kwargs: Any
    ) -> "ActorRef":
        actor_id = f"{actor_class.__name__}-{self._next_id}"
        self._next_id += 1
        actor = self._actor_factory.create(self, actor_class, actor_id, *args, **kwargs)
        actor_ref = actor.as_ref()
        self._actor_contexts[actor_id] = ActorContext(actor=actor, actor_ref=actor_ref)
        return actor_ref

    def start_actor(
        self, actor_class: type[T], *args: Any, **kwargs: Any
    ) -> "ActorRef":
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

    async def tell(self, actor_id: str, event: Any):
        context = self._find_actor_context(actor_id)
        context.actor.tell(event)

    def request_actor_start(self, actor_id: str):
        context = self._find_actor_context(actor_id)
        try:
            context.actor.start()
        except Exception as e:
            logger.error(
                f"Unhandled exception while starting actor {actor_id}", exc_info=e
            )
            raise

    async def wait_for_actor_start(self, actor_id: str):
        context = self._find_actor_context(actor_id)
        await context.actor.wait_for_startup()

    def request_actor_stop(self, actor_id: str, reason: str | Exception):
        context = self._find_actor_context(actor_id)
        context.actor.stop()
        del self._actor_contexts[actor_id]

    async def wait_for_actor_stop(self, actor_id: str) -> None:
        raise NotImplementedError()

    def shutdown(self):
        logger.debug("Shutting down the hive")
        for actor_id in list(self._actor_contexts.keys()):
            self.request_actor_stop(actor_id, "shutdown")

    def _find_actor_context(self, actor_id: str) -> ActorContext:
        if (context := self._actor_contexts.get(actor_id)) is None:
            raise ValueError(f"Actor {actor_id} not found")
        return context
