import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .actor import BaseActor
    from .hive import ActorRef


@dataclass
class Envelope:
    payload: Any
    reply_to: asyncio.Future[Any] | None


@dataclass
class ActorContext:
    actor: "BaseActor"
    actor_ref: "ActorRef"
    monitored_by: set[str]
    died: bool = False
