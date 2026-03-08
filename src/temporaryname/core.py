import asyncio
from dataclasses import dataclass
from enum import Enum
from typing import Any, Generic, TypeVar


class StopReason(Enum):
    Stop = "stop"
    Shutdown = "shutdown"
    LinkDeath = "link_death"


StopReasonType = StopReason | BaseException


@dataclass
class Envelope:
    payload: Any
    reply_to: asyncio.Future[Any] | None


T_Response = TypeVar("T_Response")


class Request(Generic[T_Response]):
    pass
