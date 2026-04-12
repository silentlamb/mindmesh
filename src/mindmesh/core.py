import asyncio
from dataclasses import dataclass
from enum import Enum
from typing import Any, Generic, TypeVar


class StopReason(Enum):
    """Reason an actor was stopped normally (non-exception path)."""

    Stop = "stop"
    """Explicit stop requested via :meth:`ActorAddr.stop` or
    :meth:`BaseActor.stop_self`.
    """

    Shutdown = "shutdown"
    """Hive-wide shutdown triggered by :meth:`ActorHive.shutdown`."""

    LinkDeath = "link_death"
    """A monitored actor stopped, propagating its death to this actor."""


StopReasonType = StopReason | BaseException
"""Either a :class:`StopReason` value or an unhandled exception that caused
the actor to die.
"""


@dataclass
class Envelope:
    """Internal message wrapper used by the actor mailbox.

    Not part of the public API.
    """

    payload: Any
    reply_to: asyncio.Future[Any] | None


T_Response = TypeVar("T_Response")
"""Type variable for the response type of a :class:`Request`."""


class Request(Generic[T_Response]):
    """Base class for request messages sent via :meth:`ActorAddr.ask`.

    Subclass this to define a typed request/response pair::

        class GetCount(Request[int]):
            pass
    """
