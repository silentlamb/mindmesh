import asyncio
from dataclasses import dataclass
from typing import Any, Generic, TypeVar


@dataclass
class Envelope:
    payload: Any
    reply_to: asyncio.Future[Any] | None


T_Response = TypeVar("T_Response")


class Request(Generic[T_Response]):
    pass
