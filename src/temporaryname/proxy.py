from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .core import StopReason

if TYPE_CHECKING:
    from .core import Request, T_Response
    from .hive import ActorHive


class ActorAddr:
    def __init__(self, hive: ActorHive, actor_id: str):
        self._hive = hive
        self._id = actor_id

    def id(self) -> str:
        return self._id

    async def ask(self, request: Request[T_Response]) -> T_Response:
        return await self._hive.ask_actor(self._id, request)

    def link_actor(self, actor_ref: ActorAddr) -> None:
        self._hive.link_actors(self.id(), actor_ref.id())

    def start(self) -> None:
        self._hive.request_actor_start(self._id)

    def stop(self) -> None:
        self._hive.request_actor_stop(self._id, StopReason.Stop)

    def tell(self, message: Any) -> None:
        self._hive.tell(self._id, message)

    async def wait_for_start(self) -> None:
        await self._hive.wait_for_actor_start(self._id)

    async def wait_for_stop(self) -> None:
        await self._hive.wait_for_actor_stop(self._id)

    def __repr__(self) -> str:
        return f"ActorAddr{{id={self._id}}}"
