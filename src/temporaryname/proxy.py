from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .core import Request, T_Response
    from .hive import ActorHive


class ActorRef:
    def __init__(self, hive: "ActorHive", actor_id: str):
        self._hive = hive
        self._id = actor_id

    def id(self) -> str:
        return self._id

    async def ask(self, request: "Request[T_Response]") -> "T_Response":
        return await self._hive.ask_actor(self._id, request)

    async def tell(self, message: "Request[T_Response]"):
        await self._hive.tell(self._id, message)

    async def start(self) -> None:
        self._hive.request_actor_start(self._id)

    async def stop(self) -> None:
        self._hive.request_actor_stop(self._id, "stopped")

    async def wait_for_start(self) -> None:
        await self._hive.wait_for_actor_start(self._id)

    async def wait_for_stop(self) -> None:
        await self._hive.wait_for_actor_stop(self._id)
