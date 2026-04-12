from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .core import StopReason

if TYPE_CHECKING:
    from .core import Request, T_Response
    from .hive import ActorHive


class ActorAddr:
    """A reference to an actor.

    Use this to interact with actors without holding a direct reference.

    Obtain an ``ActorAddr`` with:
    - :meth:`ActorHive.create_actor`
    - :meth:`ActorHive.start_actor`,
    - :meth:`BaseActor.as_ref`.
    """

    def __init__(self, hive: ActorHive, actor_id: str):
        self._hive = hive
        self._id = actor_id

    def id(self) -> str:
        """Return the unique ID of the referenced actor."""
        return self._id

    async def ask(self, request: Request[T_Response]) -> T_Response:
        """Send a request and await a typed response."""
        return await self._hive.ask_actor(self._id, request)

    def monitor(self, actor_ref: ActorAddr) -> None:
        """Monitor *actor_ref* from this actor.

        Death of *actor_ref* triggers ``on_link_death``.
        """
        self._hive.link_actors(actor_ref.id(), self.id())

    def start(self) -> None:
        """Schedule the actor to start."""
        self._hive.request_actor_start(self._id)

    def stop(self) -> None:
        """Schedule the actor to stop."""
        self._hive.request_actor_stop(self._id, StopReason.Stop)

    def tell(self, message: Any) -> None:
        """Send a fire-and-forget message."""
        self._hive.tell(self._id, message)

    async def wait_for_start(self) -> None:
        """Block until the actor has finished starting up."""
        await self._hive.wait_for_actor_start(self._id)

    async def wait_for_stop(self) -> None:
        """Block until the actor has fully stopped."""
        await self._hive.wait_for_actor_stop(self._id)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, ActorAddr) and self._id == other._id

    def __hash__(self) -> int:
        return hash(self._id)

    def __repr__(self) -> str:
        return f"ActorAddr{{id={self._id}}}"
