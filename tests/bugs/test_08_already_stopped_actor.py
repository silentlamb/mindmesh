"""on_actor_stopped can skip cleanup when an actor in actors_to_stop has already
been removed from the hive by the time the stop loop processes it.

Setup:
  - B and C both monitor A
  - B's on_link_death explicitly stops C and awaits its full removal

If C is iterated first in monitored_by:
  1. C.on_link_death returns LinkAction.Stop -> C added to actors_to_stop
  2. B.on_link_death stops C, awaits its removal -> C gone from hive
  3. actors_to_stop loop hits _find_actor_context(C) -> ValueError
     - Without fix: caught by outer except, A's context never deleted (leak)
     - With fix:    caught by inner except, continues -> cleanup completes

If B is iterated first (alternate ordering):
  - C is gone before its on_link_death is called -> inner try/except in the
    monitored_by loop handles it, C never enters actors_to_stop -> no bug path
"""

import asyncio

from temporaryname import BaseActor, LinkAction
from temporaryname.core import StopReason, StopReasonType
from temporaryname.hive import ActorHive


async def test_no_context_leak_when_linked_actor_removed_mid_cleanup(
    hive: ActorHive,
) -> None:
    """Hive must clean up A's context even if an actor in actors_to_stop is
    already gone because another monitor's on_link_death removed it first."""

    class ActorC(BaseActor):
        async def on_link_death(
            self, actor_id: str, reason: StopReasonType
        ) -> LinkAction:
            return LinkAction.Stop

    class ActorB(BaseActor):
        def __init__(self, hive: ActorHive, actor_id: str, c_id: str) -> None:
            super().__init__(hive, actor_id)
            self._c_id = c_id

        async def on_link_death(
            self, actor_id: str, reason: StopReasonType
        ) -> LinkAction:
            # Stop C and block until it is fully removed from the hive.
            # If C was already added to actors_to_stop before this runs,
            # the actors_to_stop loop will encounter a missing context.
            self._hive.request_actor_stop(self._c_id, StopReason.Stop)
            await self._hive.wait_for_actor_stop(self._c_id)
            return LinkAction.Stop

    c = hive.start_actor(ActorC)
    b = hive.start_actor(ActorB, c.id())
    a = hive.start_actor(BaseActor)

    await asyncio.gather(
        a.wait_for_start(),
        b.wait_for_start(),
        c.wait_for_start(),
    )

    # Both B and C monitor A
    hive.link_actors(a.id(), b.id())
    hive.link_actors(a.id(), c.id())

    a.stop()
    await asyncio.sleep(0.2)

    assert hive.actor_ids == [], f"Leaked actor contexts: {hive.actor_ids}"
