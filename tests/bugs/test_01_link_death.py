from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from temporaryname import BaseActor, LinkAction
from temporaryname.hive import ActorHive

if TYPE_CHECKING:
    from temporaryname import StopReasonType


class WorkerActor(BaseActor):
    async def on_link_death(self, actor_id: str, reason: StopReasonType) -> LinkAction:
        return LinkAction.Stop


async def test_bidirectional_link_death_cleans_up_both_actors(hive: ActorHive) -> None:
    # Let's start two worker actors
    a = hive.start_actor(WorkerActor)
    b = hive.start_actor(WorkerActor)
    await a.wait_for_start()
    await b.wait_for_start()

    # Now link them so if one of them stops, the other one gets notification
    hive.link_actors_both(a.id(), b.id())
    assert len(hive.actor_ids) == 2

    # Now stop the actor. Both actors should be cleaned up,
    # because of link and LinkAction.STOP.
    a.stop()
    await asyncio.sleep(0.1)
    assert hive.actor_ids == [], f"Leaked actor contexts: {hive.actor_ids}"
