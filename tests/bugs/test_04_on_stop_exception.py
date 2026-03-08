"""on_stop() exception prevents cleanup.

If a user's on_stop() raises, hive.on_actor_stopped() is never called
and _stop_event is never set. This causes leaked actor contexts and
hanging wait_for_stop() futures.
"""

import asyncio

from temporaryname import BaseActor
from temporaryname.hive import ActorHive


class ExplodingOnStopActor(BaseActor):
    async def on_stop(self):
        raise RuntimeError("on_stop exploded")


async def test_on_stop_exception_still_cleans_up_context(hive: ActorHive) -> None:
    actor = hive.start_actor(ExplodingOnStopActor)
    await actor.wait_for_start()

    actor.stop()
    await asyncio.sleep(0.1)

    assert hive.actor_ids == [], f"Leaked actor contexts: {hive.actor_ids}"


async def test_on_stop_exception_still_sets_stop_event(hive: ActorHive) -> None:
    actor = hive.start_actor(ExplodingOnStopActor)
    await actor.wait_for_start()

    actor.stop()

    # wait_for_stop should complete, not hang forever
    await asyncio.wait_for(actor.wait_for_stop(), timeout=1.0)
