import asyncio

from temporaryname import BaseActor
from temporaryname.hive import ActorHive


class CrashingBgTaskActor(BaseActor):
    def on_task_create(self):
        return self._crashing_task()

    async def _crashing_task(self) -> None:
        await asyncio.sleep(0.05)
        raise RuntimeError("bang!")


async def test_bg_task_crash_stops_actor(hive: ActorHive) -> None:
    hive.start_actor(CrashingBgTaskActor)
    assert len(hive.actor_ids) == 1
    await asyncio.sleep(0.2)

    assert hive.actor_ids == [], f"Actor not cleaned up: {hive.actor_ids}"


async def test_bg_task_crash_late_wait_for_start(hive: ActorHive) -> None:
    # Start the crashing actor
    actor = hive.start_actor(CrashingBgTaskActor)
    assert len(hive.actor_ids) == 1
    await asyncio.sleep(0.2)

    await actor.wait_for_start()


async def test_bg_task_crash_late_wait_for_stop(hive: ActorHive) -> None:
    # Start the crashing actor
    actor = hive.start_actor(CrashingBgTaskActor)
    await actor.wait_for_start()

    assert len(hive.actor_ids) == 1
    await asyncio.sleep(0.2)

    # Note: maybe wait for stop should propagate exception from background task
    await actor.wait_for_stop()
