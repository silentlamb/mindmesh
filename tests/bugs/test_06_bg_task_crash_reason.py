"""Background task crash loses the exception.

When a bg task crashes, _on_bg_task_done calls self.stop() which cancels
the event loop. The _loop catches CancelledError and uses StopReason.Stop,
losing the actual crash exception.
"""

import asyncio

from mindmesh import BaseActor, LinkAction
from mindmesh.core import StopReasonType
from mindmesh.hive import ActorHive


class CrashingBgActor(BaseActor):
    def on_task_create(self):
        return self._crash()

    async def _crash(self) -> None:
        await asyncio.sleep(0.05)
        raise ValueError("bg task crashed")


class ReasonCapturingMonitor(BaseActor):
    def __init__(
        self,
        hive: ActorHive,
        actor_id: str,
        capture: list[StopReasonType],
    ):
        super().__init__(hive, actor_id)
        self.captured_reason = capture

    async def on_link_death(self, actor_id: str, reason: StopReasonType) -> LinkAction:
        self.captured_reason.append(reason)
        return LinkAction.Continue


async def test_bg_crash_reason_propagates_to_monitor(hive: ActorHive) -> None:
    """Monitor should receive the crash exception, not a generic StopReason."""

    capture: list[StopReasonType] = []
    worker = hive.start_actor(CrashingBgActor)
    monitor = hive.start_actor(ReasonCapturingMonitor, capture)
    await worker.wait_for_start()
    await monitor.wait_for_start()

    hive.link_actors(worker.id(), monitor.id())

    await asyncio.sleep(0.2)
    assert len(capture) == 1, "Expected to capture exactly one StopReason"

    # Bug: reason is StopReason.LinkDeath (or StopReason.Stop) instead of the ValueError
    assert isinstance(capture[0], BaseException), (
        f"Expected the crash exception, got {capture[0]}"
    )
