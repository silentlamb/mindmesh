"""Stop reason is lost in two places.

- request_actor_stop ignores the reason parameter
- on_link_death receives hardcoded StopReason.LinkDeath instead of original reason
"""

import asyncio

from temporaryname import BaseActor, LinkAction
from temporaryname.core import StopReason, StopReasonType
from temporaryname.hive import ActorHive


class ReasonCapturingActor(BaseActor):
    def __init__(
        self,
        hive: ActorHive,
        actor_id: str,
        capture: list[StopReasonType],
        action: LinkAction = LinkAction.Continue,
    ):
        super().__init__(hive, actor_id)
        self.captured_reason = capture
        self._action = action

    async def on_link_death(self, actor_id: str, reason: StopReasonType) -> LinkAction:
        self.captured_reason.append(reason)
        return self._action


async def test_on_link_death_receives_original_reason(hive: ActorHive) -> None:
    """Monitor should know *why* the source actor died, not just that it died."""

    capture: list[str | StopReason | Exception] = []
    source = hive.start_actor(BaseActor)
    monitor = hive.start_actor(ReasonCapturingActor, capture)
    await source.wait_for_start()
    await monitor.wait_for_start()

    hive.link_actors(source.id(), monitor.id())

    # Stop source with Shutdown reason
    hive.request_actor_stop(source.id(), StopReason.Shutdown)
    await asyncio.sleep(0.1)

    assert len(capture) == 1, "Expected to capture exactly one StopReason"

    # Bug: monitor receives StopReason.LinkDeath instead of StopReason.Shutdown
    assert capture[0] == StopReason.Shutdown, (
        f"Expected StopReason.Shutdown, got {capture[0]}"
    )


async def test_exception_does_not_leak_through_chain(hive: ActorHive) -> None:
    """
    WorkerA --[monitored_by]--> WorkerB --[monitored_by]--> Supervisor

    WorkerA throws an exception. WorkerB (direct monitor) should see the
    exception. But Supervisor should see LinkDeath, not WorkerA's exception;
    it doesn't even know WorkerA exists.
    """

    class CrashingActor(BaseActor):
        def on_task_create(self):
            return self._crash()

        async def _crash(self) -> None:
            await asyncio.sleep(0.05)
            raise RuntimeError("WorkerA crashed")

    worker_b_capture: list[StopReasonType] = []
    supervisor_capture: list[StopReasonType] = []

    worker_a = hive.start_actor(CrashingActor)
    worker_b = hive.start_actor(ReasonCapturingActor, worker_b_capture, LinkAction.Stop)
    supervisor = hive.start_actor(
        ReasonCapturingActor, supervisor_capture, LinkAction.Continue
    )

    await asyncio.gather(
        worker_a.wait_for_start(),
        worker_b.wait_for_start(),
        supervisor.wait_for_start(),
    )

    # WorkerA -> WorkerB -> Supervisor
    hive.link_actors(worker_a.id(), worker_b.id())
    hive.link_actors(worker_b.id(), supervisor.id())

    await asyncio.sleep(0.3)

    # WorkerB (direct monitor) should see the original exception
    assert len(worker_b_capture) == 1
    assert isinstance(worker_b_capture[0], RuntimeError), (
        f"WorkerB expected RuntimeError, got {worker_b_capture[0]}"
    )

    # Supervisor should see LinkDeath, not WorkerA's exception
    assert len(supervisor_capture) == 1
    assert supervisor_capture[0] == StopReason.LinkDeath, (
        f"Supervisor expected LinkDeath, got {supervisor_capture[0]}"
    )
