"""Shutdown reason is overwritten when a supervisor's on_stop() stops its workers.

When hive.shutdown() is called:
  1. All actors receive request_actor_stop(..., StopReason.Shutdown)
  2. The supervisor's _loop cancels and runs on_stop(), which calls worker.stop()
     (StopReason.Stop via ActorAddr.stop())

The bug only triggers when the supervisor task was created *before* the worker
task - i.e. the worker is spawned inside the supervisor's on_start(). In that
case asyncio delivers CancelledError to the supervisor first, so its on_stop()
runs and overwrites the worker's already-set _stop_reason = Shutdown with Stop.

Without fix: worker._stop_reason is overwritten from Shutdown to Stop.
With fix:    first stop reason wins, Shutdown is preserved.
"""

from temporaryname import ActorHive, BaseActor
from temporaryname.core import StopReason, StopReasonType


async def test_shutdown_reason_preserved(hive: ActorHive) -> None:
    """Worker spawned inside supervisor's on_start() must report StopReason.Shutdown
    during hive.shutdown(), even when the supervisor's on_stop() calls worker.stop()."""

    reasons: list[StopReasonType] = []

    class CapturingWorker(BaseActor):
        async def on_stop(self) -> None:
            reasons.append(self._stop_reason)  # type: ignore[arg-type]

    class CapturingSupervisor(BaseActor):
        async def on_start(self) -> None:
            # Worker task is created AFTER the supervisor task, mirroring the
            # real supervisor example where workers are spawned in on_start().
            self._worker = self.hive.start_actor(CapturingWorker)
            await self._worker.wait_for_start()

        async def on_stop(self) -> None:
            # Redundantly stops the worker, as a real supervisor would.
            self.hive.request_actor_stop(self._worker.id(), StopReason.Stop)

    supervisor = hive.start_actor(CapturingSupervisor)
    await supervisor.wait_for_start()

    await hive.shutdown()

    assert len(reasons) == 1
    assert reasons[0] == StopReason.Shutdown, (
        f"Expected StopReason.Shutdown, got {reasons[0]}"
    )
