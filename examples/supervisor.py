from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass
from typing import TYPE_CHECKING

from temporaryname import ActorHive, BaseActor, LinkAction

if TYPE_CHECKING:
    from temporaryname import ActorAddr, StopReasonType

logger = logging.getLogger("supervisor-example")


@dataclass
class DoSomething:
    job: str


class SupervisorActor(BaseActor):
    def __init__(self, hive: ActorHive, id: str, workers_num: int):
        super().__init__(hive, id)
        self._worker_addrs: list[ActorAddr] = []
        self._worker_actors: dict[str, int] = {}
        self._workers_num = workers_num
        self._next_worker_idx = 0

    async def on_start(self):
        for i in range(self._workers_num):
            addr = self.hive.start_actor(WorkerActor, i)
            self.as_ref().monitor(addr)
            self._worker_actors[addr.id()] = i
            await addr.wait_for_start()
            self._worker_addrs.append(addr)

    async def on_stop(self):
        for worker in self._worker_addrs:
            worker.stop()

    async def on_link_death(self, actor_id: str, reason: StopReasonType) -> LinkAction:
        if (worker_idx := self._worker_actors.pop(actor_id, -1)) < 0:
            logger.error(f"Worker actor {actor_id} not found")
            return LinkAction.Stop
        new_worker = self.hive.start_actor(WorkerActor, worker_idx)
        self.as_ref().monitor(new_worker)
        logger.debug(f"Replacing worker {actor_id} with worker {new_worker.id()}")
        self._worker_actors[new_worker.id()] = worker_idx
        self._worker_addrs[worker_idx] = new_worker
        return LinkAction.Continue

    def on_do_something(self, event: DoSomething):
        logger.debug(f"Received new job request: {event.job}")
        if self._workers_num <= 0:
            return
        worker = self._worker_addrs[self._next_worker_idx]
        self._next_worker_idx = (self._next_worker_idx + 1) % self._workers_num
        logger.debug(f"Dispatching job to worker {worker.id()}")
        worker.tell(event)


class WorkerActor(BaseActor):
    def __init__(self, hive: ActorHive, id: str, worker_id: int):
        super().__init__(hive, id)
        self._worker_id = worker_id

    async def on_do_something(self, event: DoSomething):
        logger.debug(f"Received new job request: {event.job}")
        sleep_ms = random.randint(100, 500)
        await asyncio.sleep(sleep_ms / 1000.0)
        if random.randint(0, 100) > 25:
            print(f"[Worker #{self._worker_id}]: Response: {event.job} - OK!")
        else:
            print(f"[Worker #{self._worker_id}]: Response: {event.job} - FAIL")
            self.stop_self()


async def main():
    hive = ActorHive()

    supervisor = hive.start_actor(SupervisorActor, 4)
    await supervisor.wait_for_start()

    for _ in range(0, 20):
        value = random.randint(0, 42)
        logger.info(f"Requesting new job: {value}")
        supervisor.tell(DoSomething(job=f"REQUEST={value}"))
        await asyncio.sleep(0.1)

    await asyncio.sleep(2)
    await hive.shutdown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARN)

    asyncio.run(main())
