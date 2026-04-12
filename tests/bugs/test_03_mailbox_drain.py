import asyncio

import pytest

from mindmesh import BaseActor
from mindmesh.core import Request
from mindmesh.hive import ActorHive
from mindmesh.proxy import ActorAddr


class GetValue(Request[int]):
    pass


class SlowActor(BaseActor):
    async def on_get_value(self, msg: GetValue) -> int:
        await asyncio.sleep(10)
        return 42


async def test_pending_ask_gets_exception_on_actor_stop(hive: ActorHive) -> None:
    # Start the very slow actor
    actor: ActorAddr = hive.start_actor(SlowActor)
    await actor.wait_for_start()

    # Send a message, we will be waiting 10s for it to respond
    asyncio.create_task(actor.ask(GetValue()))
    await asyncio.sleep(0.01)

    # Send another message, it is stucked in the mailbox
    task2: asyncio.Task[object] = asyncio.create_task(actor.ask(GetValue()))
    await asyncio.sleep(0.01)

    # Now stop the actor - second message is still in the mailbox
    actor.stop()
    await asyncio.sleep(0.1)

    # Now when awaiting for the result of msg in the mailbox, we receive exception
    # Note: To consider if RuntimeError is a good exception for this
    with pytest.raises(RuntimeError, match="stopped while request was pending"):
        await task2
