"""Stopping a never-started actor is a logic error and should raise."""

import pytest

from temporaryname import BaseActor
from temporaryname.hive import ActorHive


async def test_stop_never_started_actor_raises(hive: ActorHive) -> None:
    actor = hive.create_actor(BaseActor)

    with pytest.raises(RuntimeError, match="not started"):
        actor.stop()
