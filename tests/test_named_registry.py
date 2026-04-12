from __future__ import annotations

import asyncio

import pytest

from mindmesh import BaseActor
from mindmesh.hive import ActorHive


async def test_start_named_actor_is_findable_by_lookup(hive: ActorHive) -> None:
    addr = hive.start_named_actor("worker", BaseActor)
    await addr.wait_for_start()

    found = hive.lookup("worker")

    assert found is not None
    assert found.id() == addr.id()


async def test_lookup_unknown_name_returns_none(hive: ActorHive) -> None:
    assert hive.lookup("ghost-in-the-shell") is None


async def test_register_post_creation_makes_actor_findable(hive: ActorHive) -> None:
    addr = hive.start_actor(BaseActor)
    await addr.wait_for_start()

    hive.register("worker", addr)

    found = hive.lookup("worker")
    assert found is not None
    assert found.id() == addr.id()


async def test_name_released_when_actor_stops(hive: ActorHive) -> None:
    addr = hive.start_named_actor("worker", BaseActor)
    await addr.wait_for_start()

    addr.stop()
    await asyncio.sleep(0.1)

    assert hive.lookup("worker") is None


async def test_duplicate_name_raises(hive: ActorHive) -> None:
    addr1 = hive.start_named_actor("shared", BaseActor)
    await addr1.wait_for_start()

    with pytest.raises(ValueError):
        hive.start_named_actor("shared", BaseActor)

    # Only the first actor remains registered
    assert hive.actor_ids == [addr1.id()]


async def test_actor_cannot_be_registered_under_two_names(hive: ActorHive) -> None:
    addr = hive.start_actor(BaseActor)
    await addr.wait_for_start()
    hive.register("first", addr)

    with pytest.raises(ValueError):
        hive.register("second", addr)

    assert hive.lookup("first") is not None
    assert hive.lookup("second") is None
