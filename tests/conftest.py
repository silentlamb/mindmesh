from collections.abc import AsyncGenerator

import pytest

from mindmesh import ActorHive


@pytest.fixture
async def hive() -> AsyncGenerator[ActorHive, None]:
    """Create ActorHive with automatic shutdown"""
    h = ActorHive()
    yield h
    await h.shutdown()
