from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest

from temporaryname import ActorHive, BaseActor, Request


async def test_on_start_called(hive: ActorHive) -> None:
    tracker: dict[str, Any] = {}

    class MyActor(BaseActor):
        _t: dict[str, Any]

        def __init__(self, hive: ActorHive, actor_id: str, t: dict[str, Any]) -> None:
            super().__init__(hive, actor_id)
            self._t = t

        async def on_start(self) -> None:
            self._t["started"] = True

    addr = hive.start_actor(MyActor, tracker)
    await addr.wait_for_start()

    assert tracker.get("started") is True


async def test_on_stop_called_after_stop(hive: ActorHive) -> None:
    tracker: dict[str, Any] = {}

    class MyActor(BaseActor):
        _t: dict[str, Any]

        def __init__(self, hive: ActorHive, actor_id: str, t: dict[str, Any]) -> None:
            super().__init__(hive, actor_id)
            self._t = t

        async def on_stop(self) -> None:
            self._t["stopped"] = True

    addr = hive.start_actor(MyActor, tracker)
    await addr.wait_for_start()

    addr.stop()
    await addr.wait_for_stop()

    assert tracker.get("stopped") is True


async def test_on_stop_called_after_shutdown(hive: ActorHive) -> None:
    tracker: dict[str, Any] = {}

    class MyActor(BaseActor):
        _t: dict[str, Any]

        def __init__(self, hive: ActorHive, actor_id: str, t: dict[str, Any]) -> None:
            super().__init__(hive, actor_id)
            self._t = t

        async def on_stop(self) -> None:
            self._t["stopped"] = True

    addr = hive.start_actor(MyActor, tracker)
    await addr.wait_for_start()

    await hive.shutdown()

    assert tracker.get("stopped") is True


@dataclass
class Ping:
    value: int


async def test_on_message_receives_tell(hive: ActorHive) -> None:
    received: list[Any] = []

    class MyActor(BaseActor):
        _r: list[Any]

        def __init__(self, hive: ActorHive, actor_id: str, r: list[Any]) -> None:
            super().__init__(hive, actor_id)
            self._r = r

        async def on_message(self, message: Any) -> None:
            self._r.append(message)

    addr = hive.start_actor(MyActor, received)
    await addr.wait_for_start()

    addr.tell(Ping(42))
    await asyncio.sleep(0.05)

    assert received == [Ping(42)]


async def test_on_message_receives_multiple_tells(hive: ActorHive) -> None:
    received: list[Any] = []

    class MyActor(BaseActor):
        _r: list[Any]

        def __init__(self, hive: ActorHive, actor_id: str, r: list[Any]) -> None:
            super().__init__(hive, actor_id)
            self._r = r

        async def on_message(self, message: Any) -> None:
            self._r.append(message)

    addr = hive.start_actor(MyActor, received)
    await addr.wait_for_start()

    for i in range(5):
        addr.tell(Ping(i))
    await asyncio.sleep(0.05)

    assert received == [Ping(i) for i in range(5)]


@dataclass
class HelloMessage:
    text: str


@dataclass
class GoodbyeMessage:
    text: str


@dataclass
class HTTPRequest:
    url: str


async def test_type_routing_simple(hive: ActorHive) -> None:
    received: list[str] = []

    class MyActor(BaseActor):
        _r: list[str]

        def __init__(self, hive: ActorHive, actor_id: str, r: list[str]) -> None:
            super().__init__(hive, actor_id)
            self._r = r

        def on_hello_message(self, msg: HelloMessage) -> None:
            self._r.append(f"hello:{msg.text}")

    addr = hive.start_actor(MyActor, received)
    await addr.wait_for_start()

    addr.tell(HelloMessage("world"))
    await asyncio.sleep(0.05)

    assert received == ["hello:world"]


async def test_type_routing_multiple_handlers(hive: ActorHive) -> None:
    received: list[str] = []

    class MyActor(BaseActor):
        _r: list[str]

        def __init__(self, hive: ActorHive, actor_id: str, r: list[str]) -> None:
            super().__init__(hive, actor_id)
            self._r = r

        def on_hello_message(self, msg: HelloMessage) -> None:
            self._r.append(f"hello:{msg.text}")

        def on_goodbye_message(self, msg: GoodbyeMessage) -> None:
            self._r.append(f"bye:{msg.text}")

    addr = hive.start_actor(MyActor, received)
    await addr.wait_for_start()

    addr.tell(HelloMessage("a"))
    addr.tell(GoodbyeMessage("b"))
    addr.tell(HelloMessage("c"))
    await asyncio.sleep(0.05)

    assert received == ["hello:a", "bye:b", "hello:c"]


async def test_type_routing_acronym_classname(hive: ActorHive) -> None:
    """HTTPRequest -> on_http_request (acronym snake_case conversion)."""
    received: list[str] = []

    class MyActor(BaseActor):
        _r: list[str]

        def __init__(self, hive: ActorHive, actor_id: str, r: list[str]) -> None:
            super().__init__(hive, actor_id)
            self._r = r

        def on_http_request(self, msg: HTTPRequest) -> None:
            self._r.append(msg.url)

    addr = hive.start_actor(MyActor, received)
    await addr.wait_for_start()

    addr.tell(HTTPRequest("https://example.com"))
    await asyncio.sleep(0.05)

    assert received == ["https://example.com"]


async def test_type_routing_unhandled_raises_not_implemented(hive: ActorHive) -> None:
    @dataclass
    class UnknownRequest(Request[None]):
        pass

    class MyActor(BaseActor):
        pass

    addr = hive.start_actor(MyActor)
    await addr.wait_for_start()

    with pytest.raises(NotImplementedError):
        await addr.ask(UnknownRequest())


@dataclass
class AddRequest(Request[int]):
    a: int
    b: int


async def test_ask_returns_result(hive: ActorHive) -> None:
    class CalcActor(BaseActor):
        def on_add_request(self, req: AddRequest) -> int:
            return req.a + req.b

    addr = hive.start_actor(CalcActor)
    await addr.wait_for_start()

    result = await addr.ask(AddRequest(3, 4))

    assert result == 7


async def test_ask_concurrent_requests(hive: ActorHive) -> None:
    class CalcActor(BaseActor):
        def on_add_request(self, req: AddRequest) -> int:
            return req.a + req.b

    addr = hive.start_actor(CalcActor)
    await addr.wait_for_start()

    results = await asyncio.gather(
        addr.ask(AddRequest(1, 2)),
        addr.ask(AddRequest(10, 20)),
        addr.ask(AddRequest(100, 200)),
    )

    assert list(results) == [3, 30, 300]


async def test_ask_exception_propagates(hive: ActorHive) -> None:
    @dataclass
    class BoomRequest(Request[None]):
        pass

    class BoomActor(BaseActor):
        def on_boom_request(self, req: BoomRequest) -> None:
            raise ValueError("boom")

    addr = hive.start_actor(BoomActor)
    await addr.wait_for_start()

    with pytest.raises(ValueError, match="boom"):
        await addr.ask(BoomRequest())


@dataclass
class SlowRequest(Request[str]):
    delay: float


async def test_async_handler_supported(hive: ActorHive) -> None:
    class AsyncActor(BaseActor):
        async def on_slow_request(self, req: SlowRequest) -> str:
            await asyncio.sleep(req.delay)
            return "done"

    addr = hive.start_actor(AsyncActor)
    await addr.wait_for_start()

    result = await addr.ask(SlowRequest(delay=0.01))

    assert result == "done"


async def test_actor_removed_from_hive_after_stop(hive: ActorHive) -> None:
    class MyActor(BaseActor):
        pass

    addr = hive.start_actor(MyActor)
    await addr.wait_for_start()

    assert addr.id() in hive.actor_ids

    addr.stop()
    await addr.wait_for_stop()

    assert addr.id() not in hive.actor_ids
