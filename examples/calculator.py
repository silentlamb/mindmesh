from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from mindmesh import ActorHive, BaseActor, Request

if TYPE_CHECKING:
    from mindmesh import ActorAddr

logger = logging.getLogger("calculator-example")


@dataclass
class MathRequest(Request[int | float]):
    op: str
    a: int | float
    b: int | float


@dataclass
class RandomValue:
    value: int


class CalculatorActor(BaseActor):
    def __init__(self, hive: ActorHive, id: str):
        super().__init__(hive, id)
        self._random = 42

    def on_task_create(self):
        return feed_random(self.as_ref())

    def on_random_value(self, event: RandomValue):
        self._random = event.value

    def on_math_request(self, request: MathRequest) -> int | float:
        logger.debug(f"on_math_request: {request}")
        match request.op:
            case "add":
                return request.a + request.b
            case "sub":
                return request.a - request.b
            case "mul":
                return request.a * request.b
            case "div":
                if request.b == 0:
                    raise ZeroDivisionError()
                return request.a / request.b
            case "add-random":
                return request.a + request.b + self._random
            case op:
                raise ValueError(f"Unsupported operation {op}")


async def feed_random(actor: ActorAddr):
    logger.debug("Starting task: feed random")
    try:
        value = 0
        while True:
            await asyncio.sleep(0.1)
            value += 100
            actor.tell(RandomValue(value=value))

    except asyncio.CancelledError:
        logger.debug("Cancelling task: feed random")

    except Exception as e:
        logger.debug("Unhandled error in feed random", exc_info=e)

    finally:
        logger.debug("Stopping task: feed random")


async def main():
    hive = ActorHive()
    calculator = hive.start_actor(CalculatorActor)
    await calculator.wait_for_start()

    try:
        result = await calculator.ask(MathRequest("add", 2, 2))
        print(f"2 + 2 = {result}")

        result = await calculator.ask(MathRequest("sub", 2, 2))
        print(f"2 - 2 = {result}")

        result = await calculator.ask(MathRequest("mul", 42, 2))
        print(f"42 * 2 = {result}")

        result = await calculator.ask(MathRequest("div", 42, 2))
        print(f"42 / 2 = {result}")

        try:
            result = await calculator.ask(MathRequest("div", 1, 0))
            print(f"1 / 0 = {result}")
        except ZeroDivisionError:
            print("1 / 0 = NIE WOLNO TAK")

        for i in range(0, 10):
            result = await calculator.ask(MathRequest("add-random", 2, i))
            print(f"2 + {i} + random = {result}")
            await asyncio.sleep(0.25)

    except ValueError as e:
        logger.error(f"Something went wrong: {e}")

    await hive.shutdown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    asyncio.run(main())
