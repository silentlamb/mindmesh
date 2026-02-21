import asyncio
import logging
from dataclasses import dataclass

from temporaryname import ActorHive, BaseActor, Request

logger = logging.getLogger("calculator-example")


@dataclass
class MathRequest(Request[int | float]):
    op: str
    a: int | float
    b: int | float


class CalculatorActor(BaseActor):
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
            case op:
                raise ValueError(f"Unsupported operation {op}")


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

    except ValueError as e:
        logger.error(f"Something went wrong: {e}")

    hive.shutdown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    asyncio.run(main())
