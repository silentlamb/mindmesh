# mindmesh

A minimal, dependency-free actor system for asyncio.

- asyncio-native, zero external dependencies
- fire-and-forget (tell) and request/response (ask) messaging
- automatic message dispatch by message type
- actor lifecycle hooks and background task support
- actor linking and supervision (restart on death, stop propagation)


## Installation

    pip install mindmesh


Or with uv:

    uv add mindmesh


## Quick Start

```python
import asyncio
from dataclasses import dataclass
from mindmesh import ActorHive, BaseActor, Request

@dataclass
class Greet(Request[str]):
    name: str

class GreeterActor(BaseActor):
    def on_greet(self, msg: Greet) -> str:
        return f"Hello, {msg.name}!"

async def main():
    hive = ActorHive()
    greeter = hive.start_actor(GreeterActor)
    await greeter.wait_for_start()

    reply = await greeter.ask(Greet(name="world"))
    print(reply)  # Hello, world!

    await hive.shutdown()

asyncio.run(main())
```

## Core Concepts

### BaseActor

Subclass `BaseActor` to define an actor. Each actor owns a private asyncio
queue (its mailbox) and processes one message at a time - no parallelism
within a single actor.

```python
class MyActor(BaseActor):
    def __init__(self, hive: ActorHive, id: str, extra_arg: int):
        super().__init__(hive, id)
        self._extra = extra_arg
```

`hive` and `id` are always the first two constructor parameters; any additional
arguments are passed by the caller via `hive.start_actor()`.

### ActorHive

`ActorHive` is the registry and lifecycle manager. It creates, starts, and
stops actors.

```python
hive = ActorHive()
addr = hive.start_actor(MyActor, extra_arg)   # returns ActorAddr
await hive.shutdown()                         # stops all actors
```

### ActorAddr

`ActorAddr` is an opaque handle to an actor. It is the only way to interact
with an actor from outside. Never hold a direct reference to an actor instance.

```python
addr = hive.start_actor(MyActor)
addr.tell(SomeMessage())
result = await addr.ask(SomeRequest())
addr.stop()
```

## Messaging

### tell

Enqueues any object as a fire-and-forget message. Returns immediately.

```python
addr.tell(SomeMessage(payload=42))
```

### ask

Enqueues a `Request` subclass and returns an awaitable that resolves to the
handler's return value. Exceptions raised in the handler are re-raised in the
caller.

```python
@dataclass
class AddRequest(Request[int]):
    a: int
    b: int

result = await addr.ask(AddRequest(a=1, b=2))  # -> int
```

### Message dispatch

`BaseActor.on_message()` routes incoming messages automatically. For a message
of class `FooBar`, it calls `self.on_foo_bar(msg)`. You only need to define
handler methods -- no manual dispatch boilerplate. This is default (optional) behavior,
feel free to reimplement `BaseActor.on_message()` for your liking.

```python
class MyActor(BaseActor):
    def on_foo_bar(self, msg: FooBar) -> None: ...
    def on_add_request(self, msg: AddRequest) -> int: ...
```

Handler methods may be plain or async.


## Lifecycle Hooks

Override any of these on your actor class:

    on_start()
        Called before the mailbox loop begins.
        Use it to initialize resources or start child actors.

    on_stop()
        Called after the mailbox loop exits, regardless of the stop reason.
        Use it to clean up resources.

    on_task_create()
        Return a coroutine to run as a background task alongside the mailbox
        loop. If the background task raises an unhandled exception, the actor
        stops.

    on_link_death(actor_id: str, reason: StopReasonType) -> LinkAction
        Called when a monitored actor stops. Return LinkAction.Stop (default)
        to stop this actor too, or LinkAction.Continue to keep running.

All hooks may be async.


## Supervision and Linking

Actors can monitor each other. When a monitored actor stops, the monitoring
actor's `on_link_death()` hook is called.

```python
# source stops -> monitor.on_link_death() is called
hive.link_actors(source_addr, monitor_addr)

# bidirectional: each monitors the other
hive.link_actors_both(addr_a, addr_b)

# monitor from within an actor (self monitors another)
self.as_ref().monitor(other_addr)
```

### Stop reasons

`on_link_death` receives one of:

    StopReason.Stop       -- actor stopped normally
    StopReason.Shutdown   -- hive-wide shutdown
    StopReason.LinkDeath  -- a linked actor died and propagated the stop
    <exception instance>  -- actor crashed with an unhandled exception

### Restarting crashed actors

To restart a worker that crashes, return `LinkAction.Continue` from
`on_link_death` and start a replacement:

```python
async def on_link_death(self, actor_id: str, reason: StopReasonType) -> LinkAction:
    replacement = self.hive.start_actor(WorkerActor)
    self.as_ref().monitor(replacement)
    return LinkAction.Continue
```

## Examples

**examples/calculator.py**: A calculator actor that handles arithmetic requests
via `ask()` and receives periodic random values via `tell()` from a background
task.

**examples/supervisor.py**: A supervisor that manages a pool of workers, round-robins
jobs to them, and automatically restarts any worker that crashes.

Run an example:

    uv run examples/calculator.py
    uv run examples/supervisor.py


## Development

Run tests:

    uv run pytest

Lint and type check:

    uv run ruff check --fix
    uv run pyright


## License

MIT - see LICENSE file.


## Use of LLMs

This project has been made by the use of LLMs in following areas:

- Code review
- Preparing test cases for discovered issues
- Preparing and verifying documentation
