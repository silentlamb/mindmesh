# To be done

## Named actor registry

- [x] `hive.register(name, addr)` — register actor under a human-readable name
- [x] `hive.lookup(name) -> ActorAddr | None` — find actor by name
- [x] `hive.start_named_actor(name, ActorClass, ...)` — convenience method
- [x] Auto-deregister from registry when actor stops (hook into `on_actor_stopped`)

## Scheduled messages

- [ ] `addr.tell_after(delay, msg) -> asyncio.Task` — send after delay, cancel task to abort
- [ ] `addr.tell_every(interval, msg, *, immediately=False) -> asyncio.Task` — repeating send, stops automatically if actor is gone

## Broadcast

- [ ] `hive.broadcast(actor_ids, event)` — send to a specific list of actors (skip missing)
- [ ] `hive.broadcast_all(event)` — send to all currently registered actors

## Message pipelines

- [ ] `Middleware = Callable[[str, Any, NextFn], Awaitable[Any]]` type alias in `core.py`
- [ ] `hive.add_pipeline_step(middleware)` — register a hive-wide middleware
- [ ] Apply pipeline in `BaseActor._loop` via new `_dispatch()` method wrapping `on_message`
- [ ] Middleware receives `(actor_id, message, next)` — calling `next()` continues the chain

## Actor pools

- [ ] `ActorPool` class in new `pool.py` — round-robin routing over N identical actors
- [ ] `pool.tell(msg)` / `await pool.ask(request)` — routes to next actor
- [ ] `pool.broadcast(msg)` — sends to all actors in pool
- [ ] `await pool.shutdown()` — stops all pool actors
- [ ] `hive.start_pool(ActorClass, count, ...)` — convenience factory

## Supervision strategies

- [ ] `RestartPolicy(max_restarts=3, window=60.0)` dataclass
- [ ] `SupervisorActor(BaseActor)` base class in new `supervisor.py`
- [ ] `await self.start_child(ActorClass, ..., policy=...)` — starts, links, and tracks child
- [ ] Default `on_link_death` restarts crashed children (respects policy), skips clean stops
- [ ] `on_child_restart_limit(actor_id, reason)` hook — override to customise max-restarts behaviour (default: stop supervisor)
