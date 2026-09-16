# Changelog

## 0.9.0 — September 2026

- Fix `RedisDispatcher` and `AsyncRedisDispatcher` — neither could send or
  receive a message: `_broker_reachable()` now pings the server instead of
  always returning `True`, `_publish()` sends `bytes` (redis-py rejects
  `bytearray`), `_listen()` uses a non-blocking read and skips the subscribe
  confirmation, and the async pub/sub subscribes in `_listen()` instead of
  from a property; connections are released on stop (#57, #70)
- `RedisDispatcher` is now importable from the `dispatcher` module (#70)
- Payloads that fail to parse are now logged and skipped instead of crashing
  the listening loop (#58)
- `BaseDispatcher(reconnection=False)` now cleans up its resources when the
  connection to the broker is lost (#60)
- `Dispatcher._stop_threads()` and `AsyncDispatcher._stop_tasks()` no longer
  try to join/cancel the thread/task they are called from (#62, #64)
- Make resource cleanup more resilient when the main listening loop stops;
  `stop()` now clears the running flag and waits for the listening loop to
  exit (#65, #66)
- `KombuDispatcher`: the listener thread owns the listener connection,
  connections are closed exactly once, and publishing is serialized with a
  lock as py-amqp connections are not thread-safe (#63, #67)
- Add `Dispatcher._interrupt_listening()`: if the main loop has not exited
  within `stop()`'s timeout, it is forcibly interrupted and joined once more;
  `KombuDispatcher` implements it by dropping the listener socket (#68)
- Add tests for `AsyncAMQPDispatcher`, `KombuDispatcher` and the Redis
  dispatchers against real brokers; CI now provides RabbitMQ and Redis
  services (#61, #63, #70)
- Run the same broker test contract against the in-memory dispatchers and
  move the listening-loop tests to `test_abc.py`; `TestAsyncDispatcher` was a
  `unittest.TestCase` that pytest-asyncio never drove, so its tests now
  actually run (#71)
- Pin `ruff` and `ty` versions; fix the `ty` warnings they surfaced (#59)

## 0.8.1 — July 2026

- Bump `pytest` (#55)

## 0.8.0 — April 2026

- Update README with usage examples and installation instructions (#51)
- Fix repository URL in `pyproject.toml` (#50)
- Add `BaseEventHandler`; rework `{Async}EventHandler` (#53)
- Rework `{Async}RedisDispatcher` (#53)
- Add `ty` type checking to the QC pipeline (#53)

---

## 0.7.1 — February 2026

- Fix a race condition arising in Python < 3.12 in `AsyncDispatcher` (#48)
- Add back Python 3.11 to the test matrix (#48)

## 0.7.0 — February 2026

- Enforce ABC on `PubSub`, `AsyncPubSub`, and their brokers — misuse raises
  at class-definition time rather than at runtime (#46)
- Enforce ABC on `BaseDispatcher` subclasses (#40)
- Improve type safety throughout (#40)
- Optimise `{Async}Dispatcher._trigger_event()` — `need_sid` now computed once
  at handler registration rather than on every event (#45)
- `AsyncDispatcher.connect()` now raises if the dispatcher is already connected (#42)
- Remove `EventHandler.__hash__()` and `__eq__()` — their presence was misleading (#41)

## 0.6.1 — December 2025

- `reconnection` and `debug` options promoted to `BaseDispatcher` — available
  to all subclasses (#38)
- Add a `debug` flag; exception tracebacks are only logged when it is set (#36)

## 0.6.0 — December 2025

- `{Async}InMemoryDispatcher` can now publish to any available namespace (#34)
- Empty payloads no longer serialized — a dedicated data flag is used instead,
  avoiding ambiguity during deserialization (#33)

## 0.5.1 — July 2025

- Fix: `{Async}EventHandler` session identifier can now be any hashable object (#31)
- Use a `str` sentinel for empty data to avoid serialization edge cases (#28, #30)
- `AsyncDispatcher.register_event_handler()` is now synchronous (#29)

## 0.5.0 — July 2025

- Drop support for CPython ≤ 3.9 (#26)
- Allow session identifiers to be any hashable object (#24)
- `{Async}Dispatcher` listener distributes messages with `room=None` to all
  connected dispatchers (#20)
- Add fallback handler support
- Add `BaseDispatcher` as the common base of `{Async}Dispatcher`; background
  tasks can now run before the dispatcher is started, and stopping an
  already-stopped dispatcher raises (#19)
- Add a `timeout` parameter to `emit()` (#5)
- `Dispatcher` now has a default namespace, used by `emit()` (#6)
- Expose the underlying dispatcher on `{Async}EventHandler` via a `dispatcher`
  property (#7)
- Make AMQP-based dispatchers more robust to broker disconnection, and reduce
  `AsyncAMQPDispatcher`'s idle workload (#2, #3, #4, #8, #9, #10)
- Fix `bytes` payload encoding and parsing; `_encode_data()` / `_decode_data()`
  are now symmetric (#11, #12, #13, #14, #15)
- Add CI/CD pipeline (GitHub Actions); separate lint and test jobs (#16, #18)
- Add tests for the abstract `{Async}Dispatcher`, rooms, background jobs, and
  `{Async}InMemoryDispatcher` (#17, #21, #22, #23)

## 0.4.0 — March 2024

- `emit(to=...)` now accepts a `UUID` representing another dispatcher's `sid`
- `host_uid` and `sid` switched to `UUID` throughout
- Use `\x1d\x1d` as the payload separator
- Allow a custom serializer to be passed at instantiation
- Enable message acknowledgment in `AsyncAMQPDispatcher`
- Improve `AsyncAMQPDispatcher` resiliency under broker disconnection

## 0.3.0 — October 2023

- Improve stop signal propagation and handling across sync and async variants
- Uniformize `start` / `run` API between sync and async dispatchers
- Background tasks named consistently (`dispatcher-main_loop`)
- Add timeout when joining threads during stop
- Add `bytes` as a supported data type

## 0.2.1 — August 2023

- Fix `AsyncDispatcher.stop`
- Fix stop signal handling in listener loop
- Uniformize `datetime.time` serialization between `json` and `orjson`
- `connect` state now cleared by the listening loop
- Add logging points for easier debugging

## 0.2.0 — July 2023

- Improve broker disconnection handling and reconnection logic (#1)
- Use regular (non-robust) connections for `aio-pika` and `kombu` backends
- Remove default queue expiration time and message TTL
- Expose `Serializer` in the package namespace
- `orjson` now serializes `namedtuple`s as `tuple`, matching `json` behaviour
- Remove `configure_dispatcher` / `get_dispatcher` utils (leftover from
  the Gaia integration era)

## 0.1.0 — July 2022

Initial release as a standalone package, extracted from the
[gaia-ouranos](https://github.com/vaamb/gaia-ouranos) project:

- Sync (`Dispatcher`) and async (`AsyncDispatcher`) base classes with a
  Socket.IO-inspired API: `on()`, `emit()`, `connect()`, `start()`
- In-memory, Redis (sync/async), Kombu, and AMQP backends
- Namespace and room-based message routing
- `EventHandler` and `AsyncEventHandler` for class-based event handling
- Session management via context manager
- Background job support
- `orjson`-accelerated serialization with `json` fallback; handles `datetime`,
  `uuid`, and namedtuples
- Custom binary wire protocol with typed data flags
