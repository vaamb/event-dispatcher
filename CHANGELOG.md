# Changelog

## Unreleased

- Updated README with usage examples and installation instructions
- Fix repository URL in `pyproject.toml`

---

## 0.7.1 — February 2026

- Fix a race condition arising in Python < 3.12 in `AsyncDispatcher`
- Add back Python 3.11 to the test matrix

## 0.7.0 — February 2026

- Enforce ABC on `PubSub`, `AsyncPubSub`, and their brokers — misuse raises
  at class-definition time rather than at runtime
- Enforce ABC on `BaseDispatcher` subclasses
- Improve type safety throughout
- Optimise `{Async}Dispatcher._trigger_event()` — `need_sid` now computed once
  at handler registration rather than on every event
- `AsyncDispatcher.connect()` now raises if the dispatcher is already connected
- Remove `EventHandler.__hash__()` and `__eq__()` — their presence was misleading

## 0.6.1 — December 2025

- `reconnection` and `debug` options promoted to `BaseDispatcher` — available
  to all subclasses
- Add a `debug` flag; exception tracebacks are only logged when it is set

## 0.6.0 — December 2025

- `{Async}InMemoryDispatcher` can now publish to any available namespace
- Empty payloads no longer serialized — a dedicated data flag is used instead,
  avoiding ambiguity during deserialization

## 0.5.1 — July 2025

- Fix: `{Async}EventHandler` session identifier can now be any hashable object
- Use a `str` sentinel for empty data to avoid serialization edge cases
- `AsyncDispatcher.register_event_handler()` is now synchronous

## 0.5.0 — July 2025

- Drop support for CPython ≤ 3.9
- Allow session identifiers to be any hashable object
- `{Async}Dispatcher` listener distributes messages with `room=None` to all
  connected dispatchers
- Add fallback handler support
- Add tests for rooms, background jobs, and `{Async}InMemoryDispatcher`

## 0.4.0 — March 2024

- `emit(to=...)` now accepts a `UUID` representing another dispatcher's `sid`
- `host_uid` and `sid` switched to `UUID` throughout
- Use `\x1d\x1d` as the payload separator
- Allow a custom serializer to be passed at instantiation
- Enable message acknowledgment in `AsyncAMQPDispatcher`
- Improve `AsyncAMQPDispatcher` resiliency under broker disconnection
- Add CI/CD pipeline (GitHub Actions); separate lint and test jobs

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

- Improve broker disconnection handling and reconnection logic
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
