event-dispatcher
================

A broker-agnostic, Socket.IO-inspired event dispatcher for Python 3.11+.
Supports both synchronous and asynchronous usage, with pluggable backends
for in-memory, Redis, RabbitMQ (via Kombu or aio-pika), and any custom broker.

Used as the inter-process communication layer in the
[gaia-ouranos](https://github.com/vaamb/gaia-ouranos) ecosystem.

---

Features
--------

- **Sync and async parity** — `Dispatcher` and `AsyncDispatcher` share the same
  API; pick the one that matches your runtime
- **Broker-agnostic** — swap backends without changing application code
- **Namespaces and rooms** — independent pub/sub channels with targeted or
  broadcast delivery
- **Decorator and class-based handlers** — register handlers with `@dispatcher.on()`
  or by subclassing `EventHandler`
- **Session management** — per-identifier session data via a context manager
- **Background jobs** — schedule recurring tasks tied to the dispatcher lifecycle
- **Reconnection** — exponential backoff reconnection with configurable retry
- **Custom serialization** — uses `orjson` when available, falls back to `json`;
  handles `datetime`, `uuid`, and namedtuples out of the box

---

Backends
--------

| Class                      | Backend                               | Type  |
|----------------------------|---------------------------------------|-------|
| `InMemoryDispatcher`       | In-process queue                      | Sync  |
| `AsyncInMemoryDispatcher`  | In-process queue                      | Async |
| `RedisDispatcher`          | Redis pub/sub                         | Sync  |
| `AsyncRedisDispatcher`     | Redis pub/sub                         | Async |
| `KombuDispatcher`          | RabbitMQ / Redis / others (via Kombu) | Sync  |
| `AsyncAMQPDispatcher`      | RabbitMQ (via aio-pika)               | Async |

---

Quick start
-----------

**Decorator-based handlers:**

```python
from dispatcher import AsyncInMemoryDispatcher

dispatcher = AsyncInMemoryDispatcher()

@dispatcher.on("temperature_update")
async def on_temperature(data):
    print(f"Temperature: {data['value']} °C")

await dispatcher.start()

await dispatcher.emit("temperature_update", data={"value": 22.5})
```

**Class-based handlers:**

```python
from dispatcher import AsyncEventHandler, AsyncInMemoryDispatcher

class MyHandler(AsyncEventHandler):
    async def on_temperature_update(self, data):
        print(f"Temperature: {data['value']} °C")

dispatcher = AsyncInMemoryDispatcher()
handler = MyHandler()
dispatcher.register_event_handler(handler)

await dispatcher.start()

await dispatcher.emit("temperature_update", data={"value": 22.5})
```

**Get the sender address:**

```python
@dispatcher.on("double")
async def handler(sid, data):
    await dispatcher.emit("doubled", data=data*2, to=sid)
```

**Namespaces and rooms:**

```python
# Send to all subscribers in the namespace
await dispatcher.emit("event", data={"key": "value"})

# Send to a specific room only
await dispatcher.emit("event", data={"key": "value"}, room="room-name")

# Send to a specific dispatcher only
await dispatcher.emit("event", data={"key": "value"}, to=remote_sid)
```

**Session management:**

```python
async with dispatcher.session(identifier) as session:
    session["key"] = "value"
```

---

Installation
------------

```bash
pip install event-dispatcher
pip3 install git+https://github.com/vaamb/event-dispatcher.git@0.7.1
```

Optional backend dependencies:

```bash
pip install redis          # RedisDispatcher / AsyncRedisDispatcher
pip install kombu          # KombuDispatcher
pip install aio-pika       # AsyncAMQPDispatcher
```

---

Status
------

Active. `v0.7.1`. Used in production as part of the gaia-ouranos ecosystem.
Python 3.11–3.14 supported, tested in CI across all versions.
