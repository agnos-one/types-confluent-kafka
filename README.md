# types-confluent-kafka

Partial typing stubs for [`confluent-kafka`](https://pypi.org/project/confluent-kafka/) (confluent-kafka-python).

The library ships its own type information, but part of it is wrong or incomplete. With the library alone:

- mypy turns `AvroSerializer`, `AvroDeserializer` and the JSON Schema and Protobuf serializers into `Any`, and
  pyright rejects their constructor arguments
  ([confluent-kafka-python#2390](https://github.com/confluentinc/confluent-kafka-python/issues/2390)).
- `AdminClient` methods return `Future[Unknown]`, and `from confluent_kafka.admin import NewTopic` is reported as
  a private import.
- Client configuration is a `dict[str, Any]`, and several signatures don't match the code: for example,
  `version()` returns a `str`, not a tuple.

These stubs replace the modules where that happens. The package is marked `partial` (PEP 561), so every other
module keeps the library's own types.

The stubs are checked against the runtime library with mypy's `stubtest`; [Versions](#versions) lists the
`confluent-kafka` version each release was checked against.

## Install

Releases are published to the Agnos package index at `https://pkgs.agnos.one/simple/`.

Install with [uv](https://docs.astral.sh/uv/):

```toml
# pyproject.toml
[[tool.uv.index]]
name = "agnos"
url = "https://pkgs.agnos.one/simple/"
explicit = true

[tool.uv.sources]
types-confluent-kafka = { index = "agnos" }
```

```bash
uv add --dev types-confluent-kafka
```

The package has no runtime code: install `confluent-kafka` itself as a regular dependency. It requires Python
3.14 or later.

## What's covered

| Module | Stubbed |
| --- | --- |
| `confluent_kafka`, `.cimpl` | `Producer`, `Consumer`, `ShareConsumer`, `Message`, `TopicPartition`, `KafkaError`, the exceptions, constants and functions |
| `.serializing_producer`, `.deserializing_consumer` | `SerializingProducer`, `DeserializingConsumer` |
| `.admin` | `AdminClient`, with a result type for every future |
| `.schema_registry` | `SchemaRegistryClient`, and the Avro, JSON Schema and Protobuf serializers, deserializers and their builders |

Left to the library's own types: `serialization`, `error`, the data classes of `admin` (`ConfigResource`,
`TopicMetadata`, ...) and of `schema_registry` (`Schema`, `RegisteredSchema`, ...), `aio`, the `Async*` schema
registry classes (which have the same constructor problem), `DeserializingShareConsumer`, the schema rules and
the deprecated `avro` module.

```python
from confluent_kafka import Consumer, KafkaError, Message, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext


def on_delivery(err: KafkaError | None, msg: Message) -> None: ...


producer = Producer({"bootstrap.servers": "localhost:9092"})
producer.produce("orders", value=b"...", key="42", on_delivery=on_delivery)
producer.flush(10)  # int: the messages still queued

registry = SchemaRegistryClient({"url": "http://localhost:8081"})
deserializer = AvroDeserializer(registry, from_dict=dict_to_order)  # AvroDeserializer[Order]

consumer = Consumer({"bootstrap.servers": "localhost:9092", "group.id": "orders"})
consumer.subscribe(["orders"])
msg = consumer.poll(1.0)  # Message | None
if msg is not None and msg.error() is None:
    order = deserializer(msg.value(), SerializationContext("orders", MessageField.VALUE))  # Order | None
```

## Notes

- **Configuration is a `dict` with `str` keys**, of any value type: a `dict[str, str]` variable is accepted, but
  not another mapping, which raises `TypeError` at runtime. The same goes for lists: `subscribe()`, `assign()` and
  the `AdminClient` methods need a real `list`.
- **Some classes take type arguments in the stubs only.** `Message[K, V]` has the types `key()` and `value()`
  return (`bytes` unless it comes from a `DeserializingConsumer`), and `AvroSerializer[T]`, `AvroDeserializer[T]`
  and the JSON Schema and Protobuf classes have the type their converter or message class gives them. At runtime
  these classes can't be subscripted, so write `AvroDeserializer[Order]` in annotations only.
  `SerializingProducer[K, V]` and `DeserializingConsumer[K, V]` can be subscripted at runtime too.
- **Without `from_dict`, a deserializer returns `object`**: a `dict` for a record, but the schema decides. Narrow
  it, or pass `from_dict`.
- **`Message.headers()` returns a list of pairs** (or `None`), with values typed `str | bytes | None`. A received
  message only has `bytes | None` values; the wider type lets the list be passed to `SerializationContext` and
  `produce()`.
- **`Producer.produce_batch()` takes a `list[BatchMessage]`.** `BatchMessage` exists only in the stubs: import it
  from `confluent_kafka.cimpl` under `if TYPE_CHECKING:`.
- **Some signatures differ from the library's annotations**, where the code does. For example, the Protobuf
  classes take the message class (the library annotates an instance), the transaction methods of `Producer` are
  positional-only, the callbacks of `Consumer.subscribe()` can't be `None`, and `AdminClient.elect_leaders()` takes
  no timeout (passing one raises `SystemError`).
- **Deprecated members are marked `@deprecated`** (`AdminClient.alter_configs`, `AdminClient.list_groups`), so
  pyright strict reports their use (`reportDeprecated`).
- **Protobuf with mypy needs [`types-protobuf`](https://pypi.org/project/types-protobuf/).** Pyright bundles
  those stubs.

## Versions

The stubs have their own version numbers, which don't follow `confluent-kafka`'s. Each release is checked with
`stubtest` against one `confluent-kafka` version. Other versions usually work as well, as long as the stubbed
signatures haven't changed.

| Stubs | Checked against `confluent-kafka` |
| --- | --- |
| 0.1.0 | 2.16.2 |
