"""Type-checking cases for the stubs. Pyright checks this file; it is never run."""

from concurrent.futures import Future
from typing import TYPE_CHECKING, assert_type

from confluent_kafka import (
    OFFSET_BEGINNING,
    Consumer,
    DeserializingConsumer,
    KafkaError,
    KafkaException,
    Message,
    Producer,
    SerializingProducer,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic, TopicMetadata
from confluent_kafka.schema_registry import RegisteredSchema, Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer, JSONSerializer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer, ProtobufSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer
from google.protobuf.timestamp_pb2 import Timestamp

if TYPE_CHECKING:
    from types import MappingProxyType

    from confluent_kafka.cimpl import BatchMessage


class User:
    name: str


def user_to_dict(user: User, ctx: SerializationContext) -> dict[str, str]:
    return {"name": user.name}


def dict_to_user(data: dict[str, object], ctx: SerializationContext) -> User:
    return User()


def on_delivery(err: KafkaError | None, msg: Message) -> None:
    assert_type(msg.value(), bytes | None)


def on_assign(consumer: Consumer, partitions: list[TopicPartition]) -> None: ...


def check_fallback(schema: Schema, registered: RegisteredSchema, topics: dict[str, TopicMetadata]) -> None:
    # modules the stubs leave out resolve to the library's own types: these names come from unstubbed modules, some
    # through a stubbed one (`confluent_kafka.admin`) and some through the library's (`confluent_kafka.schema_registry`)
    assert_type(schema.schema_str, str | None)
    assert_type(registered.schema_id, int | None)
    assert_type(StringSerializer()("key"), bytes | None)
    assert_type(topics["topic"].topic, str | None)


def check_config(read_only: MappingProxyType[str, str]) -> None:
    # a `dict` variable of any value type is accepted, though `dict` is invariant
    strings: dict[str, str] = {"bootstrap.servers": "localhost:9092"}
    mixed: dict[str, str | bool | int] = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "g",
        "enable.auto.commit": False,
    }
    Producer(strings)
    Producer(strings, logger=None)
    Producer({"bootstrap.servers": "localhost:9092", "error_cb": print, "linger.ms": 5})
    Consumer(mixed)
    AdminClient(strings)
    SchemaRegistryClient({"url": "http://localhost:8081"})
    # the configuration has to be a real `dict`
    Producer(read_only)  # pyright: ignore[reportArgumentType]
    SchemaRegistryClient(read_only)  # pyright: ignore[reportArgumentType]


def check_producer(producer: Producer) -> None:
    producer.produce("topic", b"value", "key", on_delivery=on_delivery, headers=[("trace", b"1"), ("empty", None)])
    producer.produce("topic", headers={"trace": "1"})
    producer.produce("topic", headers=(("trace", b"1"),))  # pyright: ignore[reportArgumentType]
    # the transaction methods take no keyword arguments
    producer.init_transactions(5.0)
    producer.init_transactions(timeout=5.0)  # pyright: ignore[reportCallIssue]
    # a batch built in a variable needs the stub-only type
    batch: list[BatchMessage] = [{"value": "a"}, {"value": b"b", "key": "k", "partition": 0}]
    producer.produce_batch("topic", batch)
    producer.produce_batch("topic", [{"value": "a", "timestamp": 1}])  # pyright: ignore[reportArgumentType]


def check_consumer(consumer: Consumer) -> None:
    consumer.subscribe(["topic"], on_assign=on_assign)
    consumer.subscribe(("topic",))  # pyright: ignore[reportArgumentType]
    consumer.subscribe(["topic"], on_assign=None)  # pyright: ignore[reportArgumentType]
    consumer.assign([TopicPartition("topic", 0, OFFSET_BEGINNING)])
    TopicPartition("topic", metadata=None)  # pyright: ignore[reportArgumentType]
    msg = consumer.poll(1.0)
    if msg is None:
        return
    assert_type(msg, Message[bytes, bytes])
    assert_type(msg.value(), bytes | None)
    # a message can't be hashed
    print({msg})  # pyright: ignore[reportUnhashable]
    # the headers of a received message can be passed on
    SerializationContext("topic", MessageField.VALUE, msg.headers())
    # the result of `commit` depends on `asynchronous`
    assert_type(consumer.commit(msg), None)
    assert_type(consumer.commit(message=msg, asynchronous=False), list[TopicPartition])
    assert_type(consumer.commit(offsets=[TopicPartition("topic", 0, 1)], asynchronous=False), list[TopicPartition])


def check_serializing_clients(conf: dict[str, object]) -> None:
    # the key and value types are given where the client is created
    producer = SerializingProducer[str, User](conf)
    producer.produce("topic", key="key", value=User(), on_delivery=on_delivery)
    producer.produce("topic", key=1)  # pyright: ignore[reportArgumentType]
    consumer = DeserializingConsumer[str, User](conf)
    msg = consumer.poll(1.0)
    if msg is not None:
        assert_type(msg.key(), str | None)
        assert_type(msg.value(), User | None)
    # without them the value is an `object`
    plain = DeserializingConsumer(conf).poll(1.0)
    if plain is not None:
        assert_type(plain.value(), object | None)


def check_admin(admin: AdminClient) -> None:
    # `NewTopic` is exported from `confluent_kafka.admin`, and each future has a result type
    created = admin.create_topics([NewTopic("topic", 3, 1)], operation_timeout=30, validate_only=False)
    assert_type(created, dict[str, Future[None]])
    resource = ConfigResource(ConfigResource.Type.TOPIC, "topic")
    assert_type(admin.describe_configs([resource])[resource].result()["retention.ms"].value, str | None)
    assert_type(admin.list_topics(timeout=10).topics["topic"].topic, str | None)
    # the keyword arguments are those the method accepts
    admin.create_topics([NewTopic("topic")], timeout=30)  # pyright: ignore[reportCallIssue]
    # the result of `describe_user_scram_credentials` depends on `users`
    assert_type(len(admin.describe_user_scram_credentials().result()), int)
    assert_type(len(admin.describe_user_scram_credentials(["user"])), int)
    admin.alter_configs([resource])  # pyright: ignore[reportDeprecated]
    try:
        admin.poll(1.0)
    except KafkaException as e:
        print(e.args[0])


def check_schema_registry(client: SchemaRegistryClient, ctx: SerializationContext) -> None:
    with client as entered:
        assert_type(entered, SchemaRegistryClient)
    client.register_schema("subject", Schema("{}", "AVRO"))
    # the serializers' constructors take arguments, and a converter sets the type they work with
    serializer = AvroSerializer(client, "{}", user_to_dict)
    assert_type(serializer, AvroSerializer[User])
    assert_type(serializer(User(), ctx), bytes | None)
    serializer({"name": "a"}, ctx)  # pyright: ignore[reportArgumentType]
    deserializer = AvroDeserializer(client, "{}", dict_to_user)
    assert_type(deserializer(b"data", ctx), User | None)
    # without a converter they work with any value
    AvroSerializer(client, schema_str="{}")({"name": "a"}, ctx)
    assert_type(AvroDeserializer(client)(b"data", ctx), object | None)
    AvroSerializer(client, "{}", conf={"auto.register.schemas": False})
    AvroSerializer()  # pyright: ignore[reportCallIssue]
    assert_type(JSONSerializer("{}", client, user_to_dict)(User(), ctx), bytes | None)
    assert_type(JSONDeserializer("{}", dict_to_user)(b"data", ctx), User | None)
    # the Protobuf classes take the message class, and the deserializer returns a message
    assert_type(ProtobufSerializer(Timestamp, client)(Timestamp(), ctx), bytes | None)
    assert_type(ProtobufDeserializer(Timestamp)(b"data", ctx), Timestamp | None)
    ProtobufSerializer(Timestamp(), client)  # pyright: ignore[reportArgumentType]
