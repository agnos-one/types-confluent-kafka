from collections.abc import Callable, Mapping
from typing import Self

from confluent_kafka.schema_registry._sync.schema_registry_client import SchemaRegistryClient
from confluent_kafka.schema_registry._sync.serde import BaseDeserializer, BaseSerializer
from confluent_kafka.schema_registry.rule_registry import RuleRegistry
from confluent_kafka.serialization import DeserializerBuilder, SerializationContext, SerializerBuilder
from google.protobuf.message import Message

# At runtime these classes assign `__init__ = __init_impl`, which mypy can't type (every class becomes `Any`) and
# pyright reads as a constructor without parameters. The stubs declare a plain `__init__`.
#
# `M` is a stub-only type parameter (the runtime classes can't be subscripted, so use it in annotations only): the
# generated message class. `msg_type` / `message_type` is that class itself, not an instance, though the library
# annotates it `Message`.

class ProtobufSerializer[M: Message = Message](BaseSerializer):
    def __init__(
        self,
        msg_type: type[M],
        schema_registry_client: SchemaRegistryClient,
        conf: Mapping[str, object] | None = None,
        rule_conf: dict[str, str] | None = None,
        rule_registry: RuleRegistry | None = None,
    ) -> None: ...
    # returns `None` for `None`
    def __call__(self, message: M | None, ctx: SerializationContext | None = None) -> bytes | None: ...

# The `set_*` methods return the builder itself.
class ProtobufSerializerBuilder[M: Message = Message](SerializerBuilder):
    # `schema_registry_config` has to be a `dict`: it is what `SchemaRegistryClient` takes
    def __init__[C](
        self,
        schema_registry_config: dict[str, C] | None = None,
        schema_registry_client: SchemaRegistryClient | None = None,
        message_type: type[M] | None = None,
        serializer_config: Mapping[str, object] | None = None,
        rule_config: dict[str, str] | None = None,
        rule_registry: RuleRegistry | None = None,
        serializer_init: Callable[[ProtobufSerializer[M]], None] | None = None,
    ) -> None: ...
    def set_schema_registry_config[C](self, schema_registry_conf: dict[str, C]) -> Self: ...
    def set_schema_registry_client(self, schema_registry_client: SchemaRegistryClient) -> Self: ...
    def set_message_type(self, msg_type: type[M]) -> Self: ...
    def set_serializer_config(self, serializer_conf: Mapping[str, object]) -> Self: ...
    def set_rule_config(self, rule_conf: dict[str, str]) -> Self: ...
    def set_rule_registry(self, rule_registry: RuleRegistry) -> Self: ...
    def set_serializer_init(self, serializer_init: Callable[[ProtobufSerializer[M]], None]) -> Self: ...
    # returns the serializer and the client configuration left for the other builders
    def build(self, conf: dict[str, object], is_key: bool) -> tuple[ProtobufSerializer[M], dict[str, object]]: ...

class ProtobufDeserializer[M: Message = Message](BaseDeserializer):
    def __init__(
        self,
        message_type: type[M],
        conf: Mapping[str, object] | None = None,
        schema_registry_client: SchemaRegistryClient | None = None,
        rule_conf: dict[str, str] | None = None,
        rule_registry: RuleRegistry | None = None,
    ) -> None: ...
    # returns the message (the library annotates `bytes`), or `None` for `None`
    def __call__(self, data: bytes | None, ctx: SerializationContext | None = None) -> M | None: ...

class ProtobufDeserializerBuilder[M: Message = Message](DeserializerBuilder):
    # `schema_registry_config` has to be a `dict`: it is what `SchemaRegistryClient` takes
    def __init__[C](
        self,
        schema_registry_config: dict[str, C] | None = None,
        schema_registry_client: SchemaRegistryClient | None = None,
        message_type: type[M] | None = None,
        deserializer_config: Mapping[str, object] | None = None,
        rule_config: dict[str, str] | None = None,
        rule_registry: RuleRegistry | None = None,
        deserializer_init: Callable[[ProtobufDeserializer[M]], None] | None = None,
    ) -> None: ...
    def set_schema_registry_config[C](self, schema_registry_conf: dict[str, C]) -> Self: ...
    def set_schema_registry_client(self, schema_registry_client: SchemaRegistryClient) -> Self: ...
    def set_message_type(self, msg_type: type[M]) -> Self: ...
    def set_deserializer_config(self, deserializer_conf: Mapping[str, object]) -> Self: ...
    def set_rule_config(self, rule_conf: dict[str, str]) -> Self: ...
    def set_rule_registry(self, rule_registry: RuleRegistry) -> Self: ...
    def set_deserializer_init(self, deserializer_init: Callable[[ProtobufDeserializer[M]], None]) -> Self: ...
    # returns the deserializer and the client configuration left for the other builders
    def build(self, conf: dict[str, object], is_key: bool) -> tuple[ProtobufDeserializer[M], dict[str, object]]: ...
