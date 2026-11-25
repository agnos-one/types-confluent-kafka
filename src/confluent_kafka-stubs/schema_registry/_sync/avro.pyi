from collections.abc import Callable, Mapping
from typing import Self

from confluent_kafka.schema_registry._sync.schema_registry_client import SchemaRegistryClient
from confluent_kafka.schema_registry._sync.serde import BaseDeserializer, BaseSerializer
from confluent_kafka.schema_registry.common.schema_registry_client import Schema
from confluent_kafka.schema_registry.rule_registry import RuleRegistry
from confluent_kafka.serialization import DeserializerBuilder, SerializationContext, SerializerBuilder

# At runtime these classes assign `__init__ = __init_impl`, which mypy can't type (every class becomes `Any`) and
# pyright reads as a constructor without parameters. The stubs declare a plain `__init__`.
#
# `T` is a stub-only type parameter (the runtime classes can't be subscripted, so use it in annotations only): the
# type `to_dict` accepts or `from_dict` returns. Without a converter it is `object`: the serializer takes, and the
# deserializer returns, whatever value the Avro schema describes, a `dict` for a record.

class AvroSerializer[T = object](BaseSerializer):
    def __init__(
        self,
        schema_registry_client: SchemaRegistryClient,
        schema_str: str | Schema | None = None,
        to_dict: Callable[[T, SerializationContext], Mapping[str, object]] | None = None,
        conf: Mapping[str, object] | None = None,
        rule_conf: dict[str, str] | None = None,
        rule_registry: RuleRegistry | None = None,
    ) -> None: ...
    # returns `None` for `None`
    def __call__(self, obj: T | None, ctx: SerializationContext | None = None) -> bytes | None: ...

# The `set_*` methods return the builder itself.
class AvroSerializerBuilder[T = object](SerializerBuilder):
    # `schema_registry_config` has to be a `dict`: it is what `SchemaRegistryClient` takes
    def __init__[C](
        self,
        schema_registry_config: dict[str, C] | None = None,
        schema_registry_client: SchemaRegistryClient | None = None,
        schema: str | Schema | None = None,
        to_dict: Callable[[T, SerializationContext], Mapping[str, object]] | None = None,
        serializer_config: Mapping[str, object] | None = None,
        rule_config: dict[str, str] | None = None,
        rule_registry: RuleRegistry | None = None,
        serializer_init: Callable[[AvroSerializer[T]], None] | None = None,
    ) -> None: ...
    def set_schema_registry_config[C](self, schema_registry_conf: dict[str, C]) -> Self: ...
    def set_schema_registry_client(self, schema_registry_client: SchemaRegistryClient) -> Self: ...
    def set_schema(self, schema_str: str | Schema) -> Self: ...
    def set_to_dict(self, to_dict: Callable[[T, SerializationContext], Mapping[str, object]]) -> Self: ...
    def set_serializer_config(self, serializer_conf: Mapping[str, object]) -> Self: ...
    def set_rule_config(self, rule_conf: dict[str, str]) -> Self: ...
    def set_rule_registry(self, rule_registry: RuleRegistry) -> Self: ...
    def set_serializer_init(self, serializer_init: Callable[[AvroSerializer[T]], None]) -> Self: ...
    # returns the serializer and the client configuration left for the other builders
    def build(self, conf: dict[str, object], is_key: bool) -> tuple[AvroSerializer[T], dict[str, object]]: ...

class AvroDeserializer[T = object](BaseDeserializer):
    def __init__(
        self,
        schema_registry_client: SchemaRegistryClient,
        schema_str: str | Schema | None = None,
        from_dict: Callable[[dict[str, object], SerializationContext], T] | None = None,
        return_record_name: bool = False,
        conf: Mapping[str, object] | None = None,
        rule_conf: dict[str, str] | None = None,
        rule_registry: RuleRegistry | None = None,
    ) -> None: ...
    # returns `None` for `None`
    def __call__(self, data: bytes | None, ctx: SerializationContext | None = None) -> T | None: ...

class AvroDeserializerBuilder[T = object](DeserializerBuilder):
    # `schema_registry_config` has to be a `dict`: it is what `SchemaRegistryClient` takes
    def __init__[C](
        self,
        schema_registry_config: dict[str, C] | None = None,
        schema_registry_client: SchemaRegistryClient | None = None,
        schema: str | Schema | None = None,
        from_dict: Callable[[dict[str, object], SerializationContext], T] | None = None,
        return_record_name: bool = False,
        deserializer_config: Mapping[str, object] | None = None,
        rule_config: dict[str, str] | None = None,
        rule_registry: RuleRegistry | None = None,
        deserializer_init: Callable[[AvroDeserializer[T]], None] | None = None,
    ) -> None: ...
    def set_schema_registry_config[C](self, schema_registry_conf: dict[str, C]) -> Self: ...
    def set_schema_registry_client(self, schema_registry_client: SchemaRegistryClient) -> Self: ...
    def set_schema(self, schema_str: str | Schema) -> Self: ...
    def set_from_dict(self, from_dict: Callable[[dict[str, object], SerializationContext], T]) -> Self: ...
    def set_return_record_name(self, return_record_name: bool) -> Self: ...
    def set_deserializer_config(self, deserializer_conf: Mapping[str, object]) -> Self: ...
    def set_rule_config(self, rule_conf: dict[str, str]) -> Self: ...
    def set_rule_registry(self, rule_registry: RuleRegistry) -> Self: ...
    def set_deserializer_init(self, deserializer_init: Callable[[AvroDeserializer[T]], None]) -> Self: ...
    # returns the deserializer and the client configuration left for the other builders
    def build(self, conf: dict[str, object], is_key: bool) -> tuple[AvroDeserializer[T], dict[str, object]]: ...
