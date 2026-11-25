# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-11-26

First release, under the MIT License, published to the package index at `https://pkgs.agnos.one/simple/`. Checked
with `stubtest` against `confluent-kafka` 2.16.2.

### Added

- Partial PEP 561 stubs for `confluent_kafka`, which replace the modules whose own types are wrong or incomplete;
  the other modules keep the library's types.
- `confluent_kafka` and `confluent_kafka.cimpl`: `Producer`, `Consumer`, `ShareConsumer`, `Message`,
  `TopicPartition`, `KafkaError`, `KafkaException`, `NewTopic`, `NewPartitions`, `Uuid`, the constants and the
  partitioner functions, with the signatures of the C extension. Configuration is a `dict` of any value type, with
  no `Any`.
- `Message[K, V]`: `key()` and `value()` return the deserialized types for a `DeserializingConsumer`, and `bytes`
  otherwise.
- `SerializingProducer[K, V]` and `DeserializingConsumer[K, V]`.
- `AdminClient`: every method, with the result type of each future and the keyword arguments each method accepts;
  `NewTopic` and `NewPartitions` are exported from `confluent_kafka.admin`.
- `SchemaRegistryClient`, usable as a typed context manager.
- `AvroSerializer`, `AvroDeserializer`, `JSONSerializer`, `JSONDeserializer`, `ProtobufSerializer`,
  `ProtobufDeserializer` and their builders, with a working constructor
  ([confluent-kafka-python#2390](https://github.com/confluentinc/confluent-kafka-python/issues/2390)) and a type
  parameter for what `to_dict` accepts, `from_dict` returns, or the Protobuf message class.
- `BatchMessage`, a stub-only type for the messages of `Producer.produce_batch()`.

[Unreleased]: https://github.com/agnos-one/types-confluent-kafka/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/agnos-one/types-confluent-kafka/releases/tag/v0.1.0
