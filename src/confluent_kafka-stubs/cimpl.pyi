from builtins import str as _str
from collections.abc import Callable
from contextvars import ContextVar
from types import TracebackType
from typing import ClassVar, Literal, Self, TypedDict, overload

from confluent_kafka._model import Messages
from confluent_kafka.admin._metadata import ClusterMetadata, GroupMetadata
from typing_extensions import disjoint_base

# `cimpl` is a C extension, so stubtest only checks that these names exist: the signatures come from the argument
# parsing in the library's C sources (`src/confluent_kafka/src/*.c`). Methods the C code declares without keyword
# support are positional-only here. The classes are `@disjoint_base` (PEP 800): each has its own C layout, so a class
# can't inherit from two of them.

# Headers to send: the C code accepts a `dict` or a `list` of pairs, and nothing else (a tuple of pairs raises).
type _Headers = dict[str, str | bytes | None] | list[tuple[str, str | bytes | None]]
# The callbacks' return values are ignored.
type _DeliveryCallback = Callable[[KafkaError | None, Message], object]
type _RebalanceCallback = Callable[[Consumer, list[TopicPartition]], object]
type _AcknowledgementCommitCallback = Callable[[dict[TopicPartition, set[int]], KafkaException | None], object]
# a message whatever its key and value types
type _AnyMessage = Message[object, object]

# Internal: the identity `AIOConsumer` presents to the consumer's reentrancy check.
_reentry_identity_var: ContextVar[int]

@disjoint_base
class KafkaError:
    BROKER_NOT_AVAILABLE: int
    CLUSTER_AUTHORIZATION_FAILED: int
    CONCURRENT_TRANSACTIONS: int
    COORDINATOR_LOAD_IN_PROGRESS: int
    COORDINATOR_NOT_AVAILABLE: int
    DELEGATION_TOKEN_AUTHORIZATION_FAILED: int
    DELEGATION_TOKEN_AUTH_DISABLED: int
    DELEGATION_TOKEN_EXPIRED: int
    DELEGATION_TOKEN_NOT_FOUND: int
    DELEGATION_TOKEN_OWNER_MISMATCH: int
    DELEGATION_TOKEN_REQUEST_NOT_ALLOWED: int
    DUPLICATE_RESOURCE: int
    DUPLICATE_SEQUENCE_NUMBER: int
    DUPLICATE_VOTER: int
    ELECTION_NOT_NEEDED: int
    ELIGIBLE_LEADERS_NOT_AVAILABLE: int
    FEATURE_UPDATE_FAILED: int
    FENCED_INSTANCE_ID: int
    FENCED_LEADER_EPOCH: int
    FENCED_MEMBER_EPOCH: int
    FENCED_STATE_EPOCH: int
    FETCH_SESSION_ID_NOT_FOUND: int
    GROUP_AUTHORIZATION_FAILED: int
    GROUP_ID_NOT_FOUND: int
    GROUP_MAX_SIZE_REACHED: int
    GROUP_SUBSCRIBED_TO_TOPIC: int
    ILLEGAL_GENERATION: int
    ILLEGAL_SASL_STATE: int
    INCONSISTENT_GROUP_PROTOCOL: int
    INCONSISTENT_TOPIC_ID: int
    INCONSISTENT_VOTER_SET: int
    INVALID_COMMIT_OFFSET_SIZE: int
    INVALID_CONFIG: int
    INVALID_FETCH_SESSION_EPOCH: int
    INVALID_GROUP_ID: int
    INVALID_MSG: int
    INVALID_MSG_SIZE: int
    INVALID_PARTITIONS: int
    INVALID_PRINCIPAL_TYPE: int
    INVALID_PRODUCER_EPOCH: int
    INVALID_PRODUCER_ID_MAPPING: int
    INVALID_RECORD: int
    INVALID_RECORD_STATE: int
    INVALID_REGISTRATION: int
    INVALID_REGULAR_EXPRESSION: int
    INVALID_REPLICATION_FACTOR: int
    INVALID_REPLICA_ASSIGNMENT: int
    INVALID_REQUEST: int
    INVALID_REQUIRED_ACKS: int
    INVALID_SESSION_TIMEOUT: int
    INVALID_SHARE_SESSION_EPOCH: int
    INVALID_TIMESTAMP: int
    INVALID_TRANSACTION_TIMEOUT: int
    INVALID_TXN_STATE: int
    INVALID_UPDATE_VERSION: int
    INVALID_VOTER_KEY: int
    KAFKA_STORAGE_ERROR: int
    LEADER_NOT_AVAILABLE: int
    LISTENER_NOT_FOUND: int
    LOG_DIR_NOT_FOUND: int
    MEMBER_ID_REQUIRED: int
    MISMATCHED_ENDPOINT_TYPE: int
    MSG_SIZE_TOO_LARGE: int
    NETWORK_EXCEPTION: int
    NON_EMPTY_GROUP: int
    NOT_CONTROLLER: int
    NOT_COORDINATOR: int
    NOT_ENOUGH_REPLICAS: int
    NOT_ENOUGH_REPLICAS_AFTER_APPEND: int
    NOT_LEADER_FOR_PARTITION: int
    NO_ERROR: int
    NO_REASSIGNMENT_IN_PROGRESS: int
    OFFSET_METADATA_TOO_LARGE: int
    OFFSET_NOT_AVAILABLE: int
    OFFSET_OUT_OF_RANGE: int
    OPERATION_NOT_ATTEMPTED: int
    OUT_OF_ORDER_SEQUENCE_NUMBER: int
    POLICY_VIOLATION: int
    PREFERRED_LEADER_NOT_AVAILABLE: int
    PRINCIPAL_DESERIALIZATION_FAILURE: int
    PRODUCER_FENCED: int
    REASSIGNMENT_IN_PROGRESS: int
    REBALANCE_IN_PROGRESS: int
    REBOOTSTRAP_REQUIRED: int
    RECORD_LIST_TOO_LARGE: int
    REPLICA_NOT_AVAILABLE: int
    REQUEST_TIMED_OUT: int
    RESOURCE_NOT_FOUND: int
    SASL_AUTHENTICATION_FAILED: int
    SECURITY_DISABLED: int
    SHARE_SESSION_LIMIT_REACHED: int
    SHARE_SESSION_NOT_FOUND: int
    STALE_BROKER_EPOCH: int
    STALE_CTRL_EPOCH: int
    STALE_MEMBER_EPOCH: int
    STREAMS_INVALID_TOPOLOGY: int
    STREAMS_INVALID_TOPOLOGY_EPOCH: int
    STREAMS_TOPOLOGY_FENCED: int
    TELEMETRY_TOO_LARGE: int
    THROTTLING_QUOTA_EXCEEDED: int
    TOPIC_ALREADY_EXISTS: int
    TOPIC_AUTHORIZATION_FAILED: int
    TOPIC_DELETION_DISABLED: int
    TOPIC_EXCEPTION: int
    TRANSACTIONAL_ID_AUTHORIZATION_FAILED: int
    TRANSACTION_ABORTABLE: int
    TRANSACTION_COORDINATOR_FENCED: int
    UNACCEPTABLE_CREDENTIAL: int
    UNKNOWN: int
    UNKNOWN_CONTROLLER_ID: int
    UNKNOWN_LEADER_EPOCH: int
    UNKNOWN_MEMBER_ID: int
    UNKNOWN_PRODUCER_ID: int
    UNKNOWN_SUBSCRIPTION_ID: int
    UNKNOWN_TOPIC_ID: int
    UNKNOWN_TOPIC_OR_PART: int
    UNRELEASED_INSTANCE_ID: int
    UNSTABLE_OFFSET_COMMIT: int
    UNSUPPORTED_ASSIGNOR: int
    UNSUPPORTED_COMPRESSION_TYPE: int
    UNSUPPORTED_ENDPOINT_TYPE: int
    UNSUPPORTED_FOR_MESSAGE_FORMAT: int
    UNSUPPORTED_SASL_MECHANISM: int
    UNSUPPORTED_VERSION: int
    VOTER_NOT_FOUND: int
    _ALL_BROKERS_DOWN: int
    _APPLICATION: int
    _ASSIGNMENT_LOST: int
    _ASSIGN_PARTITIONS: int
    _AUTHENTICATION: int
    _AUTO_OFFSET_RESET: int
    _BAD_COMPRESSION: int
    _BAD_MSG: int
    _CONFLICT: int
    _CRIT_SYS_RESOURCE: int
    _DESTROY: int
    _DESTROY_BROKER: int
    _EXISTING_SUBSCRIPTION: int
    _FAIL: int
    _FATAL: int
    _FENCED: int
    _FS: int
    _GAPLESS_GUARANTEE: int
    _INCONSISTENT: int
    _INTR: int
    _INVALID_ARG: int
    _INVALID_DIFFERENT_RECORD: int
    _INVALID_TYPE: int
    _IN_PROGRESS: int
    _ISR_INSUFF: int
    _KEY_DESERIALIZATION: int
    _KEY_SERIALIZATION: int
    _LOG_TRUNCATION: int
    _MAX_POLL_EXCEEDED: int
    _MSG_TIMED_OUT: int
    _NODE_UPDATE: int
    _NOENT: int
    _NOOP: int
    _NOT_CONFIGURED: int
    _NOT_IMPLEMENTED: int
    _NO_OFFSET: int
    _OUTDATED: int
    _PARTIAL: int
    _PARTITION_EOF: int
    _PREV_IN_PROGRESS: int
    _PURGE_INFLIGHT: int
    _PURGE_QUEUE: int
    _QUEUE_FULL: int
    _READ_ONLY: int
    _RESOLVE: int
    _RETRY: int
    _REVOKE_PARTITIONS: int
    _SSL: int
    _STATE: int
    _TIMED_OUT: int
    _TIMED_OUT_QUEUE: int
    _TRANSPORT: int
    _UNDERFLOW: int
    _UNKNOWN_BROKER: int
    _UNKNOWN_GROUP: int
    _UNKNOWN_PARTITION: int
    _UNKNOWN_PROTOCOL: int
    _UNKNOWN_TOPIC: int
    _UNSUPPORTED_FEATURE: int
    _VALUE_DESERIALIZATION: int
    _VALUE_SERIALIZATION: int
    _WAIT_CACHE: int
    _WAIT_COORD: int
    # the first parameter is named `error`, though it is the error code
    def __init__(
        self,
        error: int,
        reason: _str | None = None,
        fatal: bool = False,
        retriable: bool = False,
        txn_requires_abort: bool = False,
    ) -> None: ...
    def code(self) -> int: ...
    def name(self) -> _str: ...
    def str(self) -> _str: ...
    def fatal(self) -> bool: ...
    def retriable(self) -> bool: ...
    def txn_requires_abort(self) -> bool: ...
    def __hash__(self) -> int: ...
    # compares the error code with another `KafkaError` or an `int`; anything else raises
    def __eq__(self, other: object, /) -> bool: ...
    def __ne__(self, other: object, /) -> bool: ...
    def __lt__(self, other: KafkaError | int, /) -> bool: ...
    def __le__(self, other: KafkaError | int, /) -> bool: ...
    def __gt__(self, other: KafkaError | int, /) -> bool: ...
    def __ge__(self, other: KafkaError | int, /) -> bool: ...

# `args[0]` is the `KafkaError` when the library raises it
class KafkaException(Exception): ...
class IllegalStateException(RuntimeError): ...
class ConcurrentModificationException(RuntimeError): ...

# Stub-only type parameters (the runtime class can't be subscripted, so use them in annotations only): the types
# `key()` and `value()` return. They are `bytes` for a `Consumer`'s messages and for delivery reports, and the
# deserializers' output types for a `DeserializingConsumer`'s.
@disjoint_base
class Message[K = bytes, V = bytes]:
    def __init__(
        self,
        topic: str | None = None,
        partition: int = -1,
        offset: int = -1001,
        key: K | None = None,
        value: V | None = None,
        headers: _Headers | None = None,
        error: KafkaError | None = None,
        timestamp: tuple[int, int] | None = None,  # (timestamp type, milliseconds)
        latency: float = -1,
        leader_epoch: int = -1,
        delivery_count: int = -1,
    ) -> None: ...
    def topic(self) -> str | None: ...
    def partition(self) -> int | None: ...
    def offset(self) -> int | None: ...
    def key(self) -> K | None: ...
    def value(self) -> V | None: ...
    # the same as `key()` and `value()`
    def deserialized_key(self) -> K | None: ...
    def deserialized_value(self) -> V | None: ...
    # A received message has a list of `(str, bytes | None)` pairs, or `None` without headers. The values are typed
    # `str | bytes | None` so that the list can be passed on as the headers of a `SerializationContext` or of
    # `produce()`. (A message built by hand returns whatever headers it was given.)
    def headers(self) -> list[tuple[str, str | bytes | None]] | None: ...
    def error(self) -> KafkaError | None: ...
    # (timestamp type, milliseconds): the type is one of the `TIMESTAMP_*` constants
    def timestamp(self) -> tuple[int, int]: ...
    def latency(self) -> float | None: ...
    def leader_epoch(self) -> int | None: ...
    def delivery_count(self) -> int | None: ...
    # The setters store any object: `DeserializingConsumer` uses them to replace the raw key and value.
    def set_headers(self, headers: _Headers | None, /) -> None: ...
    def set_key(self, key: object, /) -> None: ...
    def set_value(self, value: object, /) -> None: ...
    def set_topic(self, topic: str | None, /) -> None: ...
    def set_error(self, error: KafkaError | None, /) -> None: ...
    def __len__(self) -> int: ...
    # Messages compare by content (every field but the latency), so they can't be hashed: no `set` of messages, and
    # no message as a `dict` key.
    def __eq__(self, other: object, /) -> bool: ...
    def __ne__(self, other: object, /) -> bool: ...
    __hash__: ClassVar[None]  # type: ignore[assignment]

@disjoint_base
class TopicPartition:
    # `metadata` and `leader_epoch` can be left out, but not passed as `None`
    def __init__(
        self,
        topic: str,
        partition: int = -1,
        offset: int = -1001,
        metadata: str = ...,
        leader_epoch: int = ...,
    ) -> None: ...
    @property
    def topic(self) -> str: ...
    partition: int
    offset: int
    @property
    def metadata(self) -> str | None: ...
    @property
    def leader_epoch(self) -> int | None: ...
    @property
    def error(self) -> KafkaError | None: ...
    def __hash__(self) -> int: ...
    # comparing with anything but a `TopicPartition` raises `NotImplementedError`
    def __eq__(self, other: object, /) -> bool: ...
    def __ne__(self, other: object, /) -> bool: ...
    def __lt__(self, other: TopicPartition, /) -> bool: ...
    def __le__(self, other: TopicPartition, /) -> bool: ...
    def __gt__(self, other: TopicPartition, /) -> bool: ...
    def __ge__(self, other: TopicPartition, /) -> bool: ...

@disjoint_base
class Uuid:
    def __init__(self, most_significant_bits: int, least_significant_bits: int) -> None: ...
    def get_most_significant_bits(self) -> int: ...
    def get_least_significant_bits(self) -> int: ...
    def __hash__(self) -> int: ...

# Stub-only: one message of `Producer.produce_batch()`. The method needs a `list` of real `dict`s, and adds `_error`
# to those it fails to queue. Annotate a list built in a variable as `list[BatchMessage]`. `timestamp` raises
# `NotImplementedError` and `headers` are not sent, so neither is a key here.
class BatchMessage(TypedDict, total=False):
    value: str | bytes | None
    key: str | bytes | None
    partition: int | None
    callback: _DeliveryCallback | None
    _error: KafkaError

@disjoint_base
class Producer:
    # The configuration is a `dict` (any other mapping raises `TypeError`), keyword arguments, or both; the keyword
    # arguments win. Property names are passed as they are, so only names without a dot (`logger`, `error_cb`, ...)
    # can be keywords. Values are converted with `str()`, except the callbacks and the logger.
    def __init__[T](self, config: dict[str, T] = ..., /, **kwargs: object) -> None: ...
    def produce(
        self,
        topic: str,
        value: str | bytes | None = None,
        key: str | bytes | None = None,
        partition: int = -1,
        callback: _DeliveryCallback | None = None,
        on_delivery: _DeliveryCallback | None = None,  # alias of `callback`
        timestamp: int = 0,
        headers: _Headers | None = None,
    ) -> None: ...
    def produce_batch(
        self,
        topic: str,
        messages: list[BatchMessage],
        partition: int = -1,
        callback: _DeliveryCallback | None = None,
        on_delivery: _DeliveryCallback | None = None,  # alias of `callback`
    ) -> int: ...
    def poll(self, timeout: float = -1) -> int: ...
    def flush(self, timeout: float = -1) -> int: ...
    def purge(self, in_queue: bool = True, in_flight: bool = True, blocking: bool = True) -> None: ...
    def close(self) -> bool: ...
    def list_topics(self, topic: str | None = None, timeout: float = -1) -> ClusterMetadata: ...
    def cluster_id(self, timeout: float = -1) -> str: ...
    def init_transactions(self, timeout: float = -1, /) -> None: ...
    def begin_transaction(self) -> None: ...
    # `group_metadata` is what `Consumer.consumer_group_metadata()` returns
    def send_offsets_to_transaction(
        self, positions: list[TopicPartition], group_metadata: bytes, timeout: float = -1, /
    ) -> None: ...
    def commit_transaction(self, timeout: float = -1, /) -> None: ...
    def abort_transaction(self, timeout: float = -1, /) -> None: ...
    def set_sasl_credentials(self, username: str, password: str) -> None: ...
    def __len__(self) -> int: ...
    def __bool__(self) -> bool: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
        /,
    ) -> None: ...

@disjoint_base
class Consumer:
    # see `Producer.__init__`; `group.id` is required
    def __init__[T](self, config: dict[str, T] = ..., /, **kwargs: object) -> None: ...
    # `topics` has to be a `list`. The callbacks can be left out, but not passed as `None`.
    def subscribe(
        self,
        topics: list[str],
        on_assign: _RebalanceCallback = ...,
        on_revoke: _RebalanceCallback = ...,
        on_lost: _RebalanceCallback = ...,
    ) -> None: ...
    def unsubscribe(self) -> None: ...
    def poll(self, timeout: float = -1) -> Message | None: ...
    def consume(self, num_messages: int = 1, timeout: float = -1) -> list[Message]: ...
    def assign(self, partitions: list[TopicPartition], /) -> None: ...
    def unassign(self) -> None: ...
    def incremental_assign(self, partitions: list[TopicPartition], /) -> None: ...
    def incremental_unassign(self, partitions: list[TopicPartition], /) -> None: ...
    def assignment(self) -> list[TopicPartition]: ...
    # A synchronous commit returns the committed offsets. `message` and `offsets` are mutually exclusive. At runtime
    # the third positional parameter is `async`, an alias of `asynchronous` that can't be written as a keyword, so
    # `asynchronous` is keyword-only here.
    @overload
    def commit(
        self,
        message: _AnyMessage | None = None,
        offsets: list[TopicPartition] | None = None,
        *,
        asynchronous: Literal[True] = True,
    ) -> None: ...
    @overload
    def commit(
        self,
        message: _AnyMessage | None = None,
        offsets: list[TopicPartition] | None = None,
        *,
        asynchronous: Literal[False],
    ) -> list[TopicPartition]: ...
    @overload
    def commit(
        self,
        message: _AnyMessage | None = None,
        offsets: list[TopicPartition] | None = None,
        *,
        asynchronous: bool,
    ) -> list[TopicPartition] | None: ...
    # one of `message` and `offsets` is required
    def store_offsets(
        self, message: _AnyMessage | None = None, offsets: list[TopicPartition] | None = None
    ) -> None: ...
    def committed(self, partitions: list[TopicPartition], timeout: float = -1) -> list[TopicPartition]: ...
    def position(self, partitions: list[TopicPartition]) -> list[TopicPartition]: ...
    def pause(self, partitions: list[TopicPartition]) -> None: ...
    def resume(self, partitions: list[TopicPartition]) -> None: ...
    def seek(self, partition: TopicPartition) -> None: ...
    # (low, high)
    def get_watermark_offsets(
        self, partition: TopicPartition, timeout: float = -1, cached: bool = False
    ) -> tuple[int, int]: ...
    def offsets_for_times(self, partitions: list[TopicPartition], timeout: float = -1) -> list[TopicPartition]: ...
    def memberid(self) -> str | None: ...
    def close(self) -> None: ...
    def list_topics(self, topic: str | None = None, timeout: float = -1) -> ClusterMetadata: ...
    def cluster_id(self, timeout: float = -1) -> str: ...
    # opaque: only to be passed to `Producer.send_offsets_to_transaction()`
    def consumer_group_metadata(self) -> bytes: ...
    def set_sasl_credentials(self, username: str, password: str) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
        /,
    ) -> None: ...

@disjoint_base
class ShareConsumer:
    # see `Producer.__init__`; `group.id` is required
    def __init__[T](self, config: dict[str, T] = ..., /, **kwargs: object) -> None: ...
    def subscribe(self, topics: list[str]) -> None: ...
    def unsubscribe(self) -> None: ...
    def subscription(self) -> list[str]: ...
    def poll(self, timeout: float = -1) -> Messages: ...
    # `ack_type` is an `AcknowledgeType` (an `IntEnum`); the default is `AcknowledgeType.ACCEPT`
    def acknowledge(self, message: _AnyMessage, ack_type: int = ...) -> None: ...
    def acknowledge_offset(self, topic: str, partition: int, offset: int, ack_type: int = ...) -> None: ...
    def commit_sync(self, timeout: float = 60) -> dict[TopicPartition, KafkaException | None]: ...
    def commit_async(self) -> None: ...
    def set_acknowledgement_commit_callback(self, callback: _AcknowledgementCommitCallback | None) -> None: ...
    def set_sasl_credentials(self, username: str, password: str) -> None: ...
    def close(self) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
        /,
    ) -> None: ...

# The base class of `AdminClient`, which overrides most of its methods: only those `AdminClient` passes on are
# stubbed.
@disjoint_base
class _AdminClientImpl:
    def __init__[T](self, config: dict[str, T] = ..., /, **kwargs: object) -> None: ...
    def list_topics(self, topic: str | None = None, timeout: float = -1) -> ClusterMetadata: ...
    def list_groups(self, group: str | None = None, timeout: float = -1) -> list[GroupMetadata]: ...
    def cluster_id(self, timeout: float = -1) -> str: ...
    # unlike the other clients' `poll`, `timeout` is required
    def poll(self, timeout: float) -> int: ...
    def set_sasl_credentials(self, username: str, password: str) -> None: ...
    def __len__(self) -> int: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
        /,
    ) -> None: ...

@disjoint_base
class NewTopic:
    # `config` can be left out, but not passed as `None`
    def __init__(
        self,
        topic: str,
        num_partitions: int = -1,
        replication_factor: int = -1,
        replica_assignment: list[list[int]] | None = None,
        config: dict[str, str] = ...,
    ) -> None: ...
    @property
    def topic(self) -> str: ...
    num_partitions: int
    replication_factor: int
    replica_assignment: list[list[int]] | None
    config: dict[str, str] | None
    def __hash__(self) -> int: ...
    # comparing with anything but a `NewTopic` raises `NotImplementedError`
    def __eq__(self, other: object, /) -> bool: ...
    def __ne__(self, other: object, /) -> bool: ...
    def __lt__(self, other: NewTopic, /) -> bool: ...
    def __le__(self, other: NewTopic, /) -> bool: ...
    def __gt__(self, other: NewTopic, /) -> bool: ...
    def __ge__(self, other: NewTopic, /) -> bool: ...

@disjoint_base
class NewPartitions:
    def __init__(self, topic: str, new_total_count: int, replica_assignment: list[list[int]] | None = None) -> None: ...
    @property
    def topic(self) -> str: ...
    new_total_count: int
    replica_assignment: list[list[int]] | None
    def __hash__(self) -> int: ...
    # comparing with anything but a `NewPartitions` raises `NotImplementedError`
    def __eq__(self, other: object, /) -> bool: ...
    def __ne__(self, other: object, /) -> bool: ...
    def __lt__(self, other: NewPartitions, /) -> bool: ...
    def __le__(self, other: NewPartitions, /) -> bool: ...
    def __gt__(self, other: NewPartitions, /) -> bool: ...
    def __ge__(self, other: NewPartitions, /) -> bool: ...

# (version string, version as an int)
def libversion() -> tuple[str, int]: ...

# the version of confluent-kafka-python, such as "2.16.2"
def version() -> str: ...

# the partitioners librdkafka uses: the partition for `key` out of `partition_count`
def murmur2(key: bytes, partition_count: int, /) -> int: ...
def consistent(key: bytes, partition_count: int, /) -> int: ...
def fnv1a(key: bytes, partition_count: int, /) -> int: ...

ACL_OPERATION_ALL: int
ACL_OPERATION_ALTER: int
ACL_OPERATION_ALTER_CONFIGS: int
ACL_OPERATION_ANY: int
ACL_OPERATION_CLUSTER_ACTION: int
ACL_OPERATION_CREATE: int
ACL_OPERATION_DELETE: int
ACL_OPERATION_DESCRIBE: int
ACL_OPERATION_DESCRIBE_CONFIGS: int
ACL_OPERATION_IDEMPOTENT_WRITE: int
ACL_OPERATION_READ: int
ACL_OPERATION_UNKNOWN: int
ACL_OPERATION_WRITE: int
ACL_PERMISSION_TYPE_ALLOW: int
ACL_PERMISSION_TYPE_ANY: int
ACL_PERMISSION_TYPE_DENY: int
ACL_PERMISSION_TYPE_UNKNOWN: int
ALTER_CONFIG_OP_TYPE_APPEND: int
ALTER_CONFIG_OP_TYPE_DELETE: int
ALTER_CONFIG_OP_TYPE_SET: int
ALTER_CONFIG_OP_TYPE_SUBTRACT: int
CONFIG_SOURCE_DEFAULT_CONFIG: int
CONFIG_SOURCE_DYNAMIC_BROKER_CONFIG: int
CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER_CONFIG: int
CONFIG_SOURCE_DYNAMIC_TOPIC_CONFIG: int
CONFIG_SOURCE_GROUP_CONFIG: int
CONFIG_SOURCE_STATIC_BROKER_CONFIG: int
CONFIG_SOURCE_UNKNOWN_CONFIG: int
CONSUMER_GROUP_STATE_COMPLETING_REBALANCE: int
CONSUMER_GROUP_STATE_DEAD: int
CONSUMER_GROUP_STATE_EMPTY: int
CONSUMER_GROUP_STATE_PREPARING_REBALANCE: int
CONSUMER_GROUP_STATE_STABLE: int
CONSUMER_GROUP_STATE_UNKNOWN: int
CONSUMER_GROUP_TYPE_CLASSIC: int
CONSUMER_GROUP_TYPE_CONSUMER: int
CONSUMER_GROUP_TYPE_UNKNOWN: int
ELECTION_TYPE_PREFERRED: int
ELECTION_TYPE_UNCLEAN: int
ISOLATION_LEVEL_READ_COMMITTED: int
ISOLATION_LEVEL_READ_UNCOMMITTED: int
OFFSET_BEGINNING: int
OFFSET_END: int
OFFSET_INVALID: int
OFFSET_SPEC_EARLIEST: int
OFFSET_SPEC_LATEST: int
OFFSET_SPEC_MAX_TIMESTAMP: int
OFFSET_STORED: int
RESOURCE_ANY: int
RESOURCE_BROKER: int
RESOURCE_GROUP: int
RESOURCE_PATTERN_ANY: int
RESOURCE_PATTERN_LITERAL: int
RESOURCE_PATTERN_MATCH: int
RESOURCE_PATTERN_PREFIXED: int
RESOURCE_PATTERN_UNKNOWN: int
RESOURCE_TOPIC: int
RESOURCE_TRANSACTIONAL_ID: int
RESOURCE_UNKNOWN: int
SCRAM_MECHANISM_SHA_256: int
SCRAM_MECHANISM_SHA_512: int
SCRAM_MECHANISM_UNKNOWN: int
SHARE_ACKNOWLEDGE_TYPE_ACCEPT: int
SHARE_ACKNOWLEDGE_TYPE_REJECT: int
SHARE_ACKNOWLEDGE_TYPE_RELEASE: int
TIMESTAMP_CREATE_TIME: int
TIMESTAMP_LOG_APPEND_TIME: int
TIMESTAMP_NOT_AVAILABLE: int
