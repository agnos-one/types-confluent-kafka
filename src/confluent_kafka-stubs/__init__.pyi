from confluent_kafka._model import (
    AcknowledgeType as AcknowledgeType,
    ConsumerGroupState as ConsumerGroupState,
    ConsumerGroupTopicPartitions as ConsumerGroupTopicPartitions,
    ConsumerGroupType as ConsumerGroupType,
    ElectionType as ElectionType,
    IsolationLevel as IsolationLevel,
    Messages as Messages,
    Node as Node,
    TopicCollection as TopicCollection,
    TopicPartitionInfo as TopicPartitionInfo,
)
from confluent_kafka.cimpl import (
    OFFSET_BEGINNING as OFFSET_BEGINNING,
    OFFSET_END as OFFSET_END,
    OFFSET_INVALID as OFFSET_INVALID,
    OFFSET_STORED as OFFSET_STORED,
    TIMESTAMP_CREATE_TIME as TIMESTAMP_CREATE_TIME,
    TIMESTAMP_LOG_APPEND_TIME as TIMESTAMP_LOG_APPEND_TIME,
    TIMESTAMP_NOT_AVAILABLE as TIMESTAMP_NOT_AVAILABLE,
    ConcurrentModificationException as ConcurrentModificationException,
    Consumer as Consumer,
    IllegalStateException as IllegalStateException,
    KafkaError as KafkaError,
    KafkaException as KafkaException,
    Message as Message,
    Producer as Producer,
    ShareConsumer as ShareConsumer,
    TopicPartition as TopicPartition,
    Uuid as Uuid,
    consistent as consistent,
    fnv1a as fnv1a,
    libversion as libversion,
    murmur2 as murmur2,
    version as version,
)
from confluent_kafka.deserializing_consumer import DeserializingConsumer as DeserializingConsumer
from confluent_kafka.deserializing_share_consumer import DeserializingShareConsumer as DeserializingShareConsumer
from confluent_kafka.serializing_producer import SerializingProducer as SerializingProducer

__version__: str

# what a `throttle_cb` configured on a client is called with
class ThrottleEvent:
    broker_name: str
    broker_id: int
    throttle_time: float  # seconds
    def __init__(self, broker_name: str, broker_id: int, throttle_time: float) -> None: ...
