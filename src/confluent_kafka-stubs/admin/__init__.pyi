from concurrent.futures import Future
from typing import TypedDict, Unpack, overload
from warnings import deprecated

from confluent_kafka._model import (
    ConsumerGroupState,
    ConsumerGroupTopicPartitions,
    ConsumerGroupType,
    ElectionType,
    IsolationLevel,
    TopicCollection,
)
from confluent_kafka.admin._acl import (
    AclBinding as AclBinding,
    AclBindingFilter as AclBindingFilter,
    AclOperation as AclOperation,
    AclPermissionType as AclPermissionType,
)
from confluent_kafka.admin._cluster import DescribeClusterResult as DescribeClusterResult
from confluent_kafka.admin._config import (
    AlterConfigOpType as AlterConfigOpType,
    ConfigEntry as ConfigEntry,
    ConfigResource as ConfigResource,
    ConfigSource as ConfigSource,
)
from confluent_kafka.admin._group import (
    ConsumerGroupDescription as ConsumerGroupDescription,
    ConsumerGroupListing as ConsumerGroupListing,
    ListConsumerGroupsResult as ListConsumerGroupsResult,
    MemberAssignment as MemberAssignment,
    MemberDescription as MemberDescription,
)
from confluent_kafka.admin._listoffsets import (
    ListOffsetsResultInfo as ListOffsetsResultInfo,
    OffsetSpec as OffsetSpec,
)
from confluent_kafka.admin._metadata import (
    BrokerMetadata as BrokerMetadata,
    ClusterMetadata as ClusterMetadata,
    GroupMember as GroupMember,
    GroupMetadata as GroupMetadata,
    PartitionMetadata as PartitionMetadata,
    TopicMetadata as TopicMetadata,
)
from confluent_kafka.admin._records import DeletedRecords as DeletedRecords
from confluent_kafka.admin._resource import ResourcePatternType as ResourcePatternType, ResourceType as ResourceType
from confluent_kafka.admin._scram import (
    ScramCredentialInfo as ScramCredentialInfo,
    ScramMechanism as ScramMechanism,
    UserScramCredentialAlteration as UserScramCredentialAlteration,
    UserScramCredentialDeletion as UserScramCredentialDeletion,
    UserScramCredentialsDescription as UserScramCredentialsDescription,
    UserScramCredentialUpsertion as UserScramCredentialUpsertion,
)
from confluent_kafka.admin._topic import TopicDescription as TopicDescription
from confluent_kafka.cimpl import (
    CONFIG_SOURCE_DEFAULT_CONFIG as CONFIG_SOURCE_DEFAULT_CONFIG,
    CONFIG_SOURCE_DYNAMIC_BROKER_CONFIG as CONFIG_SOURCE_DYNAMIC_BROKER_CONFIG,
    CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER_CONFIG as CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER_CONFIG,
    CONFIG_SOURCE_DYNAMIC_TOPIC_CONFIG as CONFIG_SOURCE_DYNAMIC_TOPIC_CONFIG,
    CONFIG_SOURCE_GROUP_CONFIG as CONFIG_SOURCE_GROUP_CONFIG,
    CONFIG_SOURCE_STATIC_BROKER_CONFIG as CONFIG_SOURCE_STATIC_BROKER_CONFIG,
    CONFIG_SOURCE_UNKNOWN_CONFIG as CONFIG_SOURCE_UNKNOWN_CONFIG,
    RESOURCE_ANY as RESOURCE_ANY,
    RESOURCE_BROKER as RESOURCE_BROKER,
    RESOURCE_GROUP as RESOURCE_GROUP,
    RESOURCE_TOPIC as RESOURCE_TOPIC,
    RESOURCE_TRANSACTIONAL_ID as RESOURCE_TRANSACTIONAL_ID,
    RESOURCE_UNKNOWN as RESOURCE_UNKNOWN,
    KafkaException,
    NewPartitions as NewPartitions,
    NewTopic as NewTopic,
    TopicPartition,
    _AdminClientImpl,  # pyright: ignore[reportPrivateUsage]
)

# The keyword arguments each method passes on to the C extension. Timeouts are in seconds.
class _RequestKwargs(TypedDict, total=False):
    request_timeout: float

class _OperationKwargs(TypedDict, total=False):
    request_timeout: float
    operation_timeout: float

class _CreateKwargs(TypedDict, total=False):
    validate_only: bool
    request_timeout: float
    operation_timeout: float

class _DescribeConfigsKwargs(TypedDict, total=False):
    request_timeout: float
    broker: int

class _AlterConfigsKwargs(TypedDict, total=False):
    validate_only: bool
    request_timeout: float
    broker: int

class _ListConsumerGroupsKwargs(TypedDict, total=False):
    request_timeout: float
    # each has to be a `set`
    states: set[ConsumerGroupState] | None
    types: set[ConsumerGroupType] | None

class _DescribeKwargs(TypedDict, total=False):
    request_timeout: float
    include_authorized_operations: bool

class _ListConsumerGroupOffsetsKwargs(TypedDict, total=False):
    require_stable: bool
    request_timeout: float

class _ListOffsetsKwargs(TypedDict, total=False):
    isolation_level: IsolationLevel
    request_timeout: float

# Each method returns at once, with one future for the request or one per item. `result()` returns the type given
# here, or raises the `KafkaException` of a failed request. The lists passed in have to be `list`s.
class AdminClient(_AdminClientImpl):
    # see `Producer.__init__`
    def __init__[T](self, conf: dict[str, T], **kwargs: object) -> None: ...
    def create_topics(self, new_topics: list[NewTopic], **kwargs: Unpack[_CreateKwargs]) -> dict[str, Future[None]]: ...
    def delete_topics(self, topics: list[str], **kwargs: Unpack[_OperationKwargs]) -> dict[str, Future[None]]: ...
    def list_topics(self, topic: str | None = None, timeout: float = -1) -> ClusterMetadata: ...
    @deprecated("Use list_consumer_groups() and describe_consumer_groups() instead.")
    def list_groups(self, group: str | None = None, timeout: float = -1) -> list[GroupMetadata]: ...
    def cluster_id(self, timeout: float = -1) -> str: ...
    def create_partitions(
        self, new_partitions: list[NewPartitions], **kwargs: Unpack[_CreateKwargs]
    ) -> dict[str, Future[None]]: ...
    # the result is keyed by the configuration property name
    def describe_configs(
        self, resources: list[ConfigResource], **kwargs: Unpack[_DescribeConfigsKwargs]
    ) -> dict[ConfigResource, Future[dict[str, ConfigEntry]]]: ...
    @deprecated("Use incremental_alter_configs() instead.")
    def alter_configs(
        self, resources: list[ConfigResource], **kwargs: Unpack[_AlterConfigsKwargs]
    ) -> dict[ConfigResource, Future[None]]: ...
    def incremental_alter_configs(
        self, resources: list[ConfigResource], **kwargs: Unpack[_AlterConfigsKwargs]
    ) -> dict[ConfigResource, Future[None]]: ...
    def create_acls(
        self, acls: list[AclBinding], **kwargs: Unpack[_RequestKwargs]
    ) -> dict[AclBinding, Future[None]]: ...
    def describe_acls(
        self, acl_binding_filter: AclBindingFilter, **kwargs: Unpack[_RequestKwargs]
    ) -> Future[list[AclBinding]]: ...
    # the result of each future: the ACL bindings its filter deleted
    def delete_acls(
        self, acl_binding_filters: list[AclBindingFilter], **kwargs: Unpack[_RequestKwargs]
    ) -> dict[AclBindingFilter, Future[list[AclBinding]]]: ...
    def list_consumer_groups(self, **kwargs: Unpack[_ListConsumerGroupsKwargs]) -> Future[ListConsumerGroupsResult]: ...
    def describe_consumer_groups(
        self, group_ids: list[str], **kwargs: Unpack[_DescribeKwargs]
    ) -> dict[str, Future[ConsumerGroupDescription]]: ...
    def describe_topics(
        self, topics: TopicCollection, **kwargs: Unpack[_DescribeKwargs]
    ) -> dict[str, Future[TopicDescription]]: ...
    def describe_cluster(self, **kwargs: Unpack[_DescribeKwargs]) -> Future[DescribeClusterResult]: ...
    def delete_consumer_groups(
        self, group_ids: list[str], **kwargs: Unpack[_RequestKwargs]
    ) -> dict[str, Future[None]]: ...
    # Both take a list with exactly one request, and return futures keyed by the group id.
    def list_consumer_group_offsets(
        self,
        list_consumer_group_offsets_request: list[ConsumerGroupTopicPartitions],
        **kwargs: Unpack[_ListConsumerGroupOffsetsKwargs],
    ) -> dict[str, Future[ConsumerGroupTopicPartitions]]: ...
    def alter_consumer_group_offsets(
        self,
        alter_consumer_group_offsets_request: list[ConsumerGroupTopicPartitions],
        **kwargs: Unpack[_RequestKwargs],
    ) -> dict[str, Future[ConsumerGroupTopicPartitions]]: ...
    # without `users`: one future for all users, keyed by user name; with `users`: one future per user
    @overload
    def describe_user_scram_credentials(
        self, users: None = None, **kwargs: Unpack[_RequestKwargs]
    ) -> Future[dict[str, UserScramCredentialsDescription]]: ...
    @overload
    def describe_user_scram_credentials(
        self, users: list[str], **kwargs: Unpack[_RequestKwargs]
    ) -> dict[str, Future[UserScramCredentialsDescription]]: ...
    # the futures are keyed by user name
    def alter_user_scram_credentials(
        self, alterations: list[UserScramCredentialAlteration], **kwargs: Unpack[_RequestKwargs]
    ) -> dict[str, Future[None]]: ...
    def list_offsets(
        self, topic_partition_offsets: dict[TopicPartition, OffsetSpec], **kwargs: Unpack[_ListOffsetsKwargs]
    ) -> dict[TopicPartition, Future[ListOffsetsResultInfo]]: ...
    def delete_records(
        self, topic_partition_offsets: list[TopicPartition], **kwargs: Unpack[_OperationKwargs]
    ) -> dict[TopicPartition, Future[DeletedRecords]]: ...
    # Takes no keyword arguments in the stubs: at runtime `request_timeout` and `operation_timeout` both raise
    # `SystemError` (the C extension's keyword list for this method is malformed).
    def elect_leaders(
        self, election_type: ElectionType, partitions: list[TopicPartition] | None = None
    ) -> Future[dict[TopicPartition, KafkaException | None]]: ...
