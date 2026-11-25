from collections.abc import Callable

from confluent_kafka.cimpl import KafkaError, Message, Producer

type _Headers = dict[str, str | bytes | None] | list[tuple[str, str | bytes | None]]
type _DeliveryCallback = Callable[[KafkaError | None, Message], object]

# `K` and `V` are the types the configured `key.serializer` and `value.serializer` accept. They can't be worked out
# from the configuration, so give them where the producer is created: `SerializingProducer[str, User](conf)`.
class SerializingProducer[K = object, V = object](Producer):
    # a `dict`, copied before the serializers are taken out of it
    def __init__[T](self, conf: dict[str, T]) -> None: ...
    # `Producer.produce` takes `value` before `key`, and a `callback` as well
    def produce(  # type: ignore[override]
        self,
        topic: str,
        key: K | None = None,
        value: V | None = None,
        partition: int = -1,
        on_delivery: _DeliveryCallback | None = None,
        timestamp: int = 0,
        headers: _Headers | None = None,
    ) -> None: ...
