from typing import NoReturn

from confluent_kafka.cimpl import Consumer, Message

# `K` and `V` are the types the configured `key.deserializer` and `value.deserializer` return. They can't be worked
# out from the configuration, so give them where the consumer is created: `DeserializingConsumer[str, User](conf)`.
class DeserializingConsumer[K = object, V = object](Consumer):
    # a `dict`, copied before the deserializers are taken out of it
    def __init__[T](self, conf: dict[str, T]) -> None: ...
    # Raises `ConsumeError` for a message with an error, so the message returned has none. `Consumer.poll` returns
    # a message with `bytes` key and value.
    def poll(self, timeout: float = -1) -> Message[K, V] | None: ...  # type: ignore[override]
    # not implemented: always raises `NotImplementedError`
    def consume(self, num_messages: int = 1, timeout: float = -1) -> NoReturn: ...
