from typing import Callable, Generic, Mapping, Optional, Sequence

from snuba.utils.retries import NoRetryPolicy, RetryPolicy
from snuba.utils.streams.consumers.backends.abstract import ConsumerBackend
from snuba.utils.streams.consumers.types import Message, TStream, TOffset, TValue


class Consumer(Generic[TStream, TOffset, TValue]):
    """
    This class provides an interface for consuming messages from a
    multiplexed collection of streams. The specific implementation of how
    messages are consumed is delegated to the backend implementation.

    All methods are blocking unless otherise noted in the documentation for
    that method.
    """

    def __init__(
        self,
        backend: ConsumerBackend[TStream, TOffset, TValue],
        commit_retry_policy: Optional[RetryPolicy] = None,
    ):
        if commit_retry_policy is None:
            commit_retry_policy = NoRetryPolicy()

        self.__backend = backend
        self.__commit_retry_policy = commit_retry_policy

    def subscribe(
        self,
        topics: Sequence[str],
        on_assign: Optional[Callable[[Mapping[TStream, TOffset]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[TStream]], None]] = None,
    ) -> None:
        """
        Subscribe to topic streams. This replaces a previous subscription.
        This method does not block. The subscription may not be fulfilled
        immediately: instead, the ``on_assign`` and ``on_revoke`` callbacks
        are called when the subscription state changes with the updated
        assignment for this consumer.

        If provided, the ``on_assign`` callback is called with a mapping of
        streams to their offsets (at this point, the working offset and the
        committed offset are the same for each stream) on each subscription
        change. Similarly, the ``on_revoke`` callback (if provided) is called
        with a sequence of streams that are being removed from this
        consumer's assignment. (This callback does not include the offsets,
        as the working offset and committed offset may differ, in some cases
        by substantial margin.)

        Raises a ``RuntimeError`` if called on a closed consumer.
        """
        return self.__backend.subscribe(topics, on_assign, on_revoke)

    def unsubscribe(self) -> None:
        """
        Unsubscribe from streams.

        Raises a ``RuntimeError`` if called on a closed consumer.
        """
        return self.__backend.unsubscribe()

    def poll(
        self, timeout: Optional[float] = None
    ) -> Optional[Message[TStream, TOffset, TValue]]:
        """
        Return the next message available to be consumed, if one is
        available. If no message is available, this method will block up to
        the ``timeout`` value before returning ``None``. A timeout of
        ``0.0`` represents "do not block", while a timeout of ``None``
        represents "block until a message is available (or forever)".

        Calling this method may also invoke subscription state change
        callbacks.

        This method may raise ``ConsumerError`` or one of it's
        subclasses, the specific meaning of which are backend implementation
        specific.

        This method may also raise an ``EndOfStream`` error (a subtype of
        ``ConsumerError``) when the consumer has reached the end of a stream
        that it is subscribed to and no additional messages are available.
        The ``stream`` attribute of the raised exception specifies the end
        which stream has been reached. (Since this consumer is multiplexing a
        set of streams, this exception does not mean that *all* of the
        streams that the consumer is subscribed to do not have any messages,
        just that it has reached the end of one of them. This also does not
        mean that additional messages won't be available in future poll
        calls.) Not every backend implementation supports this feature or is
        configured to raise in this scenario.

        Raises a ``RuntimeError`` if called on a closed consumer.
        """
        return self.__backend.poll(timeout)

    def tell(self) -> Mapping[TStream, TOffset]:
        """
        Return the read offsets for all assigned streams.

        Raises a ``RuntimeError`` if called on a closed consumer.
        """
        return self.__backend.tell()

    def seek(self, offsets: Mapping[TStream, TOffset]) -> None:
        """
        Change the read offsets for the provided streams.

        Raises a ``RuntimeError`` if called on a closed consumer.
        """
        return self.__backend.seek(offsets)

    def pause(self, streams: Sequence[TStream]) -> None:
        """
        Pause the consumption of messages for the provided streams.

        Raises a ``RuntimeError`` if called on a closed consumer.
        """
        return self.__backend.pause(streams)

    def resume(self, streams: Sequence[TStream]) -> None:
        """
        Resume the consumption of messages for the provided streams.

        Raises a ``RuntimeError`` if called on a closed consumer.
        """
        return self.__backend.resume(streams)

    def commit(self) -> Mapping[TStream, TOffset]:
        """
        Commit staged offsets for all streams that this consumer is assigned
        to. The return value of this method is a mapping of streams with
        their committed offsets as values.

        Raises a ``RuntimeError`` if called on a closed consumer.
        """
        return self.__commit_retry_policy.call(self.__backend.commit)

    def close(self, timeout: Optional[float] = None) -> None:
        """
        Close the consumer. This stops consuming messages, *may* commit
        staged offsets (depending on the implementation), and ends its
        subscription.

        Raises a ``TimeoutError`` if the consumer is unable to be closed
        before the timeout is reached.
        """
        return self.__backend.close()
