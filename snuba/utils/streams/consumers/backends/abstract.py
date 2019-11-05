from abc import ABC, abstractmethod
from typing import Callable, Generic, Mapping, Optional, Sequence

from snuba.utils.streams.consumers.types import Message, TStream, TOffset, TValue


class ConsumerBackend(ABC, Generic[TStream, TOffset, TValue]):
    """
    An abstract class that provides methods to consume messages from a
    multiplexed collection of streams. This class is not intended to be used
    directly. Instead, it is intended to be wrapped by a ``Consumer``
    instance.

    All methods are blocking unless otherise noted in the documentation for
    that method.
    """

    @abstractmethod
    def subscribe(
        self,
        topics: Sequence[str],
        on_assign: Optional[Callable[[Sequence[TStream]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[TStream]], None]] = None,
    ) -> None:
        """
        Subscribe to topic streams. This replaces a previous subscription.
        This method does not block. The subscription may not be fulfilled
        immediately: instead, the ``on_assign`` and ``on_revoke`` callbacks
        are called when the subscription state changes with the updated
        assignment for this consumer.

        Raises a ``RuntimeError`` if called on a closed consumer.
        """
        raise NotImplementedError

    @abstractmethod
    def unsubscribe(self) -> None:
        """
        Unsubscribe from streams.

        Raises a ``RuntimeError`` if called on a closed consumer.
        """
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
    def tell(self) -> Mapping[TStream, TOffset]:
        """
        Return the read offsets for all assigned streams.

        Raises a ``RuntimeError`` if called on a closed consumer.
        """
        raise NotImplementedError

    @abstractmethod
    def seek(self, offsets: Mapping[TStream, TOffset]) -> None:
        """
        Change the read offsets for the provided streams.

        Raises a ``RuntimeError`` if called on a closed consumer.
        """
        raise NotImplementedError

    @abstractmethod
    def pause(self, streams: Sequence[TStream]) -> None:
        """
        Pause the consumption of messages for the provided streams.

        Raises a ``RuntimeError`` if called on a closed consumer.
        """
        raise NotImplementedError

    @abstractmethod
    def resume(self, streams: Sequence[TStream]) -> None:
        """
        Resume the consumption of messages for the provided streams.

        Raises a ``RuntimeError`` if called on a closed consumer.
        """
        raise NotImplementedError

    @abstractmethod
    def commit(self) -> Mapping[TStream, TOffset]:
        """
        Commit staged offsets for all streams that this consumer is assigned
        to. The return value of this method is a mapping of streams with
        their committed offsets as values.

        Raises a ``RuntimeError`` if called on a closed consumer.
        """
        raise NotImplementedError

    @abstractmethod
    def close(self, timeout: Optional[float] = None) -> None:
        """
        Close the consumer. This stops consuming messages, *may* commit
        staged offsets (depending on the implementation), and ends its
        subscription.

        Raises a ``TimeoutError`` if the consumer is unable to be closed
        before the timeout is reached.
        """
        raise NotImplementedError
