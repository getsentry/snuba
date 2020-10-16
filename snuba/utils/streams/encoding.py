from __future__ import annotations

from concurrent.futures import Future
from typing import Union

from snuba.utils.codecs import Encoder, TDecoded, TEncoded
from snuba.utils.streams.backends.abstract import Producer
from snuba.utils.streams.types import Message, Partition, Topic


class ProducerEncodingWrapper(Producer[TDecoded]):
    def __init__(
        self, producer: Producer[TEncoded], encoder: Encoder[TEncoded, TDecoded]
    ) -> None:
        self.__producer = producer
        self.__encoder = encoder

    def produce(
        self, destination: Union[Topic, Partition], payload: TDecoded
    ) -> Future[Message[TDecoded]]:
        decoded_future: Future[Message[TDecoded]] = Future()
        decoded_future.set_running_or_notify_cancel()

        def set_decoded_future_result(
            encoded_future: Future[Message[TEncoded]],
        ) -> None:
            try:
                message = encoded_future.result()
            except Exception as e:
                decoded_future.set_exception(e)
            else:
                decoded_future.set_result(
                    Message(
                        message.partition,
                        message.offset,
                        payload,
                        message.timestamp,
                        message.next_offset,
                    )
                )

        self.__producer.produce(
            destination, self.__encoder.encode(payload)
        ).add_done_callback(set_decoded_future_result)

        return decoded_future

    def close(self) -> Future[None]:
        return self.__producer.close()
