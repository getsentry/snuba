from __future__ import annotations

from concurrent.futures import Future
from typing import Union

from arroyo.backends.abstract import Producer
from arroyo.types import BrokerValue, Partition, Topic

from snuba.utils.codecs import Encoder, TDecoded, TEncoded


class ProducerEncodingWrapper(Producer[TDecoded]):
    def __init__(
        self, producer: Producer[TEncoded], encoder: Encoder[TEncoded, TDecoded]
    ) -> None:
        self.__producer = producer
        self.__encoder = encoder

    def produce(
        self, destination: Union[Topic, Partition], payload: TDecoded
    ) -> Future[BrokerValue[TDecoded]]:
        decoded_future: Future[BrokerValue[TDecoded]] = Future()
        decoded_future.set_running_or_notify_cancel()

        def set_decoded_future_result(
            encoded_future: Future[BrokerValue[TEncoded]],
        ) -> None:
            try:
                value = encoded_future.result()
            except Exception as e:
                decoded_future.set_exception(e)
            else:
                decoded_future.set_result(
                    BrokerValue(
                        payload,
                        value.partition,
                        value.offset,
                        value.timestamp,
                    )
                )

        self.__producer.produce(
            destination, self.__encoder.encode(payload)
        ).add_done_callback(set_decoded_future_result)

        return decoded_future

    def close(self) -> Future[None]:
        return self.__producer.close()
