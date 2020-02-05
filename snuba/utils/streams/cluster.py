from abc import ABC
from typing import Generic

from snuba.utils.codecs import Codec, TDecoded, TEncoded
from snuba.utils.streams.consumer import Consumer
from snuba.utils.streams.producer import Producer


class Cluster(Generic[TEncoded], ABC):
    def consumer(
        self, codec: Codec[TEncoded, TDecoded], group: str
    ) -> Consumer[TDecoded]:
        raise NotImplementedError

    def producer(self, codec: Codec[TEncoded, TDecoded]) -> Producer[TDecoded]:
        raise NotImplementedError
