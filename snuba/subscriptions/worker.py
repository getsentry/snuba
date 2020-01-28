from __future__ import annotations

import itertools
from concurrent.futures import Future, wait
from typing import NamedTuple, Optional, Sequence

from snuba.subscriptions.consumer import Tick
from snuba.subscriptions.data import Subscription
from snuba.subscriptions.executor import SubscriptionExecutor
from snuba.subscriptions.scheduler import ScheduledTask, Scheduler
from snuba.utils.metrics.timer import Timer
from snuba.utils.streams.batching import AbstractBatchWorker
from snuba.utils.streams.producer import Producer
from snuba.utils.streams.types import Message, Partition
from snuba.web.query import ClickhouseQueryResult


class SubscriptionResult(NamedTuple):
    task: ScheduledTask[Subscription]
    future: Future[ClickhouseQueryResult]


class SubscriptionWorker(AbstractBatchWorker[Tick, Sequence[SubscriptionResult]]):
    def __init__(
        self, executor: SubscriptionExecutor, producer: Producer[SubscriptionResult],
    ) -> None:
        self.__executor = executor
        self.__producer = producer

    def __get_scheduler(self, partition: Partition) -> Scheduler[Subscription]:
        """
        Returns the ``Scheduler`` for items within the provided ``Partition``.
        """
        raise NotImplementedError

    def process_message(
        self, message: Message[Tick]
    ) -> Optional[Sequence[SubscriptionResult]]:
        tick = message.payload
        timer: Timer = None  # TODO
        return [
            SubscriptionResult(task, self.__executor.execute(task, tick, timer))
            for task in self.__get_scheduler(message.partition).find(tick.timestamps)
        ]

    def __get_destination(self, subscription: Subscription) -> Partition:
        """
        Returns the ``Partition`` that the provided ``Subscription`` should
        have its results published to.
        """
        raise NotImplementedError

    def flush_batch(self, batch: Sequence[Sequence[SubscriptionResult]]) -> None:
        # TODO: Directly publishing a ``SubscribedQueryResult`` implies that
        # error handling will need to happen in the producer codec. Retries (if
        # any) will need to happen in the executor, and any failure to execute
        # futures should be actually be published to the stream (the same way
        # an HTTP response is forwarded to the client.) The codec itself will
        # need to be able to set a timeout, as well.
        # TODO: This should probably have a timeout, also need to decide what
        # to do in case of an produce error -- probably throw?
        wait(
            self.__producer.produce(self.__get_destination(result.task.task), result)
            for result in itertools.chain.from_iterable(batch)
        )
