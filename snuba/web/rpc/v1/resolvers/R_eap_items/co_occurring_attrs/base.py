from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence

from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.data_source.simple import Storage
from snuba.query.expressions import Expression


class CoOccurringAttrsSource(ABC):
    """One co-occurring-attributes roll-up, plus the query shape its schema requires.

    Everything that differs between the tables has to be reachable from here, since the
    endpoint builds a single query against whichever source it is handed. Implementations are
    stateless; each module exposes a singleton.
    """

    @property
    @abstractmethod
    def storage_key(self) -> StorageKey:
        raise NotImplementedError

    @abstractmethod
    def typed_key_arrays(
        self, requested_type: AttributeKey.Type.ValueType
    ) -> Sequence[tuple[str, str]]:
        """The (key-array column, ``AttributeKey`` type name) pairs a request's ``type`` reads.

        The type name is what the response reports each key as, so it follows the column the
        key was read from rather than the requested type. Several entries are concatenated.
        """
        raise NotImplementedError

    @abstractmethod
    def count_expression(self) -> Expression:
        """How often each key occurs, aggregated over the rows grouped by attribute key.

        Its meaning depends on what a row represents in this storage.
        """
        raise NotImplementedError

    @property
    def has_last_seen(self) -> bool:
        """Whether this storage records when an attribute was last seen."""
        return False

    def last_seen_expression(self) -> Expression:
        """The most recent time each key was seen. Only valid when ``has_last_seen``."""
        raise NotImplementedError(
            f"{self.storage_key.value} does not record last_seen; guard on has_last_seen"
        )

    @property
    def data_source(self) -> Storage:
        return Storage(
            key=self.storage_key,
            schema=get_storage(self.storage_key).get_schema().get_columns(),
            sample=None,
        )

    def key_array_columns(self, requested_type: AttributeKey.Type.ValueType) -> list[str]:
        return [col for col, _ in self.typed_key_arrays(requested_type)]

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.storage_key.value})"
