from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence

from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.data_source.simple import Storage
from snuba.query.expressions import Expression


class CoOccurringAttrsSource(ABC):
    """One of the co-occurring-attributes roll-ups, plus the query shape it requires.

    ``TraceItemAttributeNames`` builds a single query against whichever source
    ``for_request`` returns, so everything that differs between the tables has to be
    reachable from here. Implementations are stateless; each module exposes a singleton.
    """

    @property
    @abstractmethod
    def storage_key(self) -> StorageKey:
        """The storage this source reads."""
        raise NotImplementedError

    @abstractmethod
    def typed_key_arrays(
        self, requested_type: AttributeKey.Type.ValueType
    ) -> Sequence[tuple[str, str]]:
        """The (key-array column, ``AttributeKey`` type name) pairs a request's ``type``
        reads on this storage.

        The type name is what the response reports each key as, so it follows the column the
        key was actually read from rather than the requested type. One entry means a single
        typed array; several are concatenated.
        """
        raise NotImplementedError

    @abstractmethod
    def count_expression(self) -> Expression:
        """How often each key occurs, for frequency ordering.

        Aggregated over the rows grouped by attribute key, so its meaning depends on what a
        row represents in this storage.
        """
        raise NotImplementedError

    @property
    def data_source(self) -> Storage:
        """The query's FROM clause."""
        return Storage(
            key=self.storage_key,
            schema=get_storage(self.storage_key).get_schema().get_columns(),
            sample=None,
        )

    def key_array_columns(self, requested_type: AttributeKey.Type.ValueType) -> list[str]:
        """Just the column names from ``typed_key_arrays``, for the row prefilter."""
        return [col for col, _ in self.typed_key_arrays(requested_type)]

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.storage_key.value})"
