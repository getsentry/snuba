import re
from datetime import date, datetime
from typing import Iterable, Mapping, Optional
from uuid import UUID

from dateutil.tz import tz

from snuba.clickhouse.columns import Array
from snuba.clickhouse.pool import ClickhousePool
from snuba.clickhouse.query import ClickhouseQuery
from snuba.reader import Reader, Result, build_result_transformer
from snuba.writer import BatchWriter, WriterTableRow


def transform_date(value: date) -> str:
    """
    Convert a timezone-naive date object into an ISO 8601 formatted date and
    time string respresentation.
    """
    # XXX: Both Python and ClickHouse date objects do not have time zones, so
    # just assume UTC. (Ideally, we'd have just left these as timezone naive to
    # begin with and not done this transformation at all, since the time
    # portion has no benefit or significance here.)
    return datetime(*value.timetuple()[:6]).replace(tzinfo=tz.tzutc()).isoformat()


def transform_datetime(value: datetime) -> str:
    """
    Convert a timezone-naive datetime object into an ISO 8601 formatted date
    and time string representation.
    """
    if value.tzinfo is None:
        value = value.replace(tzinfo=tz.tzutc())
    else:
        value = value.astimezone(tz.tzutc())
    return value.isoformat()


def transform_uuid(value: UUID) -> str:
    """
    Convert a UUID object into a string representation.
    """
    return str(value)


transform_column_types = build_result_transformer(
    [
        (re.compile(r"^Date(\(.+\))?$"), transform_date),
        (re.compile(r"^DateTime(\(.+\))?$"), transform_datetime),
        (re.compile(r"^UUID$"), transform_uuid),
    ]
)


class NativeDriverReader(Reader[ClickhouseQuery]):
    def __init__(self, client: ClickhousePool) -> None:
        self.__client = client

    def __transform_result(self, result, with_totals: bool) -> Result:
        """
        Transform a native driver response into a response that is
        structurally similar to a ClickHouse-flavored JSON response.
        """
        data, meta = result

        # XXX: Rows are represented as mappings that are keyed by column or
        # alias, which is problematic when the result set contains duplicate
        # names. To ensure that the column headers and row data are consistent
        # duplicated names are discarded at this stage.
        columns = {c[0]: i for i, c in enumerate(meta)}

        data = [
            {column: row[index] for column, index in columns.items()} for row in data
        ]

        meta = [
            {"name": m[0], "type": m[1]} for m in [meta[i] for i in columns.values()]
        ]

        if with_totals:
            assert len(data) > 0
            totals = data.pop(-1)
            result = {"data": data, "meta": meta, "totals": totals}
        else:
            result = {"data": data, "meta": meta}

        transform_column_types(result)

        return result

    def execute(
        self,
        query: ClickhouseQuery,
        # TODO: move Clickhouse specific arguments into DictClickhouseQuery
        settings: Optional[Mapping[str, str]] = None,
        query_id: Optional[str] = None,
        with_totals: bool = False,
    ) -> Result:
        if settings is None:
            settings = {}

        kwargs = {}
        if query_id is not None:
            kwargs["query_id"] = query_id

        sql = query.format_sql()
        return self.__transform_result(
            self.__client.execute(
                sql, with_column_types=True, settings=settings, **kwargs
            ),
            with_totals=with_totals,
        )


class NativeDriverBatchWriter(BatchWriter):
    def __init__(self, schema, connection):
        self.__schema = schema
        self.__connection = connection

    def __row_to_column_list(self, columns, row):
        values = []
        for col in columns:
            value = row.get(col.flattened, None)
            if value is None and isinstance(col.type, Array):
                value = []
            values.append(value)
        return values

    def write(self, rows: Iterable[WriterTableRow]):
        columns = self.__schema.get_columns()
        self.__connection.execute_robust(
            "INSERT INTO %(table)s (%(colnames)s) VALUES"
            % {
                "colnames": ", ".join(col.escaped for col in columns),
                "table": self.__schema.get_table_name(),
            },
            [self.__row_to_column_list(columns, row) for row in rows],
            types_check=False,
        )
