from datetime import datetime, timedelta
from unittest import mock

from sentry_kafka_schemas.schema_types.events_v1 import EndMergeMessageBody

from snuba import settings
from snuba.clickhouse import DATETIME_FORMAT as CLICKHOUSE_DATETIME_FORMAT
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.schemas.tables import WritableTableSchema
from snuba.processor import ReplacementType
from snuba.replacers.errors_replacer import MergeReplacement, ReplacementContext
from snuba.replacers.replacer_processor import (
    ReplacementMessage,
    ReplacementMessageMetadata,
    ReplacerState,
)
from snuba.utils.schemas import FlattenedColumn


def test_merge_replacement_with_first_seen() -> None:
    now_dt = datetime.now()
    first_seen_dt = now_dt - timedelta(days=1)
    data: EndMergeMessageBody = {
        "transaction_id": "",
        "project_id": 100,
        "previous_group_ids": [2, 3],
        "new_group_id": 1,
        "new_group_first_seen": first_seen_dt.strftime(
            settings.PAYLOAD_DATETIME_FORMAT
        ),
        "datetime": now_dt.strftime(settings.PAYLOAD_DATETIME_FORMAT),
    }

    message = ReplacementMessage(
        action_type=ReplacementType.END_MERGE,
        data=data,
        metadata=mock.Mock(ReplacementMessageMetadata),
    )

    columns = [
        FlattenedColumn(None, c.name, c.type)
        for c in (
            get_entity(EntityKey.SEARCH_ISSUES)
            .get_all_storages()[0]
            .get_schema()
            .get_columns()
        ).columns
        if c.name in {"organization_id", "group_id", "event_id", "group_first_seen"}
    ]

    context = ReplacementContext(
        all_columns=columns,
        required_columns=[],
        state_name=ReplacerState.ERRORS,
        tag_column_map={},
        promoted_tags={},
        schema=mock.Mock(WritableTableSchema),
    )

    mr = MergeReplacement.parse_message(message, context)
    assert mr is not None
    expected = f"""INSERT INTO table (organization_id, group_id, event_id, group_first_seen)
            SELECT organization_id, 1, event_id, CAST('{first_seen_dt.strftime(CLICKHOUSE_DATETIME_FORMAT)}' AS DateTime)
            FROM table FINAL
                        PREWHERE group_id IN (2, 3)
            WHERE project_id = 100
            AND received <= CAST('{now_dt.strftime(CLICKHOUSE_DATETIME_FORMAT)}' AS DateTime)
            AND NOT deleted"""
    actual = mr.get_insert_query("table")
    assert actual is not None
    assert actual.strip() == expected


def test_merge_replacement_without_first_seen() -> None:
    now_dt = datetime.now()
    data: EndMergeMessageBody = {
        "transaction_id": "",
        "project_id": 100,
        "previous_group_ids": [2, 3],
        "new_group_id": 1,
        "datetime": now_dt.strftime(settings.PAYLOAD_DATETIME_FORMAT),
    }

    message = ReplacementMessage(
        action_type=ReplacementType.END_MERGE,
        data=data,
        metadata=mock.Mock(ReplacementMessageMetadata),
    )

    columns = [
        FlattenedColumn(None, c.name, c.type)
        for c in (
            get_entity(EntityKey.SEARCH_ISSUES)
            .get_all_storages()[0]
            .get_schema()
            .get_columns()
        ).columns
        if c.name in {"organization_id", "group_id", "event_id", "group_first_seen"}
    ]

    context = ReplacementContext(
        all_columns=columns,
        required_columns=[],
        state_name=ReplacerState.ERRORS,
        tag_column_map={},
        promoted_tags={},
        schema=mock.Mock(WritableTableSchema),
    )

    mr = MergeReplacement.parse_message(message, context)
    assert mr is not None
    expected = f"""INSERT INTO table (organization_id, group_id, event_id, group_first_seen)
            SELECT organization_id, 1, event_id, group_first_seen
            FROM table FINAL
                        PREWHERE group_id IN (2, 3)
            WHERE project_id = 100
            AND received <= CAST('{now_dt.strftime(CLICKHOUSE_DATETIME_FORMAT)}' AS DateTime)
            AND NOT deleted"""
    actual = mr.get_insert_query("table")
    assert actual is not None
    assert actual.strip() == expected
