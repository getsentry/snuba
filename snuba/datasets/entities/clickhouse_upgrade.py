from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime
from enum import Enum
from random import random
from typing import (
    List,
    MutableMapping,
    MutableSequence,
    NamedTuple,
    Optional,
    Tuple,
    Union,
    cast,
)
from uuid import UUID

from sentry_sdk import set_tag

from snuba import environment, settings
from snuba.query.logical import Query
from snuba.query.query_settings import QuerySettings
from snuba.reader import Row
from snuba.state import get_config
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.threaded_function_delegator import Result
from snuba.web import QueryResult

metrics = MetricsWrapper(environment.metrics, "query.similarity")
logger = logging.getLogger("snuba.upgrade_discrepancies")


class Option(Enum):
    ERRORS = 1
    ERRORS_V2 = 2
    TRANSACTIONS = 3
    TRANSACTIONS_V2 = 4


class Choice(NamedTuple):
    primary: Option
    secondary: Optional[Option]


class RolloutSelector:
    """
    Takes the rollout decisions during the upgrade to Clickhouse 21.8.
    This class assumes that there are two storages to choose from before
    running a query.
    The main pipeline builder needs a decision on which queries to run
    (both storages or only one) and which result to trust.

    This class can return one storage (which is the only one the query
    will run onto) or two storages, in that case the first will be the
    trusted one.

    There are multiple way to configure this rollout:
    First choice: which query should be trusted:
    - check runtime config if the default secondary should be trusted
    - check global rollout if the default secondary should be trusted
    Now we know which query will be trusted. hould we run the second ?
    Check the configs and settings in the same order.
    """

    def __init__(
        self, default_primary: Option, default_secondary: Option, config_prefix: str
    ) -> None:
        self.__default_primary = default_primary
        self.__default_secondary = default_secondary
        self.__config_prefix = config_prefix

    def __is_query_rolled_out(
        self,
        referrer: str,
        config_referrer_prefix: str,
        general_rollout_config: str,
    ) -> bool:
        rollout_percentage = get_config(
            f"rollout_upgraded_{self.__config_prefix}_{config_referrer_prefix}_{referrer}",
            None,
        )
        if rollout_percentage is None:
            rollout_percentage = get_config(general_rollout_config, 0.0)

        return random() <= cast(float, rollout_percentage)

    def choose(self, referrer: str) -> Choice:
        trust_secondary = self.__is_query_rolled_out(
            referrer,
            "trust",
            f"rollout_upgraded_{self.__config_prefix}_trust",
        )

        execute_both = self.__is_query_rolled_out(
            referrer,
            "execute",
            f"rollout_upgraded_{self.__config_prefix}_execute",
        )

        primary = (
            self.__default_secondary if trust_secondary else self.__default_primary
        )
        if not execute_both:
            return Choice(primary, None)
        else:
            return Choice(
                primary,
                self.__default_secondary
                if not trust_secondary
                else self.__default_primary,
            )


Key = Tuple[Union[str, datetime, UUID], ...]
RowValues = Tuple[Union[int, float, bool], ...]


def comparison_callback(
    _query: Query,
    _settings: QuerySettings,
    referrer: str,
    primary_result: Optional[Result[QueryResult]],
    results: List[Result[QueryResult]],
) -> None:
    """
    Compares the primary and secondary results and logs metrics about the similarity.

    The similarity is calculated fairly naively this way relying on the fact that
    most queries have a number of string/datetime fields and a numebr of decimal
    fields (int or float).

    - We match the rows that are present in both results (matching the non decimal
    fields)
    - The similarity score is the average of the similarity of each number column.
      Ex. row1 : (a, 10, 100, 1000) row2 (a, 9, 99, 990). The similarity between
      these two is the avg([(smaller value / bigger value) for each column])
    - Calculate the average of the similarity scores of all the matching rows.
    - Rows that do not have a match have a similarity score = 0.0
    """
    if primary_result is None or len(results) == 0:
        return

    secondary_result = results[0]
    primary_schema = split_metadata(primary_result.result)
    secondary_schema = split_metadata(secondary_result.result)

    if primary_schema != secondary_schema:
        metrics.increment("schema_mismatch", tags={"referrer": referrer})
        return

    if not primary_schema.complete or secondary_schema.complete:
        metrics.increment("incomplete_match", tags={"referrer": referrer})

    score = calculate_score(
        primary_result.result.result["data"],
        secondary_result.result.result["data"],
        primary_schema,
    )
    entity = _query.get_from_clause().__class__.__name__
    metrics.timing(
        "similarity_score", score, tags={"referrer": referrer, "entity": entity}
    )

    primary_result_portion = primary_result.result.result["data"][:50]
    secondary_result_portion = secondary_result.result.result["data"][:50]

    def log(title: str) -> None:
        set_tag("referrer", referrer)
        logger.warning(
            title,
            extra={
                "primary": primary_result_portion,
                "secondary": secondary_result_portion,
                "referrer": referrer,
                "schema": primary_schema,
                "score": score,
            },
            exc_info=True,
        )

    if score >= 0.90:
        # This is just a sanity check to ensure the algorithm makes sense
        if random() < cast(float, get_config("upgrade_log_perfect_match", 0.0)):
            log(
                f"Pipeline delegator good match: {score}",
            )
    elif score >= 0.75:
        if random() < cast(float, get_config("upgrade_log_avg_match", 0.0)):
            log(
                f"Pipeline delegator average match: {score}",
            )
    elif random() < cast(float, get_config("upgrade_log_low_similarity", 0.0)):
        log(
            f"Pipeline delegator low match: {score}",
        )


def calculate_score(
    data_row1: MutableSequence[Row],
    data_row2: MutableSequence[Row],
    schema: SplitSchema,
) -> float:
    """
    Calculate the similarity score of two lists of rows.
    The total score is the average of the scores of each pair of mathcing
    rows where rows are matching is they have the same key.
    Rows that do not have a match count as 0.
    The similarity score between two matching rows is the average of the
    similarities between all columns.
    """

    # Each key have a list of rows associated because a given key is
    # not necessarily unique within a result.
    result1_index: MutableMapping[Key, List[SplitRow]] = defaultdict(list)
    for i in range(min(len(data_row1), settings.MAX_ROWS_TO_CHECK_FOR_SIMILARITY)):
        row = split_row(data_row1[i], schema)
        result1_index[row.key].append(row)

    row_similarities: MutableSequence[float] = []
    result2_index: MutableMapping[Key, List[SplitRow]] = defaultdict(list)
    for i in range(min(len(data_row2), settings.MAX_ROWS_TO_CHECK_FOR_SIMILARITY)):
        row = split_row(data_row2[i], schema)
        result2_index[row.key].append(row)

        if row.key not in result1_index:
            row_similarities.append(0.0)
        else:
            matching_rows = result1_index[row.key]
            # When we find multiple rows in result1 that match a row in result2
            # We pick the one with the highest score.
            # We choose the highest match as that is most likely the row in
            # result1 that correspond to the row we are examining in reult2.
            key_similarities = [
                row_similarity(row, matching_row) for matching_row in matching_rows
            ]
            row_similarities.append(max(key_similarities))

    for key in result1_index.keys():
        if key not in result2_index:
            row_similarities.append(0.0)

    return (
        sum(row_similarities) / len(row_similarities)
        if len(row_similarities) > 0
        else 1.0
    )


class SplitSchema(NamedTuple):
    complete: bool
    key_cols: Tuple[str, ...]
    value_cols: Tuple[str, ...]


def split_metadata(result: QueryResult) -> SplitSchema:
    metadata = result.result["meta"]
    keys = []
    vals = []
    for column in metadata:
        if column["type"] in (
            "String",
            "Datetime('Universal')",
            "UUID",
            "Date('Universal')",
            "LowCardinality(String)",
        ):
            keys.append(column["name"])
        elif column["type"] in (
            "UInt8",
            "UInt16",
            "UInt32",
            "UInt64",
            "Float32",
            "Float64",
            "Boolean",
        ):
            vals.append(column["name"])
    return SplitSchema(
        complete=len(keys) + len(vals) == len(metadata),
        key_cols=tuple(keys),
        value_cols=tuple(vals),
    )


class SplitRow(NamedTuple):
    key: Tuple[Union[str, datetime, UUID], ...]
    values: Tuple[Union[int, float, bool], ...]


def split_row(row: Row, schema: SplitSchema) -> SplitRow:
    key = tuple(
        [cast(Union[str, datetime, UUID], row[field]) for field in schema.key_cols]
    )
    vals = tuple(
        [cast(Union[int, float, bool], row[field]) for field in schema.value_cols]
    )
    return SplitRow(key, vals)


def row_similarity(row1: SplitRow, row2: SplitRow) -> float:
    if row1.key != row2.key:
        return 0.0

    if len(row1.values) != len(row2.values):
        return 0.0

    if len(row1.values) == 0:
        return 1.0

    similarities = []
    for index, val in enumerate(row1.values):
        if isinstance(val, bool):
            similarities.append(0.0 if val != row2.values[index] else 1.0)
        else:
            left = min(val, row2.values[index])
            right = max(val, row2.values[index])
            if right > 0.0:
                similarities.append(left / right)
            else:
                similarities.append(1.0 if left == 0.0 else 0.0)

    return sum(similarities) / len(similarities)
