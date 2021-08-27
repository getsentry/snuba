from snuba.datasets.factory import get_dataset
from snuba.request import Language, Request
from snuba.utils.metrics.timer import Timer
from snuba.web.views import dataset_query


def test_dataset_query() -> None:

    # check if transaction and discover have different results

    query_str = """MATCH (discover)
    SELECT
        contexts[trace.span_id]
    WHERE
        timestamp >= toDateTime('2021-07-25T15:02:10') AND
        timestamp < toDateTime('2021-07-26T15:02:10') AND
        project_id IN tuple(5492900)
    """

    q = dataset_query(
        dataset=get_dataset("discover"),
        body={
            "query": query_str,
            "debug": True,
            "dataset": "discover",
            "turbo": False,
            "consistent": False,
        },
        timer=Timer(name="foo"),
        language=Language.SNQL,
    )
    assert q


def test_with_transactions() -> None:

    query_str = """MATCH (transactions)
    SELECT
        contexts[trace.span_id]
    WHERE
        start_ts >= toDateTime('2021-07-26T15:02:10') AND
        finish_ts >= toDateTime('2021-07-26T15:02:10') AND
        finish_ts < toDateTime('2021-07-26T15:02:10') AND
        project_id IN tuple(5492900)
    """

    """
    SELECT
  arrayElement(
    contexts.value,
    indexOf(
      contexts.key,
      'trace.span_id'
    )
  ) AS `_snuba_contexts[trace.span_id]` |> contexts[trace.span_id]
FROM
  Table(transactions_local)


WHERE
  and(
    greaterOrEquals(
      start_ts AS `_snuba_start_ts`,
      datetime(2021-07-26T15:02:10)
    ),
    and(
      greaterOrEquals(
        finish_ts AS `_snuba_finish_ts`,
        datetime(2021-07-26T15:02:10)
      ),
      and(
        less(
          finish_ts AS `_snuba_finish_ts`,
          datetime(2021-07-26T15:02:10)
        ),
        in(
          project_id AS `_snuba_project_id`,
          tuple(
            5492900
          )
        )
      )
    )
  )
  LIMIT 1000"""

    q = dataset_query(
        dataset=get_dataset("transactions"),
        body={
            "query": query_str,
            "debug": True,
            "dataset": "discover",
            "turbo": False,
            "consistent": False,
        },
        timer=Timer(name="foo"),
        language=Language.SNQL,
    )
    assert q
