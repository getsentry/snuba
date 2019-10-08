from tests.base import BaseEventsTest
from unittest.mock import patch

from snuba.clickhouse.query import ClickhouseQuery
from snuba.query.query import Query
from snuba.request import Request, RequestSettings


class TestClickhouseQuery(BaseEventsTest):
    def test_provided_sample_should_be_used(self):
        query = Query({
            "conditions": [],
            "aggregations": [],
            "groupby": [],
            "sample": 0.1
        })
        request_settings = RequestSettings(turbo=False, consistent=False, debug=False)
        request = Request(query=query, settings=request_settings, extensions={})

        clickhouse_query = ClickhouseQuery(dataset=self.dataset, request=request, prewhere_conditions=[])

        assert 'SAMPLE 0.1' in clickhouse_query.format()

    def test_provided_sample_should_be_used_with_turbo(self):
        query = Query({
            "conditions": [],
            "aggregations": [],
            "groupby": [],
            "sample": 0.1
        })
        request_settings = RequestSettings(turbo=True, consistent=False, debug=False)
        request = Request(query=query, settings=request_settings, extensions={})
        clickhouse_query = ClickhouseQuery(dataset=self.dataset, request=request, prewhere_conditions=[])

        assert 'SAMPLE 0.1' in clickhouse_query.format()

    @patch("snuba.settings.TURBO_SAMPLE_RATE", 0.2)
    def test_when_sample_is_not_provided_with_turbo(self):
        query = Query({
            "conditions": [],
            "aggregations": [],
            "groupby": [],
        })
        request_settings = RequestSettings(turbo=True, consistent=False, debug=False)

        request = Request(query=query, settings=request_settings, extensions={})
        clickhouse_query = ClickhouseQuery(dataset=self.dataset, request=request, prewhere_conditions=[])

        assert "SAMPLE 0.2" in clickhouse_query.format()

    def test_when_sample_is_not_provided_without_turbo(self):
        query = Query({
            "conditions": [],
            "aggregations": [],
            "groupby": [],
        })
        request_settings = RequestSettings(turbo=False, consistent=False, debug=False)

        request = Request(query=query, settings=request_settings, extensions={})
        clickhouse_query = ClickhouseQuery(dataset=self.dataset, request=request, prewhere_conditions=[])

        assert 'SAMPLE' not in clickhouse_query.format()
