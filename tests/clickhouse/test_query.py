from tests.base import BaseEventsTest
from unittest.mock import patch

from snuba.clickhouse.dictquery import DictSqlQuery
from snuba.query.logical import Query
from snuba.request.request_settings import HTTPRequestSettings


class TestDictSqlQuery(BaseEventsTest):
    def test_provided_sample_should_be_used(self):
        source = self.dataset.get_all_storages()[0].get_schema().get_data_source()
        query = Query(
            {"conditions": [], "aggregations": [], "groupby": [], "sample": 0.1},
            source,
        )
        request_settings = HTTPRequestSettings()

        clickhouse_query = DictSqlQuery(
            dataset=self.dataset, query=query, settings=request_settings,
        )

        assert "SAMPLE 0.1" in clickhouse_query.format_sql()

    def test_provided_sample_should_be_used_with_turbo(self):
        source = self.dataset.get_all_storages()[0].get_schema().get_data_source()
        query = Query(
            {"conditions": [], "aggregations": [], "groupby": [], "sample": 0.1},
            source,
        )
        request_settings = HTTPRequestSettings(turbo=True)
        clickhouse_query = DictSqlQuery(
            dataset=self.dataset, query=query, settings=request_settings,
        )

        assert "SAMPLE 0.1" in clickhouse_query.format_sql()

    @patch("snuba.settings.TURBO_SAMPLE_RATE", 0.2)
    def test_when_sample_is_not_provided_with_turbo(self):
        source = self.dataset.get_all_storages()[0].get_schema().get_data_source()
        query = Query({"conditions": [], "aggregations": [], "groupby": []}, source,)
        request_settings = HTTPRequestSettings(turbo=True)

        clickhouse_query = DictSqlQuery(
            dataset=self.dataset, query=query, settings=request_settings,
        )

        assert "SAMPLE 0.2" in clickhouse_query.format_sql()

    def test_when_sample_is_not_provided_without_turbo(self):
        source = self.dataset.get_all_storages()[0].get_schema().get_data_source()
        query = Query({"conditions": [], "aggregations": [], "groupby": []}, source,)
        request_settings = HTTPRequestSettings()

        clickhouse_query = DictSqlQuery(
            dataset=self.dataset, query=query, settings=request_settings,
        )

        assert "SAMPLE" not in clickhouse_query.format_sql()
