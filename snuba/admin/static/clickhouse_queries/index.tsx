import React from "react";
import Client from "../api_client";
import { Table } from "../table";
import QueryDisplay from "../components/query_display/query_display";
import { QueryResult } from "../components/query_display/types";

function ClickhouseQueries(props: { api: Client }) {
  function tablePopulator(queryResult: QueryResult) {
    return (
      <Table headerData={queryResult.column_names} rowData={queryResult.rows} />
    );
  }
  return QueryDisplay({
    api: props.api,
    endpoint: "run_clickhouse_system_query",
    resultDataPopulator: tablePopulator,
  });
}

export default ClickhouseQueries;
