import React from "react";
import Client from "../api_client";
import { Table } from "../table";
import QueryDisplay from "./query_display";
import { QueryResult } from "./types";

function ClickhouseQueries(props: { api: Client }) {
  function tablePopulator(queryResult: QueryResult) {
    return (
      <Table headerData={queryResult.column_names} rowData={queryResult.rows} />
    );
  }
  return QueryDisplay({
    api: props.api,
    resultDataPopulator: tablePopulator,
  });
}

export default ClickhouseQueries;
