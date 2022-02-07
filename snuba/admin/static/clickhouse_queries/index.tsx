import React from "react";
import Client from "../api_client";
import { Table } from "../table";
import QueryDisplay from "./query_display";
import { QueryResult } from "./types";

function ClickhouseQueries(props: { api: Client }) {
  function tablePopulator(queryResult: QueryResult) {
    return (
      <div style={scroll}>
        <Table
          headerData={queryResult.column_names}
          rowData={queryResult.rows}
        />
      </div>
    );
  }
  return QueryDisplay({
    api: props.api,
    resultDataPopulator: tablePopulator,
  });
}

const scroll = {
  overflowX: "scroll" as const,
  width: "100%",
};

export default ClickhouseQueries;
