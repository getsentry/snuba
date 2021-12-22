import React from "react";
import Client from "../api_client";
import QueriesDisplay from "../components/query_display/query_display";
import { QueryResult } from "../components/query_display/types";

function TracingQueries(props: { api: Client }) {
  function tablePopulator(queryResult: QueryResult) {
    return (
      <>
        <br />
        <br />
        <span>{queryResult.input_query}</span>,
        <span>
          {queryResult.trace_output?.split("\n").map((line) => (
            <p>{line}</p>
          ))}
        </span>
      </>
    );
  }
  return QueriesDisplay({
    api: props.api,
    endpoint: "clickhouse_trace_query",
    resultDataPopulator: tablePopulator,
  });
}

export default TracingQueries;
