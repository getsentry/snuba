import React from "react";
import Client from "../api_client";
import QueriesDisplay from "../components/query_display/query_display";
import { QueryResult } from "../components/query_display/types";

function TracingQueries(props: { api: Client }) {
  function tablePopulator(queryResult: QueryResult) {
    var elements = {};
    if (queryResult.error === undefined) {
      elements = { Trace: [queryResult.trace_output, 400] };
    } else {
      elements = { Error: [queryResult.error, 200] };
    }
    return tracingOutput(elements);
  }

  function tracingOutput(elements: Object) {
    console.log(elements);

    return (
      <>
        <br />
        <br />
        {Object.entries(elements).map(([title, [value, height]]) => {
          return (
            <>
              {title}
              <textarea
                spellCheck={false}
                value={value}
                style={{ width: "100%", height: height }}
                disabled
              />
            </>
          );
        })}
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
