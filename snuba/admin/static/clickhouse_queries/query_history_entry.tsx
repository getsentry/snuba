import React from "react";
import { QueryHistoryEntry as HistoryEntry } from "SnubaAdmin/query_history_entry";
import { QueryResult } from "SnubaAdmin/clickhouse_queries/types";
import { buildSystemQueryUrl } from "SnubaAdmin/clickhouse_queries/prefill";

// History entries store the query and its target together as
// "<sql> (<storage>,<host>:<port>)"; split them back apart for display.
function parseInputQuery(inputQuery: string) {
  const match = inputQuery.match(/^([\s\S]*) \(([^,()]*),([^()]*)\)$/);
  if (!match) {
    return { sql: inputQuery, storage: null, host: null };
  }
  return { sql: match[1], storage: match[2], host: match[3] };
}

function QueryHistoryEntry(props: {
  queryResult: QueryResult;
  isLatest: boolean;
  defaultExpanded: boolean;
  csv: string;
  resultDataPopulator: (queryResult: QueryResult) => JSX.Element;
}) {
  const { sql, storage, host } = parseInputQuery(props.queryResult.input_query);
  return (
    <HistoryEntry
      {...props}
      sql={sql}
      storage={storage}
      host={host}
      shareUrl={buildSystemQueryUrl({ sql, storage, host })}
      rawOutput={props.queryResult.trace_output}
    />
  );
}

export { QueryHistoryEntry, parseInputQuery };
