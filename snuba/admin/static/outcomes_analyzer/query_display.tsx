import React, { useState } from "react";
import Client from "SnubaAdmin/api_client";
import { Collapse } from "SnubaAdmin/collapse";
import { CSV } from "SnubaAdmin/utils/CSV";
import QueryEditor from "SnubaAdmin/query_editor";
import ExecuteButton from "SnubaAdmin/utils/execute_button";
import { getRecentHistory, setRecentHistory } from "SnubaAdmin/query_history";

import {
  OutcomesQueryRequest,
  OutcomesQueryResult,
  PredefinedQuery,
} from "SnubaAdmin/outcomes_analyzer/types";
import QueryResultCopier from "SnubaAdmin/utils/query_result_copier";

type QueryState = Partial<OutcomesQueryRequest>;

const HISTORY_KEY = "outcomes_analyzer";
function QueryDisplay(props: {
  api: Client;
  resultDataPopulator: (queryResult: OutcomesQueryResult) => JSX.Element;
  predefinedQueryOptions: Array<PredefinedQuery>;
}) {
  const [query, setQuery] = useState<QueryState>({});
  const [queryResultHistory, setOutcomesQueryResultHistory] = useState<
    OutcomesQueryResult[]
  >(getRecentHistory(HISTORY_KEY));

  function updateQuerySql(sql: string) {
    setQuery((prevQuery) => {
      return {
        ...prevQuery,
        sql,
      };
    });
  }

  function convertResultsToCSV(queryResult: OutcomesQueryResult) {
    return CSV.sheet([queryResult.column_names, ...queryResult.rows]);
  }

  function executeQuery() {
    return props.api
      .executeOutcomesQuery(query as OutcomesQueryRequest)
      .then((result) => {
        result.input_query = query.sql || "<Input Query>";
        setRecentHistory(HISTORY_KEY, result);
        setOutcomesQueryResultHistory((prevHistory) => [result, ...prevHistory]);
      });
  }

  return (
    <div>
      <h2>Outcomes Investigation Query</h2>
      <p style={helpStyle}>
        Query <code>outcomes_hourly_dist</code> to investigate volume spikes by
        category, org, project, outcome, and reason. Prefer hourly over raw —
        it is orders of magnitude cheaper. Common categories:{" "}
        <code>4</code> attachment bytes, <code>7</code> replay,{" "}
        <code>22</code> attachment count, <code>1</code> error,{" "}
        <code>2</code> transaction, <code>10</code> span. Outcomes:{" "}
        <code>0</code> accepted, <code>1</code> filtered, <code>2</code> rate
        limited, <code>3</code> invalid, <code>4</code> abuse,{" "}
        <code>5</code> client discard.
      </p>
      <QueryEditor
        onQueryUpdate={(sql) => {
          updateQuerySql(sql);
        }}
        predefinedQueryOptions={props.predefinedQueryOptions}
      />
      <div style={executeActionsStyle}>
        <ExecuteButton onClick={executeQuery} disabled={!query.sql} />
      </div>
      <div>
        <h2>Query results</h2>
        {queryResultHistory.map((queryResult, idx) => {
          if (idx === 0) {
            return (
              <div key={idx}>
                <p>{queryResult.input_query}</p>
                <QueryResultCopier
                  jsonInput={JSON.stringify(queryResult)}
                  csvInput={convertResultsToCSV(queryResult)}
                />
                {props.resultDataPopulator(queryResult)}
              </div>
            );
          }

          return (
            <Collapse key={idx} text={queryResult.input_query}>
              <QueryResultCopier
                jsonInput={JSON.stringify(queryResult)}
                csvInput={convertResultsToCSV(queryResult)}
              />
              {props.resultDataPopulator(queryResult)}
            </Collapse>
          );
        })}
      </div>
    </div>
  );
}

const executeActionsStyle = {
  display: "flex",
  justifyContent: "space-between",
  marginTop: 8,
};

const helpStyle = {
  fontSize: 14,
  maxWidth: 900,
  lineHeight: 1.4,
};

export default QueryDisplay;
