import React, { useState } from "react";
import Client from "SnubaAdmin/api_client";
import { Collapse } from "SnubaAdmin/collapse";
import { CSV } from "SnubaAdmin/utils/CSV";
import QueryEditor from "SnubaAdmin/query_editor";
import ExecuteButton from "SnubaAdmin/utils/execute_button";
import { getRecentHistory, setRecentHistory } from "SnubaAdmin/query_history";
import QueryResultCopier from "SnubaAdmin/utils/query_result_copier";
import {
  OutcomesQueryRequest,
  OutcomesQueryResult,
  PredefinedQuery,
} from "SnubaAdmin/outcomes_analyzer/types";

const HISTORY_KEY = "outcomes_analyzer";

function QueryDisplay(props: {
  api: Client;
  resultDataPopulator: (queryResult: OutcomesQueryResult) => JSX.Element;
  predefinedQueryOptions: Array<PredefinedQuery>;
}) {
  const [sql, setSql] = useState("");
  const [queryResultHistory, setQueryResultHistory] = useState<
    OutcomesQueryResult[]
  >(getRecentHistory(HISTORY_KEY));

  function convertResultsToCSV(queryResult: OutcomesQueryResult) {
    return CSV.sheet([queryResult.column_names, ...queryResult.rows]);
  }

  function executeQuery() {
    return props.api
      .executeOutcomesQuery({ sql } as OutcomesQueryRequest)
      .then((result) => {
        result.input_query = sql || "<Input Query>";
        setRecentHistory(HISTORY_KEY, result);
        setQueryResultHistory((prevHistory) => [result, ...prevHistory]);
      });
  }

  function renderResult(queryResult: OutcomesQueryResult, collapsed: boolean) {
    const body = (
      <>
        <QueryResultCopier
          jsonInput={JSON.stringify(queryResult)}
          csvInput={convertResultsToCSV(queryResult)}
        />
        {props.resultDataPopulator(queryResult)}
      </>
    );

    if (collapsed) {
      return (
        <Collapse text={queryResult.input_query}>{body}</Collapse>
      );
    }

    return (
      <div>
        <p>{queryResult.input_query}</p>
        {body}
      </div>
    );
  }

  return (
    <div>
      <h2>Outcomes Investigation Query</h2>
      <p style={helpStyle}>
        Query <code>outcomes_hourly_dist</code> to investigate volume spikes by
        category, org, project, outcome, and reason. Prefer hourly over raw —
        it is orders of magnitude cheaper. Use the category dropdown for common
        DataCategory IDs (for example attachment bytes = 4, replay = 7, span =
        12). Time range supports relative lookback or absolute date-time
        pickers.
      </p>
      <QueryEditor
        onQueryUpdate={setSql}
        predefinedQueryOptions={props.predefinedQueryOptions}
      />
      <div style={executeActionsStyle}>
        <ExecuteButton onClick={executeQuery} disabled={!sql} />
      </div>
      <div>
        <h2>Query results</h2>
        {queryResultHistory.map((queryResult, idx) => (
          <React.Fragment key={idx}>
            {renderResult(queryResult, idx > 0)}
          </React.Fragment>
        ))}
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
