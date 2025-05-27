import React, { useState } from "react";
import Client from "SnubaAdmin/api_client";
import { Collapse } from "SnubaAdmin/collapse";
import { CSV } from "SnubaAdmin/cardinality_analyzer/CSV";
import QueryEditor from "SnubaAdmin/query_editor";
import ExecuteButton from "SnubaAdmin/utils/execute_button";
import { getRecentHistory, setRecentHistory } from "SnubaAdmin/query_history";

import {
  CardinalityQueryRequest,
  CardinalityQueryResult,
  PredefinedQuery,
} from "SnubaAdmin/cardinality_analyzer/types";
import QueryResultCopier from "SnubaAdmin/utils/query_result_copier";

enum ClipboardFormats {
  CSV = "csv",
  JSON = "json",
}
type QueryState = Partial<CardinalityQueryRequest>;

const HISTORY_KEY = "cardinality_analyzer";
function QueryDisplay(props: {
  api: Client;
  resultDataPopulator: (queryResult: CardinalityQueryResult) => JSX.Element;
  predefinedQueryOptions: Array<PredefinedQuery>;
}) {
  const [query, setQuery] = useState<QueryState>({});
  const [queryResultHistory, setCardinalityQueryResultHistory] = useState<
    CardinalityQueryResult[]
  >(getRecentHistory(HISTORY_KEY));

  function updateQuerySql(sql: string) {
    setQuery((prevQuery) => {
      return {
        ...prevQuery,
        sql,
      };
    });
  }

  function convertResultsToCSV(queryResult: CardinalityQueryResult) {
    return CSV.sheet([queryResult.column_names, ...queryResult.rows]);
  }

  function executeQuery() {
    return props.api
      .executeCardinalityQuery(query as CardinalityQueryRequest)
      .then((result) => {
        result.input_query = query.sql || "<Input Query>";
        setRecentHistory(HISTORY_KEY, result);
        setCardinalityQueryResultHistory((prevHistory) => [
          result,
          ...prevHistory,
        ]);
      });
  }

  return (
    <div>
      <h2>Construct a Metrics Query</h2>
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

const copyButtonStyle = {
  height: 30,
  border: 0,
  padding: "4px 20px",
  marginRight: 10,
};

export default QueryDisplay;
