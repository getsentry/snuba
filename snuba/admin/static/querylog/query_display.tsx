import React, { useState } from "react";
import { Button } from "@mantine/core";
import Client from "SnubaAdmin/api_client";
import { Collapse } from "SnubaAdmin/collapse";
import QueryEditor from "SnubaAdmin/query_editor";
import { getRecentHistory, setRecentHistory } from "SnubaAdmin/query_history";

import { QuerylogRequest, QuerylogResult, PredefinedQuery } from "./types";
import ExecuteButton from "SnubaAdmin/utils/execute_button";
import QueryResultCopier from "SnubaAdmin/utils/query_result_copier";

type QueryState = Partial<QuerylogRequest>;
const HISTORY_KEY = "querylog";

function QueryDisplay(props: {
  api: Client;
  resultDataPopulator: (queryResult: QuerylogResult) => JSX.Element;
  predefinedQueryOptions: Array<PredefinedQuery>;
}) {
  const [query, setQuery] = useState<QueryState>({});
  const [queryResultHistory, setQueryResultHistory] = useState<
    QuerylogResult[]
  >(getRecentHistory(HISTORY_KEY));

  function updateQuerySql(sql: string) {
    setQuery((prevQuery) => {
      return {
        ...prevQuery,
        sql,
      };
    });
  }

  function executeQuery() {
    return props.api
      .executeQuerylogQuery(query as QuerylogRequest)
      .then((result) => {
        result.input_query = query.sql || "<Input Query>";
        setRecentHistory(HISTORY_KEY, result);
        setQueryResultHistory((prevHistory) => [result, ...prevHistory]);
      });
  }

  function getQuerylogSchema() {
    props.api
      .getQuerylogSchema()
      .then((result) => {
        result.input_query = "View Querylog Schema";
        setQueryResultHistory((prevHistory) => [result, ...prevHistory]);
      })
      .catch((err) => {
        console.log("ERROR", err);
        window.alert("An error occurred: " + err.error.message);
      });
  }

  function convertResultsToCSV(queryResult: QuerylogResult) {
    let output = queryResult.column_names.join(",");
    for (const row of queryResult.rows) {
      const escaped = row.map((v) =>
        typeof v == "string" && v.includes(",") ? '"' + v + '"' : v
      );
      output = output + "\n" + escaped.join(",");
    }
    return output;
  }

  return (
    <div>
      <h2>Construct a Querylog Query</h2>
      <QueryEditor
        onQueryUpdate={(sql) => {
          updateQuerySql(sql);
        }}
        predefinedQueryOptions={props.predefinedQueryOptions}
      />
      <div style={executeActionsStyle}>
        <div>
          <ExecuteButton onClick={executeQuery} disabled={!query.sql} />
          <Button
            onClick={(evt: any) => {
              evt.preventDefault();
              getQuerylogSchema();
            }}
          >
            View Querylog Schema
          </Button>
        </div>
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

export default QueryDisplay;
