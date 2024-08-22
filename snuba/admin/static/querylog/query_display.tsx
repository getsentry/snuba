import React, { useState } from "react";
import { Button } from "@mantine/core";
import Client from "SnubaAdmin/api_client";
import { Collapse } from "SnubaAdmin/collapse";
import QueryEditor from "SnubaAdmin/query_editor";

import { QuerylogRequest, QuerylogResult, PredefinedQuery } from "./types";
import ExecuteButton from "SnubaAdmin/utils/execute_button";

type QueryState = Partial<QuerylogRequest>;

function QueryDisplay(props: {
  api: Client;
  resultDataPopulator: (queryResult: QuerylogResult) => JSX.Element;
  predefinedQueryOptions: Array<PredefinedQuery>;
}) {
  const [query, setQuery] = useState<QueryState>({});
  const [queryResultHistory, setQueryResultHistory] = useState<
    QuerylogResult[]
  >([]);

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

  function copyText(queryResult: QuerylogResult, format: string) {
    const formatter = format == "csv" ? convertResultsToCSV : JSON.stringify;
    window.navigator.clipboard.writeText(formatter(queryResult));
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
                <p>
                  <button
                    style={executeButtonStyle}
                    onClick={() => copyText(queryResult, "json")}
                  >
                    Copy to clipboard (JSON)
                  </button>
                </p>
                <p>
                  <button
                    style={executeButtonStyle}
                    onClick={() => copyText(queryResult, "csv")}
                  >
                    Copy to clipboard (CSV)
                  </button>
                </p>
                {props.resultDataPopulator(queryResult)}
              </div>
            );
          }

          return (
            <Collapse key={idx} text={queryResult.input_query}>
              <button
                style={executeButtonStyle}
                onClick={() => copyText(queryResult, "json")}
              >
                Copy to clipboard (JSON)
              </button>
              <button
                style={executeButtonStyle}
                onClick={() => copyText(queryResult, "csv")}
              >
                Copy to clipboard (CSV)
              </button>
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

const executeButtonStyle = {
  height: 30,
  border: 0,
  padding: "4px 20px",
  marginRight: 10,
};

const selectStyle = {
  marginRight: 8,
  height: 30,
};

function TextArea(props: {
  value: string;
  onChange: (nextValue: string) => void;
}) {
  const { value, onChange } = props;
  return (
    <textarea
      spellCheck={false}
      value={value}
      onChange={(evt) => onChange(evt.target.value)}
      style={{ width: "100%", height: 140 }}
      placeholder={"Write your query here"}
    />
  );
}

const queryDescription = {
  minHeight: 10,
  width: "auto",
  fontSize: 16,
  padding: "10px 5px",
};
export default QueryDisplay;
