import React, { useState } from "react";
import Client from "../api_client";
import { Collapse } from "../collapse";

import { QuerylogRequest, QuerylogResult, QuerylogSchemaResult } from "./types";

type QueryState = Partial<QuerylogRequest>;

function QueryDisplay(props: {
  api: Client;
  resultDataPopulator: (queryResult: QuerylogResult) => JSX.Element;
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
    props.api
      .executeQuerylogQuery(query as QuerylogRequest)
      .then((result) => {
        result.input_query = query.sql || "<Input Query>";
        setQueryResultHistory((prevHistory) => [result, ...prevHistory]);
      })
      .catch((err) => {
        console.log("ERROR", err);
        window.alert("An error occurred: " + err.error);
      });
  }

  function copyText(text: string) {
    window.navigator.clipboard.writeText(text);
  }

  return (
    <div>
      <form>
        <h2>Construct a Querylog Query</h2>
        <div>
          <TextArea value={query.sql || ""} onChange={updateQuerySql} />
        </div>
        <div style={executeActionsStyle}>
          <div>
            <button
              onClick={(evt) => {
                evt.preventDefault();
                executeQuery();
              }}
              style={executeButtonStyle}
              disabled={!query.sql}
            >
              Execute query
            </button>
          </div>
        </div>
      </form>
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
                    onClick={() => copyText(JSON.stringify(queryResult))}
                  >
                    Copy to clipboard
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
                onClick={() => copyText(JSON.stringify(queryResult))}
              >
                Copy to clipboard
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
