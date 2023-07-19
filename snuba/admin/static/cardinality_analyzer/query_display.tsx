import React, { useState } from "react";
import Client from "../api_client";
import { Collapse } from "../collapse";
import QueryEditor from "../query_editor";

import { CardinalityQueryRequest, CardinalityQueryResult, PredefinedQuery } from "./types";

enum ClipboardFormats {
  CSV = "csv",
  JSON = "json",
}
type QueryState = Partial<CardinalityQueryRequest>;

function QueryDisplay(props: {
  api: Client;
  resultDataPopulator: (queryResult: CardinalityQueryResult) => JSX.Element;
  predefinedQueryOptions: Array<PredefinedQuery>;
}) {
  const [query, setQuery] = useState<QueryState>({});
  const [queryResultHistory, setCardinalityQueryResultHistory] = useState<CardinalityQueryResult[]>([]);

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
      .executeCardinalityQuery(query as CardinalityQueryRequest)
      .then((result) => {
        result.input_query = query.sql || "<Input Query>";
        setCardinalityQueryResultHistory((prevHistory) => [result, ...prevHistory]);
      })
      .catch((err) => {
        console.log("ERROR", err);
        window.alert("An error occurred: " + err.error.message);
      });
  }

  function convertResultsToCSV(queryResult: CardinalityQueryResult) {
    let output = queryResult.column_names.join(",");
    for (const row of queryResult.rows) {
      const escaped = row.map((v) => (typeof v == "string" && v.includes(",") ? '"' + v + '"' : v));
      output = output + "\n" + escaped.join(",");
    }
    return output;
  }

  function copyText(queryResult: CardinalityQueryResult, format: ClipboardFormats) {
    let formatter: (input: CardinalityQueryResult) => string = (s) => s.toString();

    if (format === ClipboardFormats.JSON) {
      formatter = JSON.stringify;
    }

    if (format === ClipboardFormats.CSV) {
      formatter = convertResultsToCSV;
    }

    window.navigator.clipboard.writeText(formatter(queryResult));
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
        <div>
          <button
            onClick={(evt) => {
              evt.preventDefault();
              executeQuery();
            }}
            style={executeButtonStyle}
            disabled={!query.sql}
          >
            Execute Query
          </button>
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
                  <button style={executeButtonStyle} onClick={() => copyText(queryResult, ClipboardFormats.JSON)}>
                    Copy to clipboard (JSON)
                  </button>
                </p>
                <p>
                  <button style={executeButtonStyle} onClick={() => copyText(queryResult, ClipboardFormats.CSV)}>
                    Copy to clipboard (CSV)
                  </button>
                </p>
                {props.resultDataPopulator(queryResult)}
              </div>
            );
          }

          return (
            <Collapse key={idx} text={queryResult.input_query}>
              <button style={executeButtonStyle} onClick={() => copyText(queryResult, ClipboardFormats.JSON)}>
                Copy to clipboard (JSON)
              </button>
              <button style={executeButtonStyle} onClick={() => copyText(queryResult, ClipboardFormats.CSV)}>
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

export default QueryDisplay;
