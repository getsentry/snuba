import React, { useEffect, useState } from "react";
import { Switch } from "@mantine/core";
import Client from "../api_client";
import QueryEditor from "../query_editor";
import { Table } from "../table";

import {
  LogLine,
  TracingRequest,
  TracingResult,
  PredefinedQuery,
} from "./types";

type QueryState = Partial<TracingRequest>;

function QueryDisplay(props: {
  api: Client;
  resultDataPopulator: (
    queryResult: TracingResult,
    showFormatted: boolean
  ) => JSX.Element;
  predefinedQueryOptions: Array<PredefinedQuery>;
}) {
  const [storages, setStorages] = useState<string[]>([]);
  const [query, setQuery] = useState<QueryState>({});
  const [queryResultHistory, setQueryResultHistory] = useState<TracingResult[]>(
    []
  );
  const [isExecuting, setIsExecuting] = useState<boolean>(false);
  const [showFormatted, setShowFormatted] = useState<boolean>(false);

  useEffect(() => {
    props.api.getClickhouseNodes().then((res) => {
      setStorages(res.map((n) => n.storage_name));
    });
  }, []);

  function updateQuerySql(sql: string) {
    setQuery((prevQuery) => {
      return {
        ...prevQuery,
        sql,
      };
    });
  }

  function executeQuery() {
    if (isExecuting) {
      window.alert("A query is already running");
    }
    setIsExecuting(true);
    props.api
      .executeTracingQuery(query as TracingRequest)
      .then((result) => {
        const tracing_result = {
          input_query: `${query.sql}`,
          timestamp: result.timestamp,
          num_rows_result: result.num_rows_result,
          cols: result.cols,
          trace_output: result.trace_output,
          formatted_trace_output: result.formatted_trace_output,
          error: result.error,
        };
        setQueryResultHistory((prevHistory) => [
          tracing_result,
          ...prevHistory,
        ]);
      })
      .catch((err) => {
        console.log("ERROR", err);
        window.alert("An error occurred: " + err.error.message);
      })
      .finally(() => {
        setIsExecuting(false);
      });
  }

  function selectStorage(storage: string) {
    setQuery((prevQuery) => {
      return {
        ...prevQuery,
        storage: storage,
      };
    });
    console.log(query);
  }

  function copyText(text: string) {
    window.navigator.clipboard.writeText(text);
  }

  return (
    <div>
      <h2>Construct a ClickHouse Query</h2>
      <a href="https://getsentry.github.io/snuba/clickhouse/death_queries.html">
        🛑 WARNING! BEFORE RUNNING QUERIES, READ THIS 🛑
      </a>
      <QueryEditor
        onQueryUpdate={(sql) => {
          updateQuerySql(sql);
        }}
        predefinedQueryOptions={props.predefinedQueryOptions}
      />
      <div style={executeActionsStyle}>
        <div>
          <select
            value={query.storage || ""}
            onChange={(evt) => selectStorage(evt.target.value)}
            style={selectStyle}
          >
            <option disabled value="">
              Select a storage
            </option>
            {storages.map((storage) => (
              <option key={storage} value={storage}>
                {storage}
              </option>
            ))}
          </select>
        </div>
        <div>
          <button
            onClick={(_) => executeQuery()}
            style={executeButtonStyle}
            disabled={isExecuting || !query.storage || !query.sql}
          >
            Execute query
          </button>
        </div>
      </div>
      <div>
        <h2>Query results</h2>
        <Switch
          checked={showFormatted}
          onChange={(evt: React.ChangeEvent<HTMLInputElement>) =>
            setShowFormatted(evt.currentTarget.checked)
          }
          onLabel="FORMATTED"
          offLabel="RAW"
          size="xl"
        />
        <Table
          headerData={["Query", "Response"]}
          rowData={queryResultHistory.map((queryResult) => [
            <span>{queryResult.input_query}</span>,
            <div>
              <button
                style={executeButtonStyle}
                onClick={() => copyText(JSON.stringify(queryResult))}
              >
                Copy to clipboard
              </button>
              {props.resultDataPopulator(queryResult, showFormatted)}
            </div>,
          ])}
          columnWidths={[1, 5]}
        />
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
