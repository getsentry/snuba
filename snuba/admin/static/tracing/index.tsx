import React, { useEffect, useState } from "react";
import Client from "../api_client";
import { TracingRequest, TracingResult } from "./types";
import { Table } from "../table";

type QueryState = Partial<TracingRequest>;

function TracingQueries(props: { api: Client }) {
  const [storages, setStorages] = useState<string[]>([]);
  const [query, setQuery] = useState<QueryState>({});
  const [queryResultHistory, setQueryResultHistory] = useState<TracingResult[]>(
    []
  );
  const [isExecuting, setIsExecuting] = useState<boolean>(false);
  const endpoint = "clickhouse_trace_query";

  useEffect(() => {
    props.api.getClickhouseNodes().then((res) => {
      setStorages(res.map((n) => n.storage_name));
    });
  }, []);

  function selectStorage(storage: string) {
    setQuery((prevQuery) => {
      return {
        ...prevQuery,
        storage: storage,
      };
    });
  }

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
          trace_output: result.trace_output,
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

  function copyText(text: string) {
    window.navigator.clipboard.writeText(text);
  }

  function tablePopulator(queryResult: TracingResult) {
    var elements = {};
    if (queryResult.error) {
      elements = { Error: [queryResult.error, 200] };
    } else {
      elements = { Trace: [queryResult.trace_output, 400] };
    }
    return tracingOutput(elements);
  }

  function tracingOutput(elements: Object) {
    return (
      <>
        <br />
        <br />
        {Object.entries(elements).map(([title, [value, height]]) => {
          return (
            <div key={value}>
              {title}
              <textarea
                spellCheck={false}
                value={value}
                style={{ width: "100%", height: height }}
                disabled
              />
            </div>
          );
        })}
      </>
    );
  }

  return (
    <div>
      <form>
        <h2>Construct a query</h2>
        <div>
          <TextArea value={query.sql || ""} onChange={updateQuerySql} />
        </div>
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
      </form>
      <div>
        <h2>Query results</h2>
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
              {tablePopulator(queryResult)}
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
      style={{ width: "100%", height: 100 }}
      placeholder={"Write your query here"}
    />
  );
}

export default TracingQueries;
