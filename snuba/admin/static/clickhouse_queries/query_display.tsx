import React, { useEffect, useState } from "react";
import Client from "../api_client";
import { Collapse } from "../collapse";
import { Table } from "../table";

import { ClickhouseNodeData, QueryRequest, QueryResult } from "./types";

type QueryState = Partial<QueryRequest>;

function QueryDisplay(props: {
  api: Client;
  resultDataPopulator: (queryResult: QueryResult) => JSX.Element;
}) {
  const [nodeData, setNodeData] = useState<ClickhouseNodeData[]>([]);
  const [query, setQuery] = useState<QueryState>({});
  const [queryResultHistory, setQueryResultHistory] = useState<QueryResult[]>(
    []
  );

  useEffect(() => {
    props.api.getClickhouseNodes().then((res) => {
      setNodeData(res);
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

  function selectHost(hostString: string) {
    const [host, portAsString] = hostString.split(":");

    setQuery((prevQuery) => {
      return {
        ...prevQuery,
        host: host,
        port: parseInt(portAsString, 10),
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
    props.api
      .executeSystemQuery(query as QueryRequest)
      .then((result) => {
        result.input_query = `${query.sql} (${query.storage},${query.host}:${query.port})`;
        setQueryResultHistory((prevHistory) => [result, ...prevHistory]);
      })
      .catch((err) => {
        console.log("ERROR", err);
        window.alert("An error occurred: " + err.error.message);
      });
  }

  function copyText(text: string) {
    window.navigator.clipboard.writeText(text);
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
              {nodeData.map((storage) => (
                <option key={storage.storage_name} value={storage.storage_name}>
                  {storage.storage_name}
                </option>
              ))}
            </select>
            <select
              disabled={!query.storage}
              value={
                query.host && query.port ? `${query.host}:${query.port}` : ""
              }
              onChange={(evt) => selectHost(evt.target.value)}
              style={selectStyle}
            >
              <option disabled value="">
                Select a host
              </option>
              {nodeData
                .find((el) => el.storage_name === query.storage)
                ?.local_nodes.map((node) => (
                  <option
                    key={`${node.host}:${node.port}`}
                    value={`${node.host}:${node.port}`}
                  >
                    {node.host}:{node.port}
                  </option>
                ))}
            </select>
          </div>
          <div>
            <button
              onClick={(evt) => {
                evt.preventDefault();
                executeQuery();
              }}
              style={executeButtonStyle}
              disabled={
                !query.storage || !query.host || !query.port || !query.sql
              }
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
      style={{ width: "100%", height: 100 }}
      placeholder={"Write your query here"}
    />
  );
}

export default QueryDisplay;
