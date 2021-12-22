import React, { useEffect, useState } from "react";
import Client from "../api_client";
import { Table } from "../table";

import { ClickhouseNodeData, QueryRequest, QueryResult } from "./types";

type QueryState = Partial<QueryRequest>;

function TracingQueries(props: { api: Client }) {
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

  function selectQuery(query: string) {
    setQuery((prevQuery) => {
      return {
        ...prevQuery,
        query_name: "tracing",
        sql: query,
      };
    });
  }

  function executeQuery() {
    props.api
      .executeQuery(query as QueryRequest, "clickhouse_trace_query")
      .then((result) => {
        result.input_query = `${query.storage},${query.host}:${query.port} =>\n${query.query_name}`;
        setQueryResultHistory((prevHistory) => [result, ...prevHistory]);
      });
  }

  return (
    <div>
      <form>
        <h2>Construct a query</h2>
        <select
          value={query.storage || ""}
          onChange={(evt) => selectStorage(evt.target.value)}
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
        {query.storage && (
          <select
            value={
              query.host && query.port ? `${query.host}:${query.port}` : ""
            }
            onChange={(evt) => selectHost(evt.target.value)}
          >
            <option disabled value="">
              Select a host
            </option>
            {nodeData
              .find((el) => el.storage_name === query.storage)
              ?.local_nodes.map((node, nodeIndex) => (
                <option
                  key={`${node.host}:${node.port}`}
                  value={`${node.host}:${node.port}`}
                >
                  {node.host}:{node.port}
                </option>
              ))}
          </select>
        )}
        {query.storage && query.host && query.port && (
          <input
            type="text"
            onChange={(evt) => selectQuery(evt.target.value)}
          />
        )}
        {query.storage && query.host && query.port && query.query_name && (
          <button onClick={(_) => executeQuery()}>Execute query</button>
        )}
      </form>
      <div>
        <h2>Query results</h2>
        <Table
          headerData={["Query", "Response", "Error"]}
          rowData={queryResultHistory.map((queryResult) => [
            <span>{queryResult.input_query}</span>,
            <span>
              {queryResult.trace_output?.split("\n").map((line) => (
                <p>{line}</p>
              ))}
            </span>,
            <span>{queryResult.error || ""}</span>,
          ])}
        />
      </div>
    </div>
  );
}

export default TracingQueries;
