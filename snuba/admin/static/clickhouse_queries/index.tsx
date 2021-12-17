import React, { useEffect, useState } from "react";
import Client from "../api_client";
import { Table } from "../table";

import {
  ClickhouseNodeData,
  ClickhouseCannedQuery,
  QueryRequest,
  QueryResult,
} from "./types";

type QueryState = Partial<QueryRequest>;

function ClickhouseQueries(props: { api: Client }) {
  const [nodeData, setNodeData] = useState<ClickhouseNodeData[]>([]);
  const [cannedQueries, setCannedQueries] = useState<ClickhouseCannedQuery[]>(
    []
  );
  const [query, setQuery] = useState<QueryState>({});
  const [queryResultHistory, setQueryResultHistory] = useState<QueryResult[]>(
    []
  );

  useEffect(() => {
    props.api.getClickhouseNodes().then((res) => {
      setNodeData(res);
    });
    props.api.getClickhouseCannedQueries().then((res) => {
      setCannedQueries(res);
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

  function selectCannedQuery(queryName: string) {
    setQuery((prevQuery) => {
      return {
        ...prevQuery,
        query_name: queryName,
      };
    });
  }

  function executeQuery() {
    props.api.executeQuery(query as QueryRequest).then((result) => {
      result.input_query = `${query.query_name}(${query.storage},${query.host}:${query.port})`;
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
          <select
            value={query.query_name || ""}
            onChange={(evt) => selectCannedQuery(evt.target.value)}
          >
            <option disabled value="">
              Select a query
            </option>
            {cannedQueries.map((cannedQuery) => (
              <option key={`${cannedQuery.name}`} value={`${cannedQuery.name}`}>
                {cannedQuery.name}: {cannedQuery.description}
              </option>
            ))}
          </select>
        )}
        {query.storage && query.host && query.port && query.query_name && (
          <button onClick={(_) => executeQuery()}>Execute query</button>
        )}
      </form>
      <div>
        <h2>Query results</h2>
        <Table
          headerData={["Query", "Response"]}
          rowData={queryResultHistory.map((queryResult) => [
            <span>{queryResult.input_query}</span>,
            <Table
              headerData={queryResult.column_names}
              rowData={queryResult.rows}
            />,
          ])}
        />
      </div>
    </div>
  );
}

export default ClickhouseQueries;
