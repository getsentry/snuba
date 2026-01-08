import React, { useEffect, useState } from "react";
import Client from "SnubaAdmin/api_client";
import { Collapse } from "SnubaAdmin/collapse";
import QueryEditor from "SnubaAdmin/query_editor";
import ExecuteButton from "SnubaAdmin/utils/execute_button";
import QueryResultCopier from "SnubaAdmin/utils/query_result_copier";
import { Switch } from "@mantine/core";
import { getRecentHistory, setRecentHistory } from "SnubaAdmin/query_history";
import { CustomSelect, getParamFromStorage } from "SnubaAdmin/select";
import { getHostsForStorage, getErrorDomElement } from "SnubaAdmin/utils/clickhouse_node_utils";

import {
  ClickhouseNodeData,
  QueryRequest,
  QueryResult,
  PredefinedQuery,
} from "SnubaAdmin/clickhouse_queries/types";

type QueryState = Partial<QueryRequest>;

const HISTORY_KEY = "clickhouse_queries";
function QueryDisplay(props: {
  api: Client;
  resultDataPopulator: (queryResult: QueryResult) => JSX.Element;
  predefinedQueryOptions: Array<PredefinedQuery>;
}) {
  const [nodeData, setNodeData] = useState<ClickhouseNodeData[]>([]);
  const [query, setQuery] = useState<QueryState>({
    storage: getParamFromStorage("storage"),
  });
  const [queryResultHistory, setQueryResultHistory] = useState<QueryResult[]>(
    getRecentHistory(HISTORY_KEY)
  );

  const [queryError, setQueryError] = useState<Error | null>(null);

  // this is used to collapse the stack trace in error messages
  const [collapseOpened, setCollapseOpened] = useState(false);

  useEffect(() => {
    props.api.getClickhouseNodes().then((res) => {
      setNodeData(res);
    });
  }, []);

  function selectStorage(storage: string) {
    setQuery((prevQuery) => {
      // clear old host port
      delete prevQuery.host;
      delete prevQuery.port;

      return {
        ...prevQuery,
        storage: storage,
      };
    });
  }

  function setSudo(value: boolean) {
    setQuery((prevQuery) => {
      return {
        ...prevQuery,
        sudo: value,
      };
    });
  }

  function setClusterlessHost(value: boolean) {
    setQuery((prevQuery) => {
      return {
        ...prevQuery,
        clusterless: value,
      };
    });
  }

  function selectHostIp(hostStringIP: string) {
    setQuery((prevQuery) => {
      return {
        ...prevQuery,
        host: hostStringIP,
        port: 9000,
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
    return props.api
      .executeSystemQuery(query as QueryRequest)
      .then((result) => {
        setQueryError(null);  // clear any previous error
        result.input_query = `${query.sql} (${query.storage},${query.host}:${query.port})`;
        setRecentHistory(HISTORY_KEY, result);
        setQueryResultHistory((prevHistory) => [result, ...prevHistory]);
      });
  }

  function handleQueryError(error: Error) {
    setQueryError(error);
  }

  function convertResultsToCSV(queryResult: QueryResult) {
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
      <form style={query.sudo ? sudoForm : standardForm}>
        <h2>Construct a ClickHouse System Query</h2>
        <div style={switchStyle}>
          <Switch
            checked={query.sudo}
            onChange={(evt: React.ChangeEvent<HTMLInputElement>) =>
              setSudo(evt.currentTarget.checked)
            }
            onLabel="Sudo mode"
            offLabel="User mode"
            size="xl"
          />
        </div>
        <div style={switchStyle}>
          <Switch
            checked={query.clusterless}
            onChange={(evt: React.ChangeEvent<HTMLInputElement>) =>
              setClusterlessHost(evt.currentTarget.checked)
            }
            onLabel="Manual Host Entry"
            offLabel="Auto Populate Nodes"
            size="xl"
          />
        </div>
        <QueryEditor
          onQueryUpdate={(sql) => {
            updateQuerySql(sql);
          }}
          predefinedQueryOptions={props.predefinedQueryOptions}
        />
        <div style={executeActionsStyle}>
          {query.clusterless ?
            (<div style={hostSelectStyle}>
              <CustomSelect
                value={query.storage || ""}
                onChange={selectStorage}
                name="storage"
                options={nodeData.map((storage) => storage.storage_name)}
              />
              <input
                style={inputStyle}
                id="clusterless"
                value={query.host || ""}
                onChange={(evt) => selectHostIp(evt.target.value)}
                name="host"
                placeholder="enter host ip..."
                type="text"
              />
            </div>) : (
              <div style={hostSelectStyle}>
                <CustomSelect
                  value={query.storage || ""}
                  onChange={selectStorage}
                  name="storage"
                  options={nodeData.map((storage) => storage.storage_name)}
                />
                <CustomSelect
                  disabled={!query.storage}
                  value={
                    query.host && query.port ? `${query.host}:${query.port}` : ""
                  }
                  onChange={selectHost}
                  name="Host"
                  options={getHostsForStorage(nodeData, query.storage)}
                />
              </div>
            )}
          <div>
            <ExecuteButton
              onError={handleQueryError}
              onClick={executeQuery}
              disabled={
                !query.storage || !query.host || !query.port || !query.sql
              }
            />
          </div>
        </div>
      </form>
      <div>
        {getErrorDomElement(queryError, collapseOpened, setCollapseOpened)}
      </div>
      <div>
        <h2>Query results</h2>
        {queryResultHistory.map((queryResult, idx) => {
          if (idx === 0) {
            return (
              <div key={idx}>
                <p>{queryResult.input_query}</p>
                <QueryResultCopier
                  rawInput={queryResult.trace_output || ""}
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
                rawInput={queryResult.trace_output || ""}
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

const sudoForm = {
  border: "8px solid red",
};

const standardForm = {};

const switchStyle = {
  margin: '8px 0px',
};

const hostSelectStyle = {
  width: '20em',
  height: '4em',
  margin: '8px 0px',
  display: 'flex',
  flexDirection: 'column' as const,
  justifyContent: 'space-between',
  // padding: "4px 20px",
}

const inputStyle = {
  fontSize: 16,
  minHeight: '2.25rem',
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

export default QueryDisplay;
