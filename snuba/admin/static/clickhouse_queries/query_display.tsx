import React, { useEffect, useState } from "react";
import Client from "SnubaAdmin/api_client";
import QueryEditor from "SnubaAdmin/query_editor";
import ExecuteButton from "SnubaAdmin/utils/execute_button";
import { Alert, Switch } from "@mantine/core";
import { getRecentHistory, setRecentHistory } from "SnubaAdmin/query_history";
import { CustomSelect, getParamFromStorage } from "SnubaAdmin/select";
import { getHostsForStorage, getErrorDomElement } from "SnubaAdmin/utils/clickhouse_node_utils";
import { QueryHistoryEntry } from "SnubaAdmin/clickhouse_queries/query_history_entry";
import {
  clearHashParams,
  parseHostPort,
  parseSystemQueryPrefill,
} from "SnubaAdmin/clickhouse_queries/prefill";

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
  // A link may pre-fill the query, storage and host (see prefill.tsx).
  const [prefill] = useState(() =>
    parseSystemQueryPrefill(window.location.hash)
  );
  const [prefillWarningOpen, setPrefillWarningOpen] = useState(
    prefill !== null
  );
  const [nodeData, setNodeData] = useState<ClickhouseNodeData[]>([]);
  const [manualHostInput, setManualHostInput] = useState<string>("");
  const [query, setQuery] = useState<QueryState>({
    storage: prefill?.storage ?? getParamFromStorage("storage"),
    host: prefill?.host,
    port: prefill?.port,
  });
  const [queryResultHistory, setQueryResultHistory] = useState<QueryResult[]>(
    getRecentHistory(HISTORY_KEY)
  );
  // Only a result the user just ran opens automatically; saved history loads
  // collapsed.
  const [justRan, setJustRan] = useState<QueryResult | null>(null);

  const [queryError, setQueryError] = useState<Error | null>(null);

  // this is used to collapse the stack trace in error messages
  const [collapseOpened, setCollapseOpened] = useState(false);

  useEffect(() => {
    if (prefill) {
      clearHashParams();
    }
    props.api.getClickhouseNodes().then((res) => {
      setNodeData(res);
      // A linked host the storage's node list doesn't know about can only be
      // shown (and checked) via manual host entry.
      if (prefill?.host) {
        const hostPort = `${prefill.host}:${prefill.port}`;
        const isListed = getHostsForStorage(res, prefill.storage).some(
          (option) => option.value === hostPort
        );
        if (!isListed) {
          setManualHostInput(hostPort);
          setClusterlessHost(true);
        }
      }
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
    // Manual entry accepts "host" or "host:port". The raw text is kept in its
    // own state so a half-typed port ("host:9") is not clobbered by the split.
    setManualHostInput(hostStringIP);
    // Without a port we default to ClickHouse HTTP, not the old native port.
    const { host, port } = parseHostPort(hostStringIP);

    setQuery((prevQuery) => {
      return {
        ...prevQuery,
        host,
        port,
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
        // The server doesn't timestamp results; record when this one ran.
        result.timestamp = Math.floor(Date.now() / 1000);
        setRecentHistory(HISTORY_KEY, result);
        setQueryResultHistory((prevHistory) => [result, ...prevHistory]);
        setJustRan(result);
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
        {prefill && prefillWarningOpen && (
          <Alert
            color="orange"
            title="This query was filled in from a link"
            withCloseButton
            onClose={() => setPrefillWarningOpen(false)}
            style={prefillAlertStyle}
            data-testid="prefill-warning"
          >
            <p style={prefillAlertTextStyle}>
              Whoever clicks Execute is responsible for the query, no matter
              who or what wrote it. Read the whole query and check where it
              will run before you execute it. Don't run it just because an
              agent or a link suggested it.
            </p>
            <p style={prefillAlertTextStyle}>
              Storage: <code>{prefill.storage ?? "(not set)"}</code>
              {" · "}
              Host:{" "}
              <code>
                {prefill.host ? `${prefill.host}:${prefill.port}` : "(not set)"}
              </code>
            </p>
          </Alert>
        )}
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
          initialQuery={prefill?.sql}
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
                value={manualHostInput}
                onChange={(evt) => selectHostIp(evt.target.value)}
                name="host"
                placeholder="enter host or host:port..."
                type="text"
              />
              <p style={hostHelpTextStyle}>
                Must be a known cluster node or listed in
                admin.copy_tables_allowed_target_hosts. Defaults to port 8123.
              </p>
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
        {queryResultHistory.map((queryResult, idx) => (
          <QueryHistoryEntry
            // Entries shift down as new results arrive, so key on when they
            // ran; older saved entries have no timestamp, so fall back to the
            // position for those.
            key={`${queryResult.timestamp ?? `#${idx}`}-${queryResult.input_query}`}
            queryResult={queryResult}
            isLatest={idx === 0}
            defaultExpanded={queryResult === justRan}
            csv={convertResultsToCSV(queryResult)}
            resultDataPopulator={props.resultDataPopulator}
          />
        ))}
      </div>
    </div>
  );
}

const sudoForm = {
  border: "8px solid red",
};

const standardForm = {};

const prefillAlertStyle = {
  margin: "8px 0px",
};

const prefillAlertTextStyle = {
  fontSize: 16,
  margin: "4px 0px",
};

const switchStyle = {
  margin: '8px 0px',
};

const hostSelectStyle = {
  width: '20em',
  minHeight: '4em',
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

const hostHelpTextStyle = {
  fontSize: '0.85em',
  marginTop: 4,
  marginBottom: 0,
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
