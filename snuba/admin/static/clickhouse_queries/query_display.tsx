import React, { useEffect, useState } from "react";
import Client from "SnubaAdmin/api_client";
import QueryEditor from "SnubaAdmin/query_editor";
import ExecuteButton from "SnubaAdmin/utils/execute_button";
import { Alert, Box, SegmentedControl, Text, Tooltip } from "@mantine/core";
import { getRecentHistory, setRecentHistory } from "SnubaAdmin/query_history";
import { COLORS, SECTION_HEADING_STYLE } from "SnubaAdmin/theme";
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

  // The first thing still needed before the query can run, if any. It's
  // flagged on its field and explains why Execute is disabled.
  const missing = !query.storage
    ? "storage"
    : !query.host || !query.port
    ? "host"
    : !query.sql
    ? "query"
    : null;
  const missingHint = {
    storage: "Select a storage to run the query.",
    host: "Select a host to run the query.",
    query: "Write or pick a query to run.",
  };

  return (
    <div>
      <form>
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
        <h2 style={SECTION_HEADING_STYLE}>Construct a ClickHouse System Query</h2>
        <Section title="Where">
          <div style={targetRowStyle}>
            <div style={{ ...targetFieldStyle, width: "auto" }}>
              <Text size="sm" fw={600}>
                Host entry
              </Text>
              <SegmentedControl
                aria-label="Host entry"
                value={query.clusterless ? "manual" : "pick"}
                onChange={(value) => setClusterlessHost(value === "manual")}
                data={[
                  { label: "Pick a node", value: "pick" },
                  { label: "Enter host manually", value: "manual" },
                ]}
              />
            </div>
            <div style={targetFieldStyle}>
              <Text size="sm" fw={600}>
                Storage
              </Text>
              <div style={missing === "storage" ? needsInputStyle : undefined}>
                <CustomSelect
                  value={query.storage || ""}
                  onChange={selectStorage}
                  name="storage"
                  options={nodeData.map((storage) => storage.storage_name)}
                />
              </div>
              {missing === "storage" && (
                <p style={needsInputHintStyle}>{missingHint.storage}</p>
              )}
            </div>
            <div style={targetFieldStyle}>
              <Text size="sm" fw={600}>
                Host
              </Text>
              {query.clusterless ? (
                <>
                  <input
                    style={
                      missing === "host"
                        ? { ...inputStyle, ...needsInputStyle }
                        : inputStyle
                    }
                    id="clusterless"
                    value={manualHostInput}
                    onChange={(evt) => selectHostIp(evt.target.value)}
                    name="host"
                    placeholder="enter host or host:port..."
                    type="text"
                  />
                  {missing === "host" && (
                    <p style={needsInputHintStyle}>{missingHint.host}</p>
                  )}
                  <p style={hostHelpTextStyle}>
                    Must be a known cluster node or listed in
                    admin.copy_tables_allowed_target_hosts. Defaults to port
                    8123.
                  </p>
                </>
              ) : (
                <>
                  <div style={missing === "host" ? needsInputStyle : undefined}>
                    <CustomSelect
                      disabled={!query.storage}
                      value={
                        query.host && query.port
                          ? `${query.host}:${query.port}`
                          : ""
                      }
                      onChange={selectHost}
                      name="Host"
                      options={getHostsForStorage(nodeData, query.storage)}
                    />
                  </div>
                  {missing === "host" && (
                    <p style={needsInputHintStyle}>{missingHint.host}</p>
                  )}
                </>
              )}
            </div>
          </div>
        </Section>
        <Section title="What">
          <QueryEditor
            onQueryUpdate={(sql) => {
              updateQuerySql(sql);
            }}
            predefinedQueryOptions={props.predefinedQueryOptions}
            initialQuery={prefill?.sql}
          />
        </Section>
        <Section title="How" danger={query.sudo}>
          <div style={executeActionsStyle}>
            <div>
              <SegmentedControl
                aria-label="Mode"
                value={query.sudo ? "sudo" : "user"}
                onChange={(value) => setSudo(value === "sudo")}
                color={query.sudo ? "red" : undefined}
                data={[
                  { label: "User mode", value: "user" },
                  { label: "Sudo mode", value: "sudo" },
                ]}
                style={segmentedStyle}
              />
              <p style={modeHelpTextStyle}>
                {query.sudo
                  ? "Sudo only runs SYSTEM, ALTER, OPTIMIZE and DROP statements. It needs the sudo role, and every sudo query is audit-logged with a notification."
                  : "SELECT, SHOW and DESCRIBE. SELECTs may only read system tables."}
              </p>
            </div>
            {/* A disabled button gets no hover events, so the tooltip
                hangs off a wrapper. */}
            <Tooltip
              label={missing ? missingHint[missing] : ""}
              disabled={!missing}
              withArrow
            >
              <div data-testid="execute-wrapper">
                <ExecuteButton
                  label={query.sudo ? "Execute Sudo Query" : undefined}
                  color={query.sudo ? "red" : undefined}
                  onError={handleQueryError}
                  onClick={executeQuery}
                  disabled={missing !== null}
                />
              </div>
            </Tooltip>
          </div>
        </Section>
      </form>
      <div>
        {getErrorDomElement(queryError, collapseOpened, setCollapseOpened)}
      </div>
      <div>
        <h2 style={SECTION_HEADING_STYLE}>Query results</h2>
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

function Section(props: {
  title: string;
  // Highlights the section in red, e.g. for sudo mode.
  danger?: boolean;
  children: React.ReactNode;
}) {
  return (
    <Box
      component="fieldset"
      p="md"
      mb="md"
      style={props.danger ? dangerSectionStyle : sectionStyle}
    >
      <legend
        style={
          props.danger
            ? { ...sectionLegendStyle, color: COLORS.DANGER }
            : sectionLegendStyle
        }
      >
        {props.title}
      </legend>
      {props.children}
    </Box>
  );
}

const sectionStyle = {
  border: "1px solid #ced4da",
  borderRadius: 4,
};

const dangerSectionStyle = {
  ...sectionStyle,
  border: `2px solid ${COLORS.DANGER}`,
  backgroundColor: "rgba(168, 50, 45, 0.06)",
};

// A step smaller than the page headings, so sections read as nested.
// Bootstrap makes legends large, full-width and floated; undo that so the
// label sits in the border.
const sectionLegendStyle = {
  ...SECTION_HEADING_STYLE,
  fontSize: 12,
  float: "none" as const,
  width: "auto",
  padding: "0 4px",
  margin: 0,
};

const segmentedStyle = {
  marginBottom: 8,
};

const targetRowStyle = {
  display: "flex",
  flexWrap: "wrap" as const,
  gap: 16,
};

const targetFieldStyle = {
  width: "20em",
  display: "flex",
  flexDirection: "column" as const,
  gap: 4,
};

// Marks the field the user needs to fill in next.
const needsInputStyle = {
  outline: `2px solid ${COLORS.WARNING}`,
  outlineOffset: 1,
  borderRadius: 4,
};

const needsInputHintStyle = {
  fontSize: "0.85em",
  color: COLORS.WARNING,
  margin: 0,
};

const modeHelpTextStyle = {
  fontSize: "0.85em",
  margin: 0,
};

const prefillAlertStyle = {
  margin: "0 0 16px 0",
};

const prefillAlertTextStyle = {
  fontSize: 16,
  margin: "4px 0px",
};

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
  alignItems: "flex-start",
  gap: 16,
};

export default QueryDisplay;
