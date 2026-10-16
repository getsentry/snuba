import React, { useEffect, useRef, useState } from "react";
import Client from "SnubaAdmin/api_client";
import QueryEditor from "SnubaAdmin/query_editor";
import ExecuteButton from "SnubaAdmin/utils/execute_button";
import {
  Alert,
  Button,
  SegmentedControl,
  Text,
  Tooltip,
  UnstyledButton,
} from "@mantine/core";
import { IconChevronDown, IconX } from "@tabler/icons-react";
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
// Set on the page to the editor pane's height; see STICKY_HEADER_TOP.
const EDITOR_PANEL_HEIGHT_VAR = "--system-queries-editor-height";
// Where content stuck to the top of the results sits: just below the pane.
const STICKY_HEADER_TOP = `var(${EDITOR_PANEL_HEIGHT_VAR}, 0px)`;
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
  // The editor sits in a sticky panel so it can be reopened from anywhere in
  // the results. It closes after a successful run to make room for them.
  const [editorOpen, setEditorOpen] = useState(true);
  const resultsHeadingRef = useRef<HTMLHeadingElement>(null);
  // Result tables pin their headers just below the pane, so they need its
  // height, which changes as it opens, closes and the query grows.
  const editorPanelRef = useRef<HTMLDivElement>(null);
  const [editorPanelHeight, setEditorPanelHeight] = useState(0);

  useEffect(() => {
    const panel = editorPanelRef.current;
    if (!panel || typeof ResizeObserver === "undefined") {
      return;
    }
    const observer = new ResizeObserver(() =>
      setEditorPanelHeight(panel.offsetHeight)
    );
    observer.observe(panel);
    return () => observer.disconnect();
  }, []);

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

  // Bring a fresh result into view, since it may be above where the user
  // scrolled to. (jsdom has no scrollIntoView.)
  useEffect(() => {
    if (justRan) {
      resultsHeadingRef.current?.scrollIntoView?.({ block: "start" });
    }
  }, [justRan]);

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
        setEditorOpen(false);
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
    <div
      style={
        {
          [EDITOR_PANEL_HEIGHT_VAR]: `${editorPanelHeight}px`,
        } as React.CSSProperties
      }
    >
      <div
        ref={editorPanelRef}
        style={editorPanelStyle}
        data-testid="editor-panel"
      >
        {!editorOpen && (
          // The whole bar opens the editor. The "button" inside is only a
          // span, since a real button can't be nested in another.
          <UnstyledButton
            onClick={() => setEditorOpen(true)}
            aria-label="Open editor"
            style={editorClosedBarStyle}
            sx={{ "&:hover": { backgroundColor: COLORS.BG_GRAY_LIGHTER } }}
          >
            <Button
              component="span"
              size="xs"
              rightIcon={<IconChevronDown size={14} />}
              style={{ flexShrink: 0 }}
            >
              Open Editor
            </Button>
            <Text size="sm" style={editorSummaryStyle}>
              {query.sudo && (
                <span style={{ color: COLORS.DANGER, fontWeight: 700 }}>
                  SUDO{" "}
                </span>
              )}
              {query.storage ?? "(no storage)"}
              {" · "}
              {query.host ? `${query.host}:${query.port}` : "(no host)"}
              {" · "}
              <code>{query.sql || "(no query)"}</code>
            </Text>
          </UnstyledButton>
        )}
        {/* Hidden rather than unmounted, so the editor keeps its state. */}
        <form style={editorOpen ? editorFormStyle : { display: "none" }}>
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
          <div style={editorTitleRowStyle}>
            <h2 style={{ ...SECTION_HEADING_STYLE, margin: 0 }}>
              Construct a ClickHouse System Query
            </h2>
            <Button
              size="xs"
              variant="light"
              color="red"
              leftIcon={<IconX size={14} />}
              aria-label="Close editor"
              onClick={() => setEditorOpen(false)}
            >
              Close
            </Button>
          </div>
          <div style={blockStyle}>
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
          </div>
          <div style={blockStyle}>
            <QueryEditor
              onQueryUpdate={(sql) => {
                updateQuerySql(sql);
              }}
              predefinedQueryOptions={props.predefinedQueryOptions}
              initialQuery={prefill?.sql}
            />
          </div>
          {/* Sudo can change data, so it gets a red frame. */}
          <div style={query.sudo ? sudoBlockStyle : blockStyle}>
            <div style={executeActionsStyle}>
              <SegmentedControl
                aria-label="Mode"
                value={query.sudo ? "sudo" : "user"}
                onChange={(value) => setSudo(value === "sudo")}
                color={query.sudo ? "red" : undefined}
                data={[
                  { label: "User mode", value: "user" },
                  { label: "Sudo mode", value: "sudo" },
                ]}
                style={{ flexShrink: 0 }}
              />
              <p style={modeHelpTextStyle}>
                {query.sudo
                  ? "Sudo only runs SYSTEM, ALTER, OPTIMIZE and DROP statements. It needs the sudo role, and every sudo query is audit-logged with a notification."
                  : "SELECT, SHOW and DESCRIBE. SELECTs may only read system tables."}
              </p>
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
          </div>
        </form>
      </div>
      <div>
        {getErrorDomElement(queryError, collapseOpened, setCollapseOpened)}
      </div>
      <div>
        <h2 ref={resultsHeadingRef} style={resultsHeadingStyle}>
          Query results
        </h2>
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

// Spaces out the target, query and execute rows.
const blockStyle = {
  marginBottom: 16,
};

const sudoBlockStyle = {
  ...blockStyle,
  padding: 12,
  border: `2px solid ${COLORS.DANGER}`,
  borderRadius: 4,
  backgroundColor: "rgba(168, 50, 45, 0.06)",
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

// Separates the results from the form above.
const resultsHeadingStyle = {
  ...SECTION_HEADING_STYLE,
  marginTop: 32,
  // Keeps the heading clear of the sticky editor bar when scrolled to.
  scrollMarginTop: 72,
};

// Sticks to the top of the page's scroll area, just below the header.
const editorPanelStyle = {
  position: "sticky" as const,
  top: 0,
  zIndex: 10,
  backgroundColor: COLORS.PANEL_BG,
  border: `1px solid ${COLORS.BORDER_GRAY}`,
  borderRadius: 4,
  boxShadow: "0 2px 6px rgba(0, 0, 0, 0.08)",
};

// Scrolls on its own when the open editor is taller than the screen.
const editorFormStyle = {
  maxHeight: "calc(100vh - 100px)",
  overflowY: "auto" as const,
  padding: "12px 16px 0",
};

const editorTitleRowStyle = {
  display: "flex",
  justifyContent: "space-between",
  alignItems: "center",
  marginBottom: 12,
};

const editorClosedBarStyle = {
  display: "flex",
  width: "100%",
  borderRadius: 4,
  alignItems: "center",
  gap: 12,
  padding: "8px 16px",
};

// One line, so the closed bar stays short whatever the query.
const editorSummaryStyle = {
  minWidth: 0,
  overflow: "hidden",
  textOverflow: "ellipsis",
  whiteSpace: "nowrap" as const,
  color: COLORS.TEXT_LIGHTER,
};

// Takes the space between the mode toggle and Execute.
const modeHelpTextStyle = {
  flex: 1,
  minWidth: 0,
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
  alignItems: "center",
  gap: 16,
};

export { STICKY_HEADER_TOP };
export default QueryDisplay;
