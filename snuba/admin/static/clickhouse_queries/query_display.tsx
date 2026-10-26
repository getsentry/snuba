import React, { useEffect, useState } from "react";
import Client from "SnubaAdmin/api_client";
import QueryEditor from "SnubaAdmin/query_editor";
import ExecuteButton from "SnubaAdmin/utils/execute_button";
import {
  ActionIcon,
  Button,
  SegmentedControl,
  Text,
  Tooltip,
  UnstyledButton,
} from "@mantine/core";
import {
  IconInfoCircle,
  IconPencil,
  IconTrash,
  IconX,
} from "@tabler/icons-react";
import {
  clearRecentHistory,
  getRecentHistory,
  setRecentHistory,
} from "SnubaAdmin/query_history";
import { COLORS, SECTION_HEADING_STYLE } from "SnubaAdmin/theme";
import { PrefillWarning } from "SnubaAdmin/editor_panel";
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
// The storage's first node, to have a host ready as soon as a storage is
// picked.
function firstHost(nodeData: ClickhouseNodeData[], storage?: string) {
  const [first] = getHostsForStorage(nodeData, storage);
  return first ? parseHostPort(first.value) : {};
}

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
  // The host is filled in once the nodes arrive, so until then it isn't
  // flagged as missing.
  const [nodesLoaded, setNodesLoaded] = useState(false);
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
  // The editor closes after a successful run to make room for the results.
  // Closed, the panel sticks to the top so it can be reopened, or the query
  // run again from beside the storage and host, from anywhere in them.
  const [editorOpen, setEditorOpen] = useState(true);
  // The query editor puts its predefined query picker here, beside Close.
  const [predefinedQuerySlot, setPredefinedQuerySlot] =
    useState<HTMLDivElement | null>(null);

  const [queryError, setQueryError] = useState<Error | null>(null);

  // this is used to collapse the stack trace in error messages
  const [collapseOpened, setCollapseOpened] = useState(false);

  useEffect(() => {
    if (prefill) {
      clearHashParams();
    }
    props.api.getClickhouseNodes().then((res) => {
      setNodeData(res);
      setNodesLoaded(true);
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
      } else if (!prefill) {
        // The saved storage comes back without a host.
        setQuery((prevQuery) =>
          prevQuery.host
            ? prevQuery
            : { ...prevQuery, ...firstHost(res, prevQuery.storage) }
        );
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
        // A typed host isn't replaced; it's up to the user.
        ...(prevQuery.clusterless ? {} : firstHost(nodeData, storage)),
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

  function clearResults() {
    clearRecentHistory(HISTORY_KEY);
    setQueryResultHistory([]);
    setJustRan(null);
  }

  function handleQueryError(error: Error) {
    setQueryError(error);
    // Errors only show in the editor, so a failed run from the closed bar
    // opens it.
    setEditorOpen(true);
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
  // What to flag on the page; Execute stays disabled either way.
  const flagged = nodesLoaded ? missing : null;

  const errorElement = getErrorDomElement(
    queryError,
    collapseOpened,
    setCollapseOpened
  );

  // Sits beside storage and host while the editor is closed, and at the
  // bottom of the editor while it's open.
  const executeControl = (
    // A disabled button gets no hover events, so the tooltip hangs off a
    // wrapper.
    <Tooltip
      label={flagged ? missingHint[flagged] : ""}
      disabled={!flagged}
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
  );

  return (
    <div>
      <div
        style={editorOpen ? editorPanelStyle : stickyEditorPanelStyle}
        data-testid="editor-panel"
      >
        {/* Storage and host stay in view while the editor is closed. */}
        <div style={targetPanelStyle}>
          {prefill && prefillWarningOpen && (
            <PrefillWarning onClose={() => setPrefillWarningOpen(false)}>
              Storage: <code>{prefill.storage ?? "(not set)"}</code>
              {" · "}
              Host:{" "}
              <code>
                {prefill.host ? `${prefill.host}:${prefill.port}` : "(not set)"}
              </code>
            </PrefillWarning>
          )}
          <div style={targetRowStyle}>
            <div style={targetFieldStyle}>
              <Text size="sm" fw={600}>
                Storage
              </Text>
              <div style={flagged === "storage" ? needsInputStyle : undefined}>
                <CustomSelect
                  value={query.storage || ""}
                  onChange={selectStorage}
                  name="storage"
                  options={nodeData.map((storage) => storage.storage_name)}
                />
              </div>
              {flagged === "storage" && (
                <p style={needsInputHintStyle}>{missingHint.storage}</p>
              )}
            </div>
            <div style={targetFieldStyle}>
              {/* The manual-entry rules live in a tooltip so they don't
                  make the row taller. */}
              <Text size="sm" fw={600} style={hostLabelStyle}>
                Host
                {query.clusterless && (
                  <Tooltip
                    label="Must be a known cluster node or listed in admin.copy_tables_allowed_target_hosts. Defaults to port 8123."
                    multiline
                    width={280}
                    withArrow
                  >
                    <span aria-label="Manual host rules" style={hostInfoStyle}>
                      <IconInfoCircle size={14} />
                    </span>
                  </Tooltip>
                )}
              </Text>
              {query.clusterless ? (
                <>
                  <input
                    style={
                      flagged === "host"
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
                  {flagged === "host" && (
                    <p style={needsInputHintStyle}>{missingHint.host}</p>
                  )}
                </>
              ) : (
                <>
                  <div style={flagged === "host" ? needsInputStyle : undefined}>
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
                  {flagged === "host" && (
                    <p style={needsInputHintStyle}>{missingHint.host}</p>
                  )}
                </>
              )}
            </div>
            <div style={fixedFieldStyle}>
              <Text size="sm" fw={600}>
                Host entry
              </Text>
              <SegmentedControl
                aria-label="Host entry"
                value={query.clusterless ? "manual" : "pick"}
                onChange={(value) => setClusterlessHost(value === "manual")}
                data={[
                  { label: "Pick a node", value: "pick" },
                  { label: "Manual", value: "manual" },
                ]}
              />
            </div>
            {!editorOpen && (
              <div style={fixedFieldStyle}>
                {/* An empty label lines the button up with the selects. */}
                <Text size="sm" fw={600} aria-hidden>
                  &nbsp;
                </Text>
                {executeControl}
              </div>
            )}
          </div>
        </div>
        {!editorOpen && (
          // The whole bar opens the editor. The icon inside is only a span,
          // since a real button can't be nested in another.
          <UnstyledButton
            onClick={() => setEditorOpen(true)}
            aria-label="Open editor"
            style={editorClosedBarStyle}
            sx={{ "&:hover": { backgroundColor: COLORS.BG_GRAY_LIGHTER } }}
          >
            <Text size="sm" style={editorSummaryStyle}>
              {query.sudo && (
                <span style={{ color: COLORS.DANGER, fontWeight: 700 }}>
                  SUDO{" "}
                </span>
              )}
              <code>{query.sql || "(no query)"}</code>
            </Text>
            {/* Where the editor's Close button sits, so opening and closing
                are in the same place. */}
            <Tooltip label="Open editor" withArrow>
              <ActionIcon
                component="span"
                variant="filled"
                color="blue"
                style={{ flexShrink: 0 }}
              >
                <IconPencil size={16} />
              </ActionIcon>
            </Tooltip>
          </UnstyledButton>
        )}
        {/* Hidden rather than unmounted, so the editor keeps its state. */}
        <form style={editorOpen ? editorFormStyle : { display: "none" }}>
          <div style={editorTitleRowStyle}>
            <h2 style={{ ...SECTION_HEADING_STYLE, margin: 0 }}>
              Construct a ClickHouse System Query
            </h2>
            <div style={editorTitleActionsStyle}>
              {/* What each mode allows is on hover, to keep the row short. */}
              <Tooltip
                label={
                  query.sudo
                    ? "Sudo only runs SYSTEM, ALTER, OPTIMIZE and DROP statements. It needs the sudo role, and every sudo query is audit-logged with a notification."
                    : "SELECT, SHOW and DESCRIBE. SELECTs may only read system tables."
                }
                multiline
                width={320}
                withArrow
              >
                <div data-testid="mode-toggle">
                  <SegmentedControl
                    aria-label="Mode"
                    size="xs"
                    value={query.sudo ? "sudo" : "user"}
                    onChange={(value) => setSudo(value === "sudo")}
                    color={query.sudo ? "red" : undefined}
                    data={[
                      { label: "User mode", value: "user" },
                      { label: "Sudo mode", value: "sudo" },
                    ]}
                  />
                </div>
              </Tooltip>
              <div ref={setPredefinedQuerySlot} />
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
          </div>
          <div style={blockStyle}>
            <QueryEditor
              onQueryUpdate={(sql) => {
                updateQuerySql(sql);
              }}
              predefinedQueryOptions={props.predefinedQueryOptions}
              initialQuery={prefill?.sql}
              predefinedQuerySlot={predefinedQuerySlot}
            />
          </div>
          {/* Sudo can change data, so it gets a red frame. */}
          <div style={query.sudo ? sudoBlockStyle : blockStyle}>
            {/* The error sits beside Execute, so it never moves the button. */}
            <div style={executeActionsStyle}>
              <div style={errorSlotStyle}>{editorOpen && errorElement}</div>
              {editorOpen && executeControl}
            </div>
          </div>
        </form>
      </div>
      <div>
        <div style={resultsHeadingRowStyle}>
          <h2 style={resultsHeadingStyle}>Query results</h2>
          <Tooltip label="Clear saved results" withArrow>
            <ActionIcon
              variant="subtle"
              color="red"
              aria-label="Clear saved results"
              disabled={queryResultHistory.length === 0}
              onClick={clearResults}
            >
              <IconTrash size={16} />
            </ActionIcon>
          </Tooltip>
        </div>
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

// Above the editor, so it shows whether the editor is open or closed.
const targetPanelStyle = {
  padding: "12px 16px",
  borderBottom: `1px solid ${COLORS.BORDER_GRAY}`,
};

const targetRowStyle = {
  display: "flex",
  flexWrap: "wrap" as const,
  gap: 16,
};

// Storage and host shrink before the row wraps, so it stays one line.
const targetFieldStyle = {
  flex: "1 1 12em",
  maxWidth: "20em",
  display: "flex",
  flexDirection: "column" as const,
  gap: 4,
};

// The host entry toggle and Execute keep their natural width.
const fixedFieldStyle = {
  ...targetFieldStyle,
  flex: "none",
  maxWidth: "none",
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
const resultsHeadingRowStyle = {
  display: "flex",
  alignItems: "center",
  gap: 8,
  marginTop: 32,
  marginBottom: 12,
};

const resultsHeadingStyle = {
  ...SECTION_HEADING_STYLE,
  margin: 0,
};

const editorPanelStyle = {
  backgroundColor: COLORS.PANEL_BG,
  border: `1px solid ${COLORS.BORDER_GRAY}`,
  borderRadius: 4,
  boxShadow: "0 2px 6px rgba(0, 0, 0, 0.08)",
};

// Closed, the panel is a short bar, so it sticks to the top of the page's
// scroll area to run or reopen from anywhere in the results. Open, it's
// tall enough to cover them, so it scrolls with the page.
const stickyEditorPanelStyle = {
  ...editorPanelStyle,
  position: "sticky" as const,
  top: 0,
  zIndex: 10,
};

const editorFormStyle = {
  padding: "12px 16px 0",
};

const editorTitleRowStyle = {
  display: "flex",
  justifyContent: "space-between",
  alignItems: "center",
  marginBottom: 12,
};

const editorTitleActionsStyle = {
  display: "flex",
  alignItems: "center",
  gap: 16,
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
  // Fills the bar, pushing the open icon to the right end.
  flex: 1,
  minWidth: 0,
  overflow: "hidden",
  textOverflow: "ellipsis",
  whiteSpace: "nowrap" as const,
  color: COLORS.TEXT_LIGHTER,
};

// Takes the space between the mode toggle and Execute.
const inputStyle = {
  fontSize: 16,
  minHeight: '2.25rem',
}

const hostLabelStyle = {
  display: "flex",
  alignItems: "center",
  gap: 4,
};

const hostInfoStyle = {
  display: "inline-flex",
  color: COLORS.TEXT_LIGHTER,
  cursor: "help",
};

// Execute stays top right, however tall the error beside it gets.
const executeActionsStyle = {
  display: "flex",
  alignItems: "flex-start",
  gap: 16,
};

const errorSlotStyle = {
  flex: 1,
  minWidth: 0,
};

export default QueryDisplay;
