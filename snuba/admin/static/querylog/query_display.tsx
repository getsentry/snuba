import React, { useEffect, useRef, useState } from "react";
import { Button, Drawer, Text, TextInput, Tooltip } from "@mantine/core";
import { IconTable } from "@tabler/icons-react";
import Client from "SnubaAdmin/api_client";
import QueryEditor from "SnubaAdmin/query_editor";
import { getRecentHistory, setRecentHistory } from "SnubaAdmin/query_history";
import { QueryHistoryEntry } from "SnubaAdmin/query_history_entry";
import {
  EditorPanelLayout,
  resultsHeadingStyle,
} from "SnubaAdmin/editor_panel";
import { Table } from "SnubaAdmin/table";
import { COLORS } from "SnubaAdmin/theme";
import { HEADER_HEIGHT_PX } from "SnubaAdmin/header";
import { clearHashParams } from "SnubaAdmin/clickhouse_queries/prefill";
import {
  buildQuerylogUrl,
  parseQuerylogPrefill,
} from "SnubaAdmin/querylog/prefill";
import { getErrorDomElement } from "SnubaAdmin/utils/clickhouse_node_utils";

import { QuerylogRequest, QuerylogResult, PredefinedQuery } from "./types";
import ExecuteButton from "SnubaAdmin/utils/execute_button";

type QueryState = Partial<QuerylogRequest>;
const HISTORY_KEY = "querylog";
// DESCRIBE returns more than fits in the drawer; these are the useful ones.
const SCHEMA_COLUMNS = ["name", "type", "comment"];
const MISSING_QUERY_HINT = "Write or pick a query to run.";

function QueryDisplay(props: {
  api: Client;
  resultDataPopulator: (queryResult: QuerylogResult) => JSX.Element;
  predefinedQueryOptions: Array<PredefinedQuery>;
}) {
  // A link may pre-fill the query (see prefill.tsx).
  const [prefill] = useState(() =>
    parseQuerylogPrefill(window.location.hash)
  );
  const [query, setQuery] = useState<QueryState>({});
  const [queryResultHistory, setQueryResultHistory] = useState<
    QuerylogResult[]
  >(getRecentHistory(HISTORY_KEY));
  // Only a result the user just ran opens automatically; saved history loads
  // collapsed.
  const [justRan, setJustRan] = useState<QuerylogResult | null>(null);
  // The editor closes after a successful run to make room for the results.
  const [editorOpen, setEditorOpen] = useState(true);
  const resultsHeadingRef = useRef<HTMLHeadingElement>(null);
  const [queryError, setQueryError] = useState<Error | null>(null);
  // this is used to collapse the stack trace in error messages
  const [collapseOpened, setCollapseOpened] = useState(false);

  // The schema is reference material, so it opens beside the editor rather
  // than among the results. It's fetched the first time it's opened.
  const [schemaOpen, setSchemaOpen] = useState(false);
  const [schema, setSchema] = useState<QuerylogResult | null>(null);
  const [schemaError, setSchemaError] = useState<string | null>(null);
  const [schemaFilter, setSchemaFilter] = useState("");

  useEffect(() => {
    if (prefill) {
      clearHashParams();
    }
  }, []);

  // Bring a fresh result into view, since it may be above where the user
  // scrolled to. (jsdom has no scrollIntoView.)
  useEffect(() => {
    if (justRan) {
      resultsHeadingRef.current?.scrollIntoView?.({ block: "start" });
    }
  }, [justRan]);

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
      .executeQuerylogQuery(query as QuerylogRequest)
      .then((result) => {
        setQueryError(null);
        result.input_query = query.sql || "<Input Query>";
        // The server doesn't timestamp results; record when this one ran.
        result.timestamp = Math.floor(Date.now() / 1000);
        setRecentHistory(HISTORY_KEY, result);
        setQueryResultHistory((prevHistory) => [result, ...prevHistory]);
        setJustRan(result);
        setEditorOpen(false);
      });
  }

  function openSchema() {
    setSchemaOpen(true);
    if (schema) {
      return;
    }
    setSchemaError(null);
    props.api
      .getQuerylogSchema()
      .then(setSchema)
      .catch((err) => {
        setSchemaError(err?.error?.message ?? String(err));
      });
  }

  function renderSchema() {
    if (schemaError) {
      return <Text color="red">An error occurred: {schemaError}</Text>;
    }
    if (!schema) {
      return <Text color="dimmed">Loading…</Text>;
    }
    const columnIndexes = SCHEMA_COLUMNS.map((name) =>
      schema.column_names.indexOf(name)
    ).filter((idx) => idx !== -1);
    const filter = schemaFilter.trim().toLowerCase();
    const rows = schema.rows
      .filter(
        (row) => !filter || String(row[0]).toLowerCase().includes(filter)
      )
      .map((row) => columnIndexes.map((idx) => row[idx]));
    return (
      <>
        <TextInput
          placeholder="Filter columns…"
          aria-label="Filter columns"
          value={schemaFilter}
          onChange={(evt) => setSchemaFilter(evt.currentTarget.value)}
          mb="sm"
        />
        <Table
          headerData={columnIndexes.map((idx) => schema.column_names[idx])}
          rowData={rows}
        />
      </>
    );
  }

  function convertResultsToCSV(queryResult: QuerylogResult) {
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
    <EditorPanelLayout
      title="Construct a Querylog Query"
      open={editorOpen}
      onOpenChange={setEditorOpen}
      summary={<code>{query.sql || "(no query)"}</code>}
      titleActions={
        <Button
          size="xs"
          variant="default"
          leftIcon={<IconTable size={14} />}
          onClick={openSchema}
        >
          Querylog schema
        </Button>
      }
      editor={
        <>
          <div style={blockStyle}>
            <QueryEditor
              onQueryUpdate={(sql) => {
                updateQuerySql(sql);
              }}
              predefinedQueryOptions={props.predefinedQueryOptions}
              initialQuery={prefill?.sql}
            />
          </div>
          <div style={{ ...blockStyle, ...executeActionsStyle }}>
            {/* A disabled button gets no hover events, so the tooltip hangs
                off a wrapper. */}
            <Tooltip
              label={MISSING_QUERY_HINT}
              disabled={!!query.sql}
              withArrow
            >
              <div data-testid="execute-wrapper">
                <ExecuteButton
                  onError={setQueryError}
                  onClick={executeQuery}
                  disabled={!query.sql}
                />
              </div>
            </Tooltip>
          </div>
        </>
      }
    >
      <Drawer
        opened={schemaOpen}
        onClose={() => setSchemaOpen(false)}
        title={<Text fw={700}>Querylog schema</Text>}
        position="right"
        size="lg"
        // Leaves the editor usable while the schema is open.
        withOverlay={false}
        lockScroll={false}
        // Starts below the page header, so the header stays in view.
        styles={{
          inner: { top: HEADER_HEIGHT_PX, height: `calc(100% - ${HEADER_HEIGHT_PX}px)` },
          content: { borderLeft: `1px solid ${COLORS.BORDER_GRAY}` },
        }}
      >
        {renderSchema()}
      </Drawer>
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
            sql={queryResult.input_query}
            shareUrl={buildQuerylogUrl(queryResult.input_query)}
            isLatest={idx === 0}
            defaultExpanded={queryResult === justRan}
            csv={convertResultsToCSV(queryResult)}
            resultDataPopulator={props.resultDataPopulator}
          />
        ))}
      </div>
    </EditorPanelLayout>
  );
}

// Spaces out the query and execute rows.
const blockStyle = {
  marginBottom: 16,
};

const executeActionsStyle = {
  display: "flex",
  justifyContent: "flex-end",
};

export default QueryDisplay;
