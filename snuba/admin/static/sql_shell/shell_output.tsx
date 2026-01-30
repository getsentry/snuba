import React, { useRef, useEffect, useCallback, useState } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { useShellStyles } from "SnubaAdmin/sql_shell/styles";
import { ShellHistoryEntry, ShellMode, OutputFormat } from "SnubaAdmin/sql_shell/types";
import {
  TracingResult,
  TracingSummary,
  QuerySummary,
  ExecuteSummary,
  IndexSummary,
  SelectSummary,
  StreamSummary,
  AggregationSummary,
  SortingSummary,
} from "SnubaAdmin/tracing/types";
import { QueryResult } from "SnubaAdmin/clickhouse_queries/types";

// Collapsible section component
function CollapsibleSection({
  title,
  badge,
  defaultExpanded = false,
  children,
  classes,
}: {
  title: string;
  badge?: string;
  defaultExpanded?: boolean;
  children: React.ReactNode;
  classes: Record<string, string>;
}) {
  const [expanded, setExpanded] = useState(defaultExpanded);

  return (
    <div className={classes.collapsibleSection}>
      <div
        className={classes.collapsibleHeader}
        onClick={() => setExpanded(!expanded)}
      >
        <span className={classes.collapsibleChevron}>
          {expanded ? "▼" : "▶"}
        </span>
        <span className={classes.collapsibleTitle}>{title}</span>
        {badge && <span className={classes.collapsibleBadge}>{badge}</span>}
      </div>
      {expanded && <div className={classes.collapsibleContent}>{children}</div>}
    </div>
  );
}

interface ShellOutputProps {
  entries: ShellHistoryEntry[];
  traceFormatted: boolean;
  mode: ShellMode;
  outputFormat: OutputFormat;
  isExecuting?: boolean;
}

function estimateEntryHeight(entry: ShellHistoryEntry): number {
  const heights: Record<ShellHistoryEntry["type"], number> = {
    command: 40,
    info: 35,
    error: 80,
    result: 300,
    system_result: 250,
    storages: 200,
    hosts: 150,
    help: 400,
  };
  return heights[entry.type] || 50;
}

export function ShellOutput({ entries, traceFormatted, mode, outputFormat, isExecuting }: ShellOutputProps) {
  const { classes } = useShellStyles();
  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const wasAtBottomRef = useRef(true);

  const virtualizer = useVirtualizer({
    count: entries.length,
    getScrollElement: () => scrollContainerRef.current,
    estimateSize: (index) => estimateEntryHeight(entries[index]),
    overscan: 5,
  });

  // Check if user is at bottom before entries change
  const checkIfAtBottom = useCallback(() => {
    const container = scrollContainerRef.current;
    if (!container) return true;
    const threshold = 50; // pixels from bottom to consider "at bottom"
    return container.scrollHeight - container.scrollTop - container.clientHeight < threshold;
  }, []);

  // Track scroll position to determine if we should auto-scroll
  useEffect(() => {
    const container = scrollContainerRef.current;
    if (!container) return;

    const handleScroll = () => {
      wasAtBottomRef.current = checkIfAtBottom();
    };

    container.addEventListener("scroll", handleScroll);
    return () => container.removeEventListener("scroll", handleScroll);
  }, [checkIfAtBottom]);

  // Auto-scroll to bottom when new entries are added (if user was at bottom)
  useEffect(() => {
    if (entries.length > 0 && wasAtBottomRef.current) {
      virtualizer.scrollToIndex(entries.length - 1, { align: "end" });
    }
  }, [entries.length, virtualizer]);

  const virtualItems = virtualizer.getVirtualItems();

  return (
    <div ref={scrollContainerRef} className={classes.outputArea}>
      <div
        style={{
          height: `${virtualizer.getTotalSize()}px`,
          width: "100%",
          position: "relative",
        }}
      >
        {virtualItems.map((virtualItem) => (
          <div
            key={virtualItem.index}
            data-index={virtualItem.index}
            ref={virtualizer.measureElement}
            style={{
              position: "absolute",
              top: 0,
              left: 0,
              width: "100%",
              transform: `translateY(${virtualItem.start}px)`,
            }}
          >
            <ShellEntry
              entry={entries[virtualItem.index]}
              traceFormatted={traceFormatted}
              mode={mode}
              outputFormat={outputFormat}
              classes={classes}
            />
          </div>
        ))}
      </div>
      {isExecuting && (
        <div className={classes.executingIndicator}>Executing query...</div>
      )}
    </div>
  );
}

function ShellEntry({
  entry,
  traceFormatted,
  mode,
  outputFormat,
  classes,
}: {
  entry: ShellHistoryEntry;
  traceFormatted: boolean;
  mode: ShellMode;
  outputFormat: OutputFormat;
  classes: Record<string, string>;
}) {
  switch (entry.type) {
    case "command":
      return <CommandLine command={entry.content} classes={classes} />;
    case "info":
      return <InfoOutput message={entry.content} classes={classes} />;
    case "error":
      return <ErrorOutput error={entry.content} classes={classes} />;
    case "result":
      return (
        <ResultsOutput
          result={entry.content}
          traceFormatted={traceFormatted}
          outputFormat={outputFormat}
          classes={classes}
        />
      );
    case "system_result":
      return (
        <SystemResultsOutput
          result={entry.content}
          outputFormat={outputFormat}
          classes={classes}
        />
      );
    case "storages":
      return <StoragesOutput storages={entry.content} classes={classes} />;
    case "hosts":
      return <HostsOutput hosts={entry.content} classes={classes} />;
    case "help":
      return <HelpOutput mode={entry.mode} classes={classes} />;
    default:
      return null;
  }
}

function CommandLine({
  command,
  classes,
}: {
  command: string;
  classes: Record<string, string>;
}) {
  return (
    <div className={classes.commandLine}>
      <span className={classes.promptDisplay}>{">"}</span>
      <span className={classes.commandText}>{command}</span>
    </div>
  );
}

function InfoOutput({
  message,
  classes,
}: {
  message: string;
  classes: Record<string, string>;
}) {
  return <div className={classes.infoText}>{message}</div>;
}

function ErrorOutput({
  error,
  classes,
}: {
  error: string | { code?: string; message?: string; type?: string } | any;
  classes: Record<string, string>;
}) {
  let errorMessage: string;
  if (typeof error === "string") {
    errorMessage = error;
  } else if (typeof error === "object" && error !== null) {
    errorMessage = error.message || error.error || JSON.stringify(error);
  } else {
    errorMessage = String(error);
  }

  // Strip stack trace from ClickHouse errors
  // ClickHouse errors typically have "Stack trace:" followed by the trace
  const stackTraceIndex = errorMessage.indexOf("Stack trace:");
  if (stackTraceIndex !== -1) {
    errorMessage = errorMessage.substring(0, stackTraceIndex).trim();
  }

  return <div className={classes.errorText}>Error: {errorMessage}</div>;
}

function StoragesOutput({
  storages,
  classes,
}: {
  storages: string[];
  classes: Record<string, string>;
}) {
  return (
    <div>
      <div className={classes.infoText}>Available storages:</div>
      <ul className={classes.storageList}>
        {storages.map((storage) => (
          <li key={storage} className={classes.storageItem}>
            {storage}
          </li>
        ))}
      </ul>
    </div>
  );
}

function HostsOutput({
  hosts,
  classes,
}: {
  hosts: string[];
  classes: Record<string, string>;
}) {
  return (
    <div>
      <div className={classes.infoText}>Available hosts:</div>
      <ul className={classes.storageList}>
        {hosts.map((host) => (
          <li key={host} className={classes.storageItem}>
            {host}
          </li>
        ))}
      </ul>
    </div>
  );
}

function HelpOutput({ mode, classes }: { mode: ShellMode; classes: Record<string, string> }) {
  const tracingCommands = [
    { cmd: "USE <storage>", desc: "Set the active storage for queries" },
    { cmd: "SHOW STORAGES", desc: "List all available storages" },
    { cmd: "PROFILE ON/OFF", desc: "Toggle profile event collection" },
    { cmd: "TRACE RAW/FORMATTED", desc: "Toggle trace output format" },
    { cmd: "FORMAT TABLE/JSON/CSV/VERTICAL", desc: "Set output format" },
    { cmd: "CLEAR", desc: "Clear the terminal output" },
    { cmd: "HELP", desc: "Show this help message" },
    { cmd: "<SQL query>", desc: "Execute SQL with tracing" },
  ];

  const systemCommands = [
    { cmd: "USE <storage>", desc: "Set the active storage for queries" },
    { cmd: "HOST <host:port>", desc: "Set the target host (e.g., HOST 127.0.0.1:9000)" },
    { cmd: "SHOW STORAGES", desc: "List all available storages" },
    { cmd: "SHOW HOSTS", desc: "List available hosts for current storage" },
    { cmd: "SUDO ON/OFF", desc: "Toggle sudo mode" },
    { cmd: "FORMAT TABLE/JSON/CSV/VERTICAL", desc: "Set output format" },
    { cmd: "CLEAR", desc: "Clear the terminal output" },
    { cmd: "HELP", desc: "Show this help message" },
    { cmd: "<SQL query>", desc: "Execute SQL query" },
  ];

  const commands = mode === "tracing" ? tracingCommands : systemCommands;

  return (
    <div>
      <div className={classes.infoText}>Available commands:</div>
      <div style={{ marginTop: "8px" }}>
        {commands.map(({ cmd, desc }) => (
          <div key={cmd} style={{ marginBottom: "4px" }}>
            <span className={classes.helpCommand}>{cmd}</span>
            <span className={classes.helpDescription}>{desc}</span>
          </div>
        ))}
      </div>
      <div style={{ marginTop: "12px", color: "#8b949e" }}>
        Keyboard shortcuts:
      </div>
      <div style={{ marginTop: "4px" }}>
        <span className={classes.helpCommand}>Enter</span>
        <span className={classes.helpDescription}>Execute command</span>
      </div>
      <div style={{ marginBottom: "4px" }}>
        <span className={classes.helpCommand}>Tab</span>
        <span className={classes.helpDescription}>Autocomplete storage/host</span>
      </div>
      <div style={{ marginBottom: "4px" }}>
        <span className={classes.helpCommand}>↑ / ↓</span>
        <span className={classes.helpDescription}>Navigate command history</span>
      </div>
      <div style={{ marginBottom: "4px" }}>
        <span className={classes.helpCommand}>Ctrl+C</span>
        <span className={classes.helpDescription}>Clear current input</span>
      </div>
      <div style={{ marginBottom: "4px" }}>
        <span className={classes.helpCommand}>Ctrl+L</span>
        <span className={classes.helpDescription}>Clear screen</span>
      </div>
      <div style={{ marginBottom: "4px" }}>
        <span className={classes.helpCommand}>Ctrl+U</span>
        <span className={classes.helpDescription}>Delete from cursor to start</span>
      </div>
      <div style={{ marginBottom: "4px" }}>
        <span className={classes.helpCommand}>Ctrl+K</span>
        <span className={classes.helpDescription}>Delete from cursor to end</span>
      </div>
      <div style={{ marginBottom: "4px" }}>
        <span className={classes.helpCommand}>Ctrl+W</span>
        <span className={classes.helpDescription}>Delete word before cursor</span>
      </div>
      <div style={{ marginBottom: "4px" }}>
        <span className={classes.helpCommand}>Ctrl+A</span>
        <span className={classes.helpDescription}>Move cursor to start</span>
      </div>
      <div style={{ marginBottom: "4px" }}>
        <span className={classes.helpCommand}>Ctrl+E</span>
        <span className={classes.helpDescription}>Move cursor to end</span>
      </div>
    </div>
  );
}

// Helper function to escape CSV values
function escapeCsvValue(value: any): string {
  if (value === null || value === undefined) {
    return "";
  }
  const str = typeof value === "object" ? JSON.stringify(value) : String(value);
  // If value contains comma, quote, or newline, wrap in quotes and escape internal quotes
  if (str.includes(",") || str.includes('"') || str.includes("\n")) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

// Render data as JSON format
function renderAsJson(
  cols: string[],
  rows: any[][],
  classes: Record<string, string>
): React.ReactNode {
  const data = rows.slice(0, 100).map((row) => {
    const obj: Record<string, any> = {};
    cols.forEach((col, idx) => {
      obj[col] = row[idx];
    });
    return obj;
  });

  return (
    <pre className={classes.jsonOutput}>
      {JSON.stringify(data, null, 2)}
    </pre>
  );
}

// Render data as CSV format
function renderAsCsv(
  cols: string[],
  rows: any[][],
  classes: Record<string, string>
): React.ReactNode {
  const headerRow = cols.map(escapeCsvValue).join(",");
  const dataRows = rows.slice(0, 100).map((row) =>
    row.map(escapeCsvValue).join(",")
  );

  return (
    <pre className={classes.csvOutput}>
      {[headerRow, ...dataRows].join("\n")}
    </pre>
  );
}

// Render data as vertical format (one column per line per row)
function renderAsVertical(
  cols: string[],
  rows: any[][],
  classes: Record<string, string>
): React.ReactNode {
  const maxColWidth = Math.max(...cols.map((c) => c.length));

  return (
    <div className={classes.verticalOutput}>
      {rows.slice(0, 100).map((row, rowIdx) => (
        <div key={rowIdx} className={classes.verticalRow}>
          <div className={classes.verticalRowHeader}>
            *************************** {rowIdx + 1}. row ***************************
          </div>
          {cols.map((col, colIdx) => {
            const value = row[colIdx];
            const displayValue =
              value === null || value === undefined
                ? "NULL"
                : typeof value === "object"
                ? JSON.stringify(value)
                : String(value);
            return (
              <div key={colIdx} className={classes.verticalField}>
                <span className={classes.verticalFieldName}>
                  {col.padStart(maxColWidth)}:
                </span>
                <span className={classes.verticalFieldValue}> {displayValue}</span>
              </div>
            );
          })}
        </div>
      ))}
    </div>
  );
}

function ResultsOutput({
  result,
  traceFormatted,
  outputFormat,
  classes,
}: {
  result: TracingResult;
  traceFormatted: boolean;
  outputFormat: OutputFormat;
  classes: Record<string, string>;
}) {
  if (!result) {
    return <ErrorOutput error="No result received" classes={classes} />;
  }

  if (result.error) {
    return <ErrorOutput error={result.error} classes={classes} />;
  }

  const cols = result.cols || [];
  const rows = result.result || [];
  const rowCount = result.num_rows_result || rows.length || 0;

  // Extract column names for format renderers (cols are [name, type] arrays)
  const colNames = cols.map((col: string[] | undefined) => col ? col[0] : "?");

  const renderResultContent = () => {
    if (cols.length === 0) {
      return <div className={classes.emptyResult}>No results returned</div>;
    }

    switch (outputFormat) {
      case "json":
        return (
          <>
            {renderAsJson(colNames, rows, classes)}
            {rows.length > 100 && (
              <div className={classes.truncatedNote}>
                ... showing first 100 of {rows.length} rows
              </div>
            )}
          </>
        );
      case "csv":
        return (
          <>
            {renderAsCsv(colNames, rows, classes)}
            {rows.length > 100 && (
              <div className={classes.truncatedNote}>
                ... showing first 100 of {rows.length} rows
              </div>
            )}
          </>
        );
      case "vertical":
        return (
          <>
            {renderAsVertical(colNames, rows, classes)}
            {rows.length > 100 && (
              <div className={classes.truncatedNote}>
                ... showing first 100 of {rows.length} rows
              </div>
            )}
          </>
        );
      case "table":
      default:
        return (
          <div className={classes.tableContainer}>
            <table className={classes.resultTableCompact}>
              <thead>
                <tr>
                  {cols.map((col: string[] | undefined, idx: number) => (
                    <th key={idx}>
                      {col ? col[0] : "?"} <small>({col ? col[1] : "?"})</small>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.slice(0, 100).map((row: any[] | undefined, rowIdx: number) => (
                  <tr key={rowIdx}>
                    {row ? row.map((cell, cellIdx) => (
                      <td key={cellIdx}>
                        {cell === null || cell === undefined
                          ? <span className={classes.nullValue}>NULL</span>
                          : typeof cell === "object"
                          ? JSON.stringify(cell)
                          : String(cell)}
                      </td>
                    )) : <td>Invalid row</td>}
                  </tr>
                ))}
              </tbody>
            </table>
            {rows.length > 100 && (
              <div className={classes.truncatedNote}>
                ... showing first 100 of {rows.length} rows
              </div>
            )}
          </div>
        );
    }
  };

  return (
    <div className={classes.resultContainer}>
      <CollapsibleSection
        title="Results"
        badge={`${rowCount} rows`}
        defaultExpanded={true}
        classes={classes}
      >
        {renderResultContent()}
      </CollapsibleSection>

      {traceFormatted && result.summarized_trace_output ? (
        <FormattedTraceOutput
          summary={result.summarized_trace_output}
          profileEvents={result.profile_events_results as any}
          classes={classes}
        />
      ) : result.trace_output ? (
        <RawTraceOutput
          traceOutput={result.trace_output}
          profileEvents={result.profile_events_results as any}
          classes={classes}
        />
      ) : null}
    </div>
  );
}

function SystemResultsOutput({
  result,
  outputFormat,
  classes,
}: {
  result: QueryResult;
  outputFormat: OutputFormat;
  classes: Record<string, string>;
}) {
  if (!result) {
    return <ErrorOutput error="No result received" classes={classes} />;
  }

  if (result.error) {
    return <ErrorOutput error={result.error} classes={classes} />;
  }

  const cols = (result.column_names || []) as string[];
  const rows = (result.rows || []) as any[][];

  const renderResultContent = () => {
    if (cols.length === 0) {
      return <div className={classes.emptyResult}>No results returned</div>;
    }

    switch (outputFormat) {
      case "json":
        return (
          <>
            {renderAsJson(cols, rows, classes)}
            {rows.length > 100 && (
              <div className={classes.truncatedNote}>
                ... showing first 100 of {rows.length} rows
              </div>
            )}
          </>
        );
      case "csv":
        return (
          <>
            {renderAsCsv(cols, rows, classes)}
            {rows.length > 100 && (
              <div className={classes.truncatedNote}>
                ... showing first 100 of {rows.length} rows
              </div>
            )}
          </>
        );
      case "vertical":
        return (
          <>
            {renderAsVertical(cols, rows, classes)}
            {rows.length > 100 && (
              <div className={classes.truncatedNote}>
                ... showing first 100 of {rows.length} rows
              </div>
            )}
          </>
        );
      case "table":
      default:
        return (
          <div className={classes.tableContainer}>
            <table className={classes.resultTableCompact}>
              <thead>
                <tr>
                  {cols.map((col: string, idx: number) => (
                    <th key={idx}>{col}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.slice(0, 100).map((row: any[] | undefined, rowIdx: number) => (
                  <tr key={rowIdx}>
                    {row ? row.map((cell, cellIdx) => (
                      <td key={cellIdx}>
                        {cell === null || cell === undefined
                          ? <span className={classes.nullValue}>NULL</span>
                          : typeof cell === "object"
                          ? JSON.stringify(cell)
                          : String(cell)}
                      </td>
                    )) : <td>Invalid row</td>}
                  </tr>
                ))}
              </tbody>
            </table>
            {rows.length > 100 && (
              <div className={classes.truncatedNote}>
                ... showing first 100 of {rows.length} rows
              </div>
            )}
          </div>
        );
    }
  };

  return (
    <div className={classes.resultContainer}>
      <CollapsibleSection
        title="Results"
        badge={`${rows.length} rows`}
        defaultExpanded={true}
        classes={classes}
      >
        {renderResultContent()}
      </CollapsibleSection>
    </div>
  );
}

function FormattedTraceOutput({
  summary,
  profileEvents,
  classes,
}: {
  summary: TracingSummary;
  profileEvents?: any;
  classes: Record<string, string>;
}) {
  if (!summary || !summary.query_summaries) {
    return (
      <CollapsibleSection
        title="Trace"
        defaultExpanded={false}
        classes={classes}
      >
        <div className={classes.emptyResult}>No trace data available</div>
      </CollapsibleSection>
    );
  }

  const querySummaries = summary.query_summaries;
  let distNode: QuerySummary | null = null;
  const nodes: QuerySummary[] = [];

  for (const [, nodeSummary] of Object.entries(querySummaries)) {
    if (nodeSummary && nodeSummary.is_distributed) {
      distNode = nodeSummary;
    } else if (nodeSummary) {
      nodes.push(nodeSummary);
    }
  }

  const nodeCount = (distNode ? 1 : 0) + nodes.length;

  return (
    <CollapsibleSection
      title="Trace"
      badge={`${nodeCount} node${nodeCount !== 1 ? "s" : ""}`}
      defaultExpanded={false}
      classes={classes}
    >
      {distNode && <NodeSummary node={distNode} classes={classes} />}
      {nodes.map((node) => (
        <NodeSummary key={node.node_name} node={node} classes={classes} />
      ))}
      {profileEvents && Object.keys(profileEvents).length > 0 && (
        <ProfileEventsOutput profileEvents={profileEvents} classes={classes} />
      )}
    </CollapsibleSection>
  );
}

function NodeSummary({
  node,
  classes,
}: {
  node: QuerySummary;
  classes: Record<string, string>;
}) {
  if (!node) return null;

  const dist = node.is_distributed ? " (Distributed)" : "";
  const execTime =
    node.execute_summaries && node.execute_summaries[0]
      ? node.execute_summaries[0].seconds
      : "N/A";

  return (
    <div style={{ marginBottom: "12px" }}>
      <div className={classes.nodeHeader}>
        {node.node_name}
        {dist}: {execTime} sec
      </div>
      {node.index_summaries &&
        node.index_summaries.map((idx: IndexSummary, i: number) => (
          <div key={i} className={classes.summaryText}>
            Index `{idx.index_name}` on {idx.table_name}: dropped{" "}
            {idx.dropped_granules}/{idx.total_granules} granules
          </div>
        ))}
      {node.select_summaries &&
        node.select_summaries.map((sel: SelectSummary, i: number) => (
          <div key={i} className={classes.summaryText}>
            {sel.table_name}: {sel.parts_selected_by_partition_key}/
            {sel.total_parts} parts, {sel.marks_selected_by_primary_key}/
            {sel.total_marks} marks
          </div>
        ))}
      {node.stream_summaries &&
        node.stream_summaries.map((str: StreamSummary, i: number) => (
          <div key={i} className={classes.summaryText}>
            {str.table_name}: {str.streams} threads
          </div>
        ))}
      {node.aggregation_summaries &&
        node.aggregation_summaries.map((agg: AggregationSummary, i: number) => (
          <div key={i} className={classes.summaryText}>
            Aggregated {agg.before_row_count} to {agg.after_row_count} rows in{" "}
            {agg.seconds} sec
          </div>
        ))}
      {node.sorting_summaries &&
        node.sorting_summaries.map((sort: SortingSummary, i: number) => (
          <div key={i} className={classes.summaryText}>
            Sorted {sort.sorted_blocks} blocks, {sort.rows} rows in{" "}
            {sort.seconds} sec
          </div>
        ))}
      {node.execute_summaries &&
        node.execute_summaries.map((exec: ExecuteSummary, i: number) => (
          <div key={i} className={classes.summaryText}>
            Read {exec.rows_read} rows, {exec.memory_size} in {exec.seconds}{" "}
            sec ({exec.rows_per_second} rows/sec)
          </div>
        ))}
    </div>
  );
}

function RawTraceOutput({
  traceOutput,
  profileEvents,
  classes,
}: {
  traceOutput: string;
  profileEvents?: any;
  classes: Record<string, string>;
}) {
  if (!traceOutput) {
    return (
      <CollapsibleSection
        title="Raw Trace"
        defaultExpanded={false}
        classes={classes}
      >
        <div className={classes.emptyResult}>No trace data available</div>
      </CollapsibleSection>
    );
  }

  const lines = traceOutput.split("\n").filter((l) => l.trim());

  return (
    <CollapsibleSection
      title="Raw Trace"
      badge={`${lines.length} lines`}
      defaultExpanded={false}
      classes={classes}
    >
      {profileEvents && Object.keys(profileEvents).length > 0 && (
        <ProfileEventsOutput profileEvents={profileEvents} classes={classes} />
      )}
      <div className={classes.rawTraceContent}>
        {lines.map((line, idx) => (
          <div key={idx} className={classes.rawTraceLine}>
            {line}
          </div>
        ))}
      </div>
    </CollapsibleSection>
  );
}

function ProfileEventsOutput({
  profileEvents,
  classes,
}: {
  profileEvents: Record<string, { column_names?: string[]; rows?: string[] }>;
  classes: Record<string, string>;
}) {
  return (
    <div style={{ marginTop: "8px", marginBottom: "8px" }}>
      <div style={{ color: "#888888", fontSize: "12px", marginBottom: "4px" }}>
        Profile Events:
      </div>
      {Object.entries(profileEvents).map(([host, data]) => (
        <div key={host} style={{ marginLeft: "8px" }}>
          <span style={{ color: "#00ffff" }}>[{host}]</span>
          {data.rows && data.rows[0] && (
            <span style={{ color: "#cccccc", marginLeft: "8px" }}>
              {data.rows[0].substring(0, 100)}
              {data.rows[0].length > 100 ? "..." : ""}
            </span>
          )}
        </div>
      ))}
    </div>
  );
}

export default ShellOutput;
