import React from "react";
import { useShellStyles } from "SnubaAdmin/sql_shell/styles";
import { ShellHistoryEntry, ShellMode } from "SnubaAdmin/sql_shell/types";
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

interface ShellOutputProps {
  entries: ShellHistoryEntry[];
  traceFormatted: boolean;
  mode: ShellMode;
}

export function ShellOutput({ entries, traceFormatted, mode }: ShellOutputProps) {
  const { classes } = useShellStyles();

  return (
    <>
      {entries.map((entry, idx) => (
        <ShellEntry key={idx} entry={entry} traceFormatted={traceFormatted} mode={mode} classes={classes} />
      ))}
    </>
  );
}

function ShellEntry({
  entry,
  traceFormatted,
  mode,
  classes,
}: {
  entry: ShellHistoryEntry;
  traceFormatted: boolean;
  mode: ShellMode;
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
          classes={classes}
        />
      );
    case "system_result":
      return (
        <SystemResultsOutput
          result={entry.content}
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
  error: string;
  classes: Record<string, string>;
}) {
  return <div className={classes.errorText}>Error: {error}</div>;
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
      <div style={{ marginTop: "12px", color: "#888888" }}>
        Keyboard shortcuts:
      </div>
      <div style={{ marginTop: "4px" }}>
        <span className={classes.helpCommand}>Enter / Cmd+Enter</span>
        <span className={classes.helpDescription}>Execute command</span>
      </div>
      <div style={{ marginBottom: "4px" }}>
        <span className={classes.helpCommand}>Up/Down Arrow</span>
        <span className={classes.helpDescription}>Navigate command history</span>
      </div>
      <div style={{ marginBottom: "4px" }}>
        <span className={classes.helpCommand}>Ctrl+L</span>
        <span className={classes.helpDescription}>Clear screen</span>
      </div>
    </div>
  );
}

function ResultsOutput({
  result,
  traceFormatted,
  classes,
}: {
  result: TracingResult;
  traceFormatted: boolean;
  classes: Record<string, string>;
}) {
  if (result.error) {
    return <ErrorOutput error={result.error} classes={classes} />;
  }

  const cols = result.cols || [];
  const rows = result.result || [];

  return (
    <div>
      <div className={classes.resultBox}>
        <div className={classes.resultHeader}>
          Query Results ({result.num_rows_result || 0} rows)
        </div>
        {cols.length > 0 ? (
          <div className={classes.tableContainer}>
            <table className={classes.resultTable}>
              <thead>
                <tr>
                  {cols.map((col: string[], idx: number) => (
                    <th key={idx}>
                      {col[0]} <small style={{ color: "#666666" }}>({col[1]})</small>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.slice(0, 100).map((row: any[], rowIdx: number) => (
                  <tr key={rowIdx}>
                    {row.map((cell, cellIdx) => (
                      <td key={cellIdx}>
                        {typeof cell === "object" ? JSON.stringify(cell) : String(cell)}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
            {rows.length > 100 && (
              <div style={{ color: "#888888", marginTop: "8px" }}>
                ... showing first 100 of {rows.length} rows
              </div>
            )}
          </div>
        ) : (
          <div style={{ color: "#888888" }}>No results returned</div>
        )}
      </div>

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
  classes,
}: {
  result: QueryResult;
  classes: Record<string, string>;
}) {
  if (result.error) {
    return <ErrorOutput error={result.error} classes={classes} />;
  }

  const cols = result.column_names || [];
  const rows = result.rows || [];

  return (
    <div>
      <div className={classes.resultBox}>
        <div className={classes.resultHeader}>
          Query Results ({rows.length} rows)
        </div>
        {cols.length > 0 ? (
          <div className={classes.tableContainer}>
            <table className={classes.resultTable}>
              <thead>
                <tr>
                  {cols.map((col: string, idx: number) => (
                    <th key={idx}>{col}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.slice(0, 100).map((row: any[], rowIdx: number) => (
                  <tr key={rowIdx}>
                    {row.map((cell, cellIdx) => (
                      <td key={cellIdx}>
                        {typeof cell === "object" ? JSON.stringify(cell) : String(cell)}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
            {rows.length > 100 && (
              <div style={{ color: "#888888", marginTop: "8px" }}>
                ... showing first 100 of {rows.length} rows
              </div>
            )}
          </div>
        ) : (
          <div style={{ color: "#888888" }}>No results returned</div>
        )}
      </div>
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
  const querySummaries = summary.query_summaries;
  let distNode: QuerySummary | null = null;
  const nodes: QuerySummary[] = [];

  for (const [, nodeSummary] of Object.entries(querySummaries)) {
    if (nodeSummary.is_distributed) {
      distNode = nodeSummary;
    } else {
      nodes.push(nodeSummary);
    }
  }

  return (
    <div className={classes.traceBox}>
      <div className={classes.resultHeader}>Trace Summary</div>
      {distNode && <NodeSummary node={distNode} classes={classes} />}
      {nodes.map((node) => (
        <NodeSummary key={node.node_name} node={node} classes={classes} />
      ))}
      {profileEvents && Object.keys(profileEvents).length > 0 && (
        <ProfileEventsOutput profileEvents={profileEvents} classes={classes} />
      )}
    </div>
  );
}

function NodeSummary({
  node,
  classes,
}: {
  node: QuerySummary;
  classes: Record<string, string>;
}) {
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
  const lines = traceOutput.split("\n").filter((l) => l.trim());

  return (
    <div className={classes.traceBox}>
      <div className={classes.resultHeader}>Raw Trace Output</div>
      {profileEvents && Object.keys(profileEvents).length > 0 && (
        <ProfileEventsOutput profileEvents={profileEvents} classes={classes} />
      )}
      <div style={{ marginTop: "8px" }}>
        {lines.map((line, idx) => (
          <div key={idx} style={{ color: "#cccccc", fontSize: "12px" }}>
            {line}
          </div>
        ))}
      </div>
    </div>
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
