import React, { useEffect, useState } from "react";
import Client from "../api_client";
import QueryDisplay from "./query_display";
import {
  LogLine,
  TracingRequest,
  TracingResult,
  PredefinedQuery,
} from "./types";
import { parseLogLine } from "./util";

type QueryState = Partial<TracingRequest>;

type BucketedLogs = Map<String, Map<MessageCategory, LogLine[]>>;

enum MessageCategory {
  housekeeping,
  select_execution,
  aggregation,
  memory_tracker,
  unknown,
}

let collapsibleStyle = {
  listStyleType: "none",
  fontFamily: "Monaco",
};

let testStyle = {
  width: "80&%",
};

function getMessageCategory(logLine: LogLine): MessageCategory {
  const component = logLine.component;
  if (
    component.match(/^ContextAccess|AccessRightsContext|^executeQuery/) &&
    !logLine.message.startsWith("Read")
  ) {
    return MessageCategory.housekeeping;
  } else if (component.match(/\(SelectExecutor\)|MergeTreeSelectProcessor/)) {
    return MessageCategory.select_execution;
  } else if (
    component.match(/^InterpreterSelectQuery|AggregatingTransform|Aggregator/)
  ) {
    return MessageCategory.aggregation;
  } else if (
    component.match(/MemoryTracker/) ||
    (component.match(/^executeQuery/) && logLine.message.startsWith("Read"))
  ) {
    return MessageCategory.memory_tracker;
  } else {
    return MessageCategory.unknown;
  }
}

function NodalDisplay(props: {
  host: string;
  category: MessageCategory;
  title?: string;
  logsBucketed: BucketedLogs;
}) {
  const [visible, setVisible] = useState<boolean>(false);

  const nodeKey = props.host + "-" + props.category;
  return (
    <li key={nodeKey}>
      <span onClick={() => setVisible(!visible)}>
        {visible ? "[-]" : "[+]"} {props.title}
      </span>

      <ol key={nodeKey + "-child"} style={collapsibleStyle}>
        {visible &&
          props.logsBucketed
            .get(props.host)
            ?.get(props.category)
            ?.map((line, index) => {
              return (
                <li key={nodeKey + index}>
                  [{line?.log_level}] {line?.component}: {line?.message}
                </li>
              );
            })}
      </ol>
    </li>
  );
}

function FormattedNodalDisplay(props: {
  header: string;
  data: string[] | string | number;
}) {
  const [visible, setVisible] = useState<boolean>(false);

  return (
    <li>
      <span onClick={() => setVisible(!visible)}>
        {visible ? "[-]" : "[+]"} {props.header.split("_").join(" ")}
      </span>

      <ol style={collapsibleStyle}>
        {visible &&
          Array.isArray(props.data) &&
          props.data.map((log: string, log_idx: number) => {
            return <li>{log}</li>;
          })}
        {visible &&
          (typeof props.data === "string" ||
            typeof props.data === "number") && <li>{props.data}</li>}
      </ol>
    </li>
  );
}

function TracingQueries(props: { api: Client }) {
  const [query, setQuery] = useState<QueryState>({});
  const [queryResultHistory, setQueryResultHistory] = useState<TracingResult[]>(
    []
  );
  const [isExecuting, setIsExecuting] = useState<boolean>(false);
  const [predefinedQueryOptions, setPredefinedQueryOptions] = useState<
    PredefinedQuery[]
  >([]);

  const endpoint = "clickhouse_trace_query";
  const hidden_formatted_trace_fields = new Set<string>([
    "thread_ids",
    "node_name",
    "node_type",
    "storage_nodes_accessed",
  ]);

  function formatSQL(sql: string) {
    const formatted = sql
      .split("\n")
      .map((line) => line.substring(4, line.length))
      .join("\n");
    return formatted.trim();
  }

  function executeQuery() {
    if (isExecuting) {
      window.alert("A query is already running");
    }
    setIsExecuting(true);
    props.api
      .executeTracingQuery(query as TracingRequest)
      .then((result) => {
        const tracing_result = {
          input_query: `${query.sql}`,
          timestamp: result.timestamp,
          num_rows_result: result.num_rows_result,
          cols: result.cols,
          trace_output: result.trace_output,
          formatted_trace_output: result.formatted_trace_output,
          error: result.error,
        };
        setQueryResultHistory((prevHistory) => [
          tracing_result,
          ...prevHistory,
        ]);
      })
      .catch((err) => {
        console.log("ERROR", err);
        window.alert("An error occurred: " + err.error.message);
      })
      .finally(() => {
        setIsExecuting(false);
      });
  }

  function copyText(text: string) {
    window.navigator.clipboard.writeText(text);
  }

  function tablePopulator(queryResult: TracingResult, showFormatted: boolean) {
    var elements = {};
    if (queryResult.error) {
      elements = { Error: [queryResult.error, 200] };
    } else {
      elements = { Trace: [queryResult, 400] };
    }
    return tracingOutput(elements, showFormatted);
  }

  function tracingOutput(elements: Object, showFormatted: boolean) {
    return (
      <>
        <br />
        <br />
        {Object.entries(elements).map(([title, [value, height]]) => {
          if (title === "Error") {
            return (
              <div key={value}>
                {title}
                <textarea
                  spellCheck={false}
                  value={value}
                  style={{ width: "100%", height: height }}
                  disabled
                />
              </div>
            );
          } else if (title === "Trace") {
            if (!showFormatted) {
              return (
                <div>
                  <br />
                  <b>Number of rows in result set:</b> {value.num_rows_result}
                  <br />
                  {heirarchicalRawTraceDisplay(title, value.trace_output)}
                </div>
              );
            } else {
              return (
                <div>
                  <br />
                  <b>Number of rows in result set:</b> {value.num_rows_result}
                  <br />
                  <pre>
                    <code>
                      {formattedTraceDisplay(
                        title,
                        value.formatted_trace_output
                      )}
                    </code>
                  </pre>
                </div>
              );
            }
          }
        })}
      </>
    );
  }

  function heirarchicalRawTraceDisplay(
    title: string,
    value: any
  ): JSX.Element | undefined {
    /*
    query execution flow:
    [high-level query node]
      [housekeeping] (access control, parsing)
      [propagation step]
      [for each storage node]
        [housekeeping]
        [select executor + MergeTreeSelectProcessor]
        [aggregating transform]
        [memory tracker]
      [aggregating transform]
      [memory tracker]
    */
    const parsedLines: Array<LogLine> = value
      .split(/\n/)
      .map(parseLogLine)
      .filter((x: LogLine | null) => x != null);

    // logsBucketed maps host -> (category -> logs)
    const logsBucketed: BucketedLogs = new Map();

    const orderedHosts: string[] = [];
    parsedLines.forEach((line) => {
      if (!orderedHosts.includes(line.host)) {
        orderedHosts.push(line.host);
      }
      if (logsBucketed.has(line.host)) {
        const hostLogs = logsBucketed.get(line.host);
        if (hostLogs?.has(getMessageCategory(line))) {
          hostLogs.get(getMessageCategory(line))?.push(line);
        } else {
          hostLogs?.set(getMessageCategory(line), [line]);
        }
      } else {
        logsBucketed.set(
          line.host,
          new Map<MessageCategory, LogLine[]>([
            [getMessageCategory(line), [line]],
          ])
        );
      }
    });

    let rootHost = orderedHosts[0];

    const CATEGORIES_ORDERED = [
      MessageCategory.housekeeping,
      MessageCategory.select_execution,
      MessageCategory.aggregation,
      MessageCategory.memory_tracker,
    ];
    const CATEGORY_HEADERS = new Map<MessageCategory, string>([
      [MessageCategory.housekeeping, "Housekeeping"],
      [MessageCategory.select_execution, "Select execution"],
      [MessageCategory.aggregation, "Aggregation"],
      [MessageCategory.memory_tracker, "Memory Tracking"],
    ]);

    return (
      <ol style={collapsibleStyle} key={title + "-root"}>
        <li key="header-root">Query node - {rootHost}</li>
        <li key="root-storages">
          <ol style={collapsibleStyle}>
            <NodalDisplay
              host={rootHost}
              title={CATEGORY_HEADERS.get(MessageCategory.housekeeping)}
              category={MessageCategory.housekeeping}
              logsBucketed={logsBucketed}
            />
            <li>
              <span>Storage nodes</span>
              <ol style={collapsibleStyle}>
                {orderedHosts.slice(1).map((host) => {
                  return (
                    <li key={"top-" + host}>
                      <span>Storage node - {host}</span>
                      {CATEGORIES_ORDERED.map((category) => {
                        return (
                          <ol
                            key={"section-" + category + "-" + host}
                            style={collapsibleStyle}
                          >
                            <NodalDisplay
                              host={host}
                              title={CATEGORY_HEADERS.get(category)}
                              category={category}
                              logsBucketed={logsBucketed}
                            />
                          </ol>
                        );
                      })}
                    </li>
                  );
                })}
              </ol>
            </li>
            <NodalDisplay
              host={rootHost}
              title={CATEGORY_HEADERS.get(MessageCategory.aggregation)}
              category={MessageCategory.aggregation}
              logsBucketed={logsBucketed}
            />
            <NodalDisplay
              host={rootHost}
              title={CATEGORY_HEADERS.get(MessageCategory.memory_tracker)}
              category={MessageCategory.memory_tracker}
              logsBucketed={logsBucketed}
            />
          </ol>
        </li>
      </ol>
    );
  }

  function formattedTraceDisplay(
    title: string,
    value: any
  ): JSX.Element | undefined {
    let node_names = Object.keys(value);
    let query_node_name = "";
    for (const node_name of node_names) {
      if (value[node_name]["node_type"] == "query") {
        query_node_name = node_name;
      }
    }
    return (
      <ol style={collapsibleStyle}>
        <li>Query node - {query_node_name}</li>
        <ol style={collapsibleStyle}>
          {Object.keys(value[query_node_name]).map(
            (header: string, idx: number) => {
              if (!hidden_formatted_trace_fields.has(header)) {
                const data = value[query_node_name][header];
                return <FormattedNodalDisplay header={header} data={data} />;
              }
            }
          )}
        </ol>
        {node_names.map((node_name, idx) => {
          if (node_name != query_node_name) {
            return (
              <ol style={collapsibleStyle}>
                <br />
                <li>Storage node - {node_name}</li>
                <ol style={collapsibleStyle}>
                  {Object.keys(value[node_name]).map(
                    (header: string, idx: number) => {
                      if (!hidden_formatted_trace_fields.has(header)) {
                        const data = value[node_name][header];
                        return (
                          <FormattedNodalDisplay header={header} data={data} />
                        );
                      }
                    }
                  )}
                </ol>
              </ol>
            );
          }
        })}
      </ol>
    );
  }

  return (
    <div>
      {QueryDisplay({
        api: props.api,
        resultDataPopulator: tablePopulator,
        predefinedQueryOptions: [],
      })}
    </div>
  );
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

function TextArea(props: {
  value: string;
  onChange: (nextValue: string) => void;
}) {
  const { value, onChange } = props;
  return (
    <textarea
      spellCheck={false}
      value={value}
      onChange={(evt) => onChange(evt.target.value)}
      style={{ width: "100%", height: 100 }}
      placeholder={"Write your query here"}
    />
  );
}

export default TracingQueries;
