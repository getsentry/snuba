import React, { useEffect, useState } from "react";
import Client from "../api_client";

import { RichTextEditor } from "@mantine/tiptap";
import { useEditor } from "@tiptap/react";
import StarterKit from "@tiptap/starter-kit";
import Placeholder from "@tiptap/extension-placeholder";
import { Prism } from "@mantine/prism";

import { Table } from "../table";
import { LogLine, TracingRequest, TracingResult } from "./types";
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

let collapsibleStyle = { listStyleType: "none", fontFamily: "Monaco" };

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

function TracingQueries(props: { api: Client }) {
  const [storages, setStorages] = useState<string[]>([]);
  const [query, setQuery] = useState<QueryState>({});
  const [queryResultHistory, setQueryResultHistory] = useState<TracingResult[]>(
    []
  );
  const [isExecuting, setIsExecuting] = useState<boolean>(false);
  const endpoint = "clickhouse_trace_query";

  useEffect(() => {
    props.api.getClickhouseNodes().then((res) => {
      setStorages(res.map((n) => n.storage_name));
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

  function updateQuerySql(sql: string) {
    setQuery((prevQuery) => {
      return {
        ...prevQuery,
        sql,
      };
    });
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

  function tablePopulator(queryResult: TracingResult) {
    var elements = {};
    if (queryResult.error) {
      elements = { Error: [queryResult.error, 200] };
    } else {
      elements = { Trace: [queryResult, 400] };
    }
    return tracingOutput(elements);
  }

  function tracingOutput(elements: Object) {
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
            return (
              <div>
                <br />
                <b>Number of rows in result set:</b> {value.num_rows_result}
                <br />
                {heirarchicalTraceDisplay(title, value.trace_output)}
              </div>
            );
          }
        })}
      </>
    );
  }

  // query execution flow:
  // [high-level query node]
  //    [housekeeping] (access control, parsing)
  //    [propagation step]
  //       [for each storage node]
  //          [housekeeping]
  //          [select executor + MergeTreeSelectProcessor]
  //          [aggregating transform]
  //          [memory tracker]
  //    [aggregating transform]
  //    [memory tracker]
  function heirarchicalTraceDisplay(
    title: string,
    value: any
  ): JSX.Element | undefined {
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

  const editor = useEditor({
    extensions: [
      StarterKit,
      Placeholder.configure({
        placeholder: "Write your query here.",
      }),
    ],
    content: `${query.sql || ""}`,
    onUpdate({ editor }) {
      updateQuerySql(editor.getText());
    },
  });

  return (
    <div>
      <form>
        <h2>Construct a ClickHouse Query</h2>
        <a href="https://getsentry.github.io/snuba/clickhouse/death_queries.html">
          🛑 WARNING! BEFORE RUNNING QUERIES, READ THIS 🛑
        </a>
        <RichTextEditor editor={editor}>
          <RichTextEditor.Content />
        </RichTextEditor>
        <Prism withLineNumbers language="sql">
          {query.sql || ""}
        </Prism>
        <div style={executeActionsStyle}>
          <div>
            <select
              value={query.storage || ""}
              onChange={(evt) => selectStorage(evt.target.value)}
              style={selectStyle}
            >
              <option disabled value="">
                Select a storage
              </option>
              {storages.map((storage) => (
                <option key={storage} value={storage}>
                  {storage}
                </option>
              ))}
            </select>
          </div>
          <div>
            <button
              onClick={(_) => executeQuery()}
              style={executeButtonStyle}
              disabled={isExecuting || !query.storage || !query.sql}
            >
              Execute query
            </button>
          </div>
        </div>
      </form>
      <div>
        <h2>Query results</h2>
        <Table
          headerData={["Query", "Response"]}
          rowData={queryResultHistory.map((queryResult) => [
            <span>{queryResult.input_query}</span>,
            <div>
              <button
                style={executeButtonStyle}
                onClick={() => copyText(JSON.stringify(queryResult))}
              >
                Copy to clipboard
              </button>
              {tablePopulator(queryResult)}
            </div>,
          ])}
          columnWidths={[1, 5]}
        />
      </div>
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

export default TracingQueries;
