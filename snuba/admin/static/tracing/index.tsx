import React, { useState } from "react";
import Client from "SnubaAdmin/api_client";
import QueryDisplay from "SnubaAdmin/tracing/query_display";
import { LogLine, TracingResult } from "SnubaAdmin/tracing/types";
import { parseLogLine } from "SnubaAdmin/tracing/util";

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
  width: "fit-content",
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

function TracingQueries(props: { api: Client }) {
  const hidden_formatted_trace_fields = new Set<string>([
    "thread_ids",
    "node_name",
    "node_type",
    "storage_nodes_accessed",
  ]);

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
            if (showFormatted) {
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
                  {rawTraceDisplay(title, value.trace_output)}
                </div>
              );
            }
          }
        })}
      </>
    );
  }

  function rawTraceDisplay(title: string, value: any): JSX.Element | undefined {
    const parsedLines: Array<string> = value.split(/\n/);

    return (
      <ol style={collapsibleStyle} key={title + "-root"}>
        {parsedLines.map((line, index) => {
          return (
            <li key={title + index}>
              <span>{line}</span>
            </li>
          );
        })}
      </ol>
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

export default TracingQueries;
