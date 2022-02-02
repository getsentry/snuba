import React, { Component } from "react";
import Client from "../api_client";
import QueryDisplay from "../components/query_display/query_display";
import { QueryResult } from "../components/query_display/types";

type LogLine = {
  host: string;
  pid: string;
  query_id: string;
  log_level: string;
  component: string;
  message: string;
};

const logLineMatcher =
  /\[ (?<hostname>\S+) \] \[ (?<pid>\d+) \] \{(?<local_query_id>[^}]+)\} <(?<log_level>[^>]+)> (?<component>[^:]+): (?<message>.*)/;

function parseLogLine(logLine: string): LogLine | null {
  const logLineRegexMatch = logLine.match(logLineMatcher);
  const host = logLineRegexMatch?.groups?.hostname;
  const pid = logLineRegexMatch?.groups?.pid;
  const local_query_id = logLineRegexMatch?.groups?.local_query_id;
  const log_level = logLineRegexMatch?.groups?.log_level;
  const message = logLineRegexMatch?.groups?.message;
  const component = logLineRegexMatch?.groups?.component;

  if (host && pid && local_query_id && log_level && message) {
    const logLineParsed: LogLine = {
      host: host,
      pid: pid!,
      query_id: local_query_id,
      log_level: log_level!,
      component: component!,
      message: message!,
    };
    return logLineParsed;
  } else {
    return null;
  }
}

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
  } else if (
    component.match(
      /^(default.(\S+) \(SelectExecutor\)|MergeTreeSelectProcessor)/
    )
  ) {
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

function TracingQueries(props: { api: Client }) {
  function tablePopulator(queryResult: QueryResult) {
    var elements = {};
    if (queryResult.error) {
      elements = { Error: [queryResult.error, 200] };
    } else {
      elements = { Trace: [queryResult.trace_output, 400] };
    }
    return tracingOutput(elements);
  }

  function tracingOutput(elements: Object) {
    return (
      <>
        <br />
        <br />
        {Object.entries(elements).map(([title, [value, height]]) => {
          console.log(title);

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
            return heirarchicalTraceDisplay(title, value);
          }
        })}
      </>
    );
  }

  return QueryDisplay({
    api: props.api,
    endpoint: "clickhouse_trace_query",
    resultDataPopulator: tablePopulator,
  });

  function heirarchicalTraceDisplay(
    title: string,
    value: any
  ): JSX.Element | undefined {
    const parsedLines: Array<LogLine> = value
      .split(/\n/)
      .map(parseLogLine)
      .filter((x: LogLine | null) => x != null);

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
    // logsBucketed maps host -> (category -> logs)
    const logsBucketed: Map<
      String,
      Map<MessageCategory, LogLine[]>
    > = new Map();

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

    console.log(logsBucketed);
    const HOUSEKEEPING = "Housekeeping";
    const SELECT_EXECUTION = "Select execution";

    return (
      <ol style={collapsibleStyle} key={title + "-root"}>
        <li>Query node - {rootHost}</li>
        <li>
          <ol style={collapsibleStyle}>
            {sectionForNode(
              rootHost,
              HOUSEKEEPING,
              MessageCategory.housekeeping
            )}
            <li>
              <span>Storage nodes</span>
              <ol style={collapsibleStyle}>
                {orderedHosts.slice(1).map((host) => {
                  logsBucketed.get(host);
                  return (
                    <li>
                      <span>Storage node - {host}</span>
                      <ol style={collapsibleStyle}>
                        {sectionForNode(
                          host,
                          HOUSEKEEPING,
                          MessageCategory.housekeeping
                        )}
                      </ol>
                      <ol style={collapsibleStyle}>
                        {sectionForNode(
                          host,
                          SELECT_EXECUTION,
                          MessageCategory.select_execution
                        )}
                      </ol>
                      <ol style={collapsibleStyle}>
                        {sectionForNode(
                          host,
                          "Aggregation",
                          MessageCategory.aggregation
                        )}
                      </ol>
                      <ol style={collapsibleStyle}>
                        {sectionForNode(
                          host,
                          "Memory tracking",
                          MessageCategory.memory_tracker
                        )}
                      </ol>
                    </li>
                  );
                })}
              </ol>
            </li>
            <li>
              <ol style={collapsibleStyle}>
                {sectionForNode(
                  rootHost,
                  "Aggregation",
                  MessageCategory.aggregation
                )}
              </ol>
            </li>
            <li>
              <ol style={collapsibleStyle}>
                {sectionForNode(
                  rootHost,
                  "Memory tracking",
                  MessageCategory.memory_tracker
                )}
              </ol>
            </li>
          </ol>
        </li>
      </ol>
    );

    function sectionForNode(
      host: string,
      title: string,
      category: MessageCategory
    ) {
      return (
        <li>
          <span>{title}</span>
          <ol
            style={collapsibleStyle}
            onClick={() => console.log("should hide/show section", host, title)}
          >
            {logsBucketed
              .get(host)
              ?.get(category)
              ?.map((line, index) => {
                return (
                  <li key={index}>
                    {line?.component}:{line?.message}
                  </li>
                );
              })}
          </ol>
        </li>
      );
    }
  }
}

export default TracingQueries;
