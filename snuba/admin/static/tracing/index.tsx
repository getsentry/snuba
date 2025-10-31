import React, { useState } from "react";
import { Accordion, Stack, Title, Text, Group, Table } from "@mantine/core";

import Client from "SnubaAdmin/api_client";
import QueryDisplay from "SnubaAdmin/tracing/query_display";
import {
  LogLine,
  TracingResult,
  TracingSummary,
  QuerySummary,
  ExecuteSummary,
  SelectSummary,
  IndexSummary,
  StreamSummary,
  AggregationSummary,
  SortingSummary,
} from "SnubaAdmin/tracing/types";

type ProfileEventValue = {
  column_names: string[];
  rows: string[];
};

type ProfileEvent = {
  [host_name: string]: ProfileEventValue;
};

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

function TracingQueries(props: { api: Client }) {
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
                  {summarizedTraceDisplay(value.summarized_trace_output, value.profile_events_results)}
                </div>
              );
            } else {
              return (
                <div>
                  <br />
                  <b>Number of rows in result set:</b> {value.num_rows_result}
                  <br />
                  {rawTraceDisplay(title, value.trace_output, value.profile_events_results)}
                </div>
              );
            }
          }
        })}
      </>
    );
  }

  function rawTraceDisplay(title: string, value: any, profileEventResults: ProfileEvent): JSX.Element | undefined {
    const parsedLines: Array<string> = value.split(/\n/);

    const profileEventRows: Array<string> = [];
    for (const [k, v] of Object.entries(profileEventResults)) {
      profileEventRows.push(k + '=>' + v.rows[0]);
    }

    return (
      <ol style={collapsibleStyle} key={title + "-root"}>
        <Title order={4}>Profile Events Output</Title>
        {profileEventRows.map((line, index) => {
          const node_name = line.split("=>")[0];
          const row = line.split("=>")[1];
          return (
            <li key={title + index}>
              <Text>[ {node_name} ] {row}</Text>
            </li>
          );
        })}
        <br />
        <Title order={4}>Trace Output</Title>
        {parsedLines.map((line, index) => {
          return (
            <li key={title + index}>
              <Text>{line}</Text>
            </li>
          );
        })}
      </ol>
    );
  }

  function indexSummary(value: IndexSummary): JSX.Element {
    return (
      <Group>
        <Text fw={600}>{value.table_name}: </Text>
        <Text>
          Index `{value.index_name}` has dropped {value.dropped_granules}/
          {value.total_granules} granules.
        </Text>
      </Group>
    );
  }

  function selectSummary(value: SelectSummary): JSX.Element {
    return (
      <Stack>
        <Group>
          <Text fw={600}>{value.table_name}:</Text>
          <Text>
            Selected {value.parts_selected_by_partition_key}/{value.total_parts}{" "}
            parts by partition key
          </Text>
        </Group>
        <Group>
          <Text fw={600}>{value.table_name}:</Text>
          <Text>
            Primary Key selected {value.parts_selected_by_primary_key} parts,{" "}
            {value.marks_selected_by_primary_key}/{value.total_marks} marks,{" "}
            {value.marks_to_read_from_ranges} total marks to process
          </Text>
        </Group>
      </Stack>
    );
  }

  function streamSummary(value: StreamSummary): JSX.Element {
    return (
      <Group>
        <Text fw={600}>{value.table_name}:</Text>
        <Text>Processing granules using {value.streams} threads</Text>
      </Group>
    );
  }

  function aggregationSummary(value: AggregationSummary): JSX.Element {
    return (
      <Text>
        Aggregated {value.before_row_count} to {value.after_row_count} rows
        (from {value.memory_size}) in {value.seconds} sec.
      </Text>
    );
  }

  function sortingSummary(value: SortingSummary): JSX.Element {
    return (
      <Text>
        Merge sorted {value.sorted_blocks} blocks, {value.rows} rows in{" "}
        {value.seconds} sec.
      </Text>
    );
  }

  function executeSummary(value: ExecuteSummary): JSX.Element {
    return (
      <Text>
        Read {value.rows_read} rows, {value.memory_size} in {value.seconds}{" "}
        sec., {value.rows_per_second} rows/sec., {value.bytes_per_second}/sec.
      </Text>
    );
  }

  function querySummary(value: QuerySummary): JSX.Element {
    const execute = value.execute_summaries ?
      value.execute_summaries[0] : null;
    const dist = value.is_distributed ? " (Distributed)" : "";
    const index_summaries = value.index_summaries
      ? value.index_summaries.map((s) => indexSummary(s))
      : null;
    const select_summaries = value.select_summaries
      ? value.select_summaries.map((s) => selectSummary(s))
      : null;
    const stream_summaries = value.stream_summaries
      ? value.stream_summaries.map((s) => streamSummary(s))
      : null;
    const show_filtering =
      index_summaries || select_summaries || stream_summaries;
    const aggregation_summaries = value.aggregation_summaries
      ? value.aggregation_summaries.map((s) => aggregationSummary(s))
      : null;
    const sorting_summaries = value.sorting_summaries
      ? value.sorting_summaries.map((s) => sortingSummary(s))
      : null;
    const show_aggregating = aggregation_summaries || sorting_summaries;
    return (
      <Accordion.Item key={value.node_name} value={value.node_name}>
        <Accordion.Control>
          <Title order={4}>
            {value.node_name} {dist}: {execute ? execute.seconds : "N/A"} sec.
          </Title>
        </Accordion.Control>
        <Accordion.Panel>
          <Stack>
            {show_filtering ? <Title order={4}>Filtering</Title> : null}
            {index_summaries}
            {select_summaries}
            {stream_summaries}
            {show_aggregating ? <Title order={4}>Aggregating</Title> : null}
            {aggregation_summaries}
            {sorting_summaries}
            <Title order={4}>Total</Title>
            {value.execute_summaries && value.execute_summaries.map((e) => executeSummary(e))}
          </Stack>
        </Accordion.Panel>
      </Accordion.Item>
    );
  }

  function summarizedTraceDisplay(
    value: TracingSummary,
    profileEventResults: ProfileEvent
  ): JSX.Element | undefined {
    let dist_node;
    let nodes = [];
    for (const [host, summary] of Object.entries(value.query_summaries)) {
      if (summary.is_distributed) {
        dist_node = summary;
      } else {
        nodes.push(summary);
      }
    }
    return (
      <Stack>
        <Accordion chevronPosition="left">
          {querySummary(dist_node as QuerySummary)}
        </Accordion>
        <Accordion chevronPosition="left">
          {nodes
            .filter((q: QuerySummary) => !q.is_distributed)
            .map((q: QuerySummary) => querySummary(q))}
        </Accordion>
        <Accordion chevronPosition="left">
          <Accordion.Item value="profile-events" key="profile-events">
            <Accordion.Control>
              <Title order={4}>Profile Events Output</Title>
            </Accordion.Control>
            <Accordion.Panel>
              {Object.entries(profileEventResults).map(([host, event]) => (
                <Accordion chevronPosition="left" key={host}>
                  <Accordion.Item value={host}>
                    <Accordion.Control>
                      <Title order={5}>{host}</Title>
                    </Accordion.Control>
                    <Accordion.Panel>
                      <Table>
                        <thead>
                          <tr>
                            <th>Event</th>
                            <th>Number of Events</th>
                          </tr>
                        </thead>
                        <tbody>
                          {event.rows.length > 0 && Object.entries(JSON.parse(event.rows[0]) as Record<string, number>).map(([key, value], rowIndex) => (
                            <tr key={rowIndex}>
                              <td>{key}</td>
                              <td>{value}</td>
                            </tr>
                          ))}
                        </tbody>
                      </Table>
                    </Accordion.Panel>
                  </Accordion.Item>
                </Accordion>
              ))}
            </Accordion.Panel>
          </Accordion.Item>
        </Accordion>
      </Stack>
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
