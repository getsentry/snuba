import React, { useState, useCallback, useEffect, useRef } from "react";
import { Accordion, Stack, Title, Text, Group, Table, Loader, Alert } from "@mantine/core";

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
  const [profileEventsCache, setProfileEventsCache] = useState<{
    [timestamp: number]: {
      loading: boolean;
      error: string | null;
      data: ProfileEvent | null;
      retryCount: number;
    };
  }>({});

  // Track pending setTimeout retries so we can clear them on unmount
  // (otherwise React warns about state updates on unmounted components and
  // the timer keeps a closure alive past the component's lifetime).
  const retryTimeoutsRef = useRef<Set<ReturnType<typeof setTimeout>>>(new Set());
  const isMountedRef = useRef(true);
  useEffect(() => {
    return () => {
      isMountedRef.current = false;
      retryTimeoutsRef.current.forEach(clearTimeout);
      retryTimeoutsRef.current.clear();
    };
  }, []);

  const fetchProfileEventsWithRetry = useCallback(async (
    querySummaries: { [nodeName: string]: QuerySummary },
    storage: string,
    timestamp: number,
    retryCount: number = 0
  ) => {
    const MAX_RETRIES = 3;
    const RETRY_DELAY_MS = 2000;

    if (!isMountedRef.current) {
      return;
    }

    setProfileEventsCache(prev => ({
      ...prev,
      [timestamp]: { loading: true, error: null, data: prev[timestamp]?.data || null, retryCount }
    }));

    try {
      const response = await props.api.fetchProfileEvents(querySummaries, storage);

      if (!isMountedRef.current) {
        return;
      }

      if (response.error) {
        throw new Error(response.error.message);
      }

      if (response.status === "not_ready" && retryCount < MAX_RETRIES) {
        // Reflect the upcoming retry in the cache immediately so the "Attempt
        // N/4" indicator advances during the wait instead of stalling on the
        // previous number until the next call lands.
        setProfileEventsCache(prev => ({
          ...prev,
          [timestamp]: {
            ...prev[timestamp],
            loading: true,
            error: null,
            retryCount: retryCount + 1,
          }
        }));
        const handle = setTimeout(() => {
          retryTimeoutsRef.current.delete(handle);
          fetchProfileEventsWithRetry(querySummaries, storage, timestamp, retryCount + 1);
        }, RETRY_DELAY_MS);
        retryTimeoutsRef.current.add(handle);
        return;
      }

      if (response.status === "not_ready") {
        setProfileEventsCache(prev => ({
          ...prev,
          [timestamp]: {
            loading: false,
            error: "Profile events not ready. Try again in a few seconds.",
            data: null,
            retryCount
          }
        }));
        return;
      }

      setProfileEventsCache(prev => ({
        ...prev,
        [timestamp]: {
          loading: false,
          error: null,
          data: response.profile_events_results as ProfileEvent,
          retryCount
        }
      }));
    } catch (error) {
      // The API client rejects non-2xx (and non-404) responses with the
      // parsed JSON body, which has the shape {error: {type, message}}.
      // Fall back to Error.message, then a generic string.
      let message = "Failed to fetch";
      if (error instanceof Error) {
        message = error.message;
      } else if (error && typeof error === "object") {
        const errObj = error as { error?: { message?: string }; message?: string };
        message = errObj.error?.message ?? errObj.message ?? message;
      }
      setProfileEventsCache(prev => ({
        ...prev,
        [timestamp]: {
          loading: false,
          error: message,
          data: null,
          retryCount
        }
      }));
    }
  }, [props.api]);

  // Plain function rather than useCallback: every consumer wraps it in an
  // inline arrow, so the memo would buy nothing, and including
  // profileEventsCache in the dep array (needed to read cached state below)
  // would recreate the callback on every cache update anyway.
  function handleProfileEventsAccordionChange(value: string | null, queryResult: TracingResult) {
    if (value !== "profile-events") {
      return;
    }
    const timestamp = queryResult.timestamp;
    const cached = profileEventsCache[timestamp];

    // Re-fetch when nothing is in flight and we don't already have data —
    // including after a previous error, so the user can retry by collapsing
    // and re-expanding the accordion.
    if (cached && (cached.loading || cached.data)) {
      return;
    }

    if (!queryResult.summarized_trace_output?.query_summaries) {
      return;
    }

    if (!queryResult.storage) {
      // History entries written before storage was added to TracingResult
      // (and persisted in localStorage) won't have it. Surface a clear
      // message so the user knows to re-run the query rather than waiting
      // for events that will never load.
      setProfileEventsCache(prev => ({
        ...prev,
        [timestamp]: {
          loading: false,
          error: "Re-run this query to load profile events; this entry was saved before lazy loading was introduced.",
          data: null,
          retryCount: 0,
        }
      }));
      return;
    }

    fetchProfileEventsWithRetry(
      queryResult.summarized_trace_output.query_summaries,
      queryResult.storage,
      timestamp,
      0
    );
  }

  function tablePopulator(queryResult: TracingResult, showFormatted: boolean) {
    var elements = {};
    if (queryResult.error) {
      elements = { Error: [queryResult.error, 200] };
    } else {
      elements = { Trace: [queryResult, 400] };
    }
    return tracingOutput(elements, showFormatted, queryResult);
  }

  function tracingOutput(elements: Object, showFormatted: boolean, queryResult: TracingResult) {
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
                  {summarizedTraceDisplay(value.summarized_trace_output, value.profile_events_results, queryResult)}
                </div>
              );
            } else {
              return (
                <div>
                  <br />
                  <b>Number of rows in result set:</b> {value.num_rows_result}
                  <br />
                  {rawTraceDisplay(title, value.trace_output, value.profile_events_results, queryResult)}
                </div>
              );
            }
          }
        })}
      </>
    );
  }

  function rawTraceDisplay(title: string, value: any, profileEventResults: ProfileEvent, queryResult: TracingResult): JSX.Element | undefined {
    const parsedLines: Array<string> = value.split(/\n/);
    const timestamp = queryResult.timestamp;
    const profileEventsState = profileEventsCache[timestamp];
    const effectiveProfileEvents = profileEventsState?.data || profileEventResults || {};

    const profileEventRows: Array<string> = [];
    for (const [k, v] of Object.entries(effectiveProfileEvents)) {
      if (v.rows.length > 0) {
        profileEventRows.push(k + '=>' + v.rows[0]);
      }
    }

    return (
      <ol style={collapsibleStyle} key={title + "-root"}>
        <Accordion
          chevronPosition="left"
          onChange={(value) => handleProfileEventsAccordionChange(value, queryResult)}
        >
          <Accordion.Item value="profile-events" key="profile-events">
            <Accordion.Control>
              <Title order={4}>Profile Events Output</Title>
            </Accordion.Control>
            <Accordion.Panel>
              {profileEventsState?.loading && (
                <li>
                  <Loader size="sm" />
                  <Text>Loading profile events...</Text>
                </li>
              )}
              {profileEventsState?.error && (
                <li>
                  <Alert color="yellow">{profileEventsState.error}</Alert>
                </li>
              )}
              {profileEventsState &&
                !profileEventsState.loading &&
                !profileEventsState.error &&
                profileEventRows.length === 0 && (
                  <li>
                    <Alert color="blue">No profile events found</Alert>
                  </li>
                )}
              {profileEventRows.map((line, index) => {
                const node_name = line.split("=>")[0];
                const row = line.split("=>")[1];
                return (
                  <li key={title + index}>
                    <Text>[ {node_name} ] {row}</Text>
                  </li>
                );
              })}
            </Accordion.Panel>
          </Accordion.Item>
        </Accordion>
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
    profileEventResults: ProfileEvent,
    queryResult: TracingResult
  ): JSX.Element | undefined {
    const timestamp = queryResult.timestamp;
    const profileEventsState = profileEventsCache[timestamp];
    const effectiveProfileEvents = profileEventsState?.data || profileEventResults || {};

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
        <Accordion chevronPosition="left" onChange={(value) => handleProfileEventsAccordionChange(value, queryResult)}>
          <Accordion.Item value="profile-events" key="profile-events">
            <Accordion.Control>
              <Title order={4}>Profile Events Output</Title>
            </Accordion.Control>
            <Accordion.Panel>
              {profileEventsState?.loading && (
                <Stack>
                  <Loader size="md" />
                  <Text>Loading profile events... (Attempt {profileEventsState.retryCount + 1}/4)</Text>
                </Stack>
              )}
              {profileEventsState?.error && (
                <Alert color="yellow" title="Profile Events Not Available">
                  {profileEventsState.error}
                </Alert>
              )}
              {profileEventsState &&
                !profileEventsState.loading &&
                !profileEventsState.error &&
                Object.keys(effectiveProfileEvents).length === 0 && (
                  <Alert color="blue" title="No Profile Events">
                    No profile events were found for this query.
                  </Alert>
                )}
              {Object.entries(effectiveProfileEvents).map(([host, event]) => (
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
