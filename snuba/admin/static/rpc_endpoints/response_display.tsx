import React from 'react';
import { Space, Switch, Accordion, Text, Code, Group } from '@mantine/core';
import { TraceLog } from 'SnubaAdmin/rpc_endpoints/trace_formatter';
import SummarizedTraceDisplay from 'SnubaAdmin/rpc_endpoints/summarized_trace';
import { ProfileEventsTable } from 'SnubaAdmin/rpc_endpoints/profile_events_table';
import { MetadataTable } from 'SnubaAdmin/rpc_endpoints/metadata_table';
import { QueryInfo, TracingSummary, HostProfileEvents } from 'SnubaAdmin/rpc_endpoints/types';
import { useStyles } from 'SnubaAdmin/rpc_endpoints/styles';
import { ResponseDisplayProps } from 'SnubaAdmin/rpc_endpoints/types';

const ToggleOption = ({ active, children }: { active: boolean; children: React.ReactNode }) => {
  const { classes } = useStyles();

  return (
    <Text
      className={classes.toggleOption}
      data-active={active}
    >
      {children}
    </Text>
  );
};

export const ResponseDisplay = ({
  response,
  showTraceLogs,
  setShowTraceLogs,
  showSummarizedView,
  setShowSummarizedView,
  summarizedTraceOutput,
  showProfileEvents,
  setShowProfileEvents,
  profileEvents,
  classes
}: ResponseDisplayProps) => (
  <>
    <Space h="md" />
    <h3>Response:</h3>
    <h4>Response Metadata:</h4>
    <Group position="left" spacing="xl">
      <ToggleOption active={!showTraceLogs}>Query Metadata</ToggleOption>
      <Switch
        className={classes.viewToggle}
        checked={showTraceLogs}
        onChange={(event) => setShowTraceLogs(event.currentTarget.checked)}
      />
      <ToggleOption active={showTraceLogs}>Trace Logs</ToggleOption>
    </Group>
    <Accordion classNames={{ item: classes.mainAccordion }}>
      <Accordion.Item value="query-info">
        <Accordion.Control>
          <Text fw={700}>
            {showTraceLogs ? "Trace Information" : "Query Information"}
          </Text>
        </Accordion.Control>
        <Accordion.Panel>
          {response.meta?.queryInfo ? (
            response.meta.queryInfo.map((queryInfo: QueryInfo, index: number) => (
              <div key={index}>
                {showTraceLogs ? (
                  <>
                    <h4>Trace Logs</h4>
                    <Group position="left" spacing="xl">
                      <ToggleOption active={!showSummarizedView}>Raw Logs</ToggleOption>
                      <Switch
                        className={classes.viewToggle}
                        checked={showSummarizedView}
                        onChange={(event) => setShowSummarizedView(event.currentTarget.checked)}
                      />
                      <ToggleOption active={showSummarizedView}>Summarized</ToggleOption>
                    </Group>
                    {queryInfo.traceLogs ? (
                      showSummarizedView ? (
                        summarizedTraceOutput ? (
                          <SummarizedTraceDisplay value={summarizedTraceOutput} classes={classes} />
                        ) : (
                          <Text>Retrieving summarized trace data...</Text>
                        )
                      ) : (
                        <TraceLog log={queryInfo.traceLogs} />
                      )
                    ) : (
                      <Text>No trace logs available</Text>
                    )}
                  </>
                ) : (
                  <>
                    <Group position="left" spacing="xl">
                      <ToggleOption active={!showProfileEvents}>Query Stats</ToggleOption>
                      <Switch
                        className={classes.viewToggle}
                        checked={showProfileEvents}
                        onChange={(event) => setShowProfileEvents(event.currentTarget.checked)}
                      />
                      <ToggleOption active={showProfileEvents}>ClickHouse Profile Events</ToggleOption>
                    </Group>
                    {showProfileEvents ? (
                      profileEvents ? (
                        <ProfileEventsTable profileEvents={profileEvents} classes={classes} />
                      ) : (
                        <Text>Either retrieving or no profile events available</Text>
                      )
                    ) : (
                      <MetadataTable queryInfo={queryInfo} classes={classes} />
                    )}
                  </>
                )}
              </div>
            ))
          ) : (
            <Text>No query info available</Text>
          )}
        </Accordion.Panel>
      </Accordion.Item>
    </Accordion>
    <Space h="md" />
    <h4>Response Data:</h4>
    <div className={classes.scrollableDataContainer}>
      <Code block>
        <pre>
          {JSON.stringify(
            (({ meta, ...rest }) => rest)(response),
            null,
            2
          )}
        </pre>
      </Code>
    </div>
  </>
);
