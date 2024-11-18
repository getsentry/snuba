import React from 'react';
import { Accordion, Text, Title, Stack, Group } from '@mantine/core';
import { ExecuteSummary, QuerySummary, TracingSummary } from 'SnubaAdmin/rpc_endpoints/types';

function executeSummary(value: ExecuteSummary): JSX.Element {
  return (
    <Text>
      Read {value.rows_read} rows, {value.memory_size} in {value.seconds} sec.,
      {value.rows_per_second} rows/sec., {value.bytes_per_second}/sec.
    </Text>
  );
}

function querySummary(value: QuerySummary): JSX.Element {
  const execute = value.execute_summaries?.[0] || null;
  const dist = value.is_distributed ? " (Distributed)" : "";

  return (
    <Accordion.Item key={value.node_name} value={value.node_name}>
      <Accordion.Control>
        <Title order={6}>
          {value.node_name} {dist}: {execute ? execute.seconds : "N/A"} sec.
        </Title>
      </Accordion.Control>
      <Accordion.Panel>
        <Stack>
          {value.index_summaries && (
            <>
              <Title order={6}>Filtering</Title>
              {value.index_summaries.map((s, idx) => (
                <Group key={idx}>
                  <Text fw={600}>{s.table_name}: </Text>
                  <Text>
                    Index `{s.index_name}` has dropped {s.dropped_granules}/
                    {s.total_granules} granules.
                  </Text>
                </Group>
              ))}
            </>
          )}
          <Title order={6}>Total</Title>
          {value.execute_summaries?.map((e, idx) => (
            <div key={idx}>{executeSummary(e)}</div>
          ))}
        </Stack>
      </Accordion.Panel>
    </Accordion.Item>
  );
}

function SummarizedTraceDisplay({ value, classes }: { value: TracingSummary, classes: Record<string, string> }): JSX.Element {
  let dist_node;
  let nodes = [];

  for (const [_, summary] of Object.entries(value.query_summaries)) {
    if (summary.is_distributed) {
      dist_node = summary;
    } else {
      nodes.push(summary);
    }
  }

  return (
    <Stack spacing="xs">
      <Accordion
        chevronPosition="left"
        variant="contained"
        radius="sm"
        classNames={{ item: classes.traceAccordion }}
      >
        {dist_node && querySummary(dist_node)}
        {nodes
          .filter((q) => !q.is_distributed)
          .map((q) => querySummary(q))}
      </Accordion>
    </Stack>
  );
}

export default SummarizedTraceDisplay;
