import React, { useState } from "react";
import {
  Accordion,
  Badge,
  Checkbox,
  Code,
  Group,
  NumberInput,
  SimpleGrid,
  Space,
  Stack,
  Table,
  Text,
  TextInput,
  Title,
  Paper,
  Progress,
  Tooltip,
} from "@mantine/core";
import Client from "SnubaAdmin/api_client";
import ExecuteButton from "SnubaAdmin/utils/execute_button";
import {
  DimensionRow,
  EapStatsRequest,
  EapStatsResult,
  ProfileCoverage,
  QueryTypeBucket,
} from "SnubaAdmin/eap_stats/types";

function formatBytes(n: number): string {
  if (!n || n <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  let v = n;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i += 1;
  }
  return `${v.toFixed(v >= 10 || i === 0 ? 0 : 1)} ${units[i]}`;
}

function formatDurationMs(ms: number): string {
  if (!ms || ms <= 0) return "0 ms";
  if (ms < 1000) return `${Math.round(ms)} ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)} s`;
  return `${(ms / 60_000).toFixed(1)} min`;
}

function formatCpuUs(us: number): string {
  if (!us || us <= 0) return "0";
  if (us < 1000) return `${Math.round(us)} µs`;
  if (us < 1_000_000) return `${(us / 1000).toFixed(1)} ms`;
  return `${(us / 1_000_000).toFixed(2)} s`;
}

function formatCount(n: number): string {
  if (n == null) return "0";
  return n.toLocaleString();
}

function formatPct(n: number): string {
  if (!n) return "0%";
  if (n < 0.1) return "<0.1%";
  return `${n.toFixed(1)}%`;
}

function ResourceCard({
  label,
  value,
  sub,
}: {
  label: string;
  value: string;
  sub?: string;
}) {
  return (
    <Paper withBorder p="md" radius="md">
      <Text size="xs" c="dimmed" tt="uppercase" fw={600}>
        {label}
      </Text>
      <Text size="xl" fw={700}>
        {value}
      </Text>
      {sub ? (
        <Text size="xs" c="dimmed">
          {sub}
        </Text>
      ) : null}
    </Paper>
  );
}

function coverageColor(pct: number): string {
  if (pct >= 80) return "teal";
  if (pct >= 50) return "yellow";
  if (pct > 0) return "orange";
  return "red";
}

function ProfileCoveragePanel({ coverage }: { coverage: ProfileCoverage }) {
  if (!coverage?.enabled) {
    return (
      <Paper withBorder p="md" radius="md">
        <Title order={4}>Profile coverage</Title>
        <Text c="dimmed" size="sm">
          ProfileEvents enrichment is off. CPU / memory / IO / network totals are unavailable.
        </Text>
      </Paper>
    );
  }

  return (
    <Paper withBorder p="md" radius="md">
      <Group position="apart" mb="sm">
        <div>
          <Title order={4}>Profile coverage</Title>
          <Text size="sm" c="dimmed">
            Share of this sample backed by ClickHouse ProfileEvents. Prefer
            bytes-weighted coverage when judging whether CPU/mem/IO/network
            aggregates are representative.
          </Text>
        </div>
        {coverage.query_ids_capped ? (
          <Badge color="orange">query_id lookup capped</Badge>
        ) : null}
      </Group>
      <SimpleGrid
        cols={3}
        breakpoints={[
          { maxWidth: "md", cols: 2 },
          { maxWidth: "sm", cols: 1 },
        ]}
      >
        <ResourceCard
          label="% queries profiled"
          value={formatPct(coverage.pct_queries_profiled)}
          sub={`${formatCount(coverage.queries_profiled)} / ${formatCount(
            coverage.queries_total
          )} requests`}
        />
        <ResourceCard
          label="% bytes profiled"
          value={formatPct(coverage.pct_bytes_profiled)}
          sub={`${formatBytes(coverage.bytes_profiled)} / ${formatBytes(
            coverage.bytes_total
          )}`}
        />
        <ResourceCard
          label="% duration profiled"
          value={formatPct(coverage.pct_duration_profiled)}
          sub={`${formatDurationMs(coverage.duration_ms_profiled)} / ${formatDurationMs(
            coverage.duration_ms_total
          )}`}
        />
        <ResourceCard
          label="% query_ids matched"
          value={formatPct(coverage.pct_query_ids_matched)}
          sub={`${formatCount(coverage.query_ids_matched)} / ${formatCount(
            coverage.query_ids_looked_up
          )} looked up`}
        />
        <ResourceCard
          label="Requests with query_id"
          value={formatCount(coverage.queries_with_query_id)}
          sub={`${formatPct(coverage.pct_queries_with_query_id_profiled)} of those profiled`}
        />
        <ResourceCard
          label="Unique query_ids sampled"
          value={formatCount(coverage.query_ids_sampled)}
          sub={
            coverage.query_ids_capped
              ? `lookup capped at ${formatCount(coverage.query_ids_looked_up)}`
              : "all unique ids looked up"
          }
        />
      </SimpleGrid>
      <Space h="sm" />
      <Text size="xs" c="dimmed" mb={4}>
        Bytes-weighted coverage
      </Text>
      <Progress
        value={Math.min(100, coverage.pct_bytes_profiled || 0)}
        size="lg"
        color={coverageColor(coverage.pct_bytes_profiled || 0)}
      />
    </Paper>
  );
}

function TotalsPanel({
  result,
}: {
  result: EapStatsResult;
}) {
  const r = result.total_resources;
  // Prefer virtual CPU time when present; it already covers threaded work.
  const cpu = r.cpu_virtual_us || r.cpu_user_us + r.cpu_system_us;
  const io = r.io_selected_bytes + r.io_read_compressed_bytes;
  const net = r.network_receive_bytes + r.network_send_bytes;
  const coverage = result.profile_coverage;

  return (
    <Stack spacing="sm">
      <Group spacing="xs">
        <Title order={3}>Sample summary</Title>
        <Badge color="blue">{formatCount(result.rows_scanned)} rows</Badge>
        <Badge color="green">{formatCount(result.rows_categorized)} categorized</Badge>
        {result.rows_failed > 0 ? (
          <Badge color="yellow">{formatCount(result.rows_failed)} uncategorized</Badge>
        ) : null}
        {coverage?.enabled ? (
          <Badge color={coverageColor(coverage.pct_bytes_profiled || 0)}>
            {formatPct(coverage.pct_bytes_profiled)} bytes profiled
          </Badge>
        ) : (
          <Badge color="gray">ProfileEvents off</Badge>
        )}
      </Group>
      <ProfileCoveragePanel coverage={coverage} />
      <SimpleGrid
        cols={4}
        breakpoints={[
          { maxWidth: "md", cols: 2 },
          { maxWidth: "sm", cols: 1 },
        ]}
      >
        <ResourceCard
          label="Bytes scanned"
          value={formatBytes(r.bytes_scanned)}
          sub={`avg ${formatBytes(
            result.rows_scanned ? r.bytes_scanned / result.rows_scanned : 0
          )} / query`}
        />
        <ResourceCard
          label="Duration"
          value={formatDurationMs(r.duration_ms)}
          sub={`avg ${formatDurationMs(
            result.rows_scanned ? r.duration_ms / result.rows_scanned : 0
          )} / query`}
        />
        <ResourceCard
          label="CPU (ProfileEvents)"
          value={formatCpuUs(cpu)}
          sub={`user ${formatCpuUs(r.cpu_user_us)} · sys ${formatCpuUs(
            r.cpu_system_us
          )}`}
        />
        <ResourceCard
          label="Memory peak"
          value={formatBytes(r.memory_peak_bytes)}
          sub={`usage sum ${formatBytes(r.memory_usage_bytes)}`}
        />
        <ResourceCard
          label="IO"
          value={formatBytes(io)}
          sub={`selected ${formatBytes(r.io_selected_bytes)} · compressed ${formatBytes(
            r.io_read_compressed_bytes
          )}`}
        />
        <ResourceCard
          label="Network"
          value={formatBytes(net)}
          sub={`rx ${formatBytes(r.network_receive_bytes)} · tx ${formatBytes(
            r.network_send_bytes
          )}`}
        />
        <ResourceCard
          label="Rows selected"
          value={formatCount(r.io_selected_rows)}
        />
        <ResourceCard
          label="Realtime (CH)"
          value={formatCpuUs(r.realtime_us)}
        />
      </SimpleGrid>
    </Stack>
  );
}

function PctBar({ value, color }: { value: number; color?: string }) {
  return (
    <Tooltip label={formatPct(value)}>
      <Progress value={Math.min(100, value)} size="sm" color={color || "blue"} />
    </Tooltip>
  );
}

function BreakdownMap({ data }: { data: Record<string, number> }) {
  const entries = Object.entries(data || {});
  if (!entries.length) return <Text c="dimmed">—</Text>;
  return (
    <Text size="sm">
      {entries
        .map(([k, v]) => `${k}: ${v.toLocaleString()}`)
        .join(" · ")}
    </Text>
  );
}

function QueryTypeTable({ buckets }: { buckets: QueryTypeBucket[] }) {
  if (!buckets.length) {
    return <Text c="dimmed">No categorized rows in this sample.</Text>;
  }

  return (
    <Accordion variant="separated" chevronPosition="left">
      {buckets.map((b) => {
        const r = b.resources;
        const cpu = r.cpu_user_us + r.cpu_system_us + r.cpu_virtual_us;
        const io = r.io_selected_bytes + r.io_read_compressed_bytes;
        const net = r.network_receive_bytes + r.network_send_bytes;
        return (
          <Accordion.Item value={b.query_type} key={b.query_type}>
            <Accordion.Control>
              <Group position="apart" noWrap style={{ width: "100%", paddingRight: 12 }}>
                <div style={{ minWidth: 180 }}>
                  <Text fw={700}>{b.query_type}</Text>
                  <Text size="xs" c="dimmed">
                    {formatCount(b.query_count)} queries · {formatPct(b.pct_of_queries)} of sample
                  </Text>
                </div>
                <div style={{ flex: 1, maxWidth: 220 }}>
                  <Text size="xs" c="dimmed">
                    Bytes {formatPct(b.pct_of_bytes)}
                  </Text>
                  <PctBar value={b.pct_of_bytes} color="blue" />
                </div>
                <div style={{ flex: 1, maxWidth: 220 }}>
                  <Text size="xs" c="dimmed">
                    CPU {formatPct(b.pct_of_cpu)}
                  </Text>
                  <PctBar value={b.pct_of_cpu} color="violet" />
                </div>
                <div style={{ minWidth: 180, textAlign: "right" }}>
                  <Text size="sm" fw={600}>
                    {formatBytes(r.bytes_scanned)}
                  </Text>
                  <Text size="xs" c="dimmed">
                    {formatCpuUs(cpu)} CPU · {formatBytes(r.memory_peak_bytes)} peak mem
                  </Text>
                  <Text size="xs" c="dimmed">
                    profiled {formatPct(b.pct_bytes_profiled)} bytes · {formatPct(b.pct_queries_profiled)} queries
                  </Text>
                </div>
              </Group>
            </Accordion.Control>
            <Accordion.Panel>
              <SimpleGrid cols={4} breakpoints={[{ maxWidth: "md", cols: 2 }]}>
                <ResourceCard label="Bytes scanned" value={formatBytes(r.bytes_scanned)} sub={`avg ${formatBytes(b.avg_bytes_scanned)}`} />
                <ResourceCard label="Duration" value={formatDurationMs(r.duration_ms)} sub={`avg ${formatDurationMs(b.avg_duration_ms)}`} />
                <ResourceCard label="CPU" value={formatCpuUs(cpu)} sub={`${formatPct(b.pct_of_cpu)} of total`} />
                <ResourceCard label="Memory peak" value={formatBytes(r.memory_peak_bytes)} sub={`${formatPct(b.pct_of_memory_peak)} of total`} />
                <ResourceCard label="IO" value={formatBytes(io)} sub={`${formatPct(b.pct_of_io)} of total`} />
                <ResourceCard label="Network" value={formatBytes(net)} sub={`${formatPct(b.pct_of_network)} of total`} />
                <ResourceCard label="Selected rows" value={formatCount(r.io_selected_rows)} />
                <ResourceCard label="ProfileEvents matched" value={formatCount(r.profile_events_matched)} />
              </SimpleGrid>
              <Space h="md" />
              <Title order={5}>Breakdowns</Title>
              <Table striped fontSize="sm" withBorder>
                <tbody>
                  <tr>
                    <td style={{ width: 160 }}>
                      <Text fw={600}>Trace item type</Text>
                    </td>
                    <td>
                      <BreakdownMap data={b.by_trace_item_type} />
                    </td>
                  </tr>
                  <tr>
                    <td>
                      <Text fw={600}>Filter profile</Text>
                    </td>
                    <td>
                      <BreakdownMap data={b.by_filter_profile} />
                    </td>
                  </tr>
                  <tr>
                    <td>
                      <Text fw={600}>Top referrers</Text>
                    </td>
                    <td>
                      <BreakdownMap data={b.by_referrer} />
                    </td>
                  </tr>
                </tbody>
              </Table>
              {b.examples?.length ? (
                <>
                  <Space h="md" />
                  <Title order={5}>Example queries</Title>
                  <Table striped fontSize="xs" withBorder>
                    <thead>
                      <tr>
                        <th>Time</th>
                        <th>Referrer</th>
                        <th>Org</th>
                        <th>Bytes</th>
                        <th>Duration</th>
                        <th>CPU</th>
                        <th>Mem peak</th>
                        <th>Filter</th>
                        <th>Request ID</th>
                      </tr>
                    </thead>
                    <tbody>
                      {b.examples.map((ex, idx) => (
                        <tr key={idx}>
                          <td>{ex.timestamp}</td>
                          <td>
                            <Code>{ex.referrer || "—"}</Code>
                          </td>
                          <td>{ex.organization ?? "—"}</td>
                          <td>{formatBytes(ex.bytes_scanned || 0)}</td>
                          <td>{formatDurationMs(ex.duration_ms || 0)}</td>
                          <td>{formatCpuUs(ex.cpu_us || 0)}</td>
                          <td>{formatBytes(ex.memory_peak_bytes || 0)}</td>
                          <td>
                            <Code>{ex.query_info?.filter_profile || "—"}</Code>
                          </td>
                          <td>
                            <Code>{ex.request_id}</Code>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </Table>
                </>
              ) : null}
            </Accordion.Panel>
          </Accordion.Item>
        );
      })}
    </Accordion>
  );
}

function DimensionTable({
  title,
  rows,
}: {
  title: string;
  rows: DimensionRow[];
}) {
  return (
    <Stack spacing="xs">
      <Title order={4}>{title}</Title>
      {!rows.length ? (
        <Text c="dimmed">No data</Text>
      ) : (
        <Table striped withBorder fontSize="sm">
          <thead>
            <tr>
              <th>{title}</th>
              <th>Queries</th>
              <th>Bytes scanned</th>
              <th>% bytes</th>
              <th>CPU</th>
              <th>% CPU</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.key}>
                <td>
                  <Code>{row.key}</Code>
                </td>
                <td>{formatCount(row.query_count)}</td>
                <td>{formatBytes(row.total_bytes_scanned)}</td>
                <td style={{ minWidth: 120 }}>
                  <Text size="xs">{formatPct(row.pct_of_bytes)}</Text>
                  <PctBar value={row.pct_of_bytes} />
                </td>
                <td>{formatCpuUs(row.total_cpu_us)}</td>
                <td style={{ minWidth: 120 }}>
                  <Text size="xs">{formatPct(row.pct_of_cpu)}</Text>
                  <PctBar value={row.pct_of_cpu} color="violet" />
                </td>
              </tr>
            ))}
          </tbody>
        </Table>
      )}
    </Stack>
  );
}

function EapStats(props: { api: Client }) {
  const [hours, setHours] = useState<number | "">(6);
  const [maxRows, setMaxRows] = useState<number | "">(1000);
  const [referrer, setReferrer] = useState("");
  const [referrerContains, setReferrerContains] = useState("");
  const [organizationId, setOrganizationId] = useState<number | "">("");
  const [includeProfileEvents, setIncludeProfileEvents] = useState(true);
  const [result, setResult] = useState<EapStatsResult | null>(null);
  const [error, setError] = useState<string | null>(null);

  function execute() {
    setError(null);
    const body: EapStatsRequest = {
      hours: typeof hours === "number" ? hours : 6,
      max_rows: typeof maxRows === "number" ? maxRows : 1000,
      include_profile_events: includeProfileEvents,
    };
    if (referrer.trim()) body.referrer = referrer.trim();
    if (referrerContains.trim()) body.referrer_contains = referrerContains.trim();
    if (organizationId !== "" && organizationId != null) {
      body.organization_id = organizationId;
    }
    return props.api
      .runEapStats(body)
      .then((res) => {
        setResult(res);
      })
      .catch((err) => {
        const message =
          err?.error?.message || err?.message || JSON.stringify(err) || "Request failed";
        setError(String(message));
        setResult(null);
      });
  }

  return (
    <div style={{ padding: 8, maxWidth: 1400 }}>
      <Title order={2}>EAP Stats</Title>
      <Text c="dimmed" maw={900}>
        Sample recent EAP rows from the Snuba querylog, categorize each request by
        query shape, and aggregate resource cost (bytes scanned, duration, and
        ClickHouse ProfileEvents for CPU / memory / IO / network). Queries run with
        max_threads=0 so whole-dataset scans can use all ClickHouse cores. Check
        profile coverage % before trusting CPU/mem/IO/network totals.
      </Text>
      <Space h="md" />

      <Paper withBorder p="md" radius="md">
        <SimpleGrid
          cols={3}
          breakpoints={[
            { maxWidth: "md", cols: 2 },
            { maxWidth: "sm", cols: 1 },
          ]}
        >
          <NumberInput
            label="Lookback hours"
            description="Max 168 (7 days)"
            min={1}
            max={168}
            value={hours}
            onChange={setHours}
          />
          <NumberInput
            label="Max rows"
            description="Sample size (max 500000). Uses max_threads=0."
            min={1}
            max={500000}
            step={1000}
            value={maxRows}
            onChange={setMaxRows}
          />
          <NumberInput
            label="Organization ID"
            description="Optional filter"
            value={organizationId}
            onChange={setOrganizationId}
            min={1}
          />
          <TextInput
            label="Referrer equals"
            description="Exact match"
            value={referrer}
            onChange={(e) => setReferrer(e.currentTarget.value)}
            placeholder="api.organization_events"
          />
          <TextInput
            label="Referrer contains"
            description="Substring match"
            value={referrerContains}
            onChange={(e) => setReferrerContains(e.currentTarget.value)}
            placeholder="eap"
          />
          <div style={{ display: "flex", alignItems: "end", paddingBottom: 4 }}>
            <Checkbox
              label="Enrich with ClickHouse ProfileEvents (CPU / mem / IO / network)"
              checked={includeProfileEvents}
              onChange={(e) => setIncludeProfileEvents(e.currentTarget.checked)}
            />
          </div>
        </SimpleGrid>
        <Space h="md" />
        <Group>
          <ExecuteButton onClick={execute} disabled={false} />
        </Group>
      </Paper>

      {error ? (
        <>
          <Space h="md" />
          <Paper withBorder p="md" style={{ borderColor: "#fa5252" }}>
            <Text c="red" fw={600}>
              Error
            </Text>
            <Code block>{error}</Code>
          </Paper>
        </>
      ) : null}

      {result ? (
        <>
          <Space h="lg" />
          <TotalsPanel result={result} />
          <Space h="lg" />
          <Title order={3}>By query type</Title>
          <Text size="sm" c="dimmed" mb="sm">
            Expand a row for full resource breakdown, filter/item-type mix, and
            example request ids.
          </Text>
          <QueryTypeTable buckets={result.by_query_type} />
          <Space h="lg" />
          <SimpleGrid cols={1}>
            <DimensionTable title="Filter profile" rows={result.by_filter_profile} />
            <DimensionTable title="Trace item type" rows={result.by_trace_item_type} />
            <DimensionTable title="Referrer" rows={result.by_referrer} />
          </SimpleGrid>
        </>
      ) : null}
    </div>
  );
}

export default EapStats;
