import React, { useEffect, useState } from "react";
import { Badge, Button, Code, Group, Loader, Space, Text } from "@mantine/core";

import Client from "SnubaAdmin/api_client";
import { Table } from "SnubaAdmin/table";
import { Collapse } from "SnubaAdmin/collapse";
import { ClusterData } from "SnubaAdmin/clusters/types";

function versionCell(cluster: ClusterData) {
  if (cluster.version) {
    return <Code>{cluster.version}</Code>;
  }
  return (
    <Text color="red" size="sm">
      {cluster.error || "unknown"}
    </Text>
  );
}

function tablesCell(cluster: ClusterData) {
  if (cluster.tables.length === 0) {
    return <Text color="dimmed">—</Text>;
  }
  return (
    <Collapse
      text={`${cluster.tables.length} table${
        cluster.tables.length === 1 ? "" : "s"
      }`}
    >
      <div style={tableListStyle}>
        {cluster.tables.map((table) => (
          <div key={table}>{table}</div>
        ))}
      </div>
    </Collapse>
  );
}

const tableListStyle = {
  maxHeight: 300,
  overflowY: "auto" as const,
  fontSize: 14,
};

function VersionSummary(props: { clusters: ClusterData[] }) {
  const counts: { [version: string]: number } = {};
  props.clusters.forEach((cluster) => {
    const version = cluster.version || "unknown";
    counts[version] = (counts[version] || 0) + 1;
  });

  return (
    <Group spacing="xs">
      <Text size="sm">ClickHouse versions in use:</Text>
      {Object.keys(counts)
        .sort()
        .map((version) => (
          <Badge
            key={version}
            color={version === "unknown" ? "red" : "blue"}
            variant="light"
          >
            {version} ({counts[version]})
          </Badge>
        ))}
    </Group>
  );
}

function Clusters(props: { api: Client }) {
  const [clusters, setClusters] = useState<ClusterData[] | null>(null);
  const [fetchError, setFetchError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  function fetchClusters() {
    setIsLoading(true);
    setFetchError(null);
    props.api
      .getClickhouseClusters()
      .then((res) => setClusters(res))
      .catch((err) => setFetchError(err.message || "Could not load clusters"))
      .finally(() => setIsLoading(false));
  }

  useEffect(() => {
    fetchClusters();
  }, []);

  if (fetchError !== null) {
    return <Text color="red">{fetchError}</Text>;
  }

  if (clusters === null) {
    return <Loader />;
  }

  const rowData = clusters.map((cluster) => [
    <Code>
      {cluster.host}:{cluster.port}
    </Code>,
    versionCell(cluster),
    cluster.single_node ? (
      <Text color="dimmed">single node</Text>
    ) : (
      cluster.cluster_name || <Text color="dimmed">not set</Text>
    ),
    cluster.distributed_cluster_name || <Text color="dimmed">—</Text>,
    cluster.database,
    <Text size="sm">{cluster.storage_sets.join(", ")}</Text>,
    tablesCell(cluster),
  ]);

  return (
    <div>
      <Group>
        <Button onClick={fetchClusters} loading={isLoading}>
          Refresh
        </Button>
        <VersionSummary clusters={clusters} />
      </Group>
      <Space h="md" />
      <Table
        headerData={[
          "Query Node",
          "ClickHouse Version",
          "Cluster Name",
          "Distributed Cluster Name",
          "Database",
          "Storage Sets",
          "Tables in default",
        ]}
        columnWidths={[3, 3, 3, 3, 2, 5, 3]}
        rowData={rowData}
      />
      <Text size="sm" color="dimmed">
        Every cluster this Snuba deployment is configured with. The version and
        the tables are read from each cluster's query node, and only tables in
        the <Code>default</Code> database are listed.
      </Text>
    </div>
  );
}

export default Clusters;
