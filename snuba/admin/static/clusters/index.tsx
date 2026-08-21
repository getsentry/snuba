import React, { useEffect, useState } from "react";
import { Badge, Button, Code, Group, Loader, Space, Text } from "@mantine/core";

import Client from "SnubaAdmin/api_client";
import { Table } from "SnubaAdmin/table";
import { Collapse } from "SnubaAdmin/collapse";
import { ClusterData, NodeVersionData } from "SnubaAdmin/clusters/types";

function queryNodeVersions(cluster: ClusterData): NodeVersionData[] {
  if (Array.isArray(cluster.query_node_versions)) {
    return cluster.query_node_versions;
  }

  // A rolling deploy can pair this frontend with the previous API response.
  // Preserve that response instead of crashing when the page is refreshed.
  if ("version" in cluster) {
    return [
      {
        host: cluster.host,
        port: cluster.port,
        version: cluster.version || null,
        error: cluster.error,
      },
    ];
  }

  return [];
}

function storageNodeVersions(cluster: ClusterData): NodeVersionData[] {
  return Array.isArray(cluster.storage_node_versions)
    ? cluster.storage_node_versions
    : [];
}

function clusterNodes(cluster: ClusterData): NodeVersionData[] {
  return [...queryNodeVersions(cluster), ...storageNodeVersions(cluster)];
}

function versionsCell(cluster: ClusterData) {
  const nodes = clusterNodes(cluster);
  const versions = Array.from(
    new Set(nodes.flatMap((node) => (node.version ? [node.version] : [])))
  ).sort();
  const errors = Array.from(
    new Set(
      [
        ...nodes.map((node) => node.error),
        cluster.query_node_error,
        cluster.storage_node_error,
      ].filter((error): error is string => Boolean(error))
    )
  );

  if (versions.length === 0 && errors.length === 0) {
    return <Text color="dimmed">not reported</Text>;
  }

  return (
    <div>
      {versions.map((version) => (
        <div key={version}>
          <Code>{version}</Code>
        </div>
      ))}
      {errors.map((error) => (
        <Text key={error} color="red" size="sm">
          {error}
        </Text>
      ))}
    </div>
  );
}

function nameCell(name: string | null, singleNode: boolean) {
  if (singleNode) {
    return <Text color="dimmed">single node</Text>;
  }
  return name || <Text color="dimmed">not set</Text>;
}

function tablesCell(cluster: ClusterData) {
  if (cluster.error) {
    return (
      <Text color="red" size="sm">
        {cluster.error}
      </Text>
    );
  }
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
  const versions = new Set<string>();
  props.clusters.forEach((cluster) => {
    clusterNodes(cluster).forEach((node) =>
      versions.add(node.version || "unknown")
    );
  });

  return (
    <Group spacing="xs">
      <Text size="sm">ClickHouse versions in use:</Text>
      {Array.from(versions).sort().map((version) => (
        <Badge
          key={version}
          color={version === "unknown" ? "red" : "blue"}
          variant="light"
        >
          {version}
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

  const rowData = (clusters || []).map((cluster) => [
    nameCell(cluster.distributed_cluster_name, cluster.single_node),
    nameCell(cluster.cluster_name, cluster.single_node),
    versionsCell(cluster),
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
        {clusters !== null && <VersionSummary clusters={clusters} />}
        {fetchError !== null && <Text color="red">{fetchError}</Text>}
      </Group>
      <Space h="md" />
      {clusters === null ? (
        isLoading && <Loader />
      ) : (
        <Table
          headerData={[
            "Query Cluster",
            "Storage Cluster",
            "ClickHouse Versions",
            "Database",
            "Storage Sets",
            "Tables in default",
          ]}
          columnWidths={[3, 3, 4, 2, 5, 3]}
          rowData={rowData}
        />
      )}
      <Text size="sm" color="dimmed">
        Every cluster this Snuba deployment is configured with. Each row shows the
        ClickHouse query and storage cluster names plus the distinct versions
        across all nodes. Tables are read from the configured query endpoint, and
        only tables in the <Code>default</Code> database are listed.
      </Text>
    </div>
  );
}

export default Clusters;
