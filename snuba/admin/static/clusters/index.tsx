import React, { useEffect, useState } from "react";
import { Badge, Button, Code, Group, Loader, Space, Text } from "@mantine/core";

import Client from "SnubaAdmin/api_client";
import { Table } from "SnubaAdmin/table";
import { Collapse } from "SnubaAdmin/collapse";
import { ClusterData, NodeVersionData } from "SnubaAdmin/clusters/types";

function versionsCell(nodes: NodeVersionData[], clusterError: string | null) {
  if (nodes.length === 0) {
    return <Text color="red">{clusterError || "unknown"}</Text>;
  }
  return (
    <div>
      {nodes.map((node) => (
        <div key={`${node.host}:${node.port}`}>
          <Code>
            {node.host}:{node.port}
          </Code>{" "}
          {node.version ? (
            <Code>{node.version}</Code>
          ) : (
            <Text color="red" size="sm" component="span">
              {node.error || "unknown"}
            </Text>
          )}
        </div>
      ))}
    </div>
  );
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
    [...cluster.query_node_versions, ...cluster.storage_node_versions].forEach(
      (node) => versions.add(node.version || "unknown")
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
    versionsCell(cluster.query_node_versions, cluster.query_node_error),
    versionsCell(cluster.storage_node_versions, cluster.storage_node_error),
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
        {clusters !== null && <VersionSummary clusters={clusters} />}
        {fetchError !== null && <Text color="red">{fetchError}</Text>}
      </Group>
      <Space h="md" />
      {clusters === null ? (
        isLoading && <Loader />
      ) : (
        <Table
          headerData={[
            "Query Nodes",
            "Storage Nodes",
            "Cluster Name",
            "Distributed Cluster Name",
            "Database",
            "Storage Sets",
            "Tables in default",
          ]}
          columnWidths={[4, 4, 3, 3, 2, 5, 3]}
          rowData={rowData}
        />
      )}
      <Text size="sm" color="dimmed">
        Every cluster this Snuba deployment is configured with. Versions are
        read directly from each query and storage node. Tables are read from the
        configured query endpoint, and only tables in the <Code>default</Code>{" "}
        database are listed.
      </Text>
    </div>
  );
}

export default Clusters;
