import React, { useEffect, useState } from "react";
import { Button, Code, Group, Loader, Space, Text } from "@mantine/core";

import Client from "SnubaAdmin/api_client";
import { Table } from "SnubaAdmin/table";
import { Collapse } from "SnubaAdmin/collapse";
import { ClusterData, NodeVersionData } from "SnubaAdmin/clusters/types";

function queryNodeVersions(cluster: ClusterData): NodeVersionData[] {
  if (Array.isArray(cluster.query_node_versions)) {
    return cluster.query_node_versions;
  }

  // A rolling deploy can pair this frontend with the original API response.
  if ("version" in cluster) {
    return [
      {
        host: cluster.host,
        port: cluster.port,
        version: cluster.version || null,
        error: null,
      },
    ];
  }

  return [];
}

function clusterVersions(cluster: ClusterData): string[] {
  if (Array.isArray(cluster.versions)) {
    return cluster.versions;
  }

  return Array.from(
    new Set(
      [
        ...(cluster.query_cluster_versions || []),
        ...(cluster.storage_cluster_versions || []),
        ...queryNodeVersions(cluster).flatMap((node) =>
          node.version ? [node.version] : []
        ),
        ...(cluster.storage_node_versions || []).flatMap((node) =>
          node.version ? [node.version] : []
        ),
      ]
    )
  ).sort();
}

function versionsCell(cluster: ClusterData) {
  // On the cluster-level API, one query fetches both versions and tables, so a
  // failure applies here too. Older APIs use cluster.error for tables only.
  const errors = Array.isArray(cluster.versions)
    ? cluster.error
      ? [cluster.error]
      : []
    : [
        cluster.query_node_error,
        cluster.storage_node_error,
        ...queryNodeVersions(cluster).map((node) => node.error),
        ...(cluster.storage_node_versions || []).map((node) => node.error),
      ].filter((error): error is string => Boolean(error));
  const versions = clusterVersions(cluster);
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
      {Array.from(new Set(errors)).map((error) => (
        <Text key={error} color="red" size="sm">
          {error}
        </Text>
      ))}
    </div>
  );
}

function listCell(values: string[]) {
  if (values.length === 0) {
    return <Text color="dimmed">—</Text>;
  }
  return (
    <div>
      {values.map((value) => (
        <div key={value}>{value}</div>
      ))}
    </div>
  );
}

function tablesCell(cluster: ClusterData) {
  if (cluster.error) {
    return <Text color="red">{cluster.error}</Text>;
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

function displayClusterName(cluster: ClusterData): string {
  if (cluster.cluster_name) {
    return cluster.cluster_name;
  }
  if (cluster.distributed_cluster_name) {
    return cluster.distributed_cluster_name;
  }
  return "single node";
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
    displayClusterName(cluster),
    versionsCell(cluster),
    listCell(cluster.storage_sets),
    tablesCell(cluster),
  ]);

  return (
    <div>
      <Group>
        <Button onClick={fetchClusters} loading={isLoading}>
          Refresh
        </Button>
        {fetchError !== null && <Text color="red">{fetchError}</Text>}
      </Group>
      <Space h="md" />
      {clusters === null ? (
        isLoading && <Loader />
      ) : (
        <Table
          headerData={[
            "Cluster Name",
            "Distinct Versions",
            "Storage Sets",
            "Tables",
          ]}
          columnWidths={[4, 3, 4, 3]}
          rowData={rowData}
        />
      )}
      <Text size="sm" color="dimmed">
        Every ClickHouse cluster this Snuba deployment uses. Versions and tables
        in the <Code>default</Code> database are read across all replicas.
      </Text>
    </div>
  );
}

export default Clusters;
