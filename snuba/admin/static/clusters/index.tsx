import React, { useEffect, useState } from "react";
import { Code, Loader, Space, Text } from "@mantine/core";

import Client from "SnubaAdmin/api_client";
import { Table } from "SnubaAdmin/table";
import { Collapse } from "SnubaAdmin/collapse";
import { ClusterData } from "SnubaAdmin/clusters/types";

function versionsCell(cluster: ClusterData) {
  if (cluster.versions.length === 0 && !cluster.versions_error) {
    return <Text color="dimmed">not reported</Text>;
  }

  return (
    <div>
      {cluster.versions.map((version) => (
        <div key={version}>
          <Code>{version}</Code>
        </div>
      ))}
      {cluster.versions_error && (
        <Text color="red" size="sm">
          {cluster.versions_error}
        </Text>
      )}
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
  if (cluster.tables.length === 0 && !cluster.tables_error) {
    return <Text color="dimmed">—</Text>;
  }
  return (
    <div>
      {cluster.tables.length > 0 && (
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
      )}
      {cluster.tables_error && <Text color="red">{cluster.tables_error}</Text>}
    </div>
  );
}

const tableListStyle = {
  maxHeight: 300,
  overflowY: "auto" as const,
  fontSize: 14,
};

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
    cluster.cluster_name,
    versionsCell(cluster),
    listCell(cluster.storage_sets),
    tablesCell(cluster),
  ]);

  return (
    <div>
      {fetchError !== null && <Text color="red">{fetchError}</Text>}
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
