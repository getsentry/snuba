import Clusters from "SnubaAdmin/clusters";
import { ClusterData } from "SnubaAdmin/clusters/types";
import Client from "SnubaAdmin/api_client";
import React from "react";
import { it, expect, jest } from "@jest/globals";
import { render, waitFor, fireEvent } from "@testing-library/react";

function cluster(overrides: Partial<ClusterData> = {}): ClusterData {
  return {
    host: "localhost",
    port: 8123,
    database: "default",
    secure: false,
    single_node: true,
    cluster_name: null,
    distributed_cluster_name: null,
    storage_sets: ["events"],
    query_node_versions: [
      {
        host: "query-localhost",
        port: 9000,
        version: "24.8.14.10459",
        error: null,
      },
    ],
    query_node_error: null,
    storage_node_versions: [
      {
        host: "storage-localhost",
        port: 9001,
        version: "24.8.14.10459",
        error: null,
      },
    ],
    storage_node_error: null,
    tables: ["errors_local", "sentry_local"],
    error: null,
    ...overrides,
  };
}

it("lists every cluster with its ClickHouse version", async () => {
  let data = [
    cluster(),
    cluster({
      host: "clickhouse-query",
      port: 9001,
      single_node: false,
      cluster_name: "cluster_one_sh",
      distributed_cluster_name: "cluster_one_sh_dist",
      storage_sets: ["metrics", "transactions"],
      query_node_versions: [
        { host: "query", port: 9000, version: "25.3.1.100", error: null },
      ],
      storage_node_versions: [
        { host: "storage", port: 9001, version: "24.8.14.10459", error: null },
      ],
      tables: ["metrics_local"],
    }),
  ];
  let mockClient = {
    ...Client(),
    getClickhouseClusters: jest
      .fn<() => Promise<ClusterData[]>>()
      .mockResolvedValueOnce(data),
  };

  let { getByText, getAllByText } = render(<Clusters api={mockClient} />);

  await waitFor(() =>
    expect(mockClient.getClickhouseClusters).toBeCalledTimes(1)
  );

  expect(getByText("query-localhost:9000", { exact: false })).toBeTruthy();
  expect(getByText("storage-localhost:9001", { exact: false })).toBeTruthy();
  expect(getByText("query:9000", { exact: false })).toBeTruthy();
  expect(getByText("storage:9001", { exact: false })).toBeTruthy();
  expect(getAllByText("24.8.14.10459", { exact: false })).toHaveLength(4);
  expect(getAllByText("25.3.1.100", { exact: false })).toHaveLength(2);
  expect(getByText("cluster_one_sh_dist", { exact: false })).toBeTruthy();
  expect(getByText("metrics, transactions", { exact: false })).toBeTruthy();
  expect(getByText("2 tables", { exact: false })).toBeTruthy();
  expect(getByText("1 table", { exact: true })).toBeTruthy();
});

it("expands the list of tables on a cluster", async () => {
  let mockClient = {
    ...Client(),
    getClickhouseClusters: jest
      .fn<() => Promise<ClusterData[]>>()
      .mockResolvedValueOnce([cluster()]),
  };

  let { getByText, queryByText, container } = render(
    <Clusters api={mockClient} />
  );

  await waitFor(() =>
    expect(mockClient.getClickhouseClusters).toBeCalledTimes(1)
  );

  expect(queryByText("errors_local")).toBeNull();
  fireEvent.click(container.querySelectorAll("a")[0]);
  expect(getByText("errors_local")).toBeTruthy();
  expect(getByText("sentry_local")).toBeTruthy();
});

it("shows the reason a cluster's version could not be fetched", async () => {
  let mockClient = {
    ...Client(),
    getClickhouseClusters: jest
      .fn<() => Promise<ClusterData[]>>()
      .mockResolvedValueOnce([
        cluster({
          query_node_versions: [
            {
              host: "query",
              port: 9000,
              version: null,
              error: "Connection refused",
            },
          ],
        }),
      ]),
  };

  let { getByText } = render(<Clusters api={mockClient} />);

  await waitFor(() =>
    expect(mockClient.getClickhouseClusters).toBeCalledTimes(1)
  );

  expect(getByText("Connection refused", { exact: false })).toBeTruthy();
});

it("shows topology errors alongside fallback node versions", async () => {
  let mockClient = {
    ...Client(),
    getClickhouseClusters: jest
      .fn<() => Promise<ClusterData[]>>()
      .mockResolvedValueOnce([
        cluster({ query_node_error: "Query topology unavailable" }),
      ]),
  };

  let { getByText } = render(<Clusters api={mockClient} />);

  await waitFor(() =>
    expect(mockClient.getClickhouseClusters).toBeCalledTimes(1)
  );

  expect(getByText("query-localhost:9000", { exact: false })).toBeTruthy();
  expect(getByText("Query topology unavailable")).toBeTruthy();
});

it("does not crash when refresh hits an older backend during a rolling deploy", async () => {
  let legacyCluster = cluster({
    host: "legacy-query",
    port: 8123,
    query_node_versions: undefined,
    query_node_error: undefined,
    storage_node_versions: undefined,
    storage_node_error: undefined,
    version: "24.8.14.10459",
  });
  let mockClient = {
    ...Client(),
    getClickhouseClusters: jest
      .fn<() => Promise<ClusterData[]>>()
      .mockResolvedValueOnce([cluster()])
      .mockResolvedValueOnce([legacyCluster]),
  };

  let { getByRole, getByText } = render(<Clusters api={mockClient} />);

  await waitFor(() =>
    expect(mockClient.getClickhouseClusters).toBeCalledTimes(1)
  );
  fireEvent.click(getByRole("button", { name: "Refresh" }));

  await waitFor(() =>
    expect(getByText("legacy-query:8123", { exact: false })).toBeTruthy()
  );
  expect(getByText("not reported")).toBeTruthy();
});

it("keeps refresh reachable when the clusters could not be loaded", async () => {
  let mockClient = {
    ...Client(),
    getClickhouseClusters: jest
      .fn<() => Promise<ClusterData[]>>()
      .mockRejectedValueOnce(new Error("No permissions on clusters"))
      .mockResolvedValueOnce([cluster()]),
  };

  let { getByText, getByRole } = render(<Clusters api={mockClient} />);

  await waitFor(() =>
    expect(getByText("No permissions on clusters")).toBeTruthy()
  );

  fireEvent.click(getByRole("button", { name: "Refresh" }));

  await waitFor(() =>
    expect(mockClient.getClickhouseClusters).toBeCalledTimes(2)
  );
  expect(getByText("query-localhost:9000", { exact: false })).toBeTruthy();
});
