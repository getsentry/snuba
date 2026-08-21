import Clusters from "SnubaAdmin/clusters";
import { ClusterData } from "SnubaAdmin/clusters/types";
import Client from "SnubaAdmin/api_client";
import React from "react";
import { it, expect, jest } from "@jest/globals";
import { fireEvent, render, waitFor } from "@testing-library/react";

function cluster(overrides: Partial<ClusterData> = {}): ClusterData {
  return {
    host: "localhost",
    port: 8123,
    database: "default",
    secure: false,
    single_node: false,
    cluster_name: "cluster_one",
    distributed_cluster_name: "cluster_one",
    storage_sets: ["events", "transactions"],
    versions: ["24.8.14.10459", "25.3.1.100"],
    query_cluster_versions: ["24.8.14.10459", "25.3.1.100"],
    query_node_versions: [],
    query_node_error: null,
    storage_cluster_versions: ["24.8.14.10459", "25.3.1.100"],
    storage_node_versions: [],
    storage_node_error: null,
    tables: ["errors_local", "transactions_local"],
    error: null,
    ...overrides,
  };
}

it("shows only cluster name, distinct versions, storage sets, and tables", async () => {
  let mockClient = {
    ...Client(),
    getClickhouseClusters: jest
      .fn<() => Promise<ClusterData[]>>()
      .mockResolvedValueOnce([cluster()]),
  };

  let { getByText, getByRole, queryByText, container } = render(
    <Clusters api={mockClient} />
  );

  await waitFor(() =>
    expect(mockClient.getClickhouseClusters).toBeCalledTimes(1)
  );

  expect(getByRole("columnheader", { name: "Cluster Name" })).toBeTruthy();
  expect(getByRole("columnheader", { name: "Distinct Versions" })).toBeTruthy();
  expect(getByRole("columnheader", { name: "Storage Sets" })).toBeTruthy();
  expect(getByRole("columnheader", { name: "Tables" })).toBeTruthy();
  expect(queryByText("Database")).toBeNull();
  expect(queryByText("Query Cluster")).toBeNull();
  expect(queryByText("Storage Cluster")).toBeNull();

  expect(getByText("cluster_one", { exact: true })).toBeTruthy();
  expect(getByText("24.8.14.10459", { exact: true })).toBeTruthy();
  expect(getByText("25.3.1.100", { exact: true })).toBeTruthy();
  expect(getByText("events", { exact: true })).toBeTruthy();
  expect(getByText("transactions", { exact: true })).toBeTruthy();

  expect(queryByText("errors_local")).toBeNull();
  fireEvent.click(container.querySelectorAll("a")[0]);
  expect(getByText("errors_local")).toBeTruthy();
  expect(getByText("transactions_local")).toBeTruthy();
});

it("shows cluster lookup errors", async () => {
  let mockClient = {
    ...Client(),
    getClickhouseClusters: jest
      .fn<() => Promise<ClusterData[]>>()
      .mockResolvedValueOnce([
        cluster({ versions: [], tables: [], error: "Connection refused" }),
      ]),
  };

  let { getAllByText } = render(<Clusters api={mockClient} />);

  await waitFor(() =>
    expect(mockClient.getClickhouseClusters).toBeCalledTimes(1)
  );
  expect(getAllByText("Connection refused")).toHaveLength(2);
});

it("does not crash when refresh hits an older backend during a rolling deploy", async () => {
  let legacyCluster = cluster({
    cluster_name: null,
    distributed_cluster_name: null,
    versions: undefined,
    query_cluster_versions: undefined,
    query_node_versions: undefined,
    query_node_error: undefined,
    storage_cluster_versions: undefined,
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
    expect(getByText("single node", { exact: true })).toBeTruthy()
  );
  expect(getByText("24.8.14.10459", { exact: true })).toBeTruthy();
});

it("keeps legacy table errors out of the versions column", async () => {
  let legacyCluster = cluster({
    versions: undefined,
    query_cluster_versions: undefined,
    query_node_versions: undefined,
    query_node_error: undefined,
    storage_cluster_versions: undefined,
    storage_node_versions: undefined,
    storage_node_error: undefined,
    version: "24.8.14.10459",
    error: "tables unavailable",
  });
  let mockClient = {
    ...Client(),
    getClickhouseClusters: jest
      .fn<() => Promise<ClusterData[]>>()
      .mockResolvedValueOnce([legacyCluster]),
  };

  let { getByText, getAllByText } = render(<Clusters api={mockClient} />);

  await waitFor(() =>
    expect(mockClient.getClickhouseClusters).toBeCalledTimes(1)
  );
  expect(getAllByText("tables unavailable")).toHaveLength(1);
  expect(getByText("24.8.14.10459", { exact: true })).toBeTruthy();
});

it("keeps refresh reachable when clusters could not be loaded", async () => {
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
  expect(getByText("cluster_one", { exact: true })).toBeTruthy();
});
