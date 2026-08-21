import Clusters from "SnubaAdmin/clusters";
import { ClusterData } from "SnubaAdmin/clusters/types";
import Client from "SnubaAdmin/api_client";
import React from "react";
import { it, expect, jest } from "@jest/globals";
import { fireEvent, render, waitFor } from "@testing-library/react";

function cluster(overrides: Partial<ClusterData> = {}): ClusterData {
  return {
    cluster_name: "cluster_one",
    storage_sets: ["events", "transactions"],
    versions: ["24.8.14.10459", "25.3.1.100"],
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

it("labels single-node clusters without showing their hostname", async () => {
  let mockClient = {
    ...Client(),
    getClickhouseClusters: jest
      .fn<() => Promise<ClusterData[]>>()
      .mockResolvedValueOnce([cluster({ cluster_name: "single node" })]),
  };

  let { getByText, queryByText } = render(<Clusters api={mockClient} />);

  await waitFor(() =>
    expect(mockClient.getClickhouseClusters).toBeCalledTimes(1)
  );
  expect(getByText("single node", { exact: true })).toBeTruthy();
  expect(queryByText("clickhouse-a")).toBeNull();
});

it("shows partial cluster data alongside lookup errors", async () => {
  let mockClient = {
    ...Client(),
    getClickhouseClusters: jest
      .fn<() => Promise<ClusterData[]>>()
      .mockResolvedValueOnce([
        cluster({
          cluster_name: "tables_failed",
          versions: ["25.3.1.100"],
          tables: [],
          error: "tables unavailable",
        }),
        cluster({
          cluster_name: "versions_failed",
          versions: [],
          tables: ["errors_local"],
          error: "versions unavailable",
        }),
      ]),
  };

  let { getByText, getAllByText, container } = render(
    <Clusters api={mockClient} />
  );

  await waitFor(() =>
    expect(mockClient.getClickhouseClusters).toBeCalledTimes(1)
  );
  expect(getByText("25.3.1.100", { exact: true })).toBeTruthy();
  expect(getAllByText("tables unavailable")).toHaveLength(2);
  expect(getAllByText("versions unavailable")).toHaveLength(2);
  fireEvent.click(container.querySelectorAll("a")[0]);
  expect(getByText("errors_local", { exact: true })).toBeTruthy();
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

it("shows cluster loading errors", async () => {
  let mockClient = {
    ...Client(),
    getClickhouseClusters: jest
      .fn<() => Promise<ClusterData[]>>()
      .mockRejectedValueOnce(new Error("No permissions on clusters")),
  };

  let { getByText } = render(<Clusters api={mockClient} />);

  await waitFor(() =>
    expect(getByText("No permissions on clusters")).toBeTruthy()
  );
  expect(mockClient.getClickhouseClusters).toBeCalledTimes(1);
});
