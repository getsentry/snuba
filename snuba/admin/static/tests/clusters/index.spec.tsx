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
    versions_error: null,
    tables_error: null,
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

it("keeps version and table lookup errors in their own columns", async () => {
  let mockClient = {
    ...Client(),
    getClickhouseClusters: jest
      .fn<() => Promise<ClusterData[]>>()
      .mockResolvedValueOnce([
        cluster({
          cluster_name: "tables_failed",
          versions: ["25.3.1.100"],
          tables: [],
          tables_error: "tables unavailable",
        }),
        cluster({
          cluster_name: "versions_failed",
          versions: [],
          tables: ["errors_local"],
          versions_error: "versions unavailable",
        }),
      ]),
  };

  let { getByText, queryByText, container } = render(
    <Clusters api={mockClient} />
  );

  await waitFor(() =>
    expect(mockClient.getClickhouseClusters).toBeCalledTimes(1)
  );
  expect(getByText("25.3.1.100", { exact: true })).toBeTruthy();
  expect(getByText("tables unavailable", { exact: true })).toBeTruthy();
  expect(getByText("versions unavailable", { exact: true })).toBeTruthy();
  // A tables-only failure must not also paint the versions column, and vice versa.
  expect(queryByText("not reported")).toBeNull();
  fireEvent.click(container.querySelectorAll("a")[0]);
  expect(getByText("errors_local", { exact: true })).toBeTruthy();
});

it("shows both lookup errors when both fail", async () => {
  let mockClient = {
    ...Client(),
    getClickhouseClusters: jest
      .fn<() => Promise<ClusterData[]>>()
      .mockResolvedValueOnce([
        cluster({
          versions: [],
          tables: [],
          versions_error: "versions unavailable",
          tables_error: "tables unavailable",
        }),
      ]),
  };

  let { getByText } = render(<Clusters api={mockClient} />);

  await waitFor(() =>
    expect(mockClient.getClickhouseClusters).toBeCalledTimes(1)
  );
  expect(getByText("versions unavailable", { exact: true })).toBeTruthy();
  expect(getByText("tables unavailable", { exact: true })).toBeTruthy();
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
