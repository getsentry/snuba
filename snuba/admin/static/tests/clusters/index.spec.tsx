import Clusters from "SnubaAdmin/clusters";
import { ClusterData } from "SnubaAdmin/clusters/types";
import Client from "SnubaAdmin/api_client";
import React from "react";
import { it, expect, jest } from "@jest/globals";
import { render, waitFor, fireEvent } from "@testing-library/react";

function cluster(overrides: Partial<ClusterData> = {}): ClusterData {
  return {
    host: "localhost",
    port: 9000,
    http_port: 8123,
    database: "default",
    secure: false,
    single_node: true,
    cluster_name: null,
    distributed_cluster_name: null,
    storage_sets: ["events"],
    version: "24.8.14.10459",
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
      version: "25.3.1.100",
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

  expect(getByText("localhost:9000", { exact: false })).toBeTruthy();
  expect(getByText("clickhouse-query:9001", { exact: false })).toBeTruthy();
  // Once in the table row, once in the summary of versions in use.
  expect(getAllByText("24.8.14.10459", { exact: false })).toHaveLength(2);
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
        cluster({ version: null, error: "Connection refused" }),
      ]),
  };

  let { getByText } = render(<Clusters api={mockClient} />);

  await waitFor(() =>
    expect(mockClient.getClickhouseClusters).toBeCalledTimes(1)
  );

  expect(getByText("Connection refused", { exact: false })).toBeTruthy();
});
