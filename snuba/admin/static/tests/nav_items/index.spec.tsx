import Nav from "../../nav";
import Client from "../../api_client";
import React from "react";
import { it, expect, jest, afterEach } from "@jest/globals";
import { render, act, waitFor } from "@testing-library/react";
import { AllowedTools } from "../../types";

it("should only display allowed tools", async () => {
  let data = {
    tools: ["snql-to-sql"],
  };
  let mockClient = {
    ...Client(),
    getAllowedTools: jest
      .fn<() => Promise<AllowedTools>>()
      .mockResolvedValueOnce(data),
  };

  function navigate(nextTab: string) {
    return;
  }

  let { getByText, queryByText } = render(
    <Nav active={null} navigate={navigate} api={mockClient} />
  );

  await waitFor(() => expect(mockClient.getAllowedTools).toBeCalledTimes(1));
  expect(getByText("SnQL to SQL", { exact: false })).toBeTruthy();
  expect(queryByText("Runtime Config", { exact: false })).toBeNull();
});

it("should only display all tools", async () => {
  let data = {
    tools: ["snql-to-sql", "all"],
  };
  let mockClient = {
    ...Client(),
    getAllowedTools: jest
      .fn<() => Promise<AllowedTools>>()
      .mockResolvedValueOnce(data),
  };

  function navigate(nextTab: string) {
    return;
  }

  let { getByText } = render(
    <Nav active={null} navigate={navigate} api={mockClient} />
  );

  await waitFor(() => expect(mockClient.getAllowedTools).toBeCalledTimes(1));
  expect(getByText("SnQL to SQL", { exact: false })).toBeTruthy();
  expect(getByText("Runtime Config", { exact: false })).toBeTruthy();
  expect(getByText("Capacity Management", { exact: false })).toBeTruthy();
  expect(getByText("System Queries", { exact: false })).toBeTruthy();
  expect(getByText("ClickHouse Migrations", { exact: false })).toBeTruthy();
  expect(getByText("ClickHouse Tracing", { exact: false })).toBeTruthy();
  expect(getByText("ClickHouse Querylog", { exact: false })).toBeTruthy();
  expect(getByText("Audit Log", { exact: false })).toBeTruthy();
  expect(getByText("Kafka", { exact: false })).toBeTruthy();
});
