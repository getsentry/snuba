import ToolGrid from "SnubaAdmin/tool_grid";
import React from "react";
import { it, expect } from "@jest/globals";
import { render } from "@testing-library/react";

it("should only display allowed tools", () => {
  let { getByText, queryByText } = render(
    <ToolGrid allowedTools={["snql-to-sql"]} />
  );

  expect(getByText("SnQL to SQL")).toBeTruthy();
  expect(queryByText("System Queries")).toBeNull();
});

it("should display all tools", () => {
  let { getByText } = render(
    <ToolGrid allowedTools={["snql-to-sql", "all"]} />
  );

  expect(getByText("SnQL to SQL")).toBeTruthy();
  expect(getByText("System Queries")).toBeTruthy();
  expect(getByText("ClickHouse Migrations")).toBeTruthy();
  expect(getByText("ClickHouse Tracing")).toBeTruthy();
  expect(getByText("ClickHouse Querylog")).toBeTruthy();
  expect(getByText("EAP Stats")).toBeTruthy();
});

it("should show shell pages when their parent page is allowed", () => {
  let { getByText, queryByText } = render(
    <ToolGrid allowedTools={["tracing"]} />
  );

  expect(getByText("Tracing Shell")).toBeTruthy();
  expect(queryByText("System Shell")).toBeNull();
});

it("should hide descriptions in compact mode", () => {
  let { getByText, queryByText } = render(
    <ToolGrid allowedTools={["snql-to-sql"]} compact />
  );

  expect(getByText("SnQL to SQL")).toBeTruthy();
  expect(queryByText("Translate a SnQL query", { exact: false })).toBeNull();
});
