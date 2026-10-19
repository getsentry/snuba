import React from "react";
import { it, expect, describe, jest, afterEach } from "@jest/globals";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import Client from "SnubaAdmin/api_client";
import QueryDisplay from "SnubaAdmin/querylog/query_display";
import { QuerylogRequest, QuerylogResult } from "SnubaAdmin/querylog/types";
import {
  buildQuerylogUrl,
  parseQuerylogPrefill,
} from "SnubaAdmin/querylog/prefill";

jest.mock("SnubaAdmin/common/components/sql_editor");

describe("parseQuerylogPrefill", () => {
  it("returns null without a query", () => {
    expect(parseQuerylogPrefill("#querylog")).toBeNull();
  });

  it("round-trips a link built from a query", () => {
    const sql = "SELECT count() FROM querylog_local\nWHERE x = 'a&b+c'";
    const url = buildQuerylogUrl(sql);
    expect(url).toContain("#querylog?");
    expect(parseQuerylogPrefill(new URL(url).hash)).toEqual({ sql });
  });
});

describe("Querylog QueryDisplay", () => {
  global.ResizeObserver = require("resize-observer-polyfill");
  afterEach(() => {
    cleanup();
    window.location.hash = "";
    localStorage.clear();
  });

  function renderWithHash(hash: string) {
    window.location.hash = hash;
    const api = {
      ...Client(),
      executeQuerylogQuery: jest
        .fn<(req: QuerylogRequest) => Promise<QuerylogResult>>()
        .mockResolvedValue({
          input_query: "",
          timestamp: 0,
          column_names: ["x"],
          rows: [["1"]],
        } as QuerylogResult),
      getQuerylogSchema: jest
        .fn<() => Promise<QuerylogResult>>()
        .mockResolvedValue({
          input_query: "",
          timestamp: 0,
          column_names: ["name", "type", "default_type", "comment"],
          rows: [
            ["request_id", "UUID", "", ""],
            ["dataset", "LowCardinality(String)", "", ""],
          ],
        } as unknown as QuerylogResult),
    };
    render(
      <QueryDisplay
        api={api}
        resultDataPopulator={() => <div />}
        predefinedQueryOptions={[]}
      />
    );
    return api;
  }

  it("fills the editor from a link and waits for Execute", async () => {
    const api = renderWithHash("#querylog?query=SELECT%201");

    // The querylog cluster isn't production, so no warning.
    expect(screen.queryByTestId("prefill-warning")).toBeNull();
    expect(screen.getByRole("textbox").textContent).toBe("SELECT 1");
    await waitFor(() => expect(window.location.hash).toBe("#querylog"));
    expect(api.executeQuerylogQuery).not.toBeCalled();
  });

  it("closes the editor after a run and shares the result", async () => {
    const writeText = jest.fn<(text: string) => Promise<void>>();
    Object.assign(window.navigator, { clipboard: { writeText } });
    const api = renderWithHash("#querylog?query=SELECT%201");

    fireEvent.click(screen.getByText("Execute Query"));
    await screen.findByText("Open Editor");
    expect(api.executeQuerylogQuery).toBeCalledWith({ sql: "SELECT 1" });
    expect(screen.getByTestId("editor-panel").textContent).toContain(
      "SELECT 1"
    );

    fireEvent.click(screen.getByText("Share query"));
    const url = writeText.mock.calls[0][0];
    expect(parseQuerylogPrefill(new URL(url).hash)).toEqual({
      sql: "SELECT 1",
    });
  });

  it("shows the schema in a drawer, not the results", async () => {
    const api = renderWithHash("#querylog");

    fireEvent.click(screen.getByText("Querylog schema"));
    expect(await screen.findByText("request_id")).toBeTruthy();
    expect(api.getQuerylogSchema).toBeCalledTimes(1);
    // Only the useful DESCRIBE columns.
    expect(screen.queryByText("default_type")).toBeNull();
    expect(screen.queryByText("Share query")).toBeNull();

    fireEvent.change(screen.getByLabelText("Filter columns"), {
      target: { value: "data" },
    });
    expect(screen.queryByText("request_id")).toBeNull();
    expect(screen.getByText("dataset")).toBeTruthy();
  });
});
