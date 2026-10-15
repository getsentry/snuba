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
import ClickhouseQueries from "SnubaAdmin/clickhouse_queries";
import QueryDisplay from "SnubaAdmin/clickhouse_queries/query_display";
import { QueryHistoryEntry } from "SnubaAdmin/clickhouse_queries/query_history_entry";
import {
  ClickhouseNodeData,
  PredefinedQuery,
  QueryRequest,
  QueryResult,
} from "SnubaAdmin/clickhouse_queries/types";
import {
  buildSystemQueryUrl,
  parseHostPort,
  parseSystemQueryPrefill,
  splitHash,
} from "SnubaAdmin/clickhouse_queries/prefill";

jest.mock("SnubaAdmin/common/components/sql_editor");

describe("parseSystemQueryPrefill", () => {
  it("returns null without params", () => {
    expect(parseSystemQueryPrefill("#system-queries")).toBeNull();
    expect(parseSystemQueryPrefill("#system-queries?")).toBeNull();
  });

  it("decodes the query, storage and host", () => {
    const sql = "SELECT count() FROM system.parts WHERE table = 'a+b'";
    const hash =
      "#system-queries?query=" +
      encodeURIComponent(sql) +
      "&storage=errors&host=clickhouse-01%3A9000";
    expect(parseSystemQueryPrefill(hash)).toEqual({
      sql,
      storage: "errors",
      host: "clickhouse-01",
      port: 9000,
    });
  });

  it("defaults the host port to 8123", () => {
    expect(parseHostPort("clickhouse-01")).toEqual({
      host: "clickhouse-01",
      port: 8123,
    });
  });

  it("round-trips a link built from a query", () => {
    const sql = "SELECT 1 + 1\nFROM system.one WHERE x = 'a&b'";
    const url = buildSystemQueryUrl({
      sql,
      storage: "errors",
      host: "clickhouse-01:9000",
    });
    expect(url).not.toContain("+");
    expect(parseSystemQueryPrefill(new URL(url).hash)).toEqual({
      sql,
      storage: "errors",
      host: "clickhouse-01",
      port: 9000,
    });
  });

  it("splits the route from the params", () => {
    expect(splitHash("#system-queries?query=x").route).toBe("#system-queries");
    expect(splitHash("#system-queries").route).toBe("#system-queries");
  });
});

describe("QueryDisplay opened from a link", () => {
  global.ResizeObserver = require("resize-observer-polyfill");
  afterEach(() => {
    cleanup();
    window.location.hash = "";
  });

  const nodes: [ClickhouseNodeData] = [
    {
      storage_name: "errors",
      local_table_name: "errors_local",
      local_nodes: [{ host: "clickhouse-01", port: 9000 }],
      dist_nodes: [],
      query_node: { host: "clickhouse-01", port: 9000 },
    },
  ];

  function renderWithHash(hash: string) {
    window.location.hash = hash;
    const api = {
      ...Client(),
      getClickhouseNodes: jest
        .fn<() => Promise<[ClickhouseNodeData]>>()
        .mockResolvedValue(nodes),
      executeSystemQuery: jest
        .fn<(req: QueryRequest) => Promise<QueryResult>>()
        .mockResolvedValue({
          input_query: "",
          timestamp: 0,
          column_names: ["x"],
          rows: [["1"]],
        } as QueryResult),
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

  it("fills the form, warns, and waits for the user to execute", async () => {
    const api = renderWithHash(
      "#system-queries?query=SELECT%201&storage=errors&host=clickhouse-01%3A9000"
    );
    await waitFor(() => expect(api.getClickhouseNodes).toBeCalled());

    expect(screen.getByTestId("prefill-warning")).toBeTruthy();
    expect(screen.getByRole("textbox").textContent).toBe("SELECT 1");
    expect(await screen.findAllByDisplayValue("clickhouse-01:9000")).not.toHaveLength(0);
    expect(
      screen.getByText("Execute Query").closest("button")?.disabled
    ).toBe(false);
    expect(api.executeSystemQuery).not.toBeCalled();
    // The params are dropped so a refresh doesn't re-apply them.
    expect(window.location.hash).toBe("#system-queries");
  });

  it("uses manual host entry for a host the storage doesn't list", async () => {
    const api = renderWithHash(
      "#system-queries?query=SELECT%201&storage=errors&host=other-host"
    );
    await waitFor(() => expect(api.getClickhouseNodes).toBeCalled());

    await waitFor(() =>
      expect(screen.getByPlaceholderText("enter host or host:port...")).toBeTruthy()
    );
    expect(screen.getByDisplayValue("other-host:8123")).toBeTruthy();
  });

  it("picks up a link pasted while already on the page", async () => {
    window.location.hash = "#system-queries";
    const api = {
      ...Client(),
      getPredefinedQueryOptions: jest
        .fn<() => Promise<[PredefinedQuery]>>()
        .mockResolvedValue([{ name: "q", sql: "SELECT 1", description: "" }]),
      getClickhouseNodes: jest
        .fn<() => Promise<[ClickhouseNodeData]>>()
        .mockResolvedValue(nodes),
    };
    render(<ClickhouseQueries api={api} />);
    await waitFor(() => expect(api.getClickhouseNodes).toBeCalled());
    expect(screen.queryByTestId("prefill-warning")).toBeNull();

    window.location.hash = "#system-queries?query=SELECT%202&storage=errors";
    window.dispatchEvent(new HashChangeEvent("hashchange"));

    expect(await screen.findByTestId("prefill-warning")).toBeTruthy();
    expect(screen.getByRole("textbox").textContent).toBe("SELECT 2");
  });

  it("flags a missing host and explains the disabled Execute", async () => {
    const api = renderWithHash("#system-queries?query=SELECT%201&storage=errors");
    await waitFor(() => expect(api.getClickhouseNodes).toBeCalled());

    const hint = "Select a host to run the query.";
    expect(screen.getAllByText(hint)).toHaveLength(1);
    expect(
      screen.getByText("Execute Query").closest("button")?.disabled
    ).toBe(true);

    fireEvent.mouseEnter(screen.getByTestId("execute-wrapper"));
    // The field hint plus the tooltip.
    await waitFor(() => expect(screen.getAllByText(hint)).toHaveLength(2));
  });

  it("drops the host cue once a host is set", async () => {
    const api = renderWithHash(
      "#system-queries?query=SELECT%201&storage=errors&host=clickhouse-01%3A9000"
    );
    await waitFor(() => expect(api.getClickhouseNodes).toBeCalled());
    expect(screen.queryByText("Select a host to run the query.")).toBeNull();
  });

  it("shows no warning without params", async () => {
    const api = renderWithHash("#system-queries");
    await waitFor(() => expect(api.getClickhouseNodes).toBeCalled());
    expect(screen.queryByTestId("prefill-warning")).toBeNull();
  });
});

describe("QueryHistoryEntry", () => {
  afterEach(cleanup);

  it("copies a link that pre-fills the entry's query", () => {
    const writeText = jest.fn<(text: string) => Promise<void>>();
    Object.assign(window.navigator, { clipboard: { writeText } });
    render(
      <QueryHistoryEntry
        queryResult={
          {
            input_query: "SELECT 1 (errors,clickhouse-01:9000)",
            timestamp: 0,
            column_names: ["x"],
            rows: [["1"]],
          } as QueryResult
        }
        isLatest
        defaultExpanded={false}
        csv="x\n1"
        resultDataPopulator={() => <div />}
      />
    );

    fireEvent.click(screen.getByText("Share query"));

    const url = writeText.mock.calls[0][0];
    expect(parseSystemQueryPrefill(new URL(url).hash)).toEqual({
      sql: "SELECT 1",
      storage: "errors",
      host: "clickhouse-01",
      port: 9000,
    });
  });
});
