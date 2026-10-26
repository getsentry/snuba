import React from "react";
import { it, expect, describe, afterEach } from "@jest/globals";
import { act, cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { CustomSelect } from "SnubaAdmin/select";
import {
  getAllNodes,
  nodeMatchesSearch,
  nodeSearchMatch,
  nodeSelectItem,
  rankNodes,
} from "SnubaAdmin/utils/clickhouse_node_utils";

describe("CustomSelect", () => {
  global.ResizeObserver = require("resize-observer-polyfill");
  afterEach(cleanup);

  const nodes = getAllNodes(
    [
      "snuba-events-analytics-platform-0-0",
      "snuba-errors-0-0",
      "snuba-generic-metrics-0-0",
    ].map((host) => ({
      storage_name: host,
      local_table_name: "",
      local_nodes: [{ host, port: 8123 }],
      dist_nodes: [],
      query_node: null as any,
    }))
  );
  const options = nodes.map(nodeSelectItem);

  function renderSelect(value = "") {
    render(
      <CustomSelect
        value={value}
        onChange={() => {}}
        options={options}
        filter={(search, item) => nodeMatchesSearch(search, item.value)}
        highlight={(search, value) =>
          nodeSearchMatch(search, value)?.indexes ?? null
        }
        sortOptions={(search) => rankNodes(search, nodes).map(nodeSelectItem)}
      />
    );
    return screen.getByTestId("select");
  }

  it("marks the characters that match the search", async () => {
    const input = renderSelect();
    await act(async () => userEvent.type(input, "eap"));
    const marks = document.querySelectorAll("mark");
    expect(Array.from(marks, (mark) => mark.textContent)).toEqual([
      "e",
      "a",
      "p",
    ]);
  });

  it("marks a match across separators as one run", async () => {
    const input = renderSelect();
    await act(async () => userEvent.type(input, "0-0"));
    const marks = document.querySelectorAll("mark");
    expect(Array.from(marks, (mark) => mark.textContent)).toEqual([
      "0-0",
      "0-0",
      "0-0",
    ]);
  });

  it("lists the best matches first", async () => {
    const input = renderSelect();
    await act(async () => userEvent.type(input, "m"));
    expect(
      screen.getAllByTestId("select-option").map((item) => item.textContent)
    ).toEqual([
      "snuba-generic-metrics-0-0:8123snuba-generic-metrics-0-0",
      "snuba-events-analytics-platform-0-0:8123snuba-events-analytics-platform-0-0",
    ]);
  });

  it("marks nothing while the box shows the selected item", async () => {
    const input = renderSelect(options[0].value);
    await act(async () => userEvent.click(input));
    expect(screen.getAllByTestId("select-option")).toHaveLength(3);
    expect(document.querySelectorAll("mark")).toHaveLength(0);
  });
});
