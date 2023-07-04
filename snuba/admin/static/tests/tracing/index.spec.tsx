import TracingQueries from "../../tracing";
import { TracingResult } from "../../tracing/types";
import Client from "../../api_client";
import React from "react";
import { it, expect, jest } from "@jest/globals";
import {
  render,
  fireEvent,
  screen,
  act,
  waitFor,
} from "@testing-library/react";
import selectEvent from "react-select-event";
import { ClickhouseNodeData } from "../../clickhouse_queries/types";

import default_response from "./fixture";

it("select executor rows should appear", async () => {
  let mockClient = {
    ...Client(),
    executeTracingQuery: jest
      .fn<() => Promise<TracingResult>>()
      .mockResolvedValueOnce(default_response),
    getClickhouseNodes: jest
      .fn<() => Promise<[ClickhouseNodeData]>>()
      .mockResolvedValueOnce([
        {
          storage_name: "test",
          local_table_name: "test",
          local_nodes: [{ host: "test", port: 1123 }],
          dist_nodes: [{ host: "test", port: 1123 }],
          query_node: { host: "test", port: 1123 },
        },
      ]),
  };

  let { getByText, getByRole, queryByText } = render(
    <TracingQueries api={mockClient} />
  );
  await waitFor(() => expect(mockClient.getClickhouseNodes).toBeCalledTimes(1));

  act(() => {
    selectEvent.select(getByRole("combobox"), "test");
  });

  expect(queryByText("Copy to clipboard", { exact: false })).toBeNull();

  fireEvent.change(getByRole("textbox"), {
    target: { value: "Foo" },
  });

  const submitButton = screen.getByText("Execute query");
  expect(submitButton.getAttribute("disabled")).toBeFalsy();

  fireEvent.click(submitButton);
  //   await waitFor(() =>
  //     expect(mockClient.executeTracingQuery).toBeCalledTimes(1)
  //   );

  //   expect(getByText("Copy to clipboard", { exact: false })).toBeTruthy();
});
