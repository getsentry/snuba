import Client from "SnubaAdmin/api_client";

import { Configurations } from "SnubaAdmin/capacity_management/allocation_policy";
import { it, expect } from "@jest/globals";
import { AllocationPolicy } from "SnubaAdmin/capacity_management/types";
import { act, fireEvent, render } from "@testing-library/react";
import React from "react";

it("should populate configs table upon render", async () => {
  let allocationPolicy: AllocationPolicy = {
    name: "some_policy",
    configs: [
      {
        name: "key1",
        value: "10",
        description: "something",
        type: "int",
        params: {},
      },
      {
        name: "key2",
        value: "20",
        description: "something params",
        type: "int",
        params: { a: "1", b: "2" },
      },
    ],
    optional_config_definitions: [
      {
        name: "key2",
        type: "int",
        default: "10",
        description: "something params definition",
        params: [
          { name: "a", type: "int" },
          { name: "b", type: "int" },
        ],
      },
    ],
    query_type: "select",
  };

  let { getByText, getByTestId } = render(
    <Configurations
      api={Client()}
      entity={{ type: "storage", name: "storage1" }}
      configurable_component={allocationPolicy}
    />
  );

  expect(getByText("key1")).toBeTruthy();
  expect(getByText("key2")).toBeTruthy();
  expect(
    getByText(JSON.stringify(allocationPolicy.configs[1].params))
  ).toBeTruthy();

  act(() => fireEvent.click(getByTestId("key1_edit")));
  expect(getByText("Editing:")).toBeTruthy(); // modal rendered
});
