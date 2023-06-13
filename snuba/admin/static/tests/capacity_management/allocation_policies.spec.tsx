import Client from "../../api_client";

import AllocationPolicyConfigs from "../../capacity_management/allocation_policy";
import { it, expect } from "@jest/globals";
import { AllocationPolicy } from "../../capacity_management/types";
import { act, fireEvent, render } from "@testing-library/react";
import React from "react";

it("should populate configs table upon render", async () => {
  let allocationPolicy: AllocationPolicy = {
    policy_name: "some_policy",
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
  };

  let { getByText, getByTestId } = render(
    <AllocationPolicyConfigs
      api={Client()}
      storage="storage1"
      policy={allocationPolicy}
    />
  );

  expect(getByText("N/A")).toBeTruthy(); // non optional key in table
  expect(getByText("key1")).toBeTruthy();
  expect(getByText("key2")).toBeTruthy();
  expect(
    getByText(JSON.stringify(allocationPolicy.configs[1].params))
  ).toBeTruthy();

  act(() => fireEvent.click(getByTestId("key1_edit")));
  expect(getByText("Editing:")).toBeTruthy(); // modal rendered
});
