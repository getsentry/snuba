import Client from "SnubaAdmin/api_client";

import { ConfigurableComponentConfigurations } from "SnubaAdmin/configurable_component/configurable_component_configurations";
import { it, expect } from "@jest/globals";
import { AllocationPolicy } from "SnubaAdmin/configurable_component/types";
import { act, fireEvent, render } from "@testing-library/react";
import React from "react";

it("should populate configs table upon render", async () => {
  let allocationPolicy: AllocationPolicy = {
    configurable_component_namespace: "some_namespace",
    configurable_component_class_name: "some_policy",
    configurations: [
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
    resource_identifier: "some_resource",
  };

  let { getByText, getByTestId } = render(
    <ConfigurableComponentConfigurations
      api={Client()}
      configurableComponentData={allocationPolicy}
    />
  );

  expect(getByText("key1")).toBeTruthy();
  expect(getByText("key2")).toBeTruthy();
  expect(
    getByText(JSON.stringify(allocationPolicy.configurations[1].params))
  ).toBeTruthy();

  act(() => fireEvent.click(getByTestId("key1_edit")));
  expect(getByText("Editing:")).toBeTruthy(); // modal rendered
});
