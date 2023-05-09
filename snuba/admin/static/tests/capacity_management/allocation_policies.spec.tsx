import Client from "../../api_client";

import AllocationPolicyConfigs from "../../capacity_management/allocation_policy";
import { it, expect, jest } from "@jest/globals";
import {
  AllocationPolicyConfig,
  AllocationPolicyOptionalConfigDefinition,
} from "../../capacity_management/types";
import { render, waitFor } from "@testing-library/react";
import React from "react";

it("should populate configs table upon render", async () => {
  let policyConfigs = [
    {
      key: "key1",
      value: "10",
      description: "something",
      type: "int",
      params: {},
    },
    {
      key: "key2",
      value: "20",
      description: "something params",
      type: "int",
      params: { a: "1", b: "2" },
    },
  ];

  let optionalConfigDefs = [
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
  ];

  let mockClient = {
    ...Client(),
    getAllocationPolicyConfigs: jest
      .fn<() => Promise<AllocationPolicyConfig[]>>()
      .mockResolvedValueOnce(policyConfigs),
    getAllocationPolicyOptionalConfigDefinitions: jest
      .fn<() => Promise<AllocationPolicyOptionalConfigDefinition[]>>()
      .mockResolvedValueOnce(optionalConfigDefs),
  };

  let { getByText } = render(
    <AllocationPolicyConfigs api={mockClient} storage="storage1" />
  );

  await waitFor(() =>
    expect(mockClient.getAllocationPolicyConfigs).toBeCalledTimes(1)
  );
  await waitFor(() =>
    expect(
      mockClient.getAllocationPolicyOptionalConfigDefinitions
    ).toBeCalledTimes(1)
  );

  expect(getByText("N/A")).toBeTruthy(); // non optional key in table
  expect(getByText("key1")).toBeTruthy();
  expect(getByText("key2")).toBeTruthy();
  expect(getByText(JSON.stringify(policyConfigs[1].params))).toBeTruthy();
});
