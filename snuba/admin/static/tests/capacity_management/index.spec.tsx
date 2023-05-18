import Client from "../../api_client";

import CapacityManagement from "../../capacity_management/index";
import { it, expect, jest } from "@jest/globals";
import {
  AllocationPolicy,
  AllocationPolicyConfig,
  AllocationPolicyOptionalConfigDefinition,
} from "../../capacity_management/types";
import { act, fireEvent, render, waitFor } from "@testing-library/react";
import React from "react";

it("should display allocation policy configs once a storage is selected", async () => {
  let data = [
    { storage_name: "storage1", allocation_policy: "policy1" },
    { storage_name: "storage2", allocation_policy: "policy2" },
  ];

  let mockClient = {
    ...Client(),
    getAllocationPolicies: jest
      .fn<() => Promise<AllocationPolicy[]>>()
      .mockResolvedValueOnce(data),
    getAllocationPolicyConfigs: jest
      .fn<() => Promise<AllocationPolicyConfig[]>>()
      .mockResolvedValueOnce([
        {
          name: "key1",
          value: "10",
          description: "something",
          type: "int",
          params: {},
        },
      ]),
    getAllocationPolicyOptionalConfigDefinitions: jest
      .fn<() => Promise<AllocationPolicyOptionalConfigDefinition[]>>()
      .mockResolvedValueOnce([]),
  };

  let { getByRole, getByText } = render(
    <CapacityManagement api={mockClient} />
  );

  await waitFor(() =>
    expect(mockClient.getAllocationPolicies).toBeCalledTimes(1)
  );

  expect(getByText("storage1")).toBeTruthy();
  expect(getByText("storage2")).toBeTruthy();

  // select a storage
  act(() =>
    fireEvent.change(getByRole("combobox"), { target: { value: "storage1" } })
  );
  expect(getByText("policy1")).toBeTruthy();

  await waitFor(() =>
    expect(mockClient.getAllocationPolicyConfigs).toBeCalledTimes(1)
  );
  await waitFor(() =>
    expect(
      mockClient.getAllocationPolicyOptionalConfigDefinitions
    ).toBeCalledTimes(1)
  );

  // config is displayed
  expect(getByText("key1")).toBeTruthy();
  expect(getByText("10")).toBeTruthy();
  expect(getByText("something")).toBeTruthy();
  expect(getByText("int")).toBeTruthy();
  expect(getByText("N/A")).toBeTruthy();
});
