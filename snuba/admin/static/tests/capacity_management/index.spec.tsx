import Client from "../../api_client";

import CapacityManagement from "../../capacity_management/index";
import { it, expect, jest } from "@jest/globals";
import { AllocationPolicy } from "../../capacity_management/types";
import { act, fireEvent, render, waitFor } from "@testing-library/react";
import React from "react";

it("should display allocation policy configs once a storage is selected", async () => {
  let storages = ["storage1", "storage2"];
  let allocationPolicies: AllocationPolicy[] = [
    {
      policy_name: "some_policy",
      configs: [
        {
          name: "key1",
          value: "10",
          description: "something",
          type: "int",
          params: {},
        },
      ],
      optional_config_definitions: [],
    },
    {
      policy_name: "some_other_policy",
      configs: [
        {
          name: "key2",
          value: "a_value",
          description: "something_else",
          type: "string",
          params: { some_param: 1 },
        },
      ],
      optional_config_definitions: [],
    },
  ];

  let mockClient = {
    ...Client(),
    getStoragesWithAllocationPolicies: jest
      .fn<() => Promise<string[]>>()
      .mockResolvedValueOnce(storages),
    getAllocationPolicies: jest
      .fn<() => Promise<AllocationPolicy[]>>()
      .mockResolvedValueOnce(allocationPolicies),
  };

  let { getByRole, getByText } = render(
    <CapacityManagement api={mockClient} />
  );

  await waitFor(() =>
    expect(mockClient.getStoragesWithAllocationPolicies).toBeCalledTimes(1)
  );

  expect(getByText("storage1")).toBeTruthy();
  expect(getByText("storage2")).toBeTruthy();

  // select a storage
  fireEvent.change(getByRole("combobox"), { target: { value: "storage1" } });
  await waitFor(() =>
    expect(mockClient.getAllocationPolicies).toBeCalledTimes(1)
  );

  await waitFor(() => expect(getByText("key1")).toBeTruthy());

  // first policy
  expect(getByText("some_policy")).toBeTruthy();
  // config is displayed
  expect(getByText("key1")).toBeTruthy();
  expect(getByText("10")).toBeTruthy();
  expect(getByText("something")).toBeTruthy();
  expect(getByText("int")).toBeTruthy();
  expect(getByText("N/A")).toBeTruthy();

  // second policy
  expect(getByText("some_other_policy")).toBeTruthy();
  // config is displayed
  expect(getByText("key2")).toBeTruthy();
  expect(getByText("a_value")).toBeTruthy();
  expect(getByText("string")).toBeTruthy();
  expect(getByText("something_else")).toBeTruthy();
  expect(getByText('{"some_param":1}')).toBeTruthy();
});
