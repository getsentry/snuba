import Client from "../../api_client";

import CapacityManagement from "../../capacity_management/index";
import { it, expect, jest } from "@jest/globals";
import { AllocationPolicy } from "../../capacity_management/types";
import { act, render, waitFor } from "@testing-library/react";
import React from "react";

it("should populate list of storages + policies when rendering", async () => {
  let data = [
    { storage_name: "storage1", allocation_policy: "policy1" },
    { storage_name: "storage2", allocation_policy: "policy2" },
  ];

  let mockClient = {
    ...Client(),
    getAllocationPolicies: jest
      .fn<() => Promise<AllocationPolicy[]>>()
      .mockResolvedValueOnce(data),
  };

  let { getByRole, getByText } = render(
    <CapacityManagement api={mockClient} />
  );

  await waitFor(() =>
    expect(mockClient.getAllocationPolicies).toBeCalledTimes(1)
  );

  expect(getByText("storage1")).toBeTruthy();
  expect(getByText("storage2")).toBeTruthy();
});
