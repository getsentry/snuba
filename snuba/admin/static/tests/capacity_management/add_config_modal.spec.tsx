import "@testing-library/react/dont-cleanup-after-each";
import { it, expect, jest } from "@jest/globals";
import { act, cleanup, fireEvent, render } from "@testing-library/react";
import React from "react";
import AddConfigModal from "../../capacity_management/add_config_modal";
import {
  AllocationPolicyConfig,
  AllocationPolicyOptionalConfigDefinition,
} from "../../capacity_management/types";

describe("Add Config Modal", () => {
  afterAll(() => {
    cleanup();
  });

  const mockedSet = jest.fn();
  const mockedSave = jest.fn();
  window.confirm = jest.fn(() => true); // always click 'yes'

  let mockedConfig = {
    name: "optional_config",
    type: "int",
    description: "some config",
  };

  let expectedConfig: AllocationPolicyConfig = {
    ...mockedConfig,
    value: "20",
    params: { some_param: "30", some_other_param: "test" },
  };

  let mockedDefs: AllocationPolicyOptionalConfigDefinition[] = [
    {
      ...mockedConfig,
      default: "10",
      params: [
        { name: "some_param", type: "int" },
        { name: "some_other_param", type: "str" },
      ],
    },
    {
      ...mockedConfig,
      name: "another_optional_config",
      default: "200",
      params: [{ name: "a_dfferent_param", type: "int" }],
    },
  ];

  let { getByRole, getByText, getByTestId } = render(
    <AddConfigModal
      currentlyAdding={true}
      setCurrentlyAdding={mockedSet}
      optionalConfigDefinitions={mockedDefs}
      saveConfig={mockedSave}
    />
  );

  it("should populate the modal with the parameters upon selecting an optional config", async () => {
    act(() =>
      fireEvent.change(getByRole("combobox"), {
        target: { value: "optional_config" },
      })
    );
    expect(getByText("some_param (int)")).toBeTruthy();
    expect(getByText("some_other_param (str)")).toBeTruthy();
  });

  it("should not save an incomplete form", async () => {
    act(() =>
      fireEvent.change(getByTestId("some_param"), { target: { value: 30 } })
    );
    const saveButton = getByRole("button", { name: "Save Changes" });
    act(() => fireEvent.click(saveButton));
    expect(mockedSave).not.toBeCalled();
  });

  it("should save a completed form upon clicking the Save button", async () => {
    act(() =>
      fireEvent.change(getByTestId("some_other_param"), {
        target: { value: "test" },
      })
    );
    act(() =>
      fireEvent.change(getByTestId("value_field"), {
        target: { value: 20 },
      })
    );
    const saveButton = getByRole("button", { name: "Save Changes" });
    act(() => fireEvent.click(saveButton));
    expect(mockedSave).toBeCalledWith(expectedConfig);
    // Close the modal as well
    expect(mockedSet).toBeCalledWith(false);
  });

  it("should call the currently_editing setter with False when closed", async () => {
    act(() => fireEvent.click(getByRole("button", { name: "Close" })));
    expect(mockedSet).toBeCalledWith(false);
  });
});
