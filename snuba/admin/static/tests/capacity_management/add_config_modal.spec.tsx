import { it, expect, jest } from "@jest/globals";
import { act, fireEvent, render } from "@testing-library/react";
import React from "react";
import AddConfigModal from "../../capacity_management/add_config_modal";
import {
  AllocationPolicyConfig,
  AllocationPolicyOptionalConfigDefinition,
} from "../../capacity_management/types";

it("should add the config as expected", async () => {
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

  // Close button should set editing to false
  act(() => fireEvent.click(getByRole("button", { name: "Close" })));
  expect(mockedSet).toBeCalledWith(false);

  // Select an optional config
  act(() =>
    fireEvent.change(getByRole("combobox"), {
      target: { value: "optional_config" },
    })
  );

  // Modal should populate with the parameters of the config
  expect(getByText("some_param (int)")).toBeTruthy();
  expect(getByText("some_other_param (str)")).toBeTruthy();

  // Save changes with a value should call the save function with the updated value
  act(() =>
    fireEvent.change(getByTestId("some_param"), { target: { value: 30 } })
  );

  const saveButton = getByRole("button", { name: "Save Changes" });
  act(() => fireEvent.click(saveButton));

  // Button should be disabled since the form isn't completed yet
  expect(mockedSave).not.toBeCalled();

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

  act(() => fireEvent.click(saveButton));

  expect(mockedSave).toBeCalledWith(expectedConfig);
  expect(mockedSet).toBeCalledWith(false);
});
