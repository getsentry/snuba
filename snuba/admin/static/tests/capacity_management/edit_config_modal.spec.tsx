import { it, expect, jest } from "@jest/globals";
import { act, fireEvent, render } from "@testing-library/react";
import React from "react";
import EditConfigModal from "../../capacity_management/edit_config_modal";

it("should modify the config as expected", async () => {
  const mockedSet = jest.fn();
  const mockedDelete = jest.fn();
  const mockedSave = jest.fn();
  window.confirm = jest.fn(() => true); // always click 'yes'

  var mockedConfig = {
    name: "my_config",
    value: "10",
    type: "int",
    description: "some config",
    params: {},
  };

  let { getByRole, getByText } = render(
    <EditConfigModal
      currentlyEditing={true}
      currentConfig={mockedConfig}
      setCurrentlyEditing={mockedSet}
      deleteConfig={mockedDelete}
      saveConfig={mockedSave}
    />
  );

  // Close button should set editing to false
  act(() => fireEvent.click(getByRole("button", { name: "Close" })));
  expect(mockedSet).toBeCalledWith(false);

  // Button should be disabled since the form isn't completed yet
  const saveButton = getByRole("button", { name: "Save Changes" });
  act(() => fireEvent.click(saveButton));
  expect(mockedSave).not.toBeCalled();

  // Save changes with a value should call the save function with the updated value
  act(() =>
    fireEvent.change(getByRole("spinbutton"), { target: { value: 20 } })
  );
  act(() => fireEvent.click(saveButton));
  expect(mockedSave).toBeCalledWith({ ...mockedConfig, value: "20" });
  expect(mockedSet).toBeCalledWith(false);

  // Not parameterized so it should say "Reset" instead of "Delete"
  expect(getByText("Reset")).toBeTruthy();

  // Reset button should call the delete config function
  act(() => fireEvent.click(getByRole("button", { name: "Reset" })));
  expect(window.confirm).toBeCalled();
  expect(mockedDelete).toBeCalled();
});
