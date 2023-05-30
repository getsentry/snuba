import "@testing-library/react/dont-cleanup-after-each";
import { it, expect, jest } from "@jest/globals";
import { act, cleanup, fireEvent, render } from "@testing-library/react";
import React from "react";
import EditConfigModal from "../../capacity_management/edit_config_modal";

describe("Edit Config Modal", () => {
  afterAll(() => {
    cleanup();
  });

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

  it("should not save the config if it isn't completed yet", async () => {
    const saveButton = getByRole("button", { name: "Save Changes" });
    act(() => fireEvent.click(saveButton));
    expect(mockedSave).not.toBeCalled();
  });

  it("should call the save function if the form is completed and the save button is clicked", async () => {
    const saveButton = getByRole("button", { name: "Save Changes" });
    act(() =>
      fireEvent.change(getByRole("spinbutton"), { target: { value: 20 } })
    );
    act(() => fireEvent.click(saveButton));
    expect(mockedSave).toBeCalledWith({ ...mockedConfig, value: "20" });
    expect(mockedSet).toBeCalledWith(false);
  });

  it("should label the delete button as Reset for a required config", async () => {
    expect(getByText("Reset")).toBeTruthy();
  });

  it("should call the delete config function upon hitting delete button", async () => {
    act(() => fireEvent.click(getByRole("button", { name: "Reset" })));
    expect(window.confirm).toBeCalled();
    expect(mockedDelete).toBeCalled();
  });

  it("should call the currently_editing setter with False when closed", async () => {
    act(() => fireEvent.click(getByRole("button", { name: "Close" })));
    expect(mockedSet).toBeCalledWith(false);
  });
});
