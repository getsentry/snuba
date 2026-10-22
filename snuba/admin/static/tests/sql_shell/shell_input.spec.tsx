import React from "react";
import { it, expect, describe, jest, afterEach } from "@jest/globals";
import { cleanup, fireEvent, render } from "@testing-library/react";
import { EditorView } from "@codemirror/view";
import { ShellInput, ShellInputProps } from "SnubaAdmin/sql_shell/shell_input";

function renderShellInput(overrides: Partial<ShellInputProps> = {}) {
  const props: ShellInputProps = {
    value: "",
    onChange: jest.fn(),
    onExecute: jest.fn(),
    onTab: jest.fn(() => false),
    onHistoryUp: jest.fn(),
    onHistoryDown: jest.fn(),
    onClear: jest.fn(),
    onClearScreen: jest.fn(),
    onDeleteToStart: jest.fn(),
    onDeleteToEnd: jest.fn(),
    onDeleteWord: jest.fn(),
    ...overrides,
  };
  const { container } = render(<ShellInput {...props} />);
  const view = EditorView.findFromDOM(
    container.querySelector(".cm-editor") as HTMLElement
  )!;
  return { props, view };
}

describe("ShellInput", () => {
  afterEach(cleanup);

  it("undoes and redoes typed text with the keyboard", () => {
    const onChange = jest.fn<(value: string) => void>();
    const { view } = renderShellInput({ onChange });

    view.dispatch({ changes: { from: 0, insert: "SELECT 1" }, userEvent: "input.type" });
    expect(onChange).toHaveBeenLastCalledWith("SELECT 1");

    // jsdom is not a Mac, so CodeMirror's "Mod" key is Ctrl
    fireEvent.keyDown(view.contentDOM, { key: "z", ctrlKey: true });
    expect(view.state.doc.toString()).toBe("");
    expect(onChange).toHaveBeenLastCalledWith("");

    fireEvent.keyDown(view.contentDOM, { key: "y", ctrlKey: true });
    expect(view.state.doc.toString()).toBe("SELECT 1");
  });
});
