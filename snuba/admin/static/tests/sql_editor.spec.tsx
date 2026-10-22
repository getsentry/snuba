import React from "react";
import { it, expect, describe, jest, afterEach } from "@jest/globals";
import { cleanup, fireEvent, render } from "@testing-library/react";
import { EditorView } from "@codemirror/view";
import { SQLEditor } from "SnubaAdmin/common/components/sql_editor";

function getView(container: HTMLElement): EditorView {
  return EditorView.findFromDOM(
    container.querySelector(".cm-editor") as HTMLElement
  )!;
}

function type(view: EditorView, text: string) {
  const end = view.state.doc.length;
  view.dispatch({ changes: { from: end, insert: text }, userEvent: "input.type" });
}

// jsdom is not a Mac, so CodeMirror's "Mod" key is Ctrl
function pressUndo(view: EditorView) {
  fireEvent.keyDown(view.contentDOM, { key: "z", ctrlKey: true });
}

function pressRedo(view: EditorView) {
  fireEvent.keyDown(view.contentDOM, { key: "y", ctrlKey: true });
}

describe("SQLEditor", () => {
  afterEach(cleanup);

  it("undoes and redoes typed text with the keyboard", () => {
    const onChange = jest.fn<(value: string) => void>();
    const { container } = render(<SQLEditor value="" onChange={onChange} />);
    const view = getView(container);

    type(view, "SELECT 1");
    expect(onChange).toHaveBeenLastCalledWith("SELECT 1");

    pressUndo(view);
    expect(view.state.doc.toString()).toBe("");
    expect(onChange).toHaveBeenLastCalledWith("");

    pressRedo(view);
    expect(view.state.doc.toString()).toBe("SELECT 1");
    expect(onChange).toHaveBeenLastCalledWith("SELECT 1");
  });

  it("can undo a value replaced from props", () => {
    const onChange = jest.fn<(value: string) => void>();
    const { container, rerender } = render(
      <SQLEditor value="" onChange={onChange} />
    );
    const view = getView(container);

    type(view, "SELECT 1");
    rerender(<SQLEditor value="SELECT 1" onChange={onChange} />);
    // e.g. the user picks a predefined query
    rerender(<SQLEditor value="SELECT 2" onChange={onChange} />);
    expect(view.state.doc.toString()).toBe("SELECT 2");

    pressUndo(view);
    expect(view.state.doc.toString()).toBe("SELECT 1");
    expect(onChange).toHaveBeenLastCalledWith("SELECT 1");
  });

  it("does not report a value set from props as a change", () => {
    const onChange = jest.fn<(value: string) => void>();
    const { rerender } = render(<SQLEditor value="" onChange={onChange} />);

    rerender(<SQLEditor value="SELECT 2" onChange={onChange} />);
    expect(onChange).not.toHaveBeenCalled();
  });
});
