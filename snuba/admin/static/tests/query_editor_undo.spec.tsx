import React from "react";
import { it, expect, describe, jest, afterEach, beforeEach } from "@jest/globals";
import { act, cleanup, fireEvent, render } from "@testing-library/react";
import { EditorView } from "@codemirror/view";
import QueryEditor from "SnubaAdmin/query_editor";

// Unlike query_editor.spec.tsx, this uses the real CodeMirror SQLEditor

// QueryEditor namespaces its storage by the URL hash, which is empty in jsdom
const STORAGE_KEY = "-query-editor-query";

describe("QueryEditor undo", () => {
  global.ResizeObserver = require("resize-observer-polyfill");
  beforeEach(() => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify("SELECT saved"));
  });
  afterEach(cleanup);

  it("does not undo the saved query loaded on mount", async () => {
    const { container } = render(<QueryEditor onQueryUpdate={jest.fn()} />);
    const view = EditorView.findFromDOM(
      container.querySelector(".cm-editor") as HTMLElement
    )!;
    expect(view.state.doc.toString()).toBe("SELECT saved");

    await act(async () => {
      fireEvent.keyDown(view.contentDOM, { key: "z", ctrlKey: true });
    });

    expect(view.state.doc.toString()).toBe("SELECT saved");
    expect(localStorage.getItem(STORAGE_KEY)).toBe(JSON.stringify("SELECT saved"));
  });
});
