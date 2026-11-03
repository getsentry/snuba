import React, { useLayoutEffect, useMemo, useRef } from "react";
import { Annotation, EditorState } from "@codemirror/state";
import { EditorView, lineNumbers, keymap } from "@codemirror/view";
import {
  history,
  historyKeymap,
  insertNewline,
  isolateHistory,
} from "@codemirror/commands";

import { sql } from "@codemirror/lang-sql";
import { theme, highlighting } from "./theme";

// Marks changes that came from the `value` prop, so they are not echoed back
// through `onChange`.
const fromProps = Annotation.define<boolean>();

export interface Props {
  value: string;
  onChange: (newValue: string) => void;
}

export function SQLEditor({ value, onChange }: Props) {
  const elementRef = useRef<HTMLDivElement | null>(null);
  const viewRef = useRef<EditorView | null>(null);
  // The editor is only created once, so read the latest callback from a ref
  const onChangeRef = useRef(onChange);
  onChangeRef.current = onChange;

  const updateListener = useMemo(() => {
    return EditorView.updateListener.of((update) => {
      if (
        update.docChanged &&
        !update.transactions.some((tr) => tr.annotation(fromProps))
      ) {
        onChangeRef.current(update.state.doc.toString());
      }
    });
  }, []);

  const multiNewLine = useMemo(() => {
    return keymap.of([{ key: "Enter", run: insertNewline }]);
  }, []);

  const extensions = [
    updateListener,
    multiNewLine,
    history(),
    keymap.of(historyKeymap),
    theme,
    highlighting,
    EditorView.lineWrapping,
    lineNumbers(),
    sql(),
  ];

  useLayoutEffect(() => {
    if (!elementRef.current) return;

    const state = EditorState.create({
      doc: value,
      extensions,
    });

    viewRef.current = new EditorView({
      state,
      parent: elementRef.current,
    });
  }, []);

  useLayoutEffect(() => {
    const view = viewRef.current;
    if (!view) return;

    const currentValue = view.state.doc.toString();

    // Dispatch rather than replace the state, so the undo history is kept and
    // the new value (e.g. a predefined query) is its own undo step
    if (value !== currentValue) {
      view.dispatch({
        changes: { from: 0, to: currentValue.length, insert: value },
        annotations: [fromProps.of(true), isolateHistory.of("full")],
      });
    }
  }, [value]);

  return <div ref={elementRef} />;
}
