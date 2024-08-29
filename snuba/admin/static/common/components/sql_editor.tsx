import React, { useLayoutEffect, useMemo, useRef } from "react";
import { EditorState } from "@codemirror/state";
import { EditorView, lineNumbers, keymap } from "@codemirror/view";
import { insertNewline } from "@codemirror/commands";

import { sql } from "@codemirror/lang-sql";
import { theme, highlighting } from "./theme";

export interface Props {
  value: string;
  onChange: (newValue: string) => void;
}

export function SQLEditor({ value, onChange }: Props) {
  const elementRef = useRef<HTMLDivElement | null>(null);
  const viewRef = useRef<EditorView | null>(null);

  const updateListener = useMemo(() => {
    return EditorView.updateListener.of((update) => {
      if (update.docChanged) {
        onChange(update.state.doc.toString());
      }
    });
  }, [onChange]);

  const multiNewLine = useMemo(() => {
    return keymap.of([{ key: "Enter", run: insertNewline }]);
  }, []);

  const extensions = [
    updateListener,
    multiNewLine,
    theme,
    highlighting,
    EditorView.lineWrapping,
    lineNumbers(),
    sql(),
  ];

  useLayoutEffect(() => {
    if (!elementRef.current) return;

    const state = EditorState.create({
      extensions,
    });

    viewRef.current = new EditorView({
      state,
      parent: elementRef.current,
    });
  }, []);

  useLayoutEffect(() => {
    if (!viewRef.current) return;

    const state = viewRef.current.state;

    if (value !== state.doc.toString()) {
      viewRef.current.setState(
        EditorState.create({
          doc: value,
          extensions,
        }),
      );
    }
  }, [value]);

  return <div ref={elementRef} />;
}
