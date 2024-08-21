import React, { useLayoutEffect, useMemo, useRef } from "react";
import { EditorState } from "@codemirror/state";
import { EditorView, lineNumbers } from "@codemirror/view";

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

  const extensions = [
    updateListener,
    theme,
    highlighting,
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
