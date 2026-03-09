import React, { useLayoutEffect, useMemo, useRef, useEffect } from "react";
import { EditorState, Compartment } from "@codemirror/state";
import { EditorView, keymap, placeholder as cmPlaceholder } from "@codemirror/view";
import { sql } from "@codemirror/lang-sql";
import { HighlightStyle, syntaxHighlighting } from "@codemirror/language";
import { tags } from "@lezer/highlight";

// Compartments for dynamic configuration
const placeholderCompartment = new Compartment();
const readOnlyCompartment = new Compartment();

// GitHub Dark theme colors for SQL highlighting
const shellHighlighting = syntaxHighlighting(
  HighlightStyle.define([
    // Keywords: SELECT, FROM, WHERE, INSERT, UPDATE, DELETE, etc.
    { tag: [tags.keyword, tags.operatorKeyword], color: "#ff7b72", fontWeight: "500" },
    // Types: INT, VARCHAR, etc.
    { tag: [tags.typeName, tags.typeOperator], color: "#ff7b72" },
    // Strings
    { tag: [tags.string], color: "#a5d6ff" },
    // Numbers
    { tag: [tags.number], color: "#79c0ff" },
    // Comments
    { tag: [tags.comment, tags.lineComment, tags.blockComment], color: "#8b949e", fontStyle: "italic" },
    // Functions
    { tag: [tags.function(tags.variableName), tags.standard(tags.name)], color: "#d2a8ff" },
    // Operators: =, <, >, +, -, *, /
    { tag: [tags.operator, tags.punctuation], color: "#e6edf3" },
    // Column/table names
    { tag: [tags.propertyName, tags.variableName, tags.name], color: "#e6edf3" },
    // Brackets
    { tag: [tags.bracket, tags.paren], color: "#8b949e" },
    // Special values: NULL, TRUE, FALSE
    { tag: [tags.bool, tags.null, tags.atom], color: "#79c0ff", fontWeight: "500" },
  ])
);

// Dark theme matching the shell container
const shellTheme = EditorView.theme({
  "&": {
    backgroundColor: "transparent",
    color: "#e6edf3",
    fontSize: "14px",
    fontFamily: '"JetBrains Mono", "Fira Code", "Source Code Pro", Consolas, monospace',
  },
  "&.cm-focused": {
    outline: "none",
  },
  ".cm-content": {
    caretColor: "#58a6ff",
    padding: "0",
    minHeight: "21px",
    lineHeight: "21px",
  },
  ".cm-line": {
    padding: "0",
  },
  ".cm-cursor": {
    borderLeftColor: "#58a6ff",
    borderLeftWidth: "2px",
  },
  ".cm-selectionBackground, &.cm-focused .cm-selectionBackground": {
    backgroundColor: "rgba(88, 166, 255, 0.2)",
  },
  ".cm-placeholder": {
    color: "#6e7681",
    fontStyle: "normal",
  },
  ".cm-scroller": {
    overflow: "auto",
    maxHeight: "210px",
    lineHeight: "21px",
    fontFamily: "inherit",
  },
  "&.cm-editor": {
    flex: 1,
  },
  // Scrollbar styling
  ".cm-scroller::-webkit-scrollbar": {
    width: "6px",
  },
  ".cm-scroller::-webkit-scrollbar-track": {
    background: "transparent",
  },
  ".cm-scroller::-webkit-scrollbar-thumb": {
    background: "#30363d",
    borderRadius: "3px",
  },
});

export interface ShellInputProps {
  value: string;
  onChange: (value: string) => void;
  onExecute: () => void;
  onTab: () => boolean;
  onHistoryUp: () => void;
  onHistoryDown: () => void;
  onClear: () => void;
  onClearScreen: () => void;
  onDeleteToStart: () => void;
  onDeleteToEnd: () => void;
  onDeleteWord: () => void;
  placeholder?: string;
  disabled?: boolean;
}

export function ShellInput({
  value,
  onChange,
  onExecute,
  onTab,
  onHistoryUp,
  onHistoryDown,
  onClear,
  onClearScreen,
  onDeleteToStart,
  onDeleteToEnd,
  onDeleteWord,
  placeholder = "",
  disabled = false,
}: ShellInputProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | null>(null);
  const callbacksRef = useRef({
    onExecute,
    onTab,
    onHistoryUp,
    onHistoryDown,
    onClear,
    onClearScreen,
    onDeleteToStart,
    onDeleteToEnd,
    onDeleteWord,
    onChange,
  });

  // Keep callbacks up to date
  useEffect(() => {
    callbacksRef.current = {
      onExecute,
      onTab,
      onHistoryUp,
      onHistoryDown,
      onClear,
      onClearScreen,
      onDeleteToStart,
      onDeleteToEnd,
      onDeleteWord,
      onChange,
    };
  });

  const customKeymap = useMemo(() => {
    return keymap.of([
      {
        key: "Enter",
        run: () => {
          callbacksRef.current.onExecute();
          return true;
        },
      },
      {
        key: "Shift-Enter",
        run: (view) => {
          // Insert newline
          const { from, to } = view.state.selection.main;
          view.dispatch({
            changes: { from, to, insert: "\n" },
            selection: { anchor: from + 1 },
          });
          return true;
        },
      },
      {
        key: "Tab",
        run: () => {
          return callbacksRef.current.onTab();
        },
      },
      {
        key: "ArrowUp",
        run: () => {
          callbacksRef.current.onHistoryUp();
          return true;
        },
      },
      {
        key: "ArrowDown",
        run: () => {
          callbacksRef.current.onHistoryDown();
          return true;
        },
      },
      {
        key: "Ctrl-c",
        run: () => {
          callbacksRef.current.onClear();
          return true;
        },
      },
      {
        key: "Ctrl-l",
        run: () => {
          callbacksRef.current.onClearScreen();
          return true;
        },
      },
      {
        key: "Ctrl-u",
        run: () => {
          callbacksRef.current.onDeleteToStart();
          return true;
        },
      },
      {
        key: "Ctrl-k",
        run: () => {
          callbacksRef.current.onDeleteToEnd();
          return true;
        },
      },
      {
        key: "Ctrl-w",
        run: () => {
          callbacksRef.current.onDeleteWord();
          return true;
        },
      },
      {
        key: "Ctrl-a",
        run: (view) => {
          view.dispatch({
            selection: { anchor: 0 },
          });
          return true;
        },
      },
      {
        key: "Ctrl-e",
        run: (view) => {
          view.dispatch({
            selection: { anchor: view.state.doc.length },
          });
          return true;
        },
      },
    ]);
  }, []);

  const updateListener = useMemo(() => {
    return EditorView.updateListener.of((update) => {
      if (update.docChanged) {
        callbacksRef.current.onChange(update.state.doc.toString());
      }
    });
  }, []);

  const extensions = useMemo(() => [
    customKeymap,
    updateListener,
    shellTheme,
    shellHighlighting,
    sql(),
    EditorView.lineWrapping,
    placeholderCompartment.of(cmPlaceholder(placeholder)),
    readOnlyCompartment.of(EditorState.readOnly.of(disabled)),
  ], [customKeymap, updateListener]);

  // Initialize editor
  useLayoutEffect(() => {
    if (!containerRef.current) return;

    const state = EditorState.create({
      doc: value,
      extensions,
    });

    viewRef.current = new EditorView({
      state,
      parent: containerRef.current,
    });

    return () => {
      viewRef.current?.destroy();
      viewRef.current = null;
    };
  }, []);

  // Focus editor after paint to ensure it sticks on page navigation
  useEffect(() => {
    requestAnimationFrame(() => {
      viewRef.current?.focus();
    });
  }, []);

  // Sync value from parent
  useLayoutEffect(() => {
    const view = viewRef.current;
    if (!view) return;

    const currentValue = view.state.doc.toString();
    if (value !== currentValue) {
      view.dispatch({
        changes: { from: 0, to: currentValue.length, insert: value },
        selection: { anchor: value.length },
      });
    }
  }, [value]);

  // Update placeholder when it changes
  useLayoutEffect(() => {
    const view = viewRef.current;
    if (!view) return;

    view.dispatch({
      effects: placeholderCompartment.reconfigure(cmPlaceholder(placeholder)),
    });
  }, [placeholder]);

  // Update disabled state when it changes
  useLayoutEffect(() => {
    const view = viewRef.current;
    if (!view) return;

    view.dispatch({
      effects: readOnlyCompartment.reconfigure(EditorState.readOnly.of(disabled)),
    });
  }, [disabled]);

  // Focus editor when clicking container
  const handleClick = () => {
    viewRef.current?.focus();
  };

  return (
    <div
      ref={containerRef}
      onClick={handleClick}
      style={{ flex: 1, minWidth: 0 }}
    />
  );
}
