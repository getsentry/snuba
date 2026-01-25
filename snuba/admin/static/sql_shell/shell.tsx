import React, { useState, useEffect, useRef, useCallback } from "react";
import Client from "SnubaAdmin/api_client";
import { useShellStyles } from "SnubaAdmin/sql_shell/styles";
import { ShellOutput } from "SnubaAdmin/sql_shell/shell_output";
import {
  ParsedCommand,
  ShellState,
  ShellHistoryEntry,
} from "SnubaAdmin/sql_shell/types";
import { TracingRequest } from "SnubaAdmin/tracing/types";

const COMMAND_HISTORY_KEY = "sql_shell_command_history";

function parseCommand(input: string): ParsedCommand {
  const trimmed = input.trim();

  // USE storage_name
  if (/^USE\s+/i.test(trimmed)) {
    const storage = trimmed.replace(/^USE\s+/i, "").trim();
    return { type: "use", storage };
  }

  // SHOW STORAGES
  if (/^SHOW\s+STORAGES$/i.test(trimmed)) {
    return { type: "show_storages" };
  }

  // PROFILE ON/OFF
  if (/^PROFILE\s+(ON|OFF)$/i.test(trimmed)) {
    const enabled = /ON$/i.test(trimmed);
    return { type: "profile", enabled };
  }

  // TRACE RAW/FORMATTED
  if (/^TRACE\s+(RAW|FORMATTED)$/i.test(trimmed)) {
    const formatted = /FORMATTED$/i.test(trimmed);
    return { type: "trace_mode", formatted };
  }

  // HELP
  if (/^HELP$/i.test(trimmed)) {
    return { type: "help" };
  }

  // CLEAR
  if (/^CLEAR$/i.test(trimmed)) {
    return { type: "clear" };
  }

  // SQL query
  return { type: "sql", query: trimmed };
}

function loadCommandHistory(): string[] {
  try {
    const saved = localStorage.getItem(COMMAND_HISTORY_KEY);
    return saved ? JSON.parse(saved) : [];
  } catch {
    return [];
  }
}

function saveCommandHistory(history: string[]) {
  try {
    localStorage.setItem(
      COMMAND_HISTORY_KEY,
      JSON.stringify(history.slice(-100))
    );
  } catch {
    // Ignore storage errors
  }
}

interface SQLShellProps {
  api: Client;
}

function SQLShell({ api }: SQLShellProps) {
  const { classes } = useShellStyles();
  const [state, setState] = useState<ShellState>({
    currentStorage: null,
    profileEnabled: true,
    traceFormatted: true,
    history: [],
    commandHistory: loadCommandHistory(),
    historyIndex: -1,
    isExecuting: false,
  });
  const [inputValue, setInputValue] = useState("");
  const [storages, setStorages] = useState<string[]>([]);
  const inputRef = useRef<HTMLInputElement>(null);
  const outputRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    api.getClickhouseNodes().then((res) => {
      setStorages(res.map((n) => n.storage_name));
    });
  }, [api]);

  useEffect(() => {
    if (outputRef.current) {
      outputRef.current.scrollTop = outputRef.current.scrollHeight;
    }
  }, [state.history]);

  const addHistoryEntry = useCallback((entry: ShellHistoryEntry) => {
    setState((prev) => ({
      ...prev,
      history: [...prev.history, entry],
    }));
  }, []);

  const executeCommand = useCallback(
    async (input: string) => {
      if (!input.trim()) return;

      const parsed = parseCommand(input);

      // Add command to history display
      addHistoryEntry({
        type: "command",
        content: input,
        timestamp: Date.now(),
      });

      // Add to command history for navigation
      setState((prev) => {
        const newCmdHistory = [...prev.commandHistory, input];
        saveCommandHistory(newCmdHistory);
        return {
          ...prev,
          commandHistory: newCmdHistory,
          historyIndex: -1,
        };
      });

      switch (parsed.type) {
        case "help":
          addHistoryEntry({ type: "help", timestamp: Date.now() });
          break;

        case "clear":
          setState((prev) => ({ ...prev, history: [] }));
          break;

        case "show_storages":
          addHistoryEntry({
            type: "storages",
            content: storages,
            timestamp: Date.now(),
          });
          break;

        case "use":
          if (storages.includes(parsed.storage)) {
            setState((prev) => ({
              ...prev,
              currentStorage: parsed.storage,
            }));
            addHistoryEntry({
              type: "info",
              content: `Storage set to: ${parsed.storage}`,
              timestamp: Date.now(),
            });
          } else {
            addHistoryEntry({
              type: "error",
              content: `Unknown storage: ${parsed.storage}. Use SHOW STORAGES to see available options.`,
              timestamp: Date.now(),
            });
          }
          break;

        case "profile":
          setState((prev) => ({
            ...prev,
            profileEnabled: parsed.enabled,
          }));
          addHistoryEntry({
            type: "info",
            content: `Profile events ${parsed.enabled ? "enabled" : "disabled"}`,
            timestamp: Date.now(),
          });
          break;

        case "trace_mode":
          setState((prev) => ({
            ...prev,
            traceFormatted: parsed.formatted,
          }));
          addHistoryEntry({
            type: "info",
            content: `Trace output mode: ${parsed.formatted ? "formatted" : "raw"}`,
            timestamp: Date.now(),
          });
          break;

        case "sql":
          if (!state.currentStorage) {
            addHistoryEntry({
              type: "error",
              content:
                "No storage selected. Use USE <storage> to select a storage first.",
              timestamp: Date.now(),
            });
            return;
          }

          setState((prev) => ({ ...prev, isExecuting: true }));

          try {
            const request: TracingRequest = {
              sql: parsed.query,
              storage: state.currentStorage,
              gather_profile_events: state.profileEnabled,
            };

            const result = await api.executeTracingQuery(request);
            addHistoryEntry({
              type: "result",
              content: result,
              timestamp: Date.now(),
            });
          } catch (err) {
            addHistoryEntry({
              type: "error",
              content:
                err instanceof Error ? err.message : "Query execution failed",
              timestamp: Date.now(),
            });
          } finally {
            setState((prev) => ({ ...prev, isExecuting: false }));
          }
          break;
      }
    },
    [api, state.currentStorage, state.profileEnabled, storages, addHistoryEntry]
  );

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLInputElement>) => {
      // Execute on Enter or Cmd+Enter
      if (e.key === "Enter") {
        e.preventDefault();
        if (!state.isExecuting) {
          executeCommand(inputValue);
          setInputValue("");
        }
        return;
      }

      // Clear screen with Ctrl+L
      if (e.key === "l" && e.ctrlKey) {
        e.preventDefault();
        setState((prev) => ({ ...prev, history: [] }));
        return;
      }

      // Navigate history with Up/Down arrows
      if (e.key === "ArrowUp") {
        e.preventDefault();
        const cmdHistory = state.commandHistory;
        if (cmdHistory.length === 0) return;

        const newIndex =
          state.historyIndex === -1
            ? cmdHistory.length - 1
            : Math.max(0, state.historyIndex - 1);

        setState((prev) => ({ ...prev, historyIndex: newIndex }));
        setInputValue(cmdHistory[newIndex]);
        return;
      }

      if (e.key === "ArrowDown") {
        e.preventDefault();
        const cmdHistory = state.commandHistory;
        if (state.historyIndex === -1) return;

        const newIndex = state.historyIndex + 1;
        if (newIndex >= cmdHistory.length) {
          setState((prev) => ({ ...prev, historyIndex: -1 }));
          setInputValue("");
        } else {
          setState((prev) => ({ ...prev, historyIndex: newIndex }));
          setInputValue(cmdHistory[newIndex]);
        }
        return;
      }
    },
    [state.commandHistory, state.historyIndex, state.isExecuting, inputValue, executeCommand]
  );

  const focusInput = () => {
    inputRef.current?.focus();
  };

  return (
    <div className={classes.shellContainer} onClick={focusInput}>
      <div ref={outputRef} className={classes.outputArea}>
        <ShellOutput
          entries={state.history}
          traceFormatted={state.traceFormatted}
        />
        {state.isExecuting && (
          <div className={classes.executingIndicator}>Executing query...</div>
        )}
      </div>
      <div className={classes.inputArea}>
        <span className={classes.prompt}>{">"}</span>
        <input
          ref={inputRef}
          type="text"
          className={classes.input}
          value={inputValue}
          onChange={(e) => setInputValue(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder={
            state.currentStorage
              ? `Enter SQL or command (storage: ${state.currentStorage})`
              : "Enter USE <storage> to select a storage, or HELP for commands"
          }
          disabled={state.isExecuting}
          autoFocus
        />
      </div>
      <div className={classes.statusBar}>
        <div className={classes.statusItem}>
          <span>Storage:</span>
          <span
            className={
              state.currentStorage ? classes.statusActive : classes.statusInactive
            }
          >
            {state.currentStorage || "none"}
          </span>
        </div>
        <div style={{ display: "flex", gap: "16px" }}>
          <div className={classes.statusItem}>
            <span>PROFILE:</span>
            <span
              className={
                state.profileEnabled
                  ? classes.statusActive
                  : classes.statusInactive
              }
            >
              {state.profileEnabled ? "ON" : "OFF"}
            </span>
          </div>
          <div className={classes.statusItem}>
            <span>TRACE:</span>
            <span className={classes.statusActive}>
              {state.traceFormatted ? "FORMATTED" : "RAW"}
            </span>
          </div>
          <div style={{ color: "#666666" }}>Execute: Enter | History: Up/Down</div>
        </div>
      </div>
    </div>
  );
}

export default SQLShell;
