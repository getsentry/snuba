import React, { useState, useEffect, useRef, useCallback } from "react";
import Client from "SnubaAdmin/api_client";
import { useShellStyles } from "SnubaAdmin/sql_shell/styles";
import { ShellOutput } from "SnubaAdmin/sql_shell/shell_output";
import {
  ParsedCommand,
  ShellState,
  ShellHistoryEntry,
  ShellMode,
} from "SnubaAdmin/sql_shell/types";
import { TracingRequest } from "SnubaAdmin/tracing/types";
import { QueryRequest, ClickhouseNodeData } from "SnubaAdmin/clickhouse_queries/types";

function getCommandHistoryKey(mode: ShellMode): string {
  return mode === "tracing" ? "sql_shell_command_history" : "system_shell_command_history";
}

function parseCommand(input: string, mode: ShellMode): ParsedCommand {
  const trimmed = input.trim();

  // USE storage_name
  if (/^USE\s+/i.test(trimmed)) {
    const storage = trimmed.replace(/^USE\s+/i, "").trim();
    return { type: "use", storage };
  }

  // HOST host:port (system mode only)
  if (/^HOST\s+/i.test(trimmed)) {
    const hostStr = trimmed.replace(/^HOST\s+/i, "").trim();
    const match = hostStr.match(/^([^:]+):(\d+)$/);
    if (match) {
      return { type: "host", host: match[1], port: parseInt(match[2], 10) };
    }
    // If no port specified, default to 9000
    return { type: "host", host: hostStr, port: 9000 };
  }

  // SHOW STORAGES
  if (/^SHOW\s+STORAGES$/i.test(trimmed)) {
    return { type: "show_storages" };
  }

  // SHOW HOSTS (system mode only)
  if (/^SHOW\s+HOSTS$/i.test(trimmed)) {
    return { type: "show_hosts" };
  }

  // PROFILE ON/OFF (tracing mode only)
  if (/^PROFILE\s+(ON|OFF)$/i.test(trimmed)) {
    const enabled = /ON$/i.test(trimmed);
    return { type: "profile", enabled };
  }

  // TRACE RAW/FORMATTED (tracing mode only)
  if (/^TRACE\s+(RAW|FORMATTED)$/i.test(trimmed)) {
    const formatted = /FORMATTED$/i.test(trimmed);
    return { type: "trace_mode", formatted };
  }

  // SUDO ON/OFF (system mode only)
  if (/^SUDO\s+(ON|OFF)$/i.test(trimmed)) {
    const enabled = /ON$/i.test(trimmed);
    return { type: "sudo", enabled };
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

function loadCommandHistory(mode: ShellMode): string[] {
  try {
    const saved = localStorage.getItem(getCommandHistoryKey(mode));
    return saved ? JSON.parse(saved) : [];
  } catch {
    return [];
  }
}

function saveCommandHistory(mode: ShellMode, history: string[]) {
  try {
    localStorage.setItem(
      getCommandHistoryKey(mode),
      JSON.stringify(history.slice(-100))
    );
  } catch {
    // Ignore storage errors
  }
}

interface SQLShellProps {
  api: Client;
  mode: ShellMode;
}

function SQLShell({ api, mode }: SQLShellProps) {
  const { classes } = useShellStyles();
  const [state, setState] = useState<ShellState>({
    currentStorage: null,
    currentHost: null,
    currentPort: null,
    profileEnabled: true,
    traceFormatted: true,
    sudoEnabled: false,
    history: [],
    commandHistory: loadCommandHistory(mode),
    historyIndex: -1,
    isExecuting: false,
  });
  const [inputValue, setInputValue] = useState("");
  const [nodeData, setNodeData] = useState<ClickhouseNodeData[]>([]);
  const [storages, setStorages] = useState<string[]>([]);
  const [suggestions, setSuggestions] = useState<string[]>([]);
  const inputRef = useRef<HTMLInputElement>(null);
  const outputRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    api.getClickhouseNodes().then((res) => {
      setNodeData(res);
      setStorages(res.map((n) => n.storage_name));
    });
  }, [api]);

  useEffect(() => {
    if (outputRef.current) {
      outputRef.current.scrollTop = outputRef.current.scrollHeight;
    }
  }, [state.history]);

  const getHostsForStorage = useCallback((storageName: string): string[] => {
    const nodeInfo = nodeData.find((n) => n.storage_name === storageName);
    if (!nodeInfo) return [];

    const hosts: string[] = [];
    nodeInfo.local_nodes.forEach((node) => {
      hosts.push(`${node.host}:${node.port}`);
    });
    nodeInfo.dist_nodes.forEach((node) => {
      const hostStr = `${node.host}:${node.port}`;
      if (!hosts.includes(hostStr)) {
        hosts.push(`${hostStr} (distributed)`);
      }
    });
    if (nodeInfo.query_node) {
      hosts.push(`${nodeInfo.query_node.host}:${nodeInfo.query_node.port} (query node)`);
    }
    return hosts;
  }, [nodeData]);

  const addHistoryEntry = useCallback((entry: ShellHistoryEntry) => {
    setState((prev) => ({
      ...prev,
      history: [...prev.history, entry],
    }));
  }, []);

  const executeCommand = useCallback(
    async (input: string) => {
      if (!input.trim()) return;

      const parsed = parseCommand(input, mode);

      // Add command to history display
      addHistoryEntry({
        type: "command",
        content: input,
        timestamp: Date.now(),
      });

      // Add to command history for navigation
      setState((prev) => {
        const newCmdHistory = [...prev.commandHistory, input];
        saveCommandHistory(mode, newCmdHistory);
        return {
          ...prev,
          commandHistory: newCmdHistory,
          historyIndex: -1,
        };
      });

      switch (parsed.type) {
        case "help":
          addHistoryEntry({ type: "help", mode, timestamp: Date.now() });
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

        case "show_hosts":
          if (mode !== "system") {
            addHistoryEntry({
              type: "error",
              content: "SHOW HOSTS is only available in system mode.",
              timestamp: Date.now(),
            });
            break;
          }
          if (!state.currentStorage) {
            addHistoryEntry({
              type: "error",
              content: "No storage selected. Use USE <storage> first.",
              timestamp: Date.now(),
            });
            break;
          }
          addHistoryEntry({
            type: "hosts",
            content: getHostsForStorage(state.currentStorage),
            timestamp: Date.now(),
          });
          break;

        case "use":
          if (storages.includes(parsed.storage)) {
            setState((prev) => ({
              ...prev,
              currentStorage: parsed.storage,
              currentHost: null,
              currentPort: null,
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

        case "host":
          if (mode !== "system") {
            addHistoryEntry({
              type: "error",
              content: "HOST command is only available in system mode.",
              timestamp: Date.now(),
            });
            break;
          }
          setState((prev) => ({
            ...prev,
            currentHost: parsed.host,
            currentPort: parsed.port,
          }));
          addHistoryEntry({
            type: "info",
            content: `Host set to: ${parsed.host}:${parsed.port}`,
            timestamp: Date.now(),
          });
          break;

        case "profile":
          if (mode !== "tracing") {
            addHistoryEntry({
              type: "error",
              content: "PROFILE command is only available in tracing mode.",
              timestamp: Date.now(),
            });
            break;
          }
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
          if (mode !== "tracing") {
            addHistoryEntry({
              type: "error",
              content: "TRACE command is only available in tracing mode.",
              timestamp: Date.now(),
            });
            break;
          }
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

        case "sudo":
          if (mode !== "system") {
            addHistoryEntry({
              type: "error",
              content: "SUDO command is only available in system mode.",
              timestamp: Date.now(),
            });
            break;
          }
          setState((prev) => ({
            ...prev,
            sudoEnabled: parsed.enabled,
          }));
          addHistoryEntry({
            type: "info",
            content: `Sudo mode ${parsed.enabled ? "enabled" : "disabled"}`,
            timestamp: Date.now(),
          });
          break;

        case "sql":
          if (!state.currentStorage) {
            addHistoryEntry({
              type: "error",
              content: "No storage selected. Use USE <storage> to select a storage first.",
              timestamp: Date.now(),
            });
            return;
          }

          if (mode === "system" && (!state.currentHost || !state.currentPort)) {
            addHistoryEntry({
              type: "error",
              content: "No host selected. Use HOST <host:port> to select a host first.",
              timestamp: Date.now(),
            });
            return;
          }

          setState((prev) => ({ ...prev, isExecuting: true }));

          try {
            if (mode === "tracing") {
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
            } else {
              const request: QueryRequest = {
                sql: parsed.query,
                storage: state.currentStorage,
                host: state.currentHost!,
                port: state.currentPort!,
                sudo: state.sudoEnabled,
              };

              const result = await api.executeSystemQuery(request);
              addHistoryEntry({
                type: "system_result",
                content: result,
                timestamp: Date.now(),
              });
            }
          } catch (err: any) {
            let errorMessage = "Query execution failed";
            if (err instanceof Error) {
              errorMessage = err.message;
            } else if (typeof err === "object" && err !== null) {
              // API returns error as JSON object
              errorMessage = err.error || err.message || JSON.stringify(err);
            } else if (typeof err === "string") {
              errorMessage = err;
            }
            addHistoryEntry({
              type: "error",
              content: errorMessage,
              timestamp: Date.now(),
            });
          } finally {
            setState((prev) => ({ ...prev, isExecuting: false }));
          }
          break;
      }
    },
    [api, mode, state.currentStorage, state.currentHost, state.currentPort, state.profileEnabled, state.sudoEnabled, storages, getHostsForStorage, addHistoryEntry]
  );

  const handleTabComplete = useCallback(() => {
    const trimmed = inputValue.trimStart();

    // Check for USE <partial> pattern
    const useMatch = trimmed.match(/^USE\s+(.*)$/i);
    if (useMatch) {
      const partial = useMatch[1].toLowerCase();
      const matches = storages.filter((s) =>
        s.toLowerCase().startsWith(partial)
      );

      if (matches.length === 1) {
        setInputValue(`USE ${matches[0]}`);
        setSuggestions([]);
      } else if (matches.length > 1) {
        // Find common prefix
        const commonPrefix = matches.reduce((prefix, storage) => {
          while (!storage.toLowerCase().startsWith(prefix.toLowerCase())) {
            prefix = prefix.slice(0, -1);
          }
          return storage.slice(0, prefix.length);
        }, matches[0]);

        if (commonPrefix.length > partial.length) {
          setInputValue(`USE ${commonPrefix}`);
        }
        // Show available options above input
        setSuggestions(matches);
      } else {
        setSuggestions([]);
      }
      return true;
    }

    // Check for HOST <partial> pattern (system mode only)
    if (mode === "system" && state.currentStorage) {
      const hostMatch = trimmed.match(/^HOST\s+(.*)$/i);
      if (hostMatch) {
        const partial = hostMatch[1].toLowerCase();
        const hosts = getHostsForStorage(state.currentStorage).map((h) =>
          h.replace(/ \(.*\)$/, "")
        );
        const matches = hosts.filter((h) => h.toLowerCase().startsWith(partial));

        if (matches.length === 1) {
          setInputValue(`HOST ${matches[0]}`);
          setSuggestions([]);
        } else if (matches.length > 1) {
          const commonPrefix = matches.reduce((prefix, host) => {
            while (!host.toLowerCase().startsWith(prefix.toLowerCase())) {
              prefix = prefix.slice(0, -1);
            }
            return host.slice(0, prefix.length);
          }, matches[0]);

          if (commonPrefix.length > partial.length) {
            setInputValue(`HOST ${commonPrefix}`);
          }
          // Show available options above input
          setSuggestions(matches);
        } else {
          setSuggestions([]);
        }
        return true;
      }
    }

    setSuggestions([]);
    return false;
  }, [inputValue, storages, mode, state.currentStorage, getHostsForStorage]);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLInputElement>) => {
      // Tab completion
      if (e.key === "Tab") {
        e.preventDefault();
        handleTabComplete();
        return;
      }

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
    [state.commandHistory, state.historyIndex, state.isExecuting, inputValue, executeCommand, handleTabComplete]
  );

  const focusInput = () => {
    inputRef.current?.focus();
  };

  const getPlaceholder = () => {
    if (mode === "tracing") {
      return state.currentStorage
        ? `Enter SQL or command (storage: ${state.currentStorage})`
        : "Enter USE <storage> to select a storage, or HELP for commands";
    } else {
      if (!state.currentStorage) {
        return "Enter USE <storage> to select a storage, or HELP for commands";
      }
      if (!state.currentHost) {
        return `Enter HOST <host:port> to select a host (storage: ${state.currentStorage})`;
      }
      return `Enter SQL or command (${state.currentStorage} @ ${state.currentHost}:${state.currentPort})`;
    }
  };

  return (
    <div className={state.sudoEnabled ? classes.shellContainerSudo : classes.shellContainer} onClick={focusInput}>
      <div ref={outputRef} className={classes.outputArea}>
        <ShellOutput
          entries={state.history}
          traceFormatted={state.traceFormatted}
          mode={mode}
        />
        {state.isExecuting && (
          <div className={classes.executingIndicator}>Executing query...</div>
        )}
      </div>
      {suggestions.length > 0 && (
        <div className={classes.suggestionsBar}>
          {suggestions.map((s, i) => (
            <span
              key={i}
              className={classes.suggestionItem}
              onClick={() => {
                const trimmed = inputValue.trimStart();
                if (/^USE\s/i.test(trimmed)) {
                  setInputValue(`USE ${s}`);
                } else if (/^HOST\s/i.test(trimmed)) {
                  setInputValue(`HOST ${s}`);
                }
                setSuggestions([]);
                inputRef.current?.focus();
              }}
            >
              {s}
            </span>
          ))}
        </div>
      )}
      <div className={classes.inputArea}>
        <span className={classes.prompt}>{">"}</span>
        <input
          ref={inputRef}
          type="text"
          className={classes.input}
          value={inputValue}
          onChange={(e) => {
            setInputValue(e.target.value);
            setSuggestions([]);
          }}
          onKeyDown={handleKeyDown}
          placeholder={getPlaceholder()}
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
          {mode === "system" && (
            <>
              <div className={classes.statusItem}>
                <span>Host:</span>
                <span
                  className={
                    state.currentHost ? classes.statusActive : classes.statusInactive
                  }
                >
                  {state.currentHost ? `${state.currentHost}:${state.currentPort}` : "none"}
                </span>
              </div>
              <div className={classes.statusItem}>
                <span>SUDO:</span>
                <span
                  className={
                    state.sudoEnabled ? classes.statusWarn : classes.statusInactive
                  }
                >
                  {state.sudoEnabled ? "ON" : "OFF"}
                </span>
              </div>
            </>
          )}
          {mode === "tracing" && (
            <>
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
            </>
          )}
          <div style={{ color: "#6e7681" }}>Tab: Autocomplete | Enter: Execute | ↑↓: History</div>
        </div>
      </div>
    </div>
  );
}

export default SQLShell;
