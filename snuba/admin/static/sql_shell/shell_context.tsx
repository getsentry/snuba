import React, { createContext, useContext, useState, useCallback, ReactNode } from "react";
import { ShellState, ShellHistoryEntry, ShellMode } from "SnubaAdmin/sql_shell/types";

const MAX_HISTORY_ENTRIES = 500;

function getCommandHistoryKey(mode: ShellMode): string {
  return mode === "tracing" ? "sql_shell_command_history" : "system_shell_command_history";
}

function loadCommandHistory(mode: ShellMode): string[] {
  try {
    const saved = sessionStorage.getItem(getCommandHistoryKey(mode));
    return saved ? JSON.parse(saved) : [];
  } catch {
    return [];
  }
}

function saveCommandHistory(mode: ShellMode, history: string[]) {
  try {
    sessionStorage.setItem(
      getCommandHistoryKey(mode),
      JSON.stringify(history.slice(-100))
    );
  } catch {
    // Ignore storage errors
  }
}

function createInitialState(mode: ShellMode): ShellState {
  return {
    currentStorage: null,
    currentHost: null,
    currentPort: null,
    profileEnabled: true,
    traceFormatted: true,
    sudoEnabled: false,
    outputFormat: "table",
    history: [],
    commandHistory: loadCommandHistory(mode),
    historyIndex: -1,
    isExecuting: false,
  };
}

interface ShellStateContextValue {
  tracingState: ShellState;
  systemState: ShellState;
  setTracingState: React.Dispatch<React.SetStateAction<ShellState>>;
  setSystemState: React.Dispatch<React.SetStateAction<ShellState>>;
  addHistoryEntry: (mode: ShellMode, entry: ShellHistoryEntry) => void;
  addCommandToHistory: (mode: ShellMode, command: string) => void;
  clearHistory: (mode: ShellMode) => void;
}

const ShellStateContext = createContext<ShellStateContextValue | null>(null);

interface ShellStateProviderProps {
  children: ReactNode;
}

export function ShellStateProvider({ children }: ShellStateProviderProps) {
  const [tracingState, setTracingState] = useState<ShellState>(() => createInitialState("tracing"));
  const [systemState, setSystemState] = useState<ShellState>(() => createInitialState("system"));

  const addHistoryEntry = useCallback((mode: ShellMode, entry: ShellHistoryEntry) => {
    const setState = mode === "tracing" ? setTracingState : setSystemState;
    setState((prev) => {
      const newHistory = [...prev.history, entry];
      return {
        ...prev,
        history: newHistory.length > MAX_HISTORY_ENTRIES
          ? newHistory.slice(-MAX_HISTORY_ENTRIES)
          : newHistory,
      };
    });
  }, []);

  const addCommandToHistory = useCallback((mode: ShellMode, command: string) => {
    const setState = mode === "tracing" ? setTracingState : setSystemState;
    setState((prev) => {
      const newCmdHistory = [...prev.commandHistory, command];
      saveCommandHistory(mode, newCmdHistory);
      return {
        ...prev,
        commandHistory: newCmdHistory,
        historyIndex: -1,
      };
    });
  }, []);

  const clearHistory = useCallback((mode: ShellMode) => {
    const setState = mode === "tracing" ? setTracingState : setSystemState;
    setState((prev) => ({ ...prev, history: [] }));
  }, []);

  return (
    <ShellStateContext.Provider
      value={{
        tracingState,
        systemState,
        setTracingState,
        setSystemState,
        addHistoryEntry,
        addCommandToHistory,
        clearHistory,
      }}
    >
      {children}
    </ShellStateContext.Provider>
  );
}

export function useShellState(mode: ShellMode) {
  const context = useContext(ShellStateContext);
  if (!context) {
    throw new Error("useShellState must be used within a ShellStateProvider");
  }

  const state = mode === "tracing" ? context.tracingState : context.systemState;
  const setState = mode === "tracing" ? context.setTracingState : context.setSystemState;

  return {
    state,
    setState,
    addHistoryEntry: (entry: ShellHistoryEntry) => context.addHistoryEntry(mode, entry),
    addCommandToHistory: (command: string) => context.addCommandToHistory(mode, command),
    clearHistory: () => context.clearHistory(mode),
  };
}
