import { TracingResult, TracingSummary } from "SnubaAdmin/tracing/types";

export type ParsedCommand =
  | { type: "use"; storage: string }
  | { type: "show_storages" }
  | { type: "profile"; enabled: boolean }
  | { type: "trace_mode"; formatted: boolean }
  | { type: "help" }
  | { type: "clear" }
  | { type: "sql"; query: string };

export type ProfileEventValue = {
  column_names: string[];
  rows: string[];
};

export type ProfileEvent = {
  [host_name: string]: ProfileEventValue;
};

export type ShellHistoryEntry =
  | { type: "command"; content: string; timestamp: number }
  | { type: "result"; content: TracingResult; timestamp: number }
  | { type: "error"; content: string; timestamp: number }
  | { type: "info"; content: string; timestamp: number }
  | { type: "storages"; content: string[]; timestamp: number }
  | { type: "help"; timestamp: number };

export interface ShellState {
  currentStorage: string | null;
  profileEnabled: boolean;
  traceFormatted: boolean;
  history: ShellHistoryEntry[];
  commandHistory: string[];
  historyIndex: number;
  isExecuting: boolean;
}
