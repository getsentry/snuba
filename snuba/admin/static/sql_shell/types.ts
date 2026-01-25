import { TracingResult, TracingSummary } from "SnubaAdmin/tracing/types";
import { QueryResult } from "SnubaAdmin/clickhouse_queries/types";

export type ShellMode = "tracing" | "system";

export type ParsedCommand =
  | { type: "use"; storage: string }
  | { type: "host"; host: string; port: number }
  | { type: "show_storages" }
  | { type: "show_hosts" }
  | { type: "profile"; enabled: boolean }
  | { type: "trace_mode"; formatted: boolean }
  | { type: "sudo"; enabled: boolean }
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
  | { type: "system_result"; content: QueryResult; timestamp: number }
  | { type: "error"; content: string; timestamp: number }
  | { type: "info"; content: string; timestamp: number }
  | { type: "storages"; content: string[]; timestamp: number }
  | { type: "hosts"; content: string[]; timestamp: number }
  | { type: "help"; mode: ShellMode; timestamp: number };

export interface ShellState {
  currentStorage: string | null;
  currentHost: string | null;
  currentPort: number | null;
  profileEnabled: boolean;
  traceFormatted: boolean;
  sudoEnabled: boolean;
  history: ShellHistoryEntry[];
  commandHistory: string[];
  historyIndex: number;
  isExecuting: boolean;
}
