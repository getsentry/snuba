import { TracingResult } from "../types";

type SnubaDatasetName = string;

type SnQLRequest = {
  dataset: string;
  query: string;
};

type SnQLQueryStats = {
  clickhouse_table: string;
  storage: string;
};

type SnQLResult = {
  input_query?: string;
  tracing_result: TracingResult;
  sql: string;
  stats: SnQLQueryStats;
};

export { SnubaDatasetName, SnQLRequest, SnQLResult };
