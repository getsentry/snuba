export type TracingRequest = {
  sql: string;
  storage: string;
  gather_profile_events: boolean;
};

type TracingResult = {
  input_query?: string;
  timestamp: number;
  trace_output?: string;
  summarized_trace_output?: TracingSummary;
  cols?: Array<Array<string>>;
  num_rows_result?: number;
  result?: Array<Array<any>>,
  profile_events_results?: Map<Map<string, string>, Object>;
  profile_events_meta?: Array<Object>;
  profile_events_profile?: {};
  error?: string;
};

type TracingSummary = {
  query_summaries: { [host: string]: QuerySummary };
};

type QuerySummary = {
  node_name: string;
  is_distributed: boolean;
  query_id: string;
  execute_summaries: Array<ExecuteSummary>;
  select_summaries: Array<SelectSummary>;
  index_summaries: Array<IndexSummary>;
  stream_summaries: Array<StreamSummary>;
  aggregation_summaries: Array<AggregationSummary>;
  sorting_summaries: Array<SortingSummary>;
};

type IndexSummary = {
  table_name: string;
  index_name: string;
  dropped_granules: number;
  total_granules: number;
};

type SelectSummary = {
  table_name: string;
  parts_selected_by_partition_key: number;
  total_parts: number;
  parts_selected_by_primary_key: number;
  marks_selected_by_primary_key: number;
  total_marks: number;
  marks_to_read_from_ranges: number;
};

type StreamSummary = {
  table_name: string;
  approximate_rows: number;
  streams: number;
};

type AggregationSummary = {
  transform: string;
  before_row_count: number;
  after_row_count: number;
  memory_size: string;
  seconds: number;
  rows_per_second: number;
  bytes_per_second: string;
};

type SortingSummary = {
  transform: string;
  sorted_blocks: number;
  rows: number;
  seconds: number;
  rows_per_second: number;
  bytes_per_second: string;
};

type ExecuteSummary = {
  rows_read: number;
  memory_size: string;
  seconds: number;
  rows_per_second: number;
  bytes_per_second: string;
};

type LogLine = {
  host: string;
  pid: string;
  query_id: string;
  log_level: string;
  component: string;
  message: string;
};

type PredefinedQuery = {
  name: string;
  sql: string;
  description: string;
};

export {
  TracingRequest,
  TracingResult,
  LogLine,
  PredefinedQuery,
  TracingSummary,
  QuerySummary,
  SelectSummary,
  IndexSummary,
  ExecuteSummary,
  AggregationSummary,
  StreamSummary,
  SortingSummary,
};
