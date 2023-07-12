type TracingRequest = {
  storage: string;
  sql: string;
};

type TracingResult = {
  input_query?: string;
  timestamp: number;
  trace_output?: string;
  formatted_trace_output?: string;
  cols?: Array<Array<string>>;
  num_rows_result?: number;
  error?: string;
};

type LogLine = {
  host: string;
  pid: string;
  query_id: string;
  log_level: string;
  component: string;
  message: string;
};

export { TracingRequest, TracingResult, LogLine };
