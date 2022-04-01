type TracingRequest = {
  storage: string;
  sql: string;
};

type TracingResult = {
  input_query?: string;
  timestamp: number;
  trace_output?: string;
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
