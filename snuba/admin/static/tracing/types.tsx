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

export { TracingRequest, TracingResult };
