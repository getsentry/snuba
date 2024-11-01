type SnQLRequest = {
  dataset: string;
  query: string;
};

type QueryResult = {
  input_query?: string;
  columns: [string];
  rows: [[string]];
  duration_ms: number;
  quota_allowance?: QuotaAllowance;
};

type QuotaAllowancePolicy = {
  can_run: boolean;
  max_threads: number;
  explanation: {
    reason?: string;
    overrides?: Record<string, unknown>;
    storage_key?: string;
    policy?: string;
    referrer?: string;
  };
};

type QuotaAllowance = {
  [policy: string]: QuotaAllowancePolicy;
};

type QueryResultColumnMeta = {
  name: string;
  type: string;
};

export { SnQLRequest, QueryResult, QueryResultColumnMeta };
