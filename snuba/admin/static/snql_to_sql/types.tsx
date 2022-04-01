type SnubaDatasetName = string;
type SnQLQueryState = Partial<SnQLRequest>;

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
  sql: string;
  stats?: SnQLQueryStats;
};

export { SnubaDatasetName, SnQLRequest, SnQLResult, SnQLQueryState };
