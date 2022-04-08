type SnubaDatasetName = string;
type SnQLQueryState = Partial<SnQLRequest>;

type SnQLRequest = {
  dataset: string;
  query: string;
};

type SnQLResult = {
  input_query?: string;
  sql: string;
};

export { SnubaDatasetName, SnQLRequest, SnQLResult, SnQLQueryState };
