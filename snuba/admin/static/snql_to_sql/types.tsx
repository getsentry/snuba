type SnubaDatasetName = string;
type SnQLQueryState = Partial<SnQLRequest>;

type SnQLRequest = {
  dataset: string;
  query: string;
};

type SnQLResult = {
  input_query?: string;
  sql: string;
  explain?: object;
};

export { SnubaDatasetName, SnQLRequest, SnQLResult, SnQLQueryState };
