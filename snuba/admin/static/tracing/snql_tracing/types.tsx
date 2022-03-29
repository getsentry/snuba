type SnubaDatasetName = string;

type ClickhouseSQL = string;

type SnQLRequest = {
  dataset: string;
  query: string;
};

type SnQLResult = {
  input_query?: string;
  sql: string;
};
