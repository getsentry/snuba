type SnubaDatasetName = string;

type ClickhouseSQL = string;

type SnQLRequest = {
  dataset: string;
  query: string;
  debug: boolean;
  dry_run: boolean;
};

type SnQLResult = {
  input_query?: string;
  sql: string;
};
