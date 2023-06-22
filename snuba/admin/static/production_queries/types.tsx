type SnQLRequest = {
  dataset: string;
  query: string;
};

type QueryResult = {
  input_query?: string;
  columns: [string];
  rows: [[string]];
};

type QueryResultColumnMeta = {
  name: string;
  type: string;
};

export { SnQLRequest, QueryResult, QueryResultColumnMeta };
