type QueryResultColumnMetadata = [string];
type QueryResultRow = [string];

type QuerylogRequest = {
  sql: string;
};

type QuerylogResult = {
  input_query: string;
  timestamp: number;
  column_names: QueryResultColumnMetadata;
  rows: [QueryResultRow];
  error?: string;
};

type PredefinedQuery = {
  name: string;
  sql: string;
  description: string;
};

export { QuerylogRequest, QuerylogResult, PredefinedQuery };
