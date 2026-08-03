type QueryResultColumnMetadata = [string];
type QueryResultRow = [string];

type OutcomesQueryRequest = {
  sql: string;
};

type OutcomesQueryResult = {
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

export { OutcomesQueryRequest, OutcomesQueryResult, PredefinedQuery };
