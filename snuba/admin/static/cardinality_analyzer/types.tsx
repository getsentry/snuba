type QueryResultColumnMetadata = [string];
type QueryResultRow = [string];

type CardinalityQueryRequest= {
  sql: string;
};

type CardinalityQueryResult = {
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

export { CardinalityQueryRequest, CardinalityQueryResult, PredefinedQuery };
