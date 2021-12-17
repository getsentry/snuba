type ClickhouseNode = {
  host: string;
  port: number;
};

type ClickhouseNodeData = {
  storage_name: string;
  local_table_name: string;
  local_nodes: ClickhouseNode[];
};

type QueryRequest = {
  storage: string;
  host: string;
  port: number;
  query_name: string;
  sql?: string;
};

type QueryResultColumnMetadata = [string];
type QueryResultRow = [string];

type QueryResult = {
  input_query?: string;
  timestamp: number;
  trace_output?: string;
  error?: string;
  column_names: QueryResultColumnMetadata;
  rows: [QueryResultRow];
};

export { ClickhouseNodeData, QueryRequest, QueryResult };
