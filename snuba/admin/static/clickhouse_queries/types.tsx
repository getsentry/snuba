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
  sql: string;
};

type QueryResultColumnMetadata = [string];
type QueryResultRow = [string];

type QueryResult = {
  input_query: string;
  timestamp: number;
  column_names: QueryResultColumnMetadata;
  rows: [QueryResultRow];
  trace_output?: string;
  error?: string;
};

export { ClickhouseNodeData, QueryRequest, QueryResult };
