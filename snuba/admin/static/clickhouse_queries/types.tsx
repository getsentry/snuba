type ClickhouseNode = {
  host: string;
  port: number;
};

type ClickhouseNodeData = {
  storage_name: string;
  local_table_name: string;
  local_nodes: ClickhouseNode[];
  dist_nodes: ClickhouseNode[];
  query_node: ClickhouseNode;
};

type QueryRequest = {
  storage: string;
  host: string;
  port: number;
  sql: string;
  sudo: boolean;
  clusterless?: boolean;
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

type PredefinedQuery = {
  name: string;
  sql: string;
  description: string;
};

export { ClickhouseNodeData, QueryRequest, QueryResult, PredefinedQuery };
