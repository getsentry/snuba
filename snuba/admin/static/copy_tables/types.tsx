import { QueryResult } from "SnubaAdmin/clickhouse_queries/types";

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

type CopyTableRequest = {
  storage: string;
  source_host: string;
  source_port: number;
  target_host: string;
  target_port: number;
  table_name: string;
  dry_run: boolean;
  on_cluster: boolean;
};


type QueryState = Partial<QueryRequest>;

type ShowTablesQueryState = {
  sourceHost: QueryState
  targetHost: QueryState
}
type ShowTablesQueryResult = {
  sourceHost: Partial<QueryResult>
  targetHost: Partial<QueryResult>
}

type CopyTable = {
  table: string
  onCluster: boolean
  previewStatement: string
}

type CopyTableResult = {
  source_host: string
  target_host: string
  table: string
  create_statement: string
  dry_run: boolean
  on_cluster: boolean
}


export { CopyTableRequest, ClickhouseNodeData, QueryRequest, CopyTable, CopyTableResult, ShowTablesQueryState, ShowTablesQueryResult };
