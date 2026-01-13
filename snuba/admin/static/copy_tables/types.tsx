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

type QueryState = Partial<QueryRequest>;

type ShowTablesQueryState = {
  sourceHost: QueryState
  targetHost: QueryState
}

type CopyTableRequest = {
  storage: string;
  source_host: string;
  source_port: number;
  target_host: string;
  target_port: number;
  dry_run: boolean;
};

type CopyTableResult = {
  source_host: string
  target_host: string
  tables: string
  dry_run: boolean
  cluster_name: string
}

export { CopyTableRequest, ClickhouseNodeData, CopyTableResult, ShowTablesQueryState, };
