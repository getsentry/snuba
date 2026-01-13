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

type CopyTableHost = {
  storage: string;
  host: string;
  port: number;
};

type CopyTableHostsState = {
  sourceHost: Partial<CopyTableHost>
  targetHost: Partial<CopyTableHost>
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

export { ClickhouseNodeData, CopyTableHostsState, CopyTableRequest, CopyTableResult };
