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
  sourceHost: Partial<CopyTableHost>;
}

type CopyTableRequest = {
  storage: string;
  source_host: string;
  source_port: number;
  dry_run: boolean;
  target_host?: string;
};

type CopyTableResult = {
  source_host: string
  tables: string
  dry_run: boolean
  cluster_name: string
  incomplete_hosts?: Record<string, string>
  verified?: number
}

export { ClickhouseNodeData, CopyTableHostsState, CopyTableRequest, CopyTableResult };
