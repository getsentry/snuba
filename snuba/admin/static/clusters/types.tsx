type NodeVersionData = {
  host: string;
  port: number;
  version: string | null;
  error: string | null;
};

type ClusterData = {
  host: string;
  port: number;
  database: string;
  secure: boolean;
  single_node: boolean;
  cluster_name: string | null;
  distributed_cluster_name: string | null;
  storage_sets: string[];
  query_node_versions: NodeVersionData[];
  query_node_error: string | null;
  storage_node_versions: NodeVersionData[];
  storage_node_error: string | null;
  tables: string[];
  error: string | null;
};

export { ClusterData, NodeVersionData };
