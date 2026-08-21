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
  // These fields are optional while older snuba-admin backends can still
  // return the previous single-version response during a rolling deploy.
  query_cluster_versions?: string[];
  query_node_versions?: NodeVersionData[];
  query_node_error?: string | null;
  storage_cluster_versions?: string[];
  storage_node_versions?: NodeVersionData[];
  storage_node_error?: string | null;
  version?: string | null;
  tables: string[];
  error: string | null;
};

export { ClusterData, NodeVersionData };
