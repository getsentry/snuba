type ClusterData = {
  host: string;
  port: number;
  http_port: number;
  database: string;
  secure: boolean;
  single_node: boolean;
  cluster_name: string | null;
  distributed_cluster_name: string | null;
  storage_sets: string[];
  version: string | null;
  tables: string[];
  error: string | null;
};

export { ClusterData };
