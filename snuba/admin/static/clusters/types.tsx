type ClusterData = {
  cluster_name: string;
  versions: string[];
  storage_sets: string[];
  tables: string[];
  versions_error: string | null;
  tables_error: string | null;
};

export { ClusterData };
