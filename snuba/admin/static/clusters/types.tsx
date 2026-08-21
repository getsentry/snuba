type ClusterData = {
  cluster_name: string;
  versions: string[];
  storage_sets: string[];
  tables: string[];
  error: string | null;
};

export { ClusterData };
