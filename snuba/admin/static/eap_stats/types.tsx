type ResourceTotals = {
  bytes_scanned: number;
  duration_ms: number;
  cpu_user_us: number;
  cpu_system_us: number;
  cpu_virtual_us: number;
  realtime_us: number;
  memory_usage_bytes: number;
  memory_peak_bytes: number;
  io_selected_bytes: number;
  io_selected_rows: number;
  io_read_compressed_bytes: number;
  io_compressed_read_buffer_bytes: number;
  io_fd_read_bytes: number;
  io_fd_write_bytes: number;
  network_receive_bytes: number;
  network_send_bytes: number;
  network_receive_us: number;
  network_send_us: number;
  profile_events_matched: number;
};

type ProfileCoverage = {
  enabled: boolean;
  queries_total: number;
  queries_with_query_id: number;
  queries_profiled: number;
  query_ids_sampled: number;
  query_ids_looked_up: number;
  query_ids_matched: number;
  query_ids_capped: boolean;
  bytes_total: number;
  bytes_profiled: number;
  duration_ms_total: number;
  duration_ms_profiled: number;
  pct_queries_profiled: number;
  pct_queries_with_query_id_profiled: number;
  pct_query_ids_matched: number;
  pct_bytes_profiled: number;
  pct_duration_profiled: number;
};

type QueryTypeBucket = {
  query_type: string;
  query_count: number;
  queries_profiled: number;
  resources: ResourceTotals;
  avg_bytes_scanned: number;
  avg_duration_ms: number;
  pct_of_bytes: number;
  pct_of_queries: number;
  pct_of_cpu: number;
  pct_of_memory_peak: number;
  pct_of_io: number;
  pct_of_network: number;
  pct_queries_profiled: number;
  pct_bytes_profiled: number;
  by_trace_item_type: Record<string, number>;
  by_filter_profile: Record<string, number>;
  by_referrer: Record<string, number>;
  examples: Array<Record<string, any>>;
};

type DimensionRow = {
  key: string;
  query_count: number;
  total_bytes_scanned: number;
  total_cpu_us: number;
  pct_of_bytes: number;
  pct_of_cpu: number;
};

type EapStatsRequest = {
  hours: number;
  max_rows: number;
  referrer?: string;
  referrer_contains?: string;
  organization_id?: number | string;
  include_profile_events: boolean;
};

type EapStatsResult = {
  hours: number;
  max_rows: number;
  rows_scanned: number;
  rows_categorized: number;
  rows_failed: number;
  profile_events_enabled: boolean;
  profile_events_matched: number;
  profile_coverage: ProfileCoverage;
  total_resources: ResourceTotals;
  by_query_type: QueryTypeBucket[];
  by_filter_profile: DimensionRow[];
  by_trace_item_type: DimensionRow[];
  by_referrer: DimensionRow[];
};

export type {
  ResourceTotals,
  ProfileCoverage,
  QueryTypeBucket,
  DimensionRow,
  EapStatsRequest,
  EapStatsResult,
};
