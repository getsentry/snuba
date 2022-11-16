export type MigrationGroupResult = {
  group: string;
  migration_ids: MigrationData[];
};

export type MigrationData = {
  can_run: boolean;
  can_reverse: boolean;
  run_reason: string;
  reverse_reason: string;
  blocking: boolean;
  status: string;
  migration_id: string;
};

export type GroupOptions = {
  [key: string]: MigrationGroupResult;
};
