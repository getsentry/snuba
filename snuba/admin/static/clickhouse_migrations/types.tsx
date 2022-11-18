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
}

export enum Action {
    Run = "run",
    Reverse = "reverse",
}

export type RunMigrationRequest = {
    group: string;
    action: Action;
    migration_id: string;
    force?: boolean;
    fake?: boolean;
    dry_run?: boolean;
};

export type RunMigrationResult = {
    stdout: string;
    error?: string;
};
