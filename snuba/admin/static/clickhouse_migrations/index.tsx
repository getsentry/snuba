import React, { useEffect, useState } from "react";
import Client from "../api_client";
import { Table } from "../table";

export type MigrationGroup = {
  group: string;
  migration_ids: MigrationData[];
};

type MigrationData = {
  can_run: boolean;
  can_reverse: boolean;
  run_reason: string;
  reverse_reason: string;
  blocking: boolean;
  status: string;
  migration_id: string;
};
type groupOptions = {
  [key: string]: MigrationGroup;
};

const Events1 = {
  can_run: false,
  can_reverse: false,
  run_reason: "already run",
  reverse_reason: "subsequent migrations must be reversed first",
  blocking: true,
  status: "completed",
  migration_id: "0001_migration",
};

const Events2 = {
  can_run: false,
  can_reverse: true,
  run_reason: "already run",
  reverse_reason: "",
  blocking: true,
  status: "completed",
  migration_id: "0002_migration",
};

// const MIGRATIONS: groupOptions = {
//   events: {
//     groupName: "events",
//     migrations: [Events1, Events2],
//   },
// };

const SQLforwards =
  "Local operations:\
\n\n\
CREATE TABLE IF NOT EXISTS replays_local (replay_id UUID, event_hash UUID, segment_id Nullable(UInt16), trace_ids Array(UUID), _trace_ids_hashed Array(UInt64) MATERIALIZED arrayMap(t -> cityHash64(t), trace_ids), title String, project_id UInt64, timestamp DateTime, platform LowCardinality(String), environment LowCardinality(Nullable(String)), release Nullable(String), dist Nullable(String), ip_address_v4 Nullable(IPv4), ip_address_v6 Nullable(IPv6), user String, user_id Nullable(String), user_name Nullable(String), user_email Nullable(String), sdk_name String, sdk_version String, tags Nested(key String, value String), retention_days UInt16, partition UInt16, offset UInt64) ENGINE ReplacingMergeTree() ORDER BY (project_id, toStartOfDay(timestamp), cityHash64(replay_id), event_hash) PARTITION BY (retention_days, toMonday(timestamp)) TTL timestamp + toIntervalDay(retention_days) SETTINGS index_granularity=8192;\
ALTER TABLE replays_local ADD INDEX IF NOT EXISTS bf_trace_ids_hashed _trace_ids_hashed TYPE bloom_filter() GRANULARITY 1;\
\n\n\
Dist operations:\
\n\n\
Skipped dist operation - single node cluster";

const SQLbackwards =
  "Local operations:\
\n\n\
DROP TABLE IF EXISTS replays_local;\
\n\n\
Dist operations:\
\n\n\
Skipped dist operation - single node cluster";

function ClickhouseMigrations(props: { api: Client }) {
  const [allGroups, setAllGroups] = useState<groupOptions>({});
  const [migrationGroup, setMigrationGroup] = useState<MigrationGroup | null>(
    null
  );
  const [migrationId, setMigrationId] = useState<string | null>(null);
  const [SQLText, setSQLText] = useState<string | null>(null);

  useEffect(() => {
    props.api.getAllMigrationGroups().then((res) => {
      let options: groupOptions = {};
      res.forEach((group: MigrationGroup) => (options[group.group] = group));
      setAllGroups(options);
    });
  }, []);

  function selectGroup(groupName: string) {
    const migrationGroup: MigrationGroup = allGroups[groupName];
    setMigrationGroup(() => migrationGroup);
  }

  function selectMigration(migrationId: string) {
    setMigrationId(() => migrationId);
    setSQLText(() => SQLforwards);
  }

  function execute(action: string) {
    const data = migrationGroup?.migration_ids.find(
      (m) => m.migration_id == migrationId
    );
    if (data?.blocking) {
      window.confirm(
        `Migration ${migrationId} is blocking, are you sure you want to execute?`
      );
    }
    console.log("executing !", action);
  }

  function rowData() {
    if (migrationGroup) {
      let data: any = [];
      allGroups[migrationGroup.group].migration_ids.forEach((migration) => {
        data.push([
          migration.migration_id,
          migration.status,
          migration.blocking.toString(),
        ]);
      });
      return data;
    }
    return null;
  }

  function renderMigrationIds() {
    if (migrationGroup) {
      return (
        <select
          value={migrationId || ""}
          onChange={(evt) => selectMigration(evt.target.value)}
          style={dropDownStyle}
        >
          <option disabled={!migrationGroup} value="">
            Select a migration id
          </option>
          {allGroups[migrationGroup.group].migration_ids.map(
            (m: MigrationData) => (
              <option key={m.migration_id} value={m.migration_id}>
                {m.migration_id} - {m.status}
              </option>
            )
          )}
        </select>
      );
    } else {
      return (
        <select
          value={migrationId || ""}
          onChange={(evt) => selectMigration(evt.target.value)}
          style={dropDownStyle}
        >
          <option disabled={true} value="">
            Select a migration id
          </option>
        </select>
      );
    }
  }

  function renderActions() {
    if (!(migrationGroup && migrationId)) {
      return null;
    }
    const data = migrationGroup?.migration_ids.find(
      (m) => m.migration_id == migrationId
    );

    return (
      <div>
        <input
          key="run"
          type="button"
          disabled={!data?.can_run}
          title={!data?.can_run ? data?.run_reason : ""}
          id="run"
          value="EXECUTE run"
          onClick={() => execute("run")}
          style={buttonStyle}
        />
        <input
          key="reverse"
          type="button"
          disabled={!data?.can_reverse}
          title={!data?.can_reverse ? data?.reverse_reason : ""}
          id="reverse"
          value="EXECUTE reverse"
          onClick={() => execute("reverse")}
          style={buttonStyle}
        />
        <div>
          {!data?.can_reverse && data?.reverse_reason && (
            <p style={textStyle}>
              ❌ <strong>You cannot reverse this migration: </strong>
              {data?.reverse_reason}
            </p>
          )}
          {!data?.can_run && data?.run_reason && (
            <p style={textStyle}>
              ❌ <strong>You cannot run this migration: </strong>
              {data?.run_reason}
            </p>
          )}
        </div>
      </div>
    );
  }
  return (
    <div style={{ display: "flex" }}>
      <form>
        <div style={migrationPicker}>
          <h4>Execute a migration</h4>
          <p style={textStyle}>
            Attempt to run or reverse a migration for one of the migration
            groups you have access to
          </p>
          <select
            value={migrationGroup?.group || ""}
            onChange={(evt) => selectGroup(evt.target.value)}
            style={dropDownStyle}
          >
            <option disabled value="">
              Select a migration group
            </option>
            {Object.keys(allGroups).map((name: string) => (
              <option key={name} value={name}>
                {name}
              </option>
            ))}
          </select>
        </div>

        <div>
          {renderMigrationIds()}
          {migrationGroup && migrationId && (
            <div style={{ display: "inline-block" }}>
              <button
                onClick={() => setSQLText(() => SQLforwards)}
                style={buttonStyle}
              >
                forwards
              </button>
              <button
                onClick={() => setSQLText(() => SQLbackwards)}
                style={buttonStyle}
              >
                backwards
              </button>
            </div>
          )}
        </div>
        {migrationGroup && migrationId && SQLText && (
          <div style={sqlBox}>
            <p style={textStyle}>
              Raw SQL for running a migration (forwards) or reversing
              (backwards). Good to do before executing a migration for real.
            </p>
            <textarea style={textareaStyle} value={SQLText} />
          </div>
        )}
        <div style={actionsStyle}>{renderActions()}</div>
      </form>
      <div>
        {migrationGroup && (
          <Table
            headerData={["Migration ID", "Status", "Blocking"]}
            rowData={rowData()}
            columnWidths={[5, 2, 2]}
          />
        )}
      </div>
    </div>
  );
}

export default ClickhouseMigrations;

const sqlBox = {
  width: "90%",
  height: "300px",
};

const migrationPicker = {
  paddingRight: "10px",
};

const actionsStyle = {
  margin: "10px 0px",
  width: "100%",
  padding: "10px 0px",
};

const buttonStyle = {
  padding: "2px 5px",
  marginRight: "10px",
};

const textStyle = {
  fontSize: 14,
};

const dropDownStyle = {
  padding: "2px 5px",
  marginBottom: "4px",
  marginRight: "10px",
};

const textareaStyle = {
  marginTop: "10px",
  width: "100%",
  height: "200px",
  overflowX: "scroll" as const,
  overflowY: "scroll" as const,
  resize: "none" as const,
};
