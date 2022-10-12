import React, { useState } from "react";

type MigrationGroup = {
  groupName: string;
  migrations: string[];
};

type groupOptions = {
  [key: string]: MigrationGroup;
};

const MIGRATIONS: groupOptions = {
  events: {
    groupName: "events",
    migrations: ["0001_migration", "0002_migration"],
  },
  transactions: {
    groupName: "transactions",
    migrations: ["0003_migration", "0004_migration"],
  },
  generic_metrics: {
    groupName: "generic_metrics",
    migrations: ["0005_migration", "0006_migration"],
  },
};
const ACTIONS = ["run", "reverse", "show"];

function ClickhouseMigrations() {
  const [migrationGroup, setMigrationGroup] = useState<MigrationGroup | null>(
    null
  );
  const [migrationId, setMigrationId] = useState<string | null>(null);
  const [migrationAction, setMigrationAction] = useState<string | null>(null);
  function selectGroup(groupName: string) {
    const migrationGroup: MigrationGroup = MIGRATIONS[groupName];
    setMigrationGroup(() => migrationGroup);
  }

  function selectMigration(migrationId: string) {
    setMigrationId(() => migrationId);
  }
  function updateAction(action: string) {
    setMigrationAction(() => action);
  }
  return (
    <div>
      <form>
        <select
          value={migrationGroup?.groupName || ""}
          onChange={(evt) => selectGroup(evt.target.value)}
        >
          <option disabled value="">
            Select a migration group
          </option>
          {Object.keys(MIGRATIONS).map((name: string) => (
            <option key={name} value={name}>
              {name}
            </option>
          ))}
        </select>
        <div>
          {ACTIONS.map((action: string) => (
            <input
              key={action}
              type="button"
              style={migrationAction == action ? selectedButton : {}}
              id={action}
              value={action}
              onClick={() => updateAction(action)}
            />
          ))}
        </div>
        {migrationGroup && migrationAction != "show" && (
          <select
            value={migrationId || ""}
            onChange={(evt) => selectMigration(evt.target.value)}
          >
            <option disabled value="">
              Select a migration id
            </option>
            {MIGRATIONS[migrationGroup.groupName].migrations.map(
              (id: string) => (
                <option key={id} value={id}>
                  {id}
                </option>
              )
            )}
          </select>
        )}
      </form>
    </div>
  );
}

export default ClickhouseMigrations;

const selectedButton = {
  backgroundColor: "white",
  color: "black",
};
