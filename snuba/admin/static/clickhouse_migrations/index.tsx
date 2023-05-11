import React, { useEffect, useState } from "react";
import Client from "../api_client";
import { Table } from "../table";
import {
  MigrationData,
  MigrationGroupResult,
  GroupOptions,
  RunMigrationRequest,
  RunMigrationResult,
  Action,
} from "./types";

function ClickhouseMigrations(props: { api: Client }) {
  const [allGroups, setAllGroups] = useState<GroupOptions>({});
  const [migrationGroup, setMigrationGroup] =
    useState<MigrationGroupResult | null>(null);
  const [migrationId, setMigrationId] = useState<string | null>(null);
  const [SQLText, setSQLText] = useState<string | null>(null);

  const [header, setHeader] = useState<string | null>(null);
  const [show_action, setShowAction] = useState<boolean | null>(false);

  const dry_run_header = "Dry run SQL output"
  const real_run_header = "Run log output"

  const [forwards_dry_run, setFowardsDryRun] = useState<string | null>(null);
  const [backwards_dry_run, setBackwardsDryRun] = useState<string | null>(null);

  useEffect(() => {
    props.api.getAllMigrationGroups().then((res) => {
      let options: GroupOptions = {};
      res.sort().forEach(
        (group: MigrationGroupResult) => (options[group.group] = group)
      );
      setAllGroups(options);
    });
  }, []);

  function clearBtnState() {
    setBackwardsDryRun(() => null)
    setFowardsDryRun(() => null)
    setSQLText(() => null);
  }

  function selectGroup(groupName: string) {
    const migrationGroup: MigrationGroupResult = allGroups[groupName];
    setMigrationGroup(() => migrationGroup);
    clearBtnState()
    setMigrationId(() => null);
    setShowAction(()=> false)
    refreshStatus(migrationGroup.group);
  }

  function selectMigration(migrationId: string) {
    setMigrationId(() => migrationId);
    clearBtnState()
    setShowAction(()=> false)
  }

  function selectForwards(dry_run_sql: string) {
    setBackwardsDryRun(() => null)
    setFowardsDryRun(() => dry_run_sql)
  }

  function selectBackwards(dry_run_sql: string) {
    setFowardsDryRun(() => null)
    setBackwardsDryRun(() => dry_run_sql)
  }

  function execute(action: Action) {
    let force = false;
    const data = migrationGroup?.migration_ids.find(
      (m) => m.migration_id == migrationId
    );
    if (action == Action.Run && !forwards_dry_run) {
      return alert("Please run a forwards dry run first")
    }
    if (action == Action.Reverse && !backwards_dry_run) {

      return alert("Please run a backwards dry run first: ")
    }
    if (data?.blocking) {
      if (
        window.confirm(
          `Migration ${migrationId} is blocking, are you sure you want to execute?`
        )
      ) {
        force = true;
      }
    }
    if (data?.status !== "not_started" && action === Action.Reverse) {
      if (
        window.confirm(
          `Migration ${migrationId} is ${data?.status}, are you sure you want to reverse?`
        )
      ) {
        force = true;
      }
    }
    executeRealRun(action, force);
  }

  function executeRun(action: Action, dry_run: boolean, force: boolean,
    cb?: (stdout: string, err?: string) => void ) {
    let req = {
      action: action,
      migration_id: migrationId,
      group: migrationGroup?.group,
      dry_run: dry_run,
      force: force,
    };
    props.api
      .runMigration(req as RunMigrationRequest)
      .then((res) => {
        console.log(res);
        if (action == Action.Run && dry_run) {
          selectForwards(res.stdout)
        }
        if (action == Action.Reverse && dry_run) {
          selectBackwards(res.stdout)
        }
        setSQLText(() => res.stdout);
        if (cb) {
          cb(res.stdout)
        }
      })
      .catch((err) => {
        console.error(err);
        setSQLText(() => JSON.stringify(err));
        if (cb) {
          cb("", JSON.stringify(err))
        }
      });
  }

  function executeDryRun(action: Action) {
    console.log("executing dry run !", migrationId, action);
    setHeader(()=> dry_run_header)
    executeRun(action, true, false)
    setShowAction(()=> true)
  }

  function executeRealRun(action: Action, force: boolean) {
    console.log("executing real run !", migrationId, action, force);
    clearBtnState()
    setHeader(()=> real_run_header)
    executeRun(action, false, force, (stdout: string, err?: string) => {
      if (stdout.indexOf("migration.completed") > -1) {
        alert(`Migration ${migrationId} ${action} completed successfully`)
      } else {
        alert(`Migration ${migrationId} ${action} didn't complete.` +
              `See run log output. \n\n ${err||""} \n ${stdout}`)
      }
      if (migrationGroup){
        refreshStatus(migrationGroup.group)
      }
    })

    if (migrationGroup){
      refreshStatus(migrationGroup.group)
    }
  }


  function refreshStatus(group: string) {
    props.api.getAllMigrationGroups().then((res) => {
      let options: GroupOptions = {};
      res.sort().forEach(
        (group: MigrationGroupResult) => (options[group.group] = group)
      );
      setAllGroups(options);
      setMigrationGroup(options[group]);
    });
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
    if (!(migrationGroup && migrationId) || !show_action) {
      return null;
    }
    const data = migrationGroup?.migration_ids.find(
      (m) => m.migration_id == migrationId
    );

    return (
      <div>
        {forwards_dry_run && <input
          key="run"
          type="button"
          disabled={!data?.can_run}
          title={!data?.can_run ? data?.run_reason : ""}
          id="run"
          defaultValue="EXECUTE run"
          onClick={() => execute(Action.Run)}
          style={buttonStyle}
        />}
        {backwards_dry_run && <input
          key="reverse"
          type="button"
          disabled={!data?.can_reverse}
          title={!data?.can_reverse ? data?.reverse_reason : ""}
          id="reverse"
          defaultValue="EXECUTE reverse"
          onClick={() => execute(Action.Reverse)}
          style={buttonStyle}
        />}
        <div>
          {!data?.can_reverse && data?.reverse_reason && backwards_dry_run && (
            <p style={textStyle}>
              ❌ <strong>You cannot reverse this migration: </strong>
              {data?.reverse_reason}
            </p>
          )}
          {!data?.can_run && data?.run_reason && forwards_dry_run && (
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
            defaultValue={migrationGroup?.group || ""}
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
            <div style={{ display: "block" }}>
              <br />
              <button type="button"
                onClick={() => executeDryRun(Action.Run)}
                style={forwards_dry_run? selectedButtonStyle : buttonStyle}
              >
                DRY RUN forwards
              </button>
              <button
                type="button"
                onClick={() => executeDryRun(Action.Reverse)}
                style={backwards_dry_run ? selectedButtonStyle : buttonStyle}
              >
                DRY RUN backwards
              </button>
            </div>
          )}
        </div>
        <p>
        Before executing a migration, do a dry run first. This will generate the raw SQL for running a migration (forwards) or reversing (backwards) a migration so that you can verify it's contents.
        </p>

        {migrationGroup && migrationId && SQLText && (
          <div style={sqlBox}>
            <p style={textStyle}>
              {header}
            </p>
            <textarea style={textareaStyle} readOnly value={SQLText} />
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

const selectedButtonStyle = {
  color: "red",
  padding: "2px 5px",
  marginRight: "10px",
}

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
