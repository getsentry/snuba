import React, { ReactNode, useState } from "react";

import Client from "SnubaAdmin/api_client";
import { containerStyle, paragraphStyle } from "SnubaAdmin/runtime_config/styles";
import { Table } from "SnubaAdmin/table";
import { ConfigChange, ConfigType, ConfigValue } from "SnubaAdmin/runtime_config/types";

function AuditLog(props: { api: Client }) {
  const { api } = props;
  const [data, setData] = useState<ConfigChange[] | null>(null);

  function fetchData() {
    api.getAuditlog().then((res) => {
      setData(res);
    });
  }

  if (data === null) {
    fetchData();
  }

  if (!data) {
    return null;
  }

  const rowData = data.map(
    ({ key, timestamp, user, before, beforeType, after, afterType }) => {
      const formattedDate = new Date(timestamp * 1000).toLocaleDateString(
        "en-US",
        {
          day: "numeric",
          month: "short",
          year: "numeric",
          hour: "numeric",
          minute: "numeric",
          second: "numeric",
          timeZoneName: "short",
        }
      );

      return [
        formattedDate,
        user,
        <code>{key}</code>,
        getActionDetail(before, beforeType, after, afterType),
      ];
    }
  );

  const usersTimeZone = Intl.DateTimeFormat().resolvedOptions().timeZone;

  return (
    <div style={containerStyle}>
      <p style={paragraphStyle}>
        Only changes to runtime configuration are currently being captured in
        the audit log
      </p>
      <Table
        headerData={[`Timestamp (${usersTimeZone})`, "User", "Key", "Action"]}
        rowData={rowData}
        columnWidths={[1, 1, 1, 1]}
      ></Table>
    </div>
  );
}

function getFormattedValue(
  value: ConfigValue | null,
  type: ConfigType | null
): string {
  if (value === null && type === null) {
    return "NULL";
  }

  if (type === "string") {
    return `"${value}"`;
  }

  if (value !== null && (type === "int" || type === "float")) {
    return value;
  }

  throw new Error("Invalid type");
}

function getActionDetail(
  before: ConfigValue | null,
  beforeType: ConfigType | null,
  after: ConfigValue | null,
  afterType: ConfigType | null
): ReactNode {
  const formattedBefore = getFormattedValue(before, beforeType);
  const formatttedAfter = getFormattedValue(after, afterType);

  if (before === null && after !== null) {
    if (after === null) {
      // Value was null before and after the change, something went wrong
      throw new Error("Unkknown action");
    } else {
      // A new value appeared
      return (
        <span>
          Config added. New value: <code>{formatttedAfter}</code>
        </span>
      );
    }
  } else {
    if (after === null) {
      // Something was deleted
      return (
        <span>
          Config deleted. Last value: <code>{formattedBefore}</code>
        </span>
      );
    } else {
      // Value was changed
      return (
        <span>
          Config changed from <code>{formattedBefore}</code> to{" "}
          <code>{formatttedAfter}</code>
        </span>
      );
    }
  }
}

export default AuditLog;
