import React, { ReactNode, useState } from "react";

import Client from "../api_client";
import { containerStyle, paragraphStyle } from "./styles";
import { Table } from "../table";
import { ConfigValue } from "./types";

function AuditLog(props: { api: Client }) {
  const { api } = props;
  const [data, setData] = useState<any[] | null>(null);

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

  const rowData = data.map(({ key, timestamp, user, before, after }) => {
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
      getActionDetail(before, after),
    ];
  });

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

function getActionDetail(
  before: ConfigValue | null,
  after: ConfigValue | null
): ReactNode {
  const formattedBefore = typeof before === "string" ? `"${before}"` : before;
  const formatttedAfter = typeof after === "string" ? `"${after}"` : after;

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
