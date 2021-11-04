import React, { ReactNode } from "react";

import { linkStyle } from "./styles";
import { EditableTableCell } from "../table";
import { ConfigKey, ConfigValue, ConfigType, RowData } from "./types";

function Space() {
  return <span style={{ display: "block", margin: 5 }}></span>;
}

function getReadonlyRow(
  key: ConfigKey,
  value: ConfigValue,
  type: ConfigType,
  showActions: boolean
): RowData {
  return [key, value, type, null];
}

export { getReadonlyRow };
