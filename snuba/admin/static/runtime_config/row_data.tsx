import React, { ReactNode } from "react";

import { linkStyle } from "./styles";
import { EditableTableCell, SelectableTableCell } from "../table";
import { ConfigKey, ConfigValue, ConfigType, RowData } from "./types";

const TYPES = ["string", "int", "float"];

function getReadonlyRow(
  key: ConfigKey,
  value: ConfigValue,
  type: ConfigType,
  showActions: boolean,
  edit: () => void
): RowData {
  return [
    key,
    value,
    type,
    showActions && (
      <a style={linkStyle} onClick={() => edit()}>
        edit
      </a>
    ),
  ];
}

function getEditableRow(
  key: ConfigKey,
  value: ConfigValue,
  type: ConfigType,
  cancelEdit: () => void
): RowData {
  return [
    key,
    value,
    type,
    // <SelectableTableCell
    //   options={types.map((type) => ({ value: type, label: type }))}
    //   selected={type}
    //   onChange={() => {}}
    // />,
    <span>
      <a style={linkStyle} onClick={() => cancelEdit()}>
        cancel editing
      </a>
      <span style={{ display: "block", margin: 5 }}></span>
      <a style={{ ...linkStyle, color: "red" }}>delete</a>
    </span>,
  ];
}

function getNewRow(
  key: ConfigKey,
  value: ConfigValue,
  type: ConfigType,
  updateKey: (key: ConfigKey) => void,
  updateValue: (value: ConfigValue) => void,
  updateType: (type: ConfigType) => void,
  save: () => void
): [ReactNode, ReactNode, ReactNode, ReactNode] {
  return [
    <EditableTableCell multiline={false} value={key} onChange={updateKey} />,
    <EditableTableCell multiline={true} value={value} onChange={updateValue} />,
    <SelectableTableCell
      options={TYPES.map((type) => ({ value: type, label: type }))}
      selected={type}
      onChange={updateType}
    />,
    <a onClick={save}>save</a>,
  ];
}

export { getReadonlyRow, getEditableRow, getNewRow };
