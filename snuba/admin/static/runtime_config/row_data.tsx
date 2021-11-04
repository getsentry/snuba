import React, { ReactNode } from "react";

import { linkStyle } from "./styles";
import { EditableTableCell, SelectableTableCell } from "../table";
import { ConfigKey, ConfigValue, ConfigType, RowData } from "./types";

const TYPES = ["string", "int", "float"];

function Space() {
  return <span style={{ display: "block", margin: 5 }}></span>;
}

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
  updateValue: (value: ConfigValue) => void,
  save: () => void,
  deleteRow: () => void,
  cancel: () => void
): RowData {
  return [
    key,
    <EditableTableCell
      multiline={false}
      value={value}
      onChange={updateValue}
    />,
    type,
    <span>
      <a style={linkStyle} onClick={() => save()}>
        <strong>save changes</strong>
      </a>
      <Space />
      <a style={{ ...linkStyle, color: "red" }} onClick={() => deleteRow()}>
        delete
      </a>
      <Space />
      <a style={linkStyle} onClick={() => cancel()}>
        cancel editing
      </a>
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
  cancel: () => void,
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
    <span>
      <a style={linkStyle} onClick={save}>
        <strong>save</strong>
      </a>
      <Space />
      <a style={linkStyle} onClick={() => cancel()}>
        cancel
      </a>
    </span>,
  ];
}

export { getReadonlyRow, getEditableRow, getNewRow };
