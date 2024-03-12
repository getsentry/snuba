import React, { ReactNode } from "react";

import { linkStyle } from "SnubaAdmin/runtime_config/styles";
import { EditableTableCell } from "SnubaAdmin/table";
import {
  ConfigKey,
  ConfigValue,
  ConfigType,
  RowData,
  ConfigDescription,
} from "SnubaAdmin/runtime_config/types";

const TYPES = ["string", "int", "float"];

function Space(props: { margin?: number }) {
  const margin = typeof props.margin !== "undefined" ? props.margin : 15;
  return <span style={{ display: "block", margin: margin }}></span>;
}

function getReadonlyRow(
  key: ConfigKey,
  value: ConfigValue,
  description: ConfigDescription,
  type: ConfigType,
  showActions: boolean,
  edit: () => void
): RowData {
  return [
    <code style={{ wordBreak: "break-all" }}>{key}</code>,
    <code style={{ wordBreak: "break-all" }}>{value}</code>,
    description,
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
  description: ConfigDescription,
  type: ConfigType,
  updateValue: (value: ConfigValue) => void,
  updateDescription: (desc: ConfigDescription) => void,
  save: () => void,
  deleteRow: () => void,
  cancel: () => void
): RowData {
  return [
    <code style={{ wordBreak: "break-all" }}>{key}</code>,
    <EditableTableCell value={value} onChange={updateValue} />,
    <EditableTableCell value={description} onChange={updateDescription} />,
    type,
    <span>
      <a style={linkStyle} onClick={() => save()}>
        <strong>save changes</strong>
      </a>
      <Space />
      <label style={{ ...linkStyle, textDecoration: "" }}>
        <input type="checkbox" id="keepDescription" />
        keep description
      </label>
      <Space margin={2} />
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
  description: ConfigDescription,
  updateKey: (key: ConfigKey) => void,
  updateValue: (value: ConfigValue) => void,
  updateDescription: (desc: ConfigDescription) => void,
  cancel: () => void,
  save: () => void
): [ReactNode, ReactNode, ReactNode, ReactNode, ReactNode] {
  return [
    <EditableTableCell value={key} onChange={updateKey} />,
    <EditableTableCell value={value} onChange={updateValue} />,
    <EditableTableCell value={description} onChange={updateDescription} />,
    null,
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
