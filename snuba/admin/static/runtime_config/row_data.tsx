import React, { ReactNode } from "react";
import { Popover, OverlayTrigger } from 'react-bootstrap';

import { linkStyle } from "SnubaAdmin/runtime_config/styles";
import { getDescription } from "SnubaAdmin/runtime_config/descriptions";
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
  const [realKey, staticDescription] = getDescription(key) || [null, "Is missing. Add it to snuba/admin/static/runtime_config/descriptions.tsx?"];
  const tooltip = (
    <Popover id="key-description">
      <Popover.Header>Information</Popover.Header>
      <Popover.Body>
        <p><strong><a target="_blank" href={`https://github.com/search?q=repo%3Agetsentry%2Fsnuba%20%2F${realKey || key}%2F&type=code`}>Search codebase for config key</a></strong></p>
        <p><strong>Static description:</strong> {staticDescription}</p>
      </Popover.Body>
    </Popover>
  );
  return [
    <OverlayTrigger trigger={["click"]} placement="bottom" overlay={tooltip}>
      <a id={`config/${key}`} href={`#config/${key}`}>
        <code style={{ wordBreak: "break-all" }}>
          {key}
        </code>
      </a>
    </OverlayTrigger>,
    <code style={{ wordBreak: "break-all", color: "black" }}>{value}</code>,
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
