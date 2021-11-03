import React, { ReactNode, useState } from "react";
import { SelectableTableCell, Table } from "./table";
import Client from "./api_client";
import { COLORS } from "./theme";

type ConfigType = "string" | "int" | "float";

const types = ["string", "int", "float"];

function RuntimeConfig(props: { api: Client }) {
  const [data, setData] = useState<Map<
    string,
    { value: string | number; type: ConfigType }
  > | null>(null);

  const [currentlyEditing, setCurrentlyEditing] = useState<string | null>(null);

  // Only load data if it was not previously loaded
  if (data === null) {
    const { api } = props;
    api.getConfigs().then((res) => {
      setData(res);
    });
  }

  function getRowData(
    key: string,
    value: string | number,
    type: ConfigType
  ): [string, string | number, ConfigType, ReactNode] {
    return [
      key,
      value,
      type,
      currentlyEditing === null && (
        <a style={linkStyle} onClick={() => setCurrentlyEditing(key)}>
          edit
        </a>
      ),
    ];
  }

  function getEditRowData(
    key: string,
    value: string | number,
    type: ConfigType
  ): [string, string | number, ReactNode, ReactNode] {
    return [
      key,
      value,
      <SelectableTableCell
        options={types.map((type) => ({ value: type, label: type }))}
        selected={type}
        onChange={onChangeType}
      />,
      <span>
        <a style={linkStyle} onClick={() => setCurrentlyEditing(null)}>
          cancel editing
        </a>
        <span style={{ display: "block", margin: 5 }}></span>
        <a style={{ ...linkStyle, color: "red" }}>delete</a>
      </span>,
    ];
  }

  function onChangeType() {}

  if (data) {
    const rowData: [string, string | number, ReactNode, ReactNode][] =
      Object.entries(data).map((row) => {
        const [key, { value, type }] = row;
        const isEditing = key === currentlyEditing;
        return isEditing
          ? getEditRowData(key, value, type)
          : getRowData(key, value, type);
      });

    return (
      <div style={containerStyle}>
        <Table
          headerData={["Key", "Value", "Type", "Actions"]}
          rowData={rowData}
          columnWidths={[3, 5, 2, 1]}
        />
        <a style={linkStyle}>add new</a>
      </div>
    );
  } else {
    return null;
  }
}

const containerStyle = {
  width: 1200,
  maxWidth: "100%",
};

const linkStyle = {
  cursor: "pointer",
  fontSize: 13,
  color: COLORS.TEXT_LIGHTER,
  textDecoration: "underline",
};

export default RuntimeConfig;
