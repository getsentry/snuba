import React, { ReactNode, useState } from "react";
import { EditableTableCell, SelectableTableCell, Table } from "./table";
import Client from "./api_client";
import { COLORS } from "./theme";
import { ConfigType } from "./types";

const types = ["string", "int", "float"];

function RuntimeConfig(props: { api: Client }) {
  const { api } = props;

  // Data from the API
  const [data, setData] = useState<
    { key: string; value: string | number; type: ConfigType }[] | null
  >(null);

  // Key of existing row being edited (if any)
  const [currentlyEditing, setCurrentlyEditing] = useState<string | null>(null);

  // True if we are adding a brand new config, otherwise false
  const [addingNew, setAddingNew] = useState(false);

  // Unsaved state of the row currently being edited
  const [currentRowData, setCurrentRowData] = useState<{
    key: string;
    value: string | number;
    type: ConfigType;
  }>({ key: "", value: "", type: "string" });

  function resetCurrentRowData() {
    setCurrentRowData({ key: "", value: "", type: "string" });
  }

  // Load data if it was not previously loaded
  if (data === null) {
    fetchData();
  }

  function fetchData() {
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
      type,
      // <SelectableTableCell
      //   options={types.map((type) => ({ value: type, label: type }))}
      //   selected={type}
      //   onChange={() => {}}
      // />,
      <span>
        <a style={linkStyle} onClick={() => setCurrentlyEditing(null)}>
          cancel editing
        </a>
        <span style={{ display: "block", margin: 5 }}></span>
        <a style={{ ...linkStyle, color: "red" }}>delete</a>
      </span>,
    ];
  }

  function getNewRowData(
    key: string,
    value: string | number,
    type: ConfigType
  ): [ReactNode, ReactNode, ReactNode, ReactNode] {
    return [
      <EditableTableCell
        multiline={false}
        value={key}
        onChange={(newKey) => {
          setCurrentRowData((prev) => {
            return { ...prev, key: newKey };
          });
        }}
      />,
      <EditableTableCell
        multiline={true}
        value={value}
        onChange={(newValue) => {
          setCurrentRowData((prev) => {
            return { ...prev, value: newValue };
          });
        }}
      />,
      <SelectableTableCell
        options={types.map((type) => ({ value: type, label: type }))}
        selected={type}
        onChange={(newType) => {
          setCurrentRowData((prev) => {
            return { ...prev, type: newType };
          });
        }}
      />,
      <a
        onClick={() => {
          api
            .createNewConfig(
              currentRowData.key,
              currentRowData.value,
              currentRowData.type
            )
            .then((res) => {
              setData((prev) => {
                if (prev) {
                  return [...prev, res];
                } else {
                  return prev;
                }
              });
              resetForm();
            });
        }}
      >
        save
      </a>,
    ];
  }

  function addNewConfig() {
    setCurrentlyEditing(null);
    setAddingNew(true);
    resetCurrentRowData();
  }

  function resetForm() {
    setCurrentlyEditing(null);
    setAddingNew(false);
    resetCurrentRowData();
  }

  if (data) {
    const rowData: [ReactNode, ReactNode, ReactNode, ReactNode][] = data.map(
      (row) => {
        const { key, value, type } = row;
        const isEditing = key === currentlyEditing;
        return isEditing
          ? getEditRowData(key, value, type)
          : getRowData(key, value, type);
      }
    );

    if (addingNew) {
      rowData.push(
        getNewRowData(
          currentRowData.key,
          currentRowData.value,
          currentRowData.type
        )
      );
    }

    if (!data) {
      return null;
    }

    return (
      <div style={containerStyle}>
        <Table
          headerData={["Key", "Value", "Type", "Actions"]}
          rowData={rowData}
          columnWidths={[3, 5, 2, 1]}
        />
        <a onClick={addNewConfig} style={linkStyle}>
          add new
        </a>
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
