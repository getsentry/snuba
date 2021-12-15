import React, { useState } from "react";

import { Table } from "../table";
import Client from "../api_client";
import { ConfigKey, ConfigValue, ConfigType, RowData } from "./types";
import { getReadonlyRow, getNewRow } from "./row_data";
import { containerStyle, linkStyle, paragraphStyle } from "./styles";

function RuntimeConfig(props: { api: Client }) {
  const { api } = props;

  // Data from the API
  const [data, setData] = useState<
    { key: ConfigKey; value: ConfigValue; type: ConfigType }[] | null
  >(null);

  // Key of existing row being edited (if any)
  const [currentlyEditing, setCurrentlyEditing] = useState<ConfigKey | null>(
    null
  );

  // True if we are adding a brand new config, otherwise false
  const [addingNew, setAddingNew] = useState(false);

  // Unsaved state of the row currently being edited
  const [currentRowData, setCurrentRowData] = useState<{
    key: ConfigKey;
    value: ConfigValue;
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
    const rowData: RowData[] = data.map((row) => {
      const { key, value, type } = row;

      return getReadonlyRow(key, value, type, false, () => {});
    });

    if (addingNew) {
      rowData.push(
        getNewRow(
          currentRowData.key,
          currentRowData.value,
          (newKey) =>
            setCurrentRowData((prev) => {
              return { ...prev, key: newKey };
            }),
          (newValue) =>
            setCurrentRowData((prev) => {
              return { ...prev, value: newValue };
            }),
          resetForm,
          () => {
            api
              .createNewConfig(currentRowData.key, currentRowData.value)
              .then((res) => {
                setData((prev) => {
                  if (prev) {
                    return [...prev, res];
                  } else {
                    return prev;
                  }
                });
                resetForm();
              })
              .catch((err) => {
                window.alert(err);
              });
          }
        )
      );
    }

    if (!data) {
      return null;
    }

    return (
      <div style={containerStyle}>
        <p style={paragraphStyle}>These are the current configurations.</p>
        <Table
          headerData={["Key", "Value", "Type", "Actions"]}
          rowData={rowData}
          columnWidths={[3, 5, 2, 1]}
        />
        {!addingNew && !currentlyEditing && (
          <a onClick={addNewConfig} style={linkStyle}>
            add new
          </a>
        )}
      </div>
    );
  } else {
    return null;
  }
}

export default RuntimeConfig;
