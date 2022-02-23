import React, { useState } from "react";

import { Table } from "../table";
import Client from "../api_client";
import {
  ConfigKey,
  ConfigValue,
  ConfigType,
  ConfigDescription,
  RowData,
} from "./types";
import { getEditableRow, getReadonlyRow, getNewRow } from "./row_data";
import { containerStyle, linkStyle, paragraphStyle } from "./styles";

function RuntimeConfig(props: { api: Client }) {
  const { api } = props;

  // Data from the API
  const [data, setData] = useState<
    | {
        key: ConfigKey;
        value: ConfigValue;
        description: ConfigDescription;
        type: ConfigType;
      }[]
    | null
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
    description: ConfigDescription;
    type: ConfigType;
  }>({ key: "", value: "", description: "", type: "string" });

  function resetCurrentRowData() {
    setCurrentRowData({ key: "", value: "", description: "", type: "string" });
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

  function enterEditMode(
    key: ConfigKey,
    value: ConfigValue,
    description: ConfigDescription,
    type: ConfigType
  ) {
    setCurrentRowData({ key, value, description, type });
    setCurrentlyEditing(key);
  }

  if (data) {
    const rowData: RowData[] = data.map((row) => {
      const { key, value, description, type } = row;
      const isEditing = key === currentlyEditing;
      const showActions = currentlyEditing === null && addingNew === false;
      return isEditing
        ? getEditableRow(
            currentRowData.key,
            currentRowData.value,
            currentRowData.description,
            currentRowData.type,
            (newValue) => {
              setCurrentRowData((prev) => {
                return { ...prev, value: newValue };
              });
            },
            (newDescription) => {
              setCurrentRowData((prev) => {
                return { ...prev, description: newDescription };
              });
            },
            () => {
              if (
                window.confirm(
                  `Are you sure you want to update ${key} to ${currentRowData.value}?`
                )
              ) {
                api
                  .editConfig(
                    key,
                    currentRowData.value,
                    currentRowData.description
                  )
                  .then((res) => {
                    setData((prev) => {
                      if (prev) {
                        const row = prev.find(
                          (config) => config.key === res.key
                        );
                        if (!row) {
                          throw new Error("An error occurred");
                        }
                        row.value = res.value;
                        row.description = res.description;
                      }
                      return prev;
                    });
                    resetForm();
                  })
                  .catch((err) => {
                    window.alert(err);
                  });
              }
            },
            () => {
              if (window.confirm(`Are you sure you want to delete ${key}?`)) {
                api
                  .deleteConfig(
                    key,
                    (
                      document.getElementById(
                        "keepDescription"
                      ) as HTMLInputElement
                    ).checked
                  )
                  .then(() => {
                    setData((prev) => {
                      if (prev) {
                        return prev.filter((config) => config.key !== key);
                      }

                      return prev;
                    });
                    resetForm();
                  });
              }
            },
            () => setCurrentlyEditing(null)
          )
        : getReadonlyRow(key, value, description, type, showActions, () =>
            enterEditMode(key, value, description, type)
          );
    });

    if (addingNew) {
      rowData.push(
        getNewRow(
          currentRowData.key,
          currentRowData.value,
          currentRowData.description,
          (newKey) =>
            setCurrentRowData((prev) => {
              return { ...prev, key: newKey };
            }),
          (newValue) =>
            setCurrentRowData((prev) => {
              return { ...prev, value: newValue };
            }),
          (newDescription) =>
            setCurrentRowData((prev) => {
              return { ...prev, description: newDescription };
            }),
          resetForm,
          () => {
            api
              .createNewConfig(
                currentRowData.key,
                currentRowData.value,
                currentRowData.description
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
          headerData={["Key", "Value", "Description", "Type", "Actions"]}
          rowData={rowData}
          columnWidths={[3, 5, 5, 1, 2]}
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
