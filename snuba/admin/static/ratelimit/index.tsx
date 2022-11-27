import React, { ChangeEvent, useState } from "react";

import { Table } from "../table";
import Client from "../api_client";
import {
  Config,
  ConfigKey,
  ConfigValue,
  ConfigType,
  ConfigDescription,
  ConfigDescriptions,
  RowData,
} from "./types";
import { getEditableRow, getReadonlyRow, getNewRow } from "./row_data";
import {
  containerStyle,
  defaultOptionStyle,
  linkStyle,
  paragraphStyle,
} from "./styles";

function RuntimeConfig(props: { api: Client }) {
  const { api } = props;

  // Original data from the API
  const [data, setData] = useState<
    | {
        key: ConfigKey;
        value: ConfigValue;
        description: ConfigDescription;
        type: ConfigType;
      }[]
    | null
  >(null);

  // Data after column filters are applied (dynamically changes)
  const [filteredData, setSearchFilteredData] = useState<
    | {
        key: ConfigKey;
        value: ConfigValue;
        description: ConfigDescription;
        type: ConfigType;
      }[]
    | null
  >(null);

  // All descriptions from API
  const [allDescriptions, setDescriptions] = useState<ConfigDescriptions>({});

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

  // All rate limit dimensions, parameters, and targets that are selectable on the filter dropdowns
  const [selectableDimensions, setSelectableDimensions] = useState<
    Array<string>
  >([]);
  const [selectableParameters, setSelectableParameters] = useState<
    Array<string>
  >([]);
  const [selectableTargets, setSelectableTargets] = useState<Array<string>>([]);

  // The current applied filters
  const [filterTerm, setFilterTerms] = useState<any>({
    dimension: "",
    parameter: "",
    target: "",
  });

  // All supported rate limiter dimensions and parameters in Snuba
  const availableDimensions = [
    "project_referrer",
    "project",
    "referrer",
    "organization",
  ];
  const availableParameters = ["per_second_limit", "concurrent_limit"];

  function resetCurrentRowData() {
    setCurrentRowData({ key: "", value: "", description: "", type: "string" });
  }

  // Load data if it was not previously loaded
  if (data === null) {
    fetchData();
  }

  function fetchData() {
    api.getConfigs().then((res) => {
      let data = filterOnlyRateLimitConfigs(res);
      populateFilterLists(data);
      setSearchFilteredData(data);
      setData(data);
    });
  }

  function filterOnlyRateLimitConfigs(configs: Array<Config>) {
    // Rate limit run time configs must include per_second_limit or concurrent_limit as substring
    return configs.filter((obj) => {
      return (
        obj.key.includes("per_second_limit") ||
        obj.key.includes("concurrent_limit")
      );
    });
  }

  function populateFilterLists(configs: Array<Config>) {
    // Populate filter dropdowns with options in data
    let seen_dimensions: Array<string> = [];
    let seen_parameters: Array<string> = [];
    let seen_targets: Array<string> = [];
    configs.forEach((obj) => {
      // Example key = "project_referrer_concurrent_limit_search_5588344"
      let key = obj.key;
      for (const dimension of availableDimensions) {
        if (key.includes(dimension)) {
          if (!seen_dimensions.includes(dimension)) {
            seen_dimensions.push(dimension);
          }
          key = key.replace(dimension, "");
          // Example key = "_concurrent_limit_search_5588344"
          break;
        }
      }
      for (const parameter of availableParameters) {
        if (key.includes(parameter)) {
          if (!seen_parameters.includes(parameter)) {
            seen_parameters.push(parameter);
          }
          key = key.replace(parameter, "");
          // Example key = "__search_5588344"
          break;
        }
      }
      key = key.substring(2);
      // Example key = "search_5588344"
      if (!seen_targets.includes(key)) {
        seen_targets.push(key);
      }
    });
    setSelectableDimensions(seen_dimensions);
    setSelectableParameters(seen_parameters);
    setSelectableTargets(seen_targets);
  }

  function filterSearch(
    event: ChangeEvent<HTMLSelectElement>,
    data: Array<Config>
  ) {
    // Update the filteredData table with the selected filters
    const searchTerm: string = event.target.value;
    const filterBy: string = event.target.name;
    Object.keys(filterTerm).forEach((key, index) => {
      if (key === filterBy) {
        filterTerm[filterBy] = searchTerm;
      }
    });
    setFilterTerms(filterTerm);

    let searchData = data
      .filter((obj) => {
        if (filterTerm.dimension === "project") {
          return (
            obj.key.includes(filterTerm.dimension) &&
            !obj.key.includes("project_referrer")
          );
        }
        return obj.key.includes(filterTerm.dimension);
      })
      .filter((obj) => {
        return obj.key.includes(filterTerm.parameter);
      })
      .filter((obj) => {
        return obj.key.includes(filterTerm.target);
      });
    setSearchFilteredData(searchData);
  }

  function fetchDescriptions() {
    api.getDescriptions().then((res) => {
      setDescriptions(res);
    });
  }

  function addNewConfig() {
    setCurrentlyEditing(null);
    setAddingNew(true);
    resetCurrentRowData();
    fetchDescriptions();
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

  function updateDescription(newDescription: string) {
    setCurrentRowData((prev) => {
      return { ...prev, description: newDescription };
    });
  }

  if (filteredData) {
    const rowData: RowData[] = filteredData.map((row) => {
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
            updateDescription,
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
                        const configs = prev.filter(
                          (config) => config.key !== key
                        );
                        populateFilterLists(configs);
                        setSearchFilteredData(configs);
                        return configs;
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
          (newKey) => {
            setCurrentRowData((prev) => {
              return { ...prev, key: newKey };
            });
            if (newKey in allDescriptions) {
              updateDescription(allDescriptions[newKey]);
            }
          },
          (newValue) =>
            setCurrentRowData((prev) => {
              return { ...prev, value: newValue };
            }),
          updateDescription,
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
                    populateFilterLists([...prev, res]);
                    setSearchFilteredData([...prev, res]);
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
        <h4>Manage rate limit configs.</h4>
        <div className="filter">
          <p>
            Filter: &nbsp;
            <label style={paragraphStyle}>
              <select
                name="dimension"
                onChange={(event) => filterSearch(event, data)}
              >
                <option value="" style={defaultOptionStyle}>
                  Dimension
                </option>
                {selectableDimensions.map((dimension) => (
                  <option value={dimension}>{dimension}</option>
                ))}
              </select>
            </label>
            <label style={paragraphStyle}>
              <select
                name="parameter"
                onChange={(event) => filterSearch(event, data)}
              >
                <option value="" style={defaultOptionStyle}>
                  Parameter
                </option>
                {selectableParameters.map((parameter) => (
                  <option value={parameter}>{parameter}</option>
                ))}
              </select>
            </label>
            <label style={paragraphStyle}>
              <select
                name="target"
                onChange={(event) => filterSearch(event, data)}
              >
                <option value="" style={defaultOptionStyle}>
                  Target
                </option>
                {selectableTargets.map((target) => (
                  <option value={target}>{target}</option>
                ))}
              </select>
            </label>
          </p>
        </div>
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
