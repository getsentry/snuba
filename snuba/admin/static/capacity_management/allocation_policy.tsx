import React, { useEffect, useState } from "react";

import { Table, createCustomTableStyles } from "../table";
import { COLORS } from "SnubaAdmin/theme";
import Client from "SnubaAdmin/api_client";
import {
  AllocationPolicy,
  Configuration,
  Entity,
  ConfigurableComponent,
} from "SnubaAdmin/capacity_management/types";
import { containerStyle, linkStyle, paragraphStyle } from "SnubaAdmin/capacity_management/styles";
import { getReadonlyRow } from "SnubaAdmin/capacity_management/row_data";
import EditConfigModal from "SnubaAdmin/capacity_management/edit_config_modal";
import AddConfigModal from "SnubaAdmin/capacity_management/add_config_modal";

function getTableColor(configs: Configuration[]): string {
  let policyIsActive = false;
  let policyIsEnforced = false;
  configs.forEach((config) => {
    if (config.name == "is_active") {
      if (parseInt(config.value) === 1) {
        policyIsActive = true;
      } else {
        policyIsActive = false;
      }
    }
    if (config.name == "is_enforced") {
      if (parseInt(config.value) === 1) {
        policyIsEnforced = true;
      } else {
        policyIsEnforced = false;
      }
    }
  });
  if (policyIsActive && policyIsEnforced) {
    return COLORS.SNUBA_BLUE;
  } else if (policyIsActive && !policyIsEnforced) {
    return "orange";
  } else {
    return "gray";
  }
}

function Configurations(props: {
  api: Client;
  entity: Entity;
  configurable_component: ConfigurableComponent;
}) {
  const { api, entity, configurable_component } = props;

  const [configs, setConfigs] = useState<Configuration[]>([]);

  useEffect(() => {
    configurable_component.configs.sort();
    setConfigs(configurable_component.configs);
  }, [configurable_component]);

  const [currentlyEditing, setCurrentlyEditing] = useState(false);
  const [currentConfig, setCurrentConfig] = useState<Configuration>({
    name: "",
    value: "",
    description: "",
    type: "",
    params: {},
  });
  const [addingNew, setAddingNew] = useState(false);

  function enterEditMode(config: Configuration) {
    setCurrentlyEditing(true);
    setCurrentConfig(config);
  }

  function deleteConfig(toDelete: Configuration) {
    api
      .deleteConfiguration(
        entity,
        configurable_component.name,
        toDelete.name,
        toDelete.params
      )
      .then(() => {
        setConfigs((prev) =>
          Object.keys(currentConfig.params).length
            ? prev.filter((config) => config != toDelete)
            : prev
        );
      })
      .catch((err) => {
        window.alert(err);
      });
  }

  function saveConfig(config: Configuration) {
    api
      .setConfiguration(
        entity,
        configurable_component.name,
        config.name,
        config.value,
        config.params
      )
      .catch((err) => {
        window.alert(err);
      });
  }

  function addConfig(config: Configuration) {
    saveConfig(config);
    setConfigs((prev) => [...prev, config]);
  }

  return (
    <>
      <EditConfigModal
        currentlyEditing={currentlyEditing}
        currentConfig={currentConfig}
        setCurrentlyEditing={setCurrentlyEditing}
        deleteConfig={deleteConfig}
        saveConfig={saveConfig}
      />
      <AddConfigModal
        currentlyAdding={addingNew}
        setCurrentlyAdding={setAddingNew}
        optionalConfigDefinitions={configurable_component.optional_config_definitions}
        saveConfig={addConfig}
      />
      <div style={containerStyle}>
        <p>{configurable_component.name}</p>
        <p style={paragraphStyle}>These are the global configurations.</p>
        <Table
          headerData={["Key", "Value", "Description", "Type", "Actions"]}
          rowData={configs
            .filter((configs) => Object.keys(configs.params).length == 0)
            .map((config) =>
              getReadonlyRow(config, () => enterEditMode(config))
            )
            .map((row_data) => [
              row_data.name,
              row_data.value,
              row_data.description,
              row_data.type,
              row_data.edit,
            ])}
          columnWidths={[3, 2, 5, 1, 1]}
          customStyles={createCustomTableStyles({
            headerStyle: { backgroundColor: getTableColor(configurable_component.configs) },
          })}
        />
        <p style={paragraphStyle}>
          These are the tenant specific configurations.
        </p>
        <Table
          headerData={[
            "Key",
            "Params",
            "Value",
            "Description",
            "Type",
            "Actions",
          ]}
          rowData={configs
            .filter((config) => Object.keys(config.params).length > 0)
            .map((config) =>
              getReadonlyRow(config, () => enterEditMode(config))
            )
            .map((row_data) => [
              row_data.name,
              row_data.params,
              row_data.value,
              row_data.description,
              row_data.type,
              row_data.edit,
            ])}
          columnWidths={[3, 3, 2, 5, 1, 1]}
          customStyles={createCustomTableStyles({
            headerStyle: { backgroundColor: getTableColor(configurable_component.configs) },
          })}
        />
        {!addingNew && configurable_component.optional_config_definitions.length != 0 && (
          <a onClick={() => setAddingNew(true)} style={linkStyle}>
            add new
          </a>
        )}
      </div>
      <br />
    </>
  );
}

export { Configurations, getTableColor };
