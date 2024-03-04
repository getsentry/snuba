import React, { useEffect, useState } from "react";

import { Table, createCustomTableStyles } from "../table";
import { COLORS } from "../theme";
import Client from "../api_client";
import { AllocationPolicy, AllocationPolicyConfig } from "./types";
import { containerStyle, linkStyle, paragraphStyle } from "./styles";
import { getReadonlyRow } from "./row_data";
import EditConfigModal from "./edit_config_modal";
import AddConfigModal from "./add_config_modal";

function getTableColor(configs: AllocationPolicyConfig[]): string {
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

function AllocationPolicyConfigs(props: {
  api: Client;
  storage: string;
  policy: AllocationPolicy;
}) {
  const { api, storage, policy } = props;

  const [configs, setConfigs] = useState<AllocationPolicyConfig[]>([]);

  useEffect(() => {
    policy.configs.sort();
    setConfigs(policy.configs);
  }, [policy]);

  const [currentlyEditing, setCurrentlyEditing] = useState(false);
  const [currentConfig, setCurrentConfig] = useState<AllocationPolicyConfig>({
    name: "",
    value: "",
    description: "",
    type: "",
    params: {},
  });
  const [addingNew, setAddingNew] = useState(false);

  function enterEditMode(config: AllocationPolicyConfig) {
    setCurrentlyEditing(true);
    setCurrentConfig(config);
  }

  function deleteConfig(toDelete: AllocationPolicyConfig) {
    api
      .deleteAllocationPolicyConfig(
        storage,
        policy.policy_name,
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

  function saveConfig(config: AllocationPolicyConfig) {
    api
      .setAllocationPolicyConfig(
        storage,
        policy.policy_name,
        config.name,
        config.value,
        config.params
      )
      .catch((err) => {
        window.alert(err);
      });
  }

  function addConfig(config: AllocationPolicyConfig) {
    saveConfig(config);
    setConfigs((prev) => [...prev, config]);
  }

  return (
    <>
      <link
        rel="stylesheet"
        href="https://cdn.jsdelivr.net/npm/bootstrap@5.2.3/dist/css/bootstrap.min.css"
        integrity="sha384-rbsA2VBKQhggwzxH7pPCaAqO46MgnOM80zW1RWuH61DGLwZJEdK2Kadq2F9CUG65"
        crossOrigin="anonymous"
      />
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
        optionalConfigDefinitions={policy.optional_config_definitions}
        saveConfig={addConfig}
      />
      <div style={containerStyle}>
        <p>{policy.policy_name}</p>
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
            headerStyle: { backgroundColor: getTableColor(policy.configs) },
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
            headerStyle: { backgroundColor: getTableColor(policy.configs) },
          })}
        />
        {!addingNew && policy.optional_config_definitions.length != 0 && (
          <a onClick={() => setAddingNew(true)} style={linkStyle}>
            add new
          </a>
        )}
      </div>
      <br />
    </>
  );
}

export { AllocationPolicyConfigs, getTableColor };
