import React, { useEffect, useState } from "react";

import { Table } from "../table";
import Client from "../api_client";
import {
  AllocationPolicyConfig,
  AllocationPolicyOptionalConfigDefinition,
} from "./types";
import { containerStyle, linkStyle, paragraphStyle } from "./styles";
import { getReadonlyRow } from "./row_data";
import EditConfigModal from "./edit_config_modal";
import AddConfigModal from "./add_config_modal";

function AllocationPolicyConfigs(props: { api: Client; storage: string }) {
  const { api, storage } = props;

  const [configs, setConfigs] = useState<AllocationPolicyConfig[]>([]);

  const [currentlyEditing, setCurrentlyEditing] = useState(false);
  const [currentConfig, setCurrentConfig] = useState<AllocationPolicyConfig>({
    name: "",
    value: "",
    description: "",
    type: "",
    params: {},
  });
  const [addingNew, setAddingNew] = useState(false);

  const [optionalConfigDefinitions, setDefinitions] = useState<
    AllocationPolicyOptionalConfigDefinition[]
  >([]);

  useEffect(() => {
    api
      .getAllocationPolicyConfigs(storage)
      .then((res) => {
        setConfigs(res);
      })
      .catch((err) => {
        window.alert(err);
      });
  }, [storage]);

  useEffect(() => {
    api
      .getAllocationPolicyOptionalConfigDefinitions(storage)
      .then((res) => {
        setDefinitions(res);
      })
      .catch((err) => {
        window.alert(err);
      });
  }, [storage]);

  function enterEditMode(config: AllocationPolicyConfig) {
    setCurrentlyEditing(true);
    setCurrentConfig(config);
  }

  function deleteConfig(toDelete: AllocationPolicyConfig) {
    api
      .deleteAllocationPolicyConfig(storage, toDelete.name, toDelete.params)
      .catch((err) => {
        window.alert(err);
      })
      .then(() => {
        setConfigs((prev) =>
          Object.keys(currentConfig.params).length
            ? prev.filter((config) => config != toDelete)
            : prev
        );
      });
  }

  function saveConfig(config: AllocationPolicyConfig) {
    api
      .setAllocationPolicyConfig(
        storage,
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
        optionalConfigDefinitions={optionalConfigDefinitions}
        saveConfig={addConfig}
      />
      <div style={containerStyle}>
        <p style={paragraphStyle}>These are the current configurations.</p>
        <Table
          headerData={[
            "Key",
            "Params",
            "Value",
            "Description",
            "Type",
            "Actions",
          ]}
          rowData={configs.map((config) =>
            getReadonlyRow(config, () => enterEditMode(config))
          )}
          columnWidths={[3, 3, 2, 5, 1, 1]}
        />
        {!addingNew && optionalConfigDefinitions.length != 0 && (
          <a onClick={() => setAddingNew(true)} style={linkStyle}>
            add new
          </a>
        )}
      </div>
    </>
  );
}

export default AllocationPolicyConfigs;
