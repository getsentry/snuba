import React, { useEffect, useState } from "react";

import { Table } from "../table";
import Client from "../api_client";
import {
  AllocationPolicyConfig,
  AllocationPolicyParametrizedConfigDefinition,
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
    key: "",
    value: "",
    description: "",
    type: "",
    params: {},
  });
  const [addingNew, setAddingNew] = useState(false);

  const [parameterizedConfigDefinitions, setDefinitions] = useState<
    AllocationPolicyParametrizedConfigDefinition[]
  >([]);

  useEffect(() => {
    api.getAllocationPolicyConfigs(storage).then((res) => {
      setConfigs(res);
    });
  }, [storage]);

  useEffect(() => {
    api
      .getAllocationPolicyParametrizedConfigDefinitions(storage)
      .then((res) => {
        setDefinitions(res);
      });
  }, []);

  function enterEditMode(config: AllocationPolicyConfig) {
    setCurrentlyEditing(true);
    setCurrentConfig(config);
  }

  function deleteConfig(config: AllocationPolicyConfig) {
    console.log("deleting " + config.key);
  }

  function saveConfig(config: AllocationPolicyConfig) {
    console.log("saving...");
    console.log(config);
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
        api={api}
        currentlyEditing={currentlyEditing}
        currentConfig={currentConfig}
        setCurrentlyEditing={setCurrentlyEditing}
        deleteConfig={deleteConfig}
        saveConfig={saveConfig}
      />
      <AddConfigModal
        api={api}
        currentlyAdding={addingNew}
        setCurrentlyAdding={setAddingNew}
        parameterizedConfigDefinitions={parameterizedConfigDefinitions}
        saveConfig={saveConfig}
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
        {!addingNew && parameterizedConfigDefinitions.length != 0 && (
          <a onClick={() => setAddingNew(true)} style={linkStyle}>
            add new
          </a>
        )}
      </div>
    </>
  );
}

export default AllocationPolicyConfigs;
