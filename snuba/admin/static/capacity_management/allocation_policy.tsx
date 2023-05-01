import React, { useEffect, useState } from "react";

import { Table } from "../table";
import Client from "../api_client";
import { AllocationPolicyConfig } from "./types";
import { containerStyle, linkStyle, paragraphStyle } from "./styles";
import { getReadonlyRow } from "./row_data";
import ConfigModal from "./edit_config_modal";

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

  useEffect(() => {
    api.getAllocationPolicyConfigs(storage).then((res) => {
      setConfigs(res);
    });
  }, [storage]);

  function addNewConfig() {
    setCurrentlyEditing(true);
    setAddingNew(true);
    api
      .getAllocationPolicyParametrizedConfigDefinitions(storage)
      .then((res) => {
        console.log(res);
      });
  }

  function enterEditMode(config: AllocationPolicyConfig) {
    setCurrentlyEditing(true);
    setCurrentConfig(config);
  }

  return (
    <>
      <ConfigModal
        api={api}
        currentlyEditing={currentlyEditing}
        currentConfig={currentConfig}
        setCurrentlyEditing={setCurrentlyEditing}
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
        {!addingNew && !currentlyEditing && (
          <a onClick={addNewConfig} style={linkStyle}>
            add new
          </a>
        )}
      </div>
    </>
  );
}

export default AllocationPolicyConfigs;
