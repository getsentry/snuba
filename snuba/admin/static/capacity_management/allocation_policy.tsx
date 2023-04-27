import React, { useEffect, useState } from "react";

import { Table } from "../table";
import Client from "../api_client";
import { AllocationPolicyConfig } from "./types";
import { containerStyle, linkStyle, paragraphStyle } from "./styles";
import { getReadonlyRow } from "./row_data";

function AllocationPolicyConfigs(props: { api: Client; storage: string }) {
  const { api, storage } = props;

  const [configs, setConfigs] = useState<AllocationPolicyConfig[]>([]);

  const [currentlyEditing, setCurrentlyEditing] = useState<string | null>(null);
  const [addingNew, setAddingNew] = useState(false);

  useEffect(() => {
    api.getAllocationPolicyConfigs(storage).then((res) => {
      setConfigs(res);
    });
  }, [storage]);

  function addNewConfig() {
    setCurrentlyEditing(null);
    setAddingNew(true);
    api
      .getAllocationPolicyParametrizedConfigDefinitions(storage)
      .then((res) => {
        console.log(res);
      });
  }

  return (
    <div style={containerStyle}>
      <p style={paragraphStyle}>These are the current configurations.</p>
      <Table
        headerData={["Key", "Params", "Value", "Description", "Type"]}
        rowData={configs.map((config) => getReadonlyRow(config))}
        columnWidths={[3, 2, 5, 5, 1]}
      />
      {!addingNew && !currentlyEditing && (
        <a onClick={addNewConfig} style={linkStyle}>
          add new
        </a>
      )}
    </div>
  );
}

export default AllocationPolicyConfigs;
