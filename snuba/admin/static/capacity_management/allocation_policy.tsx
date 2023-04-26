import React, { useEffect, useState } from "react";

import { Table } from "../table";
import Client from "../api_client";
import { AllocationPolicyConfig } from "./types";
import { containerStyle, paragraphStyle } from "./styles";
import { getReadonlyRow } from "./row_data";

function AllocationPolicyConfigs(props: { api: Client; storage: string }) {
  const { api, storage } = props;

  const [configs, setConfigs] = useState<AllocationPolicyConfig[]>([]);

  useEffect(() => {
    api.getAllocationPolicyConfigs(storage).then((res) => {
      setConfigs(res);
    });
  }, [storage]);

  return (
    <div style={containerStyle}>
      <p style={paragraphStyle}>These are the current configurations.</p>
      <Table
        headerData={["Key", "Value", "Description", "Type", "Actions"]}
        rowData={configs.map((config) =>
          getReadonlyRow(
            config.key,
            config.value,
            config.description,
            config.type,
            false,
            () => console.log("editing")
          )
        )}
        columnWidths={[3, 5, 5, 1, 2]}
      />
    </div>
  );
}

export default AllocationPolicyConfigs;
