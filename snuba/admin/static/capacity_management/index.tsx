import React, { useEffect, useState } from "react";
import Client from "../api_client";
import { AllocationPolicy } from "./types";
import { selectStyle } from "./styles";
import AllocationPolicyConfigs from "./allocation_policy";

function CapacityManagement(props: { api: Client }) {
  const { api } = props;

  const [allocationPolicies, setPolicies] = useState<AllocationPolicy[]>([]);
  const [selectedStoragePolicy, selectStoragePolicy] =
    useState<AllocationPolicy>();

  useEffect(() => {
    api.getAllocationPolicies().then((res) => {
      setPolicies(res);
    });
  }, []);

  function selectStorage(storage: string) {
    allocationPolicies.map((policy) => {
      if (policy.storage_name == storage) {
        selectStoragePolicy(policy);
      }
    });
  }

  return (
    <div>
      <p>
        Storage:
        <select
          value={selectedStoragePolicy?.storage_name || ""}
          onChange={(evt) => selectStorage(evt.target.value)}
          style={selectStyle}
        >
          <option disabled value="">
            Select a storage
          </option>
          {allocationPolicies.map((policy) => (
            <option key={policy.storage_name} value={policy.storage_name}>
              {policy.storage_name}
            </option>
          ))}
        </select>
      </p>

      {selectedStoragePolicy ? (
        <div>
          <p>{selectedStoragePolicy.allocation_policy}</p>
          <AllocationPolicyConfigs
            api={api}
            storage={selectedStoragePolicy.storage_name}
          />
        </div>
      ) : (
        <p>Storage not selected.</p>
      )}
    </div>
  );
}

export default CapacityManagement;
