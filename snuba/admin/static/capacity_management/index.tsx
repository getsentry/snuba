import React, { useEffect, useState } from "react";
import Client from "../api_client";
import { selectStyle } from "./styles";
import AllocationPolicyConfigs from "./allocation_policy";
import { AllocationPolicy } from "./types";

function CapacityManagement(props: { api: Client }) {
  const { api } = props;

  const [storages, setStorages] = useState<string[]>([]);
  const [selectedStorage, setStorage] = useState<string>();
  const [allocationPolicies, setAllocationPolicies] = useState<
    AllocationPolicy[]
  >([]);

  useEffect(() => {
    api.getStoragesWithAllocationPolicies().then((res) => {
      setStorages(res);
    });
  }, []);

  function selectStorage(storage: string) {
    setStorage(storage);
    loadAllocationPolicies(storage);
  }

  function loadAllocationPolicies(storage: string) {
    api
      .getAllocationPolicyConfigs(storage)
      .then((res) => {
        console.log(res);
        setAllocationPolicies(res);
      })
      .catch((err) => {
        window.alert(err);
      });
  }

  return (
    <div>
      <p>
        Storage:
        <select
          value={selectedStorage || ""}
          onChange={(evt) => selectStorage(evt.target.value)}
          style={selectStyle}
        >
          <option disabled value="">
            Select a storage
          </option>
          {storages.map((storage_name) => (
            <option key={storage_name} value={storage_name}>
              {storage_name}
            </option>
          ))}
        </select>
      </p>

      {selectedStorage && allocationPolicies ? (
        allocationPolicies.map((policy: AllocationPolicy) => (
          <>
            <AllocationPolicyConfigs
              api={api}
              storage={selectedStorage}
              policy={policy}
              key={selectedStorage + policy.policy_name}
            />
            <br />
          </>
        ))
      ) : (
        <p>Storage not selected.</p>
      )}
    </div>
  );
}

export default CapacityManagement;
