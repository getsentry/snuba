import React, { useEffect, useState } from "react";
import Client from "SnubaAdmin/api_client";
import { AllocationPolicyConfigs } from "SnubaAdmin/capacity_management/allocation_policy";
import { AllocationPolicy } from "SnubaAdmin/capacity_management/types";
import { CustomSelect, getParamFromStorage } from "SnubaAdmin/select";
import { COLORS } from "SnubaAdmin/theme";

function CapacityManagement(props: { api: Client }) {
  const { api } = props;

  const [storages, setStorages] = useState<string[]>([]);
  const [selectedStorage, setStorage] = useState<string | undefined>();
  const [allocationPolicies, setAllocationPolicies] = useState<
    AllocationPolicy[]
  >([]);

  useEffect(() => {
    api.getStoragesWithAllocationPolicies().then((res) => {
      setStorages(res);
      const previousStorage = getParamFromStorage("storage");
      if (previousStorage) {
        selectStorage(previousStorage);
      }
    });
  }, []);

  function selectStorage(storage: string) {
    setStorage(storage);
    loadAllocationPolicies(storage);
  }

  function loadAllocationPolicies(storage: string) {
    api
      .getAllocationPolicies(storage)
      .then((res) => {
        setAllocationPolicies(res);
      })
      .catch((err) => {
        window.alert(err);
      });
  }

  function renderPolicies(policies: AllocationPolicy[]) {
    if (!selectedStorage) {
      return <p>Storage not selected.</p>;
    }
    if (policies.length == 0) {
      return null;
    }
    return (
      <div>
        <p style={policyTypeStyle}>
          Policy Type: {policies[0].query_type.toUpperCase()}
        </p>
        {policies.map((policy: AllocationPolicy) => (
          <AllocationPolicyConfigs
            api={api}
            storage={selectedStorage}
            policy={policy}
            key={selectedStorage + policy.policy_name}
          />
        ))}
      </div>
    );
  }

  return (
    <div>
      <p>
        Storage:
        <CustomSelect
          value={selectedStorage || ""}
          onChange={selectStorage}
          name="storage"
          options={storages}
        />
      </p>

      {renderPolicies(
        allocationPolicies.filter((policy) => policy.query_type == "select")
      )}
      {renderPolicies(
        allocationPolicies.filter((policy) => policy.query_type == "delete")
      )}
    </div>
  );
}

const policyTypeStyle = {
  fontSize: 18,
  fontWeight: 600,
  color: COLORS.HEADER_TEXT,
  backgroundColor: COLORS.TEXT_LIGHTER,
  maxWidth: "100%",
  margin: "10px 0px",
  padding: "5px",
};

export default CapacityManagement;
