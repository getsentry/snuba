import React, { useEffect, useState } from "react";
import Client from "SnubaAdmin/api_client";
import { AllocationPolicy } from "SnubaAdmin/capacity_management/types";
import { PolicyRenderer } from "SnubaAdmin/capacity_management/policy_renderer";
import { CustomSelect, getParamFromStorage } from "SnubaAdmin/select";

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

      <PolicyRenderer
        api={api}
        policies={allocationPolicies}
        selectedItem={selectedStorage}
        itemType="storage"
      />
    </div>
  );
}

export default CapacityManagement;
