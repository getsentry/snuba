import React, { useEffect, useState } from "react";
import Client from "../api_client";
import RuntimeConfig from "../runtime_config";
import { AllocationPolicy } from "./types";
import { selectStyle } from "./styles";

function CapacityManagement(props: { api: Client }) {
  const { api } = props;

  const [allocationPolicies, setPolicies] = useState<AllocationPolicy[]>([]);
  const [storage, setStorage] = useState<string>();
  const [policy, setPolicy] = useState<string>();

  useEffect(() => {
    props.api.getAllocationPolicies().then((res) => {
      setPolicies(res);
    });
  }, []);

  function selectStorage(storage: string) {
    setStorage(storage);
    allocationPolicies.map((policy) => {
      if (policy.storage_name == storage) {
        setPolicy(policy.allocation_policy);
      }
    });
  }

  return (
    <div>
      <p>Storage:</p>

      <select
        value={storage || ""}
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

      {policy ? (
        <div>
          <p>{policy}</p>
          <p>Configs:</p>
          <RuntimeConfig api={api} />
        </div>
      ) : (
        <p>Storage not selected.</p>
      )}
    </div>
  );
}

export default CapacityManagement;
