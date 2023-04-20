import React, { useEffect, useState } from "react";
import Client from "../api_client";
import RuntimeConfig from "../runtime_config";
import { AllocationPolicy } from "./types";

function CapacityManagement(props: { api: Client }) {
  const { api } = props;

  const [allocationPolicies, setPolicies] = useState<AllocationPolicy[]>([]);

  useEffect(() => {
    props.api.getAllocationPolicies().then((res) => {
      setPolicies(res);
    });
  }, []);

  return (
    <div>
      <p>Policies:</p>
      {allocationPolicies.map((policy) => (
        <p>{policy.storage_name + ": " + policy.allocation_policy}</p>
      ))}{" "}
      <p>Configs:</p>
      {RuntimeConfig({ api: api })}
    </div>
  );
}

export default CapacityManagement;
