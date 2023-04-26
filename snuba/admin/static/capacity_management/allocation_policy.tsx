import React, { useEffect, useState } from "react";
import Client from "../api_client";
import { AllocationPolicyConfig } from "./types";

function AllocationPolicyConfigs(props: { api: Client; storage: string }) {
  const { api, storage } = props;

  const [configs, setConfigs] = useState<AllocationPolicyConfig[]>([]);

  useEffect(() => {
    props.api.getAllocationPolicyConfigs(storage).then((res) => {
      setConfigs(res);
    });
  }, [storage]);

  function configForm(config: AllocationPolicyConfig) {
    return <p key={config.key}>Config key: {config.key}</p>;
  }

  return (
    <div>
      {configs ? (
        configs.map((config) => configForm(config))
      ) : (
        <p>No Configs</p>
      )}
    </div>
  );
}

export default AllocationPolicyConfigs;
