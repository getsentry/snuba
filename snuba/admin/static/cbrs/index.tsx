import React, { useEffect, useState } from "react";
import Client from "SnubaAdmin/api_client";
import { AllocationPolicyConfigs } from "SnubaAdmin/capacity_management/allocation_policy";
import { AllocationPolicy } from "SnubaAdmin/capacity_management/types";
import { CustomSelect, getParamFromStorage } from "SnubaAdmin/select";
import { COLORS } from "SnubaAdmin/theme";

function CapacityBasedRoutingSystem(props: { api: Client }) {
  const { api } = props;

  const [strategies, setStrategies] = useState<string[]>([]);
  const [selectedStrategy, setStrategy] = useState<string | undefined>();
  const [allocationPolicies, setAllocationPolicies] = useState<
    AllocationPolicy[]
  >([]);

  useEffect(() => {
    api.getRoutingStrategies().then((res) => {
      setStrategies(res);
      const previousStrategy = getParamFromStorage("strategy");
      if (previousStrategy) {
        selectStrategy(previousStrategy);
      }
    });
  }, []);

  function selectStrategy(strategy: string) {
    setStrategy(strategy);
    loadAllocationPolicies(strategy);
  }

  function loadAllocationPolicies(strategy: string) {
    api
      .getAllocationPolicies({ type: "strategy", name: strategy })
      .then((res) => {
        setAllocationPolicies(res);
      })
      .catch((err) => {
        window.alert(err);
      });
  }

  function renderPolicies(policies: AllocationPolicy[]) {
    if (!selectedStrategy) {
      return <p>Strategy not selected.</p>;
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
            entity={{ type: "strategy", name: selectedStrategy }}
            policy={policy}
            key={selectedStrategy + policy.policy_name}
          />
        ))}
      </div>
    );
  }

  return (
    <div>
      <p>
        Strategy:
        <CustomSelect
          value={selectedStrategy || ""}
          onChange={selectStrategy}
          name="strategy"
          options={strategies}
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

export default CapacityBasedRoutingSystem;
