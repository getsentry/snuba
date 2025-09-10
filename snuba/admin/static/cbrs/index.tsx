import React, { useEffect, useState } from "react";
import Client from "SnubaAdmin/api_client";
import { Configurations } from "SnubaAdmin/capacity_management/allocation_policy";
import { AllocationPolicy, Configuration, ConfigurableComponentData, StrategyData } from "SnubaAdmin/capacity_management/types";
import { PolicyRenderer } from "SnubaAdmin/capacity_management/policy_renderer";
import { CustomSelect, getParamFromStorage } from "SnubaAdmin/select";
import { COLORS } from "SnubaAdmin/theme";

function CapacityBasedRoutingSystem(props: { api: Client }) {
  const { api } = props;

  const [strategies, setStrategies] = useState<string[]>([]);
  const [selectedStrategy, setStrategy] = useState<string | undefined>();
  const [strategyConfigs, setStrategyConfigs] = useState<StrategyData>();

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
    loadStrategyConfigs(strategy);
  }

  function loadStrategyConfigs(strategy: string) {
    api
      .getRoutingStrategyConfigs(strategy)
      .then((res) => {
        setStrategyConfigs(res);
      })
      .catch((err) => {
        window.alert(err);
      });
  }

  function renderStrategy() {
    if (!selectedStrategy) {
      return <p>Strategy not selected.</p>;
    }
    if (strategyConfigs === undefined) {
      return <p>No strategy configurations found.</p>;
    }

    return (
      <div>
        <p style={policyTypeStyle}>Strategy Configurations</p>
        <Configurations
          api={api}
          configurableComponentData={strategyConfigs}
        />

        <PolicyRenderer
          api={api}
          policies={strategyConfigs.policies_data}
          selectedItem={selectedStrategy}
          itemType="strategy"
        />
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

      {renderStrategy()}
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
