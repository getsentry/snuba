import React, { useEffect, useState } from "react";
import Client from "SnubaAdmin/api_client";
import { Configurations } from "SnubaAdmin/capacity_management/allocation_policy";
import { PolicyRenderer, policyTypeStyle } from "SnubaAdmin/capacity_management/policy_renderer";
import { StrategyData } from "SnubaAdmin/shared/types";
import { ConfigurableComponentRenderer } from "SnubaAdmin/shared/render_configurable_component";

function CapacityBasedRoutingSystem(props: { api: Client }) {
  return (
    <ConfigurableComponentRenderer<StrategyData>
      api={props.api}
      resourceType="strategy"
      getOptions={() => props.api.getRoutingStrategies()}
      loadData={(strategy) => props.api.getRoutingStrategyConfigs(strategy)}
      renderContent={(strategyConfigs, selectedStrategy) => {
        if (!selectedStrategy) return <p>Strategy not selected.</p>;
        if (strategyConfigs === undefined) return <p>No strategy configurations found.</p>;

        return (
          <div>
            <p style={policyTypeStyle}>Strategy Configurations</p>
            <Configurations
              api={props.api}
              configurableComponentData={strategyConfigs}
            />
            <PolicyRenderer
              api={props.api}
              policies={strategyConfigs.policies_data}
              resourceIdentifier={selectedStrategy}
              resourceType="strategy"
            />
          </div>
        );
      }}
    />
  );
}

export default CapacityBasedRoutingSystem;
