import React, { useEffect, useState } from "react";
import Client from "SnubaAdmin/api_client";
import { ConfigurableComponentConfigurations } from "SnubaAdmin/configurable_component/configurable_component_configurations";
import { PolicyRenderer, policyTypeStyle } from "SnubaAdmin/capacity_management/policy_renderer";
import { StrategyData } from "SnubaAdmin/configurable_component/types";
import { ConfigurableComponentDropdownRenderer } from "SnubaAdmin/configurable_component/render_configurable_component";

function CapacityBasedRoutingSystem(props: { api: Client }) {
  return (
    <ConfigurableComponentDropdownRenderer<StrategyData>
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
            <ConfigurableComponentConfigurations
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
