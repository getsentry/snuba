import React from "react";
import Client from "SnubaAdmin/api_client";
import { Configurations } from "SnubaAdmin/capacity_management/allocation_policy";
import { AllocationPolicy } from "SnubaAdmin/configurable_component/types";
import { COLORS } from "SnubaAdmin/theme";

interface PolicyRendererProps {
  api: Client;
  policies: AllocationPolicy[];
  resourceIdentifier: string | undefined;
  resourceType: string;
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

export { policyTypeStyle };
export function PolicyRenderer({ api, policies, resourceIdentifier, resourceType }: PolicyRendererProps) {
  function renderPolicies(policies: AllocationPolicy[]) {
    if (!resourceIdentifier) {
      return <p>{resourceType} not selected.</p>;
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
          <Configurations
            api={api}
            configurableComponentData={policy}
            key={resourceIdentifier + policy.configurable_component_class_name}
          />
        ))}
      </div>
    );
  }

  return (
    <>
      {renderPolicies(policies.filter((policy) => policy.query_type == "select"))}
      {renderPolicies(policies.filter((policy) => policy.query_type == "delete"))}
    </>
  );
}
