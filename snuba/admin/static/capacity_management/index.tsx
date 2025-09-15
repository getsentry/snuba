import React from "react";
import Client from "SnubaAdmin/api_client";
import { AllocationPolicy } from "SnubaAdmin/configurable_component/types";
import { PolicyRenderer } from "SnubaAdmin/capacity_management/policy_renderer";
import { ConfigurableComponentDropdownRenderer } from "SnubaAdmin/configurable_component/render_configurable_component";

function CapacityManagement(props: { api: Client }) {
  return (
    <ConfigurableComponentDropdownRenderer<AllocationPolicy[]>
      api={props.api}
      resourceType="storage"
      getOptions={() => props.api.getStoragesWithAllocationPolicies()}
      loadData={(storage) => props.api.getAllocationPolicies(storage)}
      renderContent={(policies, selectedStorage) => (
        <PolicyRenderer
          api={props.api}
          policies={policies || []}
          resourceIdentifier={selectedStorage}
          resourceType="storage"
        />
      )}
    />
  );
}

export default CapacityManagement;
