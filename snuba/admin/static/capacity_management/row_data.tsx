import React from "react";
import { AllocationPolicyConfig, RowData } from "./types";

function getReadonlyRow(config: AllocationPolicyConfig): RowData {
  return [
    <code style={{ wordBreak: "break-all" }}>{config.key}</code>,
    <code style={{ wordBreak: "break-all" }}>
      {Object.keys(config.params).length
        ? JSON.stringify(config.params)
        : "N/A"}
    </code>,
    <code style={{ wordBreak: "break-all" }}>{config.value}</code>,
    config.description,
    config.type,
  ];
}

export { getReadonlyRow };
