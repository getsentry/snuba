import React from "react";
import { AllocationPolicyConfig, RowData } from "./types";
import Button from "react-bootstrap/Button";

function getReadonlyRow(
  config: AllocationPolicyConfig,
  edit: () => void
): RowData {
  return [
    <code style={{ wordBreak: "break-all", color: "black" }}>
      {config.key}
    </code>,
    <code style={{ wordBreak: "break-all", color: "black" }}>
      {Object.keys(config.params).length
        ? JSON.stringify(config.params)
        : "N/A"}
    </code>,
    <code style={{ wordBreak: "break-all", color: "black" }}>
      {config.value}
    </code>,
    config.description,
    config.type,
    <Button variant="outline-secondary" onClick={() => edit()}>
      edit
    </Button>,
  ];
}

export { getReadonlyRow };
