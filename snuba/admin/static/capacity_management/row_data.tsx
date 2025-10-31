import React from "react";
import { Configuration, RowData } from "SnubaAdmin/configurable_component/types";
import Button from "react-bootstrap/Button";

function getReadonlyRow(
  config: Configuration,
  edit: () => void
): RowData {
  return {
    name: (
      <code style={{ wordBreak: "break-all", color: "black" }}>
        {config.name}
      </code>
    ),
    params: (
      <code style={{ wordBreak: "break-all", color: "black" }}>
        {Object.keys(config.params).length
          ? JSON.stringify(config.params)
          : "N/A"}
      </code>
    ),
    value: (
      <code style={{ wordBreak: "break-all", color: "black" }}>
        {config.value}
      </code>
    ),
    description: (
      <code
        style={{
          wordBreak: "normal",
          overflowWrap: "anywhere",
          color: "black",
        }}
      >
        {config.description}
      </code>
    ),
    type: config.type,
    edit: (
      <Button
        variant="outline-secondary"
        onClick={() => edit()}
        data-testid={config.name + "_edit"}
      >
        edit
      </Button>
    ),
  };
}

export { getReadonlyRow };
