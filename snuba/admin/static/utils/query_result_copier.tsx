import React from "react";
import { Button, Group } from "@mantine/core";

function QueryResultCopier(props: {
  rawInput?: string;
  jsonInput?: string;
  csvInput?: string;
  // Small, quiet buttons with short labels, for tight spaces.
  compact?: boolean;
}) {
  function copyText(text: string) {
    window.navigator.clipboard.writeText(text);
  }

  const buttonProps = props.compact
    ? { size: "xs" as const, variant: "default" as const }
    : {};
  const label = (format: string) =>
    props.compact ? `Copy ${format}` : `Copy to clipboard (${format})`;

  return <Group spacing={props.compact ? "xs" : undefined}>
    {props.rawInput && (
      <Button
        {...buttonProps}
        onClick={() => copyText(props.rawInput || "")}
      >
        {label("Raw")}
      </Button>
    )}
    {props.jsonInput && (
      <Button {...buttonProps} onClick={() => copyText(props.jsonInput || "")}>
        {label("JSON")}
      </Button>
    )}
    {props.csvInput && (
      <Button {...buttonProps} onClick={() => copyText(props.csvInput || "")}>
        {label("CSV")}
      </Button>
    )}
  </Group>;
}

export default QueryResultCopier;
