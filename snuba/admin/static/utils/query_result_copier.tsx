import React from "react";
import { Button, Group } from "@mantine/core";

function QueryResultCopier(props: {
  rawInput?: string;
  jsonInput?: string;
  csvInput?: string;
}) {
  function copyText(text: string) {
    window.navigator.clipboard.writeText(text);
  }

  return <Group>
    {props.rawInput && (
      <Button
        onClick={() => copyText(props.rawInput || "")}
      >
        Copy to clipboard (Raw)
      </Button>
    )}
    {props.jsonInput && (
      <Button onClick={() => copyText(props.jsonInput || "")}>
        Copy to clipboard (JSON)
      </Button>
    )}
    {props.csvInput && (
      <Button onClick={() => copyText(props.csvInput || "")}>
        Copy to clipboard (CSV)
      </Button>
    )}
  </Group>;
}

export default QueryResultCopier;
