import React, { useEffect, useState } from "react";
import { ActionIcon, Button, Group, Tooltip } from "@mantine/core";
import {
  IconBraces,
  IconCheck,
  IconCopy,
  IconFileSpreadsheet,
  IconFileText,
  IconLink,
  IconMarkdown,
} from "@tabler/icons-react";

// The Clipboard API only exists on secure pages (https or localhost), so
// admin served over plain http falls back to the older copy command.
function copyText(text: string): Promise<void> {
  if (window.navigator.clipboard) {
    try {
      return Promise.resolve(window.navigator.clipboard.writeText(text));
    } catch (err) {
      return Promise.reject(err);
    }
  }
  const textarea = document.createElement("textarea");
  textarea.value = text;
  textarea.style.position = "fixed";
  textarea.style.opacity = "0";
  document.body.appendChild(textarea);
  textarea.select();
  const copied = document.execCommand("copy");
  document.body.removeChild(textarea);
  return copied ? Promise.resolve() : Promise.reject(new Error("copy failed"));
}

// An icon that copies on click and briefly turns into a check.
function CopyIcon(props: {
  label: string;
  icon: React.ReactNode;
  text: string;
}) {
  const [copied, setCopied] = useState(false);

  useEffect(() => {
    if (!copied) {
      return;
    }
    const timer = setTimeout(() => setCopied(false), 1500);
    return () => clearTimeout(timer);
  }, [copied]);

  return (
    <Tooltip label={copied ? "Copied" : props.label} withArrow>
      <ActionIcon
        variant="default"
        aria-label={props.label}
        onClick={() =>
          copyText(props.text).then(
            () => setCopied(true),
            (err) => console.error("Copy failed: ", err)
          )
        }
      >
        {copied ? <IconCheck size={16} /> : props.icon}
      </ActionIcon>
    </Tooltip>
  );
}

function QueryResultCopier(props: {
  rawInput?: string;
  jsonInput?: string;
  csvInput?: string;
  // Tab-separated, which spreadsheets and docs paste as a table.
  tableInput?: string;
  // A Markdown table, for pasting into docs, PRs or chat.
  markdownInput?: string;
  linkInput?: string;
  // Icons with tooltips instead of labelled buttons, for tight spaces.
  compact?: boolean;
}) {
  if (props.compact) {
    return (
      <Group spacing="xs">
        {props.tableInput && (
          <CopyIcon
            label="Copy as table"
            icon={<IconCopy size={16} />}
            text={props.tableInput}
          />
        )}
        {props.markdownInput && (
          <CopyIcon
            label="Copy as Markdown"
            icon={<IconMarkdown size={16} />}
            text={props.markdownInput}
          />
        )}
        {props.rawInput && (
          <CopyIcon
            label="Copy Raw"
            icon={<IconFileText size={16} />}
            text={props.rawInput}
          />
        )}
        {props.jsonInput && (
          <CopyIcon
            label="Copy JSON"
            icon={<IconBraces size={16} />}
            text={props.jsonInput}
          />
        )}
        {props.csvInput && (
          <CopyIcon
            label="Copy CSV"
            icon={<IconFileSpreadsheet size={16} />}
            text={props.csvInput}
          />
        )}
        {props.linkInput && (
          <CopyIcon
            label="Share query"
            icon={<IconLink size={16} />}
            text={props.linkInput}
          />
        )}
      </Group>
    );
  }

  return <Group>
    {props.rawInput && (
      <Button onClick={() => copyText(props.rawInput || "")}>
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
    {props.linkInput && (
      <Button onClick={() => copyText(props.linkInput || "")}>
        Share query
      </Button>
    )}
  </Group>;
}

export default QueryResultCopier;
