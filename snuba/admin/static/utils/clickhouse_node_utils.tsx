import React from "react";
import { SelectItem, Alert, Group, Text, Collapse as MantineCollapse } from "@mantine/core";
import { IconChevronDown, IconChevronRight } from '@tabler/icons-react';
import { ClickhouseNodeData } from "SnubaAdmin/clickhouse_queries/types";

/**
 * Converts ClickhouseNodeData to SelectItem options for a specific storage.
 * Returns local nodes, distributed nodes (marked), and query nodes (if present).
 *
 * @param nodeData - Array of ClickhouseNodeData
 * @param storageName - The storage name to filter by
 * @returns Array of SelectItem options for host selection
 */
export function getHostsForStorage(
  nodeData: ClickhouseNodeData[],
  storageName: string | undefined
): SelectItem[] {
  if (!storageName) {
    return [];
  }

  const nodeInfo = nodeData.find((el) => el.storage_name === storageName);
  if (!nodeInfo) {
    return [];
  }

  // Map local nodes to select options
  const localHosts = nodeInfo.local_nodes.map((node) => ({
    value: `${node.host}:${node.port}`,
    label: `${node.host}:${node.port}`,
  }));

  // Map distributed nodes (excluding those that are also local) to select options
  const distHosts = nodeInfo.dist_nodes
    .filter((node) => !nodeInfo.local_nodes.includes(node))
    .map((node) => ({
      value: `${node.host}:${node.port}`,
      label: `${node.host}:${node.port} (distributed)`,
    }));

  const hosts = localHosts.concat(distHosts);

  // Add query node if present
  if (nodeInfo.query_node) {
    hosts.push({
      value: `${nodeInfo.query_node.host}:${nodeInfo.query_node.port}`,
      label: `${nodeInfo.query_node.host}:${nodeInfo.query_node.port} (query node)`,
    });
  }

  return hosts;
}

/**
 * Renders an error alert with optional collapsible stack trace.
 *
 * @param queryError - The error object to display, or null if no error
 * @param collapseOpened - Whether the stack trace collapse is open
 * @param setCollapseOpened - Function to toggle the collapse state
 * @returns JSX element with the error alert, or empty string if no error
 */
export function getErrorDomElement(
  queryError: Error | null,
  collapseOpened: boolean,
  setCollapseOpened: (value: boolean | ((prev: boolean) => boolean)) => void
): JSX.Element | string {
  if (queryError === null) {
    return "";
  }

  let title: string;
  let bodyDOM: JSX.Element | JSX.Element[];

  if (queryError.name === "Error" && queryError.message.includes("Stack trace:")) {
    // Extract and display stack trace in a collapsible section
    const split = queryError.message.indexOf("Stack trace:");
    title = queryError.message.slice(0, split);

    const stackTrace = queryError.message
      .slice(split + "Stack trace:".length)
      .split("\n")
      .map((line, idx) => (
        <React.Fragment key={idx}>
          {line}
          <br />
        </React.Fragment>
      ));

    bodyDOM = (
      <div>
        <Group spacing="xs" onClick={() => setCollapseOpened((o) => !o)} style={{ cursor: 'pointer' }}>
          {collapseOpened ? <IconChevronDown size={16} /> : <IconChevronRight size={16} />}
          <Text weight={500}>Stack Trace</Text>
        </Group>

        <MantineCollapse in={collapseOpened}>
          <Text mt="sm">{stackTrace}</Text>
        </MantineCollapse>
      </div>
    );
  } else {
    title = queryError.name;
    bodyDOM = queryError.message
      .split("\n")
      .map((line, idx) => (
        <React.Fragment key={idx}>
          {line}
          <br />
        </React.Fragment>
      ));
  }

  return <Alert title={title} color="red">{bodyDOM}</Alert>;
}
