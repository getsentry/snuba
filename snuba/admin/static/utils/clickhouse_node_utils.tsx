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

export type NodeOption = SelectItem & {
  value: string;
  label: string;
  // Every storage whose cluster lists this node.
  storages: string[];
  // Only a distributed node, which is rarely what a system query wants, so
  // it's listed after the others.
  distributed: boolean;
};

/**
 * Lists every node of every storage once, keyed by host:port, with the
 * storages that know about it.
 *
 * @param nodeData - Array of ClickhouseNodeData
 * @returns Array of NodeOption, sorted by host:port
 */
export function getAllNodes(nodeData: ClickhouseNodeData[]): NodeOption[] {
  const byHostPort = new Map<
    string,
    { storages: Set<string>; local: boolean; dist: boolean; query: boolean }
  >();
  function add(
    node: { host: string; port: number },
    storage: string,
    role: "local" | "dist" | "query"
  ) {
    const key = `${node.host}:${node.port}`;
    let entry = byHostPort.get(key);
    if (!entry) {
      entry = { storages: new Set(), local: false, dist: false, query: false };
      byHostPort.set(key, entry);
    }
    entry.storages.add(storage);
    entry[role] = true;
  }
  for (const nodeInfo of nodeData) {
    const name = nodeInfo.storage_name;
    nodeInfo.local_nodes.forEach((node) => add(node, name, "local"));
    nodeInfo.dist_nodes.forEach((node) => add(node, name, "dist"));
    if (nodeInfo.query_node) {
      add(nodeInfo.query_node, name, "query");
    }
  }

  return Array.from(byHostPort.entries())
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([hostPort, entry]) => {
      const roles = [
        ...(entry.dist && !entry.local ? ["distributed"] : []),
        ...(entry.query ? ["query node"] : []),
      ];
      const storages = Array.from(entry.storages).sort();
      return {
        value: hostPort,
        label: roles.length
          ? `${hostPort} (${roles.join(", ")})`
          : hostPort,
        description:
          storages.length > 3
            ? `${storages.slice(0, 3).join(", ")} +${storages.length - 3} more`
            : storages.join(", "),
        storages,
        distributed: entry.dist && !entry.local && !entry.query,
      };
    });
}

/**
 * The node as a select item. The select hands every field of an item to the
 * DOM, so this leaves out the node-only ones.
 */
export function nodeSelectItem(node: NodeOption): SelectItem {
  return { value: node.value, label: node.label, description: node.description };
}

/**
 * The storage to send with a query to this node. The backend only uses it to
 * find the node's cluster, so any storage that lists the node works. The
 * preferred storage is kept if it lists the node; otherwise querylog wins
 * (its queries get a longer timeout), then the shortest name.
 */
export function storageForNode(node: NodeOption, preferred?: string): string {
  if (preferred && node.storages.includes(preferred)) {
    return preferred;
  }
  if (node.storages.includes("querylog")) {
    return "querylog";
  }
  return [...node.storages].sort(
    (a, b) => a.length - b.length || (a < b ? -1 : a > b ? 1 : 0)
  )[0];
}

// Searchable text without separators, where each of its characters sits in
// the host:port, which characters start a word, and which are initials
// standing in for a whole word.
type SearchVariant = {
  text: string;
  indexes: number[];
  wordStarts: boolean[];
  initials: boolean[];
};

const SEPARATORS = /[-_.:\s]/g;

const variantsByHostPort = new Map<string, SearchVariant[]>();

/**
 * The host:port, then the same with any run of two or more consecutive words
 * of the host shortened to their initials, so "eap" finds
 * snuba-events-analytics-platform-0-1 (as snuba-eap-0-1). Separators are
 * left out, so they don't have to be typed.
 */
function searchVariants(hostPort: string): SearchVariant[] {
  const cached = variantsByHostPort.get(hostPort);
  if (cached) {
    return cached;
  }
  const separator = hostPort.lastIndexOf(":");
  const host = separator === -1 ? hostPort : hostPort.slice(0, separator);
  type Word = { text: string; start: number };
  const words: Word[] = [];
  const wordPattern = /[^-_.]+/g;
  for (let match; (match = wordPattern.exec(host.toLowerCase())); ) {
    words.push({ text: match[0], start: match.index });
  }
  if (separator !== -1) {
    words.push({ text: hostPort.slice(separator + 1), start: separator + 1 });
  }

  type Char = { char: string; index: number; wordStart: boolean; initial: boolean };
  function wordChars(word: Word): Char[] {
    return Array.from(word.text, (char, k) => ({
      char,
      index: word.start + k,
      wordStart: k === 0,
      initial: false,
    }));
  }
  function variant(chars: Char[]): SearchVariant {
    return {
      text: chars.map(({ char }) => char).join(""),
      indexes: chars.map(({ index }) => index),
      wordStarts: chars.map(({ wordStart }) => wordStart),
      initials: chars.map(({ initial }) => initial),
    };
  }

  const hostWords = separator === -1 ? words.length : words.length - 1;
  const variants = [variant(words.flatMap(wordChars))];
  for (let start = 0; start < hostWords; start++) {
    for (let end = start + 2; end <= hostWords; end++) {
      const run = words.slice(start, end);
      if (!run.every((word) => /^[a-z]/.test(word.text))) {
        break;
      }
      variants.push(
        variant([
          ...words.slice(0, start).flatMap(wordChars),
          ...run.map((word) => ({
            char: word.text[0],
            index: word.start,
            wordStart: true,
            initial: true,
          })),
          ...words.slice(end).flatMap(wordChars),
        ])
      );
    }
  }
  variantsByHostPort.set(hostPort, variants);
  return variants;
}

// Adds the separators that sit between two matched characters, so "0-0"
// is marked as one run.
function withSeparatorsBetween(hostPort: string, indexes: number[]): number[] {
  return indexes.flatMap((index, idx) => {
    const next = indexes[idx + 1];
    if (next === undefined) {
      return [index];
    }
    const between = hostPort.slice(index + 1, next);
    return /^[-_.:]+$/.test(between)
      ? [index, ...Array.from(between, (_, k) => index + 1 + k)]
      : [index];
  });
}

// How well the search matched, best first.
const MATCH_WORD_START = 0;
const MATCH_INITIALS = 1;
const MATCH_MID_WORD = 2;

export type NodeMatch = {
  // The indexes of the matched characters in host:port.
  indexes: number[];
  // MATCH_WORD_START, MATCH_INITIALS or MATCH_MID_WORD.
  rank: number;
  // How many places the search and the host disagree on where words break:
  // the host starts a new word where no separator was typed, or the other
  // way around, or the search ends partway through a word. Fewer is a closer
  // match, so "eap-11" is closer to ...-platform-11-1 (none) than to
  // ...-platform-1-1 (one), and "eap-1-1" is closer to ...-platform-1-1
  // (none) than to ...-platform-1-11 (one).
  boundaryMisses: number;
};

/** Orders matches, best first. */
function compareMatches(a: NodeMatch, b: NodeMatch): number {
  return a.rank - b.rank || a.boundaryMisses - b.boundaryMisses;
}

// Where the needle matched in a variant: the variant positions of its
// characters, and the boundaryMisses so far.
type VariantMatch = { positions: number[]; boundaryMisses: number };

/**
 * The closest match of the needle in the variant starting at `start`, or null.
 * The needle's characters are matched in order, and where a separator was
 * typed, the rest of the current host word may be skipped, so "err-1" finds
 * errors-1.
 */
function matchVariantFrom(
  variant: SearchVariant,
  needle: string,
  typedBreaks: boolean[],
  start: number
): VariantMatch | null {
  const { text, wordStarts, initials } = variant;
  function closest(a: VariantMatch | null, b: VariantMatch | null) {
    return a === null || (b !== null && b.boundaryMisses < a.boundaryMisses)
      ? b
      : a;
  }
  function step(k: number, at: number): VariantMatch | null {
    if (k === needle.length) {
      // Ending partway through a word is a miss.
      const endsMidWord = at < text.length && !wordStarts[at];
      return { positions: [], boundaryMisses: endsMidWord ? 1 : 0 };
    }
    function take(position: number, misses: number): VariantMatch | null {
      if (text[position] !== needle[k]) {
        return null;
      }
      const rest = step(k + 1, position + 1);
      return (
        rest && {
          positions: [position, ...rest.positions],
          boundaryMisses: rest.boundaryMisses + misses,
        }
      );
    }
    if (k === 0) {
      return take(at, 0);
    }
    // Initials are each a word, but they're typed together.
    const hostBreak =
      wordStarts[at] && !(initials[at] && initials[at - 1]);
    let best = take(at, hostBreak !== typedBreaks[k] ? 1 : 0);
    if (typedBreaks[k] && !wordStarts[at]) {
      // Skip to the next word. Leaving a word unfinished is a miss.
      let next = at + 1;
      while (next < text.length && !wordStarts[next]) {
        next++;
      }
      best = closest(best, take(next, 1));
    }
    return best;
  }
  return step(0, start);
}

/**
 * The best match for what was typed into the node search in host:port, or
 * null if it doesn't match. Separators needn't be typed, so "eap1" finds
 * snuba-events-analytics-platform-1-0, and a typed one may skip the rest of
 * a word, so "err-1" finds snuba-errors-1-0. A match at the start of a word
 * beats one using initials (see searchVariants), which beats one in the
 * middle of a word, so "m" ranks generic-metrics above
 * events-analytics-platform. Ties go to the match with fewer boundaryMisses.
 */
export function nodeSearchMatch(
  search: string,
  hostPort: string
): NodeMatch | null {
  const needle = search.toLowerCase().replace(SEPARATORS, "");
  if (!needle) {
    return { indexes: [], rank: MATCH_WORD_START, boundaryMisses: 0 };
  }
  // Whether a separator was typed before each of the needle's characters.
  const typedBreaks: boolean[] = [];
  search
    .toLowerCase()
    .split(SEPARATORS)
    .forEach((piece, idx) =>
      Array.from(piece).forEach((_, k) => typedBreaks.push(idx > 0 && k === 0))
    );

  let best: NodeMatch | null = null;
  for (const variant of searchVariants(hostPort)) {
    for (let start = 0; start < variant.text.length; start++) {
      // Nothing beats this.
      if (best && best.rank === MATCH_WORD_START && best.boundaryMisses === 0) {
        break;
      }
      const found = matchVariantFrom(variant, needle, typedBreaks, start);
      if (!found) {
        continue;
      }
      const rank = found.positions.some((position) => variant.initials[position])
        ? MATCH_INITIALS
        : variant.wordStarts[start]
        ? MATCH_WORD_START
        : MATCH_MID_WORD;
      const match = {
        indexes: found.positions.map((position) => variant.indexes[position]),
        rank,
        boundaryMisses: found.boundaryMisses,
      };
      if (best === null || compareMatches(match, best) < 0) {
        best = match;
      }
    }
  }
  return best && {
    ...best,
    indexes: withSeparatorsBetween(hostPort, best.indexes),
  };
}

/** Whether a node matches what was typed into the node search. */
export function nodeMatchesSearch(search: string, hostPort: string): boolean {
  return nodeSearchMatch(search, hostPort) !== null;
}

/**
 * Orders the nodes for what was typed into the node search: better matches
 * first (see compareMatches), distributed nodes after the others, then by
 * host:port. Nodes that don't match go last; the select filters them out.
 */
export function rankNodes(search: string, nodes: NodeOption[]): NodeOption[] {
  const keyed = nodes.map((node) => ({
    node,
    match: nodeSearchMatch(search, node.value),
  }));
  keyed.sort(
    (a, b) =>
      (a.match && b.match
        ? compareMatches(a.match, b.match)
        : Number(!a.match) - Number(!b.match)) ||
      Number(a.node.distributed) - Number(b.node.distributed) ||
      (a.node.value < b.node.value ? -1 : a.node.value > b.node.value ? 1 : 0)
  );
  return keyed.map(({ node }) => node);
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
