import { it, expect, describe } from "@jest/globals";
import {
  getAllNodes,
  getStorageOptions,
  nodeMatchesSearch,
  nodeSearchMatch,
  rankNodes,
  storageForNode,
} from "SnubaAdmin/utils/clickhouse_node_utils";
import { ClickhouseNodeData } from "SnubaAdmin/clickhouse_queries/types";

function storage(
  name: string,
  local: string[],
  dist: string[] = [],
  query: string | null = null
): ClickhouseNodeData {
  const node = (host: string) => ({ host, port: 9000 });
  return {
    storage_name: name,
    local_table_name: `${name}_local`,
    local_nodes: local.map(node),
    dist_nodes: dist.map(node),
    query_node: query ? { host: query, port: 9001 } : (null as any),
  };
}

describe("getAllNodes", () => {
  it("lists each node once with every storage that knows it", () => {
    const nodes = getAllNodes([
      storage("errors_ro", ["errors-1"], [], "errors-query"),
      storage("errors", ["errors-1"], ["errors-dist"], "errors-query"),
      storage("spans", ["spans-1"]),
    ]);
    expect(nodes.map((node) => [node.value, node.label, node.storages])).toEqual([
      ["errors-1:9000", "errors-1:9000", ["errors", "errors_ro"]],
      ["errors-dist:9000", "errors-dist:9000 (distributed)", ["errors"]],
      ["errors-query:9001", "errors-query:9001 (query node)", ["errors", "errors_ro"]],
      ["spans-1:9000", "spans-1:9000", ["spans"]],
    ]);
  });

  it("doesn't call a node distributed if it's also local", () => {
    const [node] = getAllNodes([storage("errors", ["h"], ["h"])]);
    expect(node.label).toBe("h:9000");
  });

  it("summarizes long storage lists", () => {
    const [node] = getAllNodes(
      ["a", "b", "c", "d", "e"].map((name) => storage(name, ["h"]))
    );
    expect(node.description).toBe("a, b, c +2 more");
  });
});

describe("getStorageOptions", () => {
  const eap = [
    "eap_items",
    "eap_items_ro",
    "eap_items_2",
    "eap_items_2_ro",
    "eap_items_downsample_8",
    "eap_items_downsample_8_ro",
    "eap_item_co_occurring_attrs",
    "eap_item_co_occurring_attrs_ro",
  ];

  it("folds eap_items storages into eap_items and eap_items_ro", () => {
    expect(getStorageOptions([...eap, "errors"])).toEqual([
      "eap_items",
      "eap_items_ro",
      "errors",
    ]);
  });

  it("folds the outcomes storage set into outcomes_raw", () => {
    expect(
      getStorageOptions([
        "outcomes_raw",
        "outcomes_hourly",
        "outcomes_daily",
        "outcomes_cross_org_hourly",
        "llm_proxy_cost_raw",
        "querylog",
      ])
    ).toEqual(["outcomes_raw", "querylog"]);
  });

  it("keeps the given storage", () => {
    expect(getStorageOptions(eap, "eap_items_downsample_8")).toEqual([
      "eap_items",
      "eap_items_downsample_8",
      "eap_items_ro",
    ]);
  });

  it("only folds into a top-level storage that's listed", () => {
    expect(getStorageOptions(["eap_items_2", "eap_items_2_ro", "eap_items_ro"])).toEqual([
      "eap_items_2",
      "eap_items_ro",
    ]);
  });

  it("folds a node's storage list too", () => {
    const [node] = getAllNodes(eap.map((name) => storage(name, ["h"])));
    expect(node.description).toBe("eap_items, eap_items_ro");
    expect(node.storages).toHaveLength(eap.length);
  });
});

describe("storageForNode", () => {
  const [node] = getAllNodes([
    storage("errors_ro", ["h"]),
    storage("errors", ["h"]),
    storage("search_issues", ["h"]),
  ]);

  it("keeps the preferred storage if it lists the node", () => {
    expect(storageForNode(node, "errors_ro")).toBe("errors_ro");
  });

  it("otherwise uses the shortest storage name", () => {
    expect(storageForNode(node)).toBe("errors");
    expect(storageForNode(node, "spans")).toBe("errors");
  });

  it("prefers querylog for its longer timeout", () => {
    const [shared] = getAllNodes([storage("errors", ["h"]), storage("querylog", ["h"])]);
    expect(storageForNode(shared)).toBe("querylog");
  });
});

describe("nodeMatchesSearch", () => {
  const host = "snuba-events-analytics-platform-tier1-0-1:9000";

  it("matches substrings of host:port", () => {
    expect(nodeMatchesSearch("", host)).toBe(true);
    expect(nodeMatchesSearch("Analytics", host)).toBe(true);
    expect(nodeMatchesSearch("tier1-0-1:9000", host)).toBe(true);
    expect(nodeMatchesSearch("errors", host)).toBe(false);
  });

  it("matches words shortened to their initials", () => {
    expect(nodeMatchesSearch("eap", host)).toBe(true);
    expect(nodeMatchesSearch("snuba-eap-tier1", host)).toBe(true);
    // tier1 is skipped as a whole word.
    expect(nodeMatchesSearch("eap-0-1", host)).toBe(true);
    expect(nodeMatchesSearch("eapt-0-1", host)).toBe(true);
    expect(nodeMatchesSearch("seap", host)).toBe(true);
    expect(nodeMatchesSearch("gm", host)).toBe(false);
  });
});

describe("nodeSearchMatch", () => {
  const host = "snuba-events-analytics-platform-0-1:9000";
  const indexes = (search: string, hostPort = host) =>
    nodeSearchMatch(search, hostPort)?.indexes ?? null;
  const marked = (search: string) =>
    indexes(search)?.map((idx) => host[idx]).join("");
  const rank = (search: string, hostPort = host) =>
    nodeSearchMatch(search, hostPort)?.rank ?? null;

  it("returns the matched substring's characters", () => {
    expect(indexes("")).toEqual([]);
    expect(indexes("events")).toEqual([6, 7, 8, 9, 10, 11]);
    expect(indexes("errors")).toBeNull();
  });

  it("returns the initials of shortened words", () => {
    expect(indexes("eap")).toEqual([6, 13, 23]);
    expect(marked("snuba-eap-0-1:90")).toBe("snuba-eap0-1:90");
    expect(indexes("eap-0")).toEqual([6, 13, 23, 32]);
  });

  it("ignores separators", () => {
    expect(marked("eap-1")).toBeUndefined();
    expect(marked("eap01")).toBe("eap0-1");
    expect(marked("eap 0-1")).toBe("eap0-1");
    expect(marked("platform01")).toBe("platform-0-1");
    expect(rank("eap-1", "snuba-events-analytics-platform-1-0:8123")).toBe(1);
    expect(rank("eap-1", "snuba-events-analytics-platform-12-0:8123")).toBe(1);
    expect(rank("eap-1", "snuba-events-analytics-platform-0-1:8123")).toBeNull();
  });

  it("skips the rest of a word at a typed separator", () => {
    const errors = "snuba-errors-1-0:8123";
    expect(
      nodeSearchMatch("err-1", errors)?.indexes.map((idx) => errors[idx]).join("")
    ).toBe("err1");
    expect(nodeSearchMatch("err-1", errors)?.rank).toBe(0);
    expect(nodeSearchMatch("err1", errors)).toBeNull();
    expect(nodeSearchMatch("sn-err-1-0", errors)).not.toBeNull();
    // The rest of one word, then at most one whole word.
    expect(nodeSearchMatch("snuba-1", errors)).not.toBeNull();
    expect(nodeSearchMatch("snuba-1", "snuba-generic-metrics-1-0:8123")).toBeNull();
    expect(nodeSearchMatch("sn-1", errors)?.boundaryMisses).toBe(2);
    expect(nodeSearchMatch("sn-1", "snuba-generic-metrics-1-0:8123")).toBeNull();
  });

  it("skips one whole word at a typed separator", () => {
    const arm = "snuba-events-analytics-platform-arm-1-1:8123";
    const mz = "snuba-events-analytics-platform-mz-1-1:8123";
    expect(
      nodeSearchMatch("eap-1-1", arm)?.indexes.map((idx) => arm[idx]).join("")
    ).toBe("eap1-1");
    expect(nodeSearchMatch("eap-1-1", mz)?.boundaryMisses).toBe(1);
    expect(nodeSearchMatch("eap-arm-1-1", arm)?.boundaryMisses).toBe(0);
    // Not without a typed separator.
    expect(nodeSearchMatch("eap11", arm)).toBeNull();
    // Number words aren't skipped.
    expect(
      nodeSearchMatch("eap-1", "snuba-events-analytics-platform-0-1:8123")
    ).toBeNull();
  });

  it("prefers a match at the start of a word", () => {
    // "a" first appears in "snuba", but "analytics" starts with it.
    expect(indexes("a")).toEqual([13]);
    expect(rank("a")).toBe(0);
    expect(rank("eap")).toBe(1);
    expect(rank("m")).toBe(2);
    expect(rank("m", "snuba-generic-metrics-0-0:8123")).toBe(0);
  });
});

describe("boundaryMisses", () => {
  const misses = (search: string, hostPort: string) =>
    nodeSearchMatch(search, hostPort)?.boundaryMisses ?? null;
  const eap = (shards: string) =>
    `snuba-events-analytics-platform-${shards}:8123`;

  it("counts word breaks the search and host disagree on", () => {
    expect(misses("eap-11", eap("11-1"))).toBe(0);
    expect(misses("eap-11", eap("1-1"))).toBe(1);
    expect(misses("eap11", eap("11-1"))).toBe(1);
    expect(misses("eap11", eap("1-1"))).toBe(2);
    expect(misses("eap-1-1", eap("11-1"))).toBe(1);
    expect(misses("eap-1-1", eap("1-1"))).toBe(0);
  });

  it("counts ending partway through a word", () => {
    expect(misses("eap-1", eap("1-0"))).toBe(0);
    expect(misses("eap-1", eap("10-0"))).toBe(1);
    expect(misses("eap-1-1", eap("1-11"))).toBe(1);
    expect(misses("err-1", "snuba-errors-1-0:8123")).toBe(1);
    expect(misses("errors-1", "snuba-errors-1-0:8123")).toBe(0);
    // The port is a word too.
    expect(misses("eap-1-1:81", eap("1-1"))).toBe(1);
  });

  it("picks the closest of a node's matches", () => {
    // "1-1" starts words twice; only the second sits on word breaks.
    expect(nodeSearchMatch("1-1", "snuba-x-11-1-1:8123")).toEqual({
      indexes: [11, 12, 13],
      rank: 0,
      boundaryMisses: 0,
    });
  });
});

describe("rankNodes", () => {
  const nodes = getAllNodes([
    storage("eap", ["snuba-events-analytics-platform-0-0"], ["snuba-eap-dist"]),
    storage("metrics", ["snuba-generic-metrics-0-0"], ["snuba-metrics-dist"]),
  ]);
  const order = (search: string) =>
    rankNodes(search, nodes).map((node) => node.value);

  it("lists distributed nodes last", () => {
    expect(order("")).toEqual([
      "snuba-events-analytics-platform-0-0:9000",
      "snuba-generic-metrics-0-0:9000",
      "snuba-eap-dist:9000",
      "snuba-metrics-dist:9000",
    ]);
  });

  it("lists closer matches first", () => {
    const shards = getAllNodes(
      ["1-1", "11-1", "1-11"].map((shard) =>
        storage("eap", [`snuba-events-analytics-platform-${shard}`])
      )
    );
    const order = (search: string) =>
      rankNodes(search, shards).map((node) => node.value);
    expect(order("eap-11")[0]).toBe("snuba-events-analytics-platform-11-1:9000");
    expect(order("eap-1-1")[0]).toBe("snuba-events-analytics-platform-1-1:9000");
  });

  it("lists hosts without a skipped word first", () => {
    const hosts = getAllNodes(
      ["arm-1-1", "1-1", "mz-1-1"].map((shard) =>
        storage("eap", [`snuba-events-analytics-platform-${shard}`])
      )
    );
    expect(rankNodes("eap-1-1", hosts).map((node) => node.value)).toEqual([
      "snuba-events-analytics-platform-1-1:9000",
      "snuba-events-analytics-platform-arm-1-1:9000",
      "snuba-events-analytics-platform-mz-1-1:9000",
    ]);
  });

  it("lists word-start matches before mid-word ones", () => {
    expect(order("m")).toEqual([
      "snuba-generic-metrics-0-0:9000",
      "snuba-metrics-dist:9000",
      "snuba-events-analytics-platform-0-0:9000",
      "snuba-eap-dist:9000",
    ]);
  });
});
