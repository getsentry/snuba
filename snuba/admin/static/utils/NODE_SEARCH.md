# System Queries node search

How the **Host** autocomplete in System Queries ("Search nodes" mode) finds, ranks and highlights ClickHouse nodes, and how storage lists are shortened. The code is in [`clickhouse_node_utils.tsx`](clickhouse_node_utils.tsx) (`getAllNodes`, `nodeSearchMatch`, `rankNodes`), and the select it drives is `CustomSelect` in [`../select.tsx`](../select.tsx).

## The node list

- Every node of every storage in `/clickhouse_nodes` appears once, keyed by `host:port`. This includes local nodes, distributed nodes and query nodes.
- The label is `host:port`, with `(distributed)` and/or `(query node)` appended where that applies. A node that is also local is never labelled distributed.
- The line under the label lists the storages whose cluster includes the node, for example `errors, errors_ro, search_issues +4 more`. Storages that share a top-level storage's nodes are folded into it (see [Storage lists](#storage-lists)).
- A node counts as **distributed** for ranking only if it is distributed and nothing else: not local, and not a query node.

## Picking a node sets the storage

The backend still needs a storage, because the storage decides the cluster, which supplies the credentials, database, TLS settings and node validation. When a node is picked, the storage sent with the query is:

1. the current storage (from a shared link or the previous pick), if it lists the node; otherwise
2. `querylog`, if it lists the node, because querylog queries run without the 25s timeout; otherwise
3. the shortest storage name, with ties broken alphabetically.

Manual host entry still shows a storage picker, since a typed host has no other way to name a cluster.

"By storage" mode is the old way of picking a host, kept as an alternative: pick a storage, its first host fills in, and the host list shows only that storage's nodes. The page remembers whether you last used "Search nodes" or "By storage". Switching between them keeps the current host and storage.

## Storage lists

Many storages are known to use the same nodes as a top-level storage. Storage lists show only the top-level storage. This applies to the storage line under each node and to the storage pickers in "By storage" and "Manual" modes.

| Folded storages | Shown as | Why they share nodes |
| --- | --- | --- |
| Every `eap_items*` and `eap_item_*` storage (`eap_items_2`, the `downsample_*` ones, `eap_item_co_occurring_attrs`, …), except the `_ro` ones | `eap_items` | Known to use the same cluster. |
| The `_ro` ones of those | `eap_items_ro` | Known to use the same cluster. |
| `outcomes_hourly`, `outcomes_daily`, `outcomes_cross_org_hourly`, `llm_proxy_cost_raw` | `outcomes_raw` | They're all in the `outcomes` storage set, and a storage set belongs to exactly one cluster (asserted in `snuba/clusters/cluster.py`). |

- A storage is folded into a top-level one only if that top-level storage is in the same list. On a node that lists `eap_items_2` but not `eap_items`, `eap_items_2` is shown.
- The selected storage is always in the picker, even if it would be folded, so a shared link to `eap_items_downsample_8` still shows it.
- Folding only affects what's listed. The storage sent with a query is still chosen from every storage that lists the node.

The folding rule is `topLevelStorage` in `clickhouse_node_utils.tsx`.

## Words

Matching works on **words**:

- The host splits into words on `-`, `_` and `.`. The port is one more word. Case is ignored.
- `snuba-events-analytics-platform-11-1:8123` has the words `snuba` `events` `analytics` `platform` `11` `1` `8123`.

In the search, the separators are `-`, `_`, `.`, `:` and whitespace.

## Matching rules

A node matches if the search, with its separators removed, can be matched character by character, in order, against the host's words:

1. **Separators don't have to be typed.** Inside the matched span, the host's separators are skipped. `eap1`, `eap-1` and `eap 1` all match `…-platform-1-0`.
2. **Initials.** Any run of two or more consecutive host words can be typed as their first letters, for example `eap` for `events-analytics-platform`.
   - Every word in the run has to start with a letter. Number words and the port can't be part of a run.
   - Only one run per match.
   - `eapt` covers `events-analytics-platform-tier1`. `eap-0-1` also matches it, but by skipping `tier1` (rule 3).
3. **A typed separator can skip ahead.** Where you type a separator, the next character may skip:
   - **the rest of the current host word**: `err-1` matches `snuba-errors-1-0` as `err`…`1`; and/or
   - **one whole host word that starts with a letter**: `eap-1-1` matches `…-platform-arm-1-1` and `…-platform-mz-1-1`, skipping `arm` or `mz`.

   Limits:
   - At most one whole word per separator. `snuba-1` matches `snuba-errors-1-0` but not `snuba-generic-metrics-1-0`.
   - Number words (shards, replicas, the port) are never skipped, so their positions still count. `eap-1` doesn't match `…-platform-0-1`.
   - An initial from rule 2 stands for a word you typed, so it can't be skipped.
   - Without a typed separator, nothing is skipped. `err1` doesn't match `errors-1`, and `eap11` doesn't match `…-arm-1-1`.
4. **The match can start anywhere,** including mid-word, for example `m` in `platform`. These matches rank last; see below.

An empty search matches every node.

## Ranking

The dropdown is re-sorted on every keystroke. Each node is scored by its best match, and the comparison goes in this order:

1. **Match rank**, best first:
   1. *Word start*: the match starts at the start of a host word and doesn't use initials. `m` matches **m**etrics.
   2. *Initials*: the match uses at least one initial from rule 2. `eap` matches `events-analytics-platform`.
   3. *Mid-word*: the match starts inside a word. `m` matches platfor**m**.
2. **Boundary misses**, fewest first. A miss is one place where the search and the host disagree about word breaks:
   - The host starts a new word where you typed no separator. Initials typed together don't count.
   - You typed a separator where the host keeps going in the same word, without skipping (rule 3).
   - A typed separator skipped the rest of a word (rule 3).
   - A typed separator skipped a whole word (rule 3). So `eap-1-1` lists `…-platform-1-1` before `…-platform-arm-1-1`.
   - The search ends partway through a host word.
3. **Distributed nodes last.**
4. **`host:port`, alphabetically.** Because `-` sorts before digits, `…-1-0` comes before `…-10-0`.

Nodes that don't match are filtered out of the dropdown.

The comparison order means a closer match of a worse rank never beats a word-start match. Distributed nodes only drop to the bottom within a group with the same rank and misses. So typing `m` lists the metrics distributed nodes before the mid-word `platform` matches.

### Examples

| Search | Host | Rank | Misses | Why |
| --- | --- | --- | --- | --- |
| `m` | `snuba-generic-metrics-0-0` | word start | 1 | ends partway through `metrics` |
| `m` | `snuba-events-analytics-platform-0-0` | mid-word | 0 | `m` is the end of `platform` |
| `eap-11` | `…-platform-11-1` | initials | 0 | breaks line up |
| `eap-11` | `…-platform-1-1` | initials | 1 | host breaks between the `1`s, search doesn't |
| `eap-11` | `…-platform-1-11` | initials | 2 | that break, and ends partway through `11` |
| `eap-1-1` | `…-platform-1-1` | initials | 0 | breaks line up |
| `eap-1-1` | `…-platform-1-11` | initials | 1 | ends partway through `11` |
| `eap-1` | `…-platform-10-0` | initials | 1 | ends partway through `10` |
| `eap-1-1` | `…-platform-arm-1-1` | initials | 1 | skips `arm` |
| `eap-arm-1-1` | `…-platform-arm-1-1` | initials | 0 | breaks line up |
| `err-1` | `snuba-errors-1-0` | word start | 1 | skips the rest of `errors` |
| `snuba-1` | `snuba-errors-1-0` | word start | 1 | skips `errors` |
| `errors-1` | `snuba-errors-1-0` | word start | 0 | breaks line up |

## Highlighting

- The characters of the node's best match are marked in its label.
- A separator in the host that sits between two matched characters is marked as well, so `0-0` shows as one block.
- Separators and letters the match skipped are not marked. For `eap`, only the `e`, `a` and `p` are marked. For `err-1`, only `err` and the `1`.
- Right after a node is picked, the box shows the node's full label and Mantine lists every option. Until you type something else, nothing is marked or re-sorted.
