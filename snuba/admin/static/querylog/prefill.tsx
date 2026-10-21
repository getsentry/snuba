// Like System Queries (see clickhouse_queries/prefill.tsx), the Querylog page
// can be opened with the editor pre-filled from the URL:
//
//   /#querylog?query=SELECT%20count()%20FROM%20querylog_local
//
// Nothing here runs the query: the user still has to click Execute. Unlike
// System Queries there's no warning, since the querylog cluster isn't
// production.
import {
  buildHashUrl,
  splitHash,
} from "SnubaAdmin/clickhouse_queries/prefill";

/** Returns the query encoded in the hash, or null if there is none. */
export function parseQuerylogPrefill(hash: string): { sql: string } | null {
  const sql = splitHash(hash).params.get("query");
  return sql ? { sql } : null;
}

/** Builds a link that opens the Querylog page pre-filled with this query. */
export function buildQuerylogUrl(sql: string): string {
  return buildHashUrl("querylog", { query: sql });
}
