// System Queries can be opened with the editor pre-filled from the URL, so a
// tool or agent can link someone straight to a query to review and run:
//
//   /#system-queries?query=SELECT%201&storage=errors&host=clickhouse-01:8123
//
// Params live inside the hash because the admin routes on the hash. Values
// are URL-encoded (encodeURIComponent / URLSearchParams). `host` may omit the
// port, in which case it defaults to ClickHouse HTTP (8123). Nothing here
// runs the query: the user still has to review it and click Execute.

type SystemQueryPrefill = {
  sql?: string;
  storage?: string;
  host?: string;
  port?: number;
};

const DEFAULT_PORT = 8123;

/** Splits "#page?a=b" into the route ("#page") and its params. */
export function splitHash(hash: string): {
  route: string;
  params: URLSearchParams;
} {
  const separator = hash.indexOf("?");
  if (separator === -1) {
    return { route: hash, params: new URLSearchParams() };
  }
  return {
    route: hash.slice(0, separator),
    params: new URLSearchParams(hash.slice(separator + 1)),
  };
}

/** Parses "host" or "host:port", defaulting the port to ClickHouse HTTP. */
export function parseHostPort(value: string): { host: string; port: number } {
  const trimmed = value.trim();
  const separator = trimmed.lastIndexOf(":");
  const maybePort = separator === -1 ? "" : trimmed.slice(separator + 1);
  if (/^\d+$/.test(maybePort)) {
    return {
      host: trimmed.slice(0, separator),
      port: parseInt(maybePort, 10),
    };
  }
  return { host: trimmed, port: DEFAULT_PORT };
}

/** Returns the prefill encoded in the hash, or null if there is none. */
export function parseSystemQueryPrefill(
  hash: string
): SystemQueryPrefill | null {
  const { params } = splitHash(hash);
  const sql = params.get("query") || undefined;
  const storage = params.get("storage") || undefined;
  const hostParam = params.get("host")?.trim();
  if (!sql && !storage && !hostParam) {
    return null;
  }
  return {
    sql,
    storage,
    ...(hostParam ? parseHostPort(hostParam) : {}),
  };
}

/** Builds a link that opens System Queries pre-filled with these values. */
export function buildSystemQueryUrl(prefill: {
  sql?: string;
  storage?: string | null;
  host?: string | null;
}): string {
  const params = new URLSearchParams();
  if (prefill.sql) params.set("query", prefill.sql);
  if (prefill.storage) params.set("storage", prefill.storage);
  if (prefill.host) params.set("host", prefill.host);
  // %20 reads better than "+" when the link is pasted somewhere.
  const query = params.toString().replace(/\+/g, "%20");
  return `${window.location.origin}${window.location.pathname}#system-queries?${query}`;
}

/**
 * Drops the params from the address bar once they've been applied, so a
 * refresh doesn't clobber edits by re-applying the link.
 */
export function clearHashParams() {
  const { route } = splitHash(window.location.hash);
  if (route !== window.location.hash) {
    window.history.replaceState(
      null,
      "",
      window.location.pathname + window.location.search + route
    );
  }
}

export { SystemQueryPrefill };
