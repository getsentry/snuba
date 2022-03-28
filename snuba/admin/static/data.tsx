import RuntimeConfig from "./runtime_config";
import AuditLog from "./runtime_config/auditlog";
import ClickhouseQueries from "./clickhouse_queries";
import TracingQueries from "./tracing";
import SnQLTracing from "./tracing/snql_tracing";

function Placeholder(props: any) {
  return null;
}

const NAV_ITEMS = [
  { id: "overview", display: "🤿 Overview", component: Placeholder },
  { id: "config", display: "⚙️ Runtime Config", component: RuntimeConfig },
  {
    id: "clickhouse",
    display: "🏚️ ClickHouse",
    component: ClickhouseQueries,
  },
  {
    id: "tracing",
    display: "🔎 ClickHouse Tracing",
    component: TracingQueries,
  },
  {
    id: "snql-tracing",
    display: "🔍 SnQL Tracing",
    component: SnQLTracing,
  },
  {
    id: "auditlog",
    display: "📝 Audit Log",
    component: AuditLog,
  },
];

export { NAV_ITEMS };
