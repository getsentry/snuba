import RuntimeConfig from "./runtime_config";
import AuditLog from "./runtime_config/auditlog";
import ClickhouseMigrations from "./clickhouse_migrations";
import ClickhouseQueries from "./clickhouse_queries";
import TracingQueries from "./tracing";
import SnQLToSQL from "./snql_to_sql";

function Placeholder(props: any) {
  return null;
}

const NAV_ITEMS = [
  { id: "overview", display: "🤿 Overview", component: Placeholder },
  { id: "config", display: "⚙️ Runtime Config", component: RuntimeConfig },
  {
    id: "snql-to-sql",
    display: "🌐 SnQL to SQL",
    component: SnQLToSQL,
  },
  {
    id: "clickhouse",
    display: "🏚️ System Queries",
    component: ClickhouseQueries,
  },
  {
    id: "clickhouse-migrations",
    display: "🚧 ClickHouse Migrations",
    component: ClickhouseMigrations,
  },
  {
    id: "tracing",
    display: "🔎 ClickHouse Tracing",
    component: TracingQueries,
  },
  {
    id: "auditlog",
    display: "📝 Audit Log",
    component: AuditLog,
  },
];

export { NAV_ITEMS };
