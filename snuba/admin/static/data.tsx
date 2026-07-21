import AutoReplacementsBypassProjects from "SnubaAdmin/auto_replacements_bypass_projects";
import AuditLog from "SnubaAdmin/runtime_config/auditlog";
import ClickhouseMigrations from "SnubaAdmin/clickhouse_migrations";
import ClickhouseQueries from "SnubaAdmin/clickhouse_queries";
import CopyTables from "SnubaAdmin/copy_tables";
import TracingQueries from "SnubaAdmin/tracing";
import SQLShellPage, { SystemShellPage } from "SnubaAdmin/sql_shell";
import SnQLToSQL from "SnubaAdmin/snql_to_sql";
import QuerylogQueries from "SnubaAdmin/querylog";
import ProductionQueries from "SnubaAdmin/production_queries";
import SnubaExplain from "SnubaAdmin/snuba_explain";
import Welcome from "SnubaAdmin/welcome";
import ViewCustomJobs from "SnubaAdmin/manual_jobs";
import RpcEndpoints from "SnubaAdmin/rpc_endpoints";

const NAV_ITEMS = [
  { id: "overview", display: "🤿 Snuba Admin", component: Welcome },
  {
    id: "auto-replacements-bypass-projects",
    display: "👻 Replacements",
    component: AutoReplacementsBypassProjects,
  },
  {
    id: "snql-to-sql",
    display: "🌐 SnQL to SQL",
    component: SnQLToSQL,
  },
  {
    id: "snuba-explain",
    display: "🩺 Snubsplain",
    component: SnubaExplain,
  },
  {
    id: "system-queries",
    display: "🏚️ System Queries",
    component: ClickhouseQueries,
  },
  {
    id: "system-shell",
    display: "💻 System Shell",
    component: SystemShellPage,
  },
  {
    id: "copy-tables",
    display: "📋 Copy Tables",
    component: CopyTables,
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
    id: "tracing-shell",
    display: "💻 Tracing Shell",
    component: SQLShellPage,
  },
  {
    id: "rpc-endpoints",
    display: "🔌 RPC Endpoints",
    component: RpcEndpoints,
  },
  {
    id: "querylog",
    display: "🔍 ClickHouse Querylog",
    component: QuerylogQueries,
  },
  {
    id: "auditlog",
    display: "📝 Audit Log",
    component: AuditLog,
  },
  {
    id: "production-queries",
    display: "🔦 Production Queries",
    component: ProductionQueries,
  },
  {
    id: "run-custom-jobs",
    display: "▶️ View/Run Custom Jobs",
    component: ViewCustomJobs,
  },
];

export { NAV_ITEMS };
