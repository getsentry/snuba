import RuntimeConfig from "SnubaAdmin/runtime_config";
import AutoReplacementsBypassProjects from "SnubaAdmin/auto_replacements_bypass_projects";
import AuditLog from "SnubaAdmin/runtime_config/auditlog";
import ClickhouseMigrations from "SnubaAdmin/clickhouse_migrations";
import ClickhouseQueries from "SnubaAdmin/clickhouse_queries";
import CopyTables from "SnubaAdmin/copy_tables";
import TracingQueries from "SnubaAdmin/tracing";
import SQLShellPage, { SystemShellPage } from "SnubaAdmin/sql_shell";
import SnQLToSQL from "SnubaAdmin/snql_to_sql";
import Kafka from "SnubaAdmin/kafka";
import QuerylogQueries from "SnubaAdmin/querylog";
import DeadLetterQueue from "SnubaAdmin/dead_letter_queue";
import ProductionQueries from "SnubaAdmin/production_queries";
import MQLQueries from "SnubaAdmin/mql_queries";
import SnubaExplain from "SnubaAdmin/snuba_explain";
import Welcome from "SnubaAdmin/welcome";
import DeleteTool from "SnubaAdmin/delete_tool";
import ViewCustomJobs from "SnubaAdmin/manual_jobs";
import DatabaseClusters from "./database_clusters";
import RpcEndpoints from "SnubaAdmin/rpc_endpoints";

const NAV_ITEMS = [
  { id: "overview", display: "🤿 Snuba Admin", component: Welcome },
  { id: "config", display: "⚙️ Runtime Config", component: RuntimeConfig },
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
    id: "kafka",
    display: "🪵 Kafka",
    component: Kafka,
  },
  {
    id: "dlq",
    display: "♻️ Dead Letter Queue",
    component: DeadLetterQueue,
  },
  {
    id: "production-queries",
    display: "🔦 Production Queries",
    component: ProductionQueries,
  },
  {
    id: "mql-queries",
    display: "🎨 MQL Queries",
    component: MQLQueries,
  },
  {
    id: "delete-tool",
    display: "🗑️ Delete Tool",
    component: DeleteTool,
  },
  {
    id: "run-custom-jobs",
    display: "▶️ View/Run Custom Jobs",
    component: ViewCustomJobs,
  },
  {
    id: "database-clusters",
    display: "🗂️ Database Clusters",
    component: DatabaseClusters,
  },
];

export { NAV_ITEMS };
