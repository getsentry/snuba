import RuntimeConfig from "SnubaAdmin/runtime_config";
import AutoReplacementsBypassProjects from "SnubaAdmin/auto_replacements_bypass_projects";
import AuditLog from "SnubaAdmin/runtime_config/auditlog";
import ClickhouseMigrations from "SnubaAdmin/clickhouse_migrations";
import ClickhouseQueries from "SnubaAdmin/clickhouse_queries";
import TracingQueries from "SnubaAdmin/tracing";
import SQLShellPage from "SnubaAdmin/sql_shell";
import SnQLToSQL from "SnubaAdmin/snql_to_sql";
import Kafka from "SnubaAdmin/kafka";
import QuerylogQueries from "SnubaAdmin/querylog";
import CapacityManagement from "SnubaAdmin/capacity_management";
import CapacityBasedRoutingSystem from "SnubaAdmin/cbrs";
import DeadLetterQueue from "SnubaAdmin/dead_letter_queue";
import CardinalityAnalyzer from "SnubaAdmin/cardinality_analyzer";
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
    id: "capacity-management",
    display: "🪫 Capacity Management",
    component: CapacityManagement,
  },
  {
    id: "capacity-based-routing-system",
    display: "🔄 Capacity Based Routing System",
    component: CapacityBasedRoutingSystem,
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
    id: "sql-shell",
    display: "💻 SQL Shell",
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
    id: "cardinality-analyzer",
    display: "🔢 Cardinality Analyzer!!!",
    component: CardinalityAnalyzer,
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
