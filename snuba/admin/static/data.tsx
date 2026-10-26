import AutoReplacementsBypassProjects from "SnubaAdmin/auto_replacements_bypass_projects";
import ClickhouseMigrations from "SnubaAdmin/clickhouse_migrations";
import ClickhouseQueries from "SnubaAdmin/clickhouse_queries";
import Clusters from "SnubaAdmin/clusters";
import CopyTables from "SnubaAdmin/copy_tables";
import TracingQueries from "SnubaAdmin/tracing";
import SQLShellPage, { SystemShellPage } from "SnubaAdmin/sql_shell";
import SnQLToSQL from "SnubaAdmin/snql_to_sql";
import QuerylogQueries from "SnubaAdmin/querylog";
import OutcomesAnalyzer from "SnubaAdmin/outcomes_analyzer";
import ProductionQueries from "SnubaAdmin/production_queries";
import SnubaExplain from "SnubaAdmin/snuba_explain";
import Welcome from "SnubaAdmin/welcome";
import ViewCustomJobs from "SnubaAdmin/manual_jobs";
import RpcEndpoints from "SnubaAdmin/rpc_endpoints";
import EapStats from "SnubaAdmin/eap_stats";

const NAV_ITEMS = [
  {
    id: "overview",
    display: "🤿 Snuba Admin",
    description: "",
    component: Welcome,
  },
  {
    id: "clusters",
    display: "🗄️ ClickHouse Clusters",
    description:
      "Inspect the ClickHouse clusters, nodes and storages in this region.",
    component: Clusters,
  },
  {
    id: "system-queries",
    display: "🏚️ System Queries",
    description:
      "Run predefined queries against ClickHouse system tables.",
    component: ClickhouseQueries,
  },
  {
    id: "system-shell",
    display: "💻 System Shell",
    description:
      "Interactive SQL shell for querying ClickHouse system tables.",
    component: SystemShellPage,
  },
  {
    id: "copy-tables",
    display: "📋 Copy Tables",
    description:
      "Copy table definitions from a source ClickHouse host to other hosts.",
    component: CopyTables,
  },
  {
    id: "clickhouse-migrations",
    display: "🚧 ClickHouse Migrations",
    description:
      "Check migration status and run or reverse migrations by group.",
    component: ClickhouseMigrations,
  },
  {
    id: "tracing",
    display: "🔎 ClickHouse Tracing",
    description:
      "Run a query with tracing enabled and inspect ClickHouse's trace logs.",
    component: TracingQueries,
  },
  {
    id: "tracing-shell",
    display: "💻 Tracing Shell",
    description:
      "Interactive SQL shell that runs queries with ClickHouse tracing.",
    component: SQLShellPage,
  },
  {
    id: "rpc-endpoints",
    display: "🔌 RPC Endpoints",
    description:
      "Send requests to EAP RPC endpoints and inspect the responses.",
    component: RpcEndpoints,
  },
  {
    id: "querylog",
    display: "🔍 ClickHouse Querylog",
    description:
      "Search the Snuba querylog for what ran, who ran it and how it performed.",
    component: QuerylogQueries,
  },
  {
    id: "outcomes-analyzer",
    display: "📈 Outcomes Analyzer",
    description:
      "Query outcomes to see accepted, filtered and dropped events.",
    component: OutcomesAnalyzer,
  },
  {
    id: "production-queries",
    display: "🔦 Production Queries",
    description:
      "Run SnQL queries against production data for allowed projects.",
    component: ProductionQueries,
  },
  {
    id: "run-custom-jobs",
    display: "▶️ View/Run Custom Jobs",
    description:
      "List manual jobs, check their status and run them.",
    component: ViewCustomJobs,
  },
  {
    id: "snuba-explain",
    display: "🩺 Snubsplain",
    description:
      "Step through how Snuba processes a query, from parsing to the final SQL.",
    component: SnubaExplain,
  },
  {
    id: "snql-to-sql",
    display: "🌐 SnQL to SQL",
    description:
      "Translate a SnQL query into the ClickHouse SQL that Snuba would run.",
    component: SnQLToSQL,
  },
  {
    id: "eap-stats",
    display: "📊 EAP Stats",
    description:
      "Sample EAP data to see item and attribute coverage.",
    component: EapStats,
  },
  {
    id: "auto-replacements-bypass-projects",
    display: "👻 Replacements",
    description:
      "See which projects are currently bypassing the replacements pipeline.",
    component: AutoReplacementsBypassProjects,
  },
];

// Shell pages inherit permissions from their parent pages
const PERMISSION_OVERRIDES: Record<string, string> = {
  "tracing-shell": "tracing",
  "system-shell": "system-queries",
};

function isToolAllowed(itemId: string, allowedTools: string[] | null): boolean {
  const permissionId = PERMISSION_OVERRIDES[itemId] ?? itemId;
  return (
    allowedTools?.includes(permissionId) || allowedTools?.includes("all") || false
  );
}

export { NAV_ITEMS, isToolAllowed };
