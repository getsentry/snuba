import RuntimeConfig from "./runtime_config";
import AuditLog from "./runtime_config/auditlog";
import ClickhouseMigrations from "./clickhouse_migrations";
import ClickhouseQueries from "./clickhouse_queries";
import TracingQueries from "./tracing";
import SnQLToSQL from "./snql_to_sql";
import Kafka from "./kafka";
import QuerylogQueries from "./querylog";
import CapacityManagement from "./capacity_management";
import DeadLetterQueue from "./dead_letter_queue";
import CardinalityAnalyzer from "./cardinality_analyzer";
import ProductionQueries from "./production_queries";

function Placeholder(props: any) {
  return null;
}

const NAV_ITEMS = [
  { id: "overview", display: "🤿 Snuba Admin", component: Placeholder },
  { id: "config", display: "⚙️ Runtime Config", component: RuntimeConfig },
  {
    id: "capacity-management",
    display: "🪫 Capacity Management",
    component: CapacityManagement,
  },
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
    display: "🔢Cardinality Analyzer!!!",
    component: CardinalityAnalyzer,
  },
  {
    id: "production-queries",
    display: "🔦 Production Queries",
    component: ProductionQueries,
  },
];

export { NAV_ITEMS };
