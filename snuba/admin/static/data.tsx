import RuntimeConfig from "SnubaAdmin/runtime_config";
import AuditLog from "SnubaAdmin/runtime_config/auditlog";
import ClickhouseMigrations from "SnubaAdmin/clickhouse_migrations";
import ClickhouseQueries from "SnubaAdmin/clickhouse_queries";
import TracingQueries from "SnubaAdmin/tracing";
import SnQLToSQL from "SnubaAdmin/snql_to_sql";
import Kafka from "SnubaAdmin/kafka";
import QuerylogQueries from "SnubaAdmin/querylog";
import CapacityManagement from "SnubaAdmin/capacity_management";
import DeadLetterQueue from "SnubaAdmin/dead_letter_queue";
import CardinalityAnalyzer from "SnubaAdmin/cardinality_analyzer";
import ProductionQueries from "SnubaAdmin/production_queries";
import SnubaExplain from "SnubaAdmin/snuba_explain";
import Welcome from "SnubaAdmin/welcome";

const NAV_ITEMS = [
  { id: "overview", display: "🤿 Snuba Admin", component: Welcome },
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
];

export { NAV_ITEMS };
