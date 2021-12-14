import RuntimeConfig from "./runtime_config";
import AuditLog from "./runtime_config/auditlog";
import ClickhouseQueries from "./clickhouse_queries";

function Placeholder(props: any) {
  return null;
}

const NAV_ITEMS = [
  { id: "overview", display: "Overview", component: Placeholder },
  { id: "config", display: "Runtime config", component: RuntimeConfig },
  {
    id: "clickhouse",
    display: "ClickHouse🏚️",
    component: ClickhouseQueries,
  },
  {
    id: "auditlog",
    display: "Audit log",
    component: AuditLog,
  },
];

export { NAV_ITEMS };
