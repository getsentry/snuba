import RuntimeConfig from "./runtime_config";
import AuditLog from "./runtime_config/auditlog";

function Placeholder(props: any) {
  return null;
}

const NAV_ITEMS = [
  { id: "overview", display: "Overview", component: Placeholder },
  { id: "config", display: "Runtime config", component: RuntimeConfig },
  {
    id: "clickhouse",
    display: "ClickHouse🏚️",
    component: Placeholder,
  },
  {
    id: "auditlog",
    display: "Audit log",
    component: AuditLog,
  },
];

export { NAV_ITEMS };
