import RunttimeConfig from "./runtime_config";

function Placeholder() {
  return null;
}

const NAV_ITEMS = [
  { id: "overview", display: "Overview", component: Placeholder },
  { id: "config", display: "Runtime config", component: RunttimeConfig },
  { id: "clickhouse", display: "ClickHouse", component: Placeholder },
];

export { NAV_ITEMS };
