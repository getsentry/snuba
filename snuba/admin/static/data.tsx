import RunttimeConfig from "./runtime_config";
import ClickhouseSystemQueries from "./clickhouse"

function Placeholder() {
  return null;
}

const NAV_ITEMS = [
  { id: "overview", display: "Overview", component: Placeholder },
  { id: "config", display: "Runtime config", component: RunttimeConfig },
  { id: "clickhouse", display: "ClickHouse🏚️", component: ClickhouseSystemQueries},
];

export { NAV_ITEMS };
