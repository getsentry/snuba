import ClickhouseSystemQueries from "./clickhouse";
import RuntimeConfig from "./runtime_config";

function Placeholder(props: any) {
  return null;
}

const NAV_ITEMS = [
  { id: "overview", display: "Overview", component: Placeholder },
  { id: "config", display: "Runtime config", component: RuntimeConfig },
  {
    id: "clickhouse",
    display: "ClickHouse🏚️",
    component: ClickhouseSystemQueries,
  },
];

export { NAV_ITEMS };
