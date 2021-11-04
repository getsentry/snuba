function Placeholder(props: any) {
  return null;
}

const NAV_ITEMS = [
  { id: "overview", display: "Overview", component: Placeholder },
  { id: "config", display: "Runtime config", component: Placeholder },
  {
    id: "clickhouse",
    display: "ClickHouse🏚️",
    component: Placeholder,
  },
];

export { NAV_ITEMS };
