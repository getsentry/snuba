import React, { useState } from "react";
import Table from "./table";
import Client from "./api_client";

function RuntimeConfig(props: { api: Client }) {
  const [data, setData] = useState<Map<string, string | number> | null>(null);

  // Only load data if it was not previously loaded
  if (data === null) {
    const { api } = props;
    api.getConfigs().then((res) => {
      setData(res);
    });
  }

  if (data) {
    return (
      <Table headerData={["Key", "Value"]} rowData={Object.entries(data)} />
    );
  } else {
    return null;
  }
}

export default RuntimeConfig;
