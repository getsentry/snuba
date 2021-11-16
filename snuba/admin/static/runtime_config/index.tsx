import React, { useState } from "react";
import { Table } from "../table";
import Client from "../api_client";
import { ConfigKey, ConfigValue, ConfigType, RowData } from "./types";

import { containerStyle, paragraphStyle } from "./styles";

function RuntimeConfig(props: { api: Client }) {
  const { api } = props;

  // Data from the API
  const [data, setData] = useState<
    { key: ConfigKey; value: ConfigValue; type: ConfigType }[] | null
  >(null);

  // Load data if it was not previously loaded
  if (data === null) {
    fetchData();
  }

  function fetchData() {
    api.getConfigs().then((res) => {
      setData(res);
    });
  }

  if (data) {
    const rowData: RowData[] = data.map((row) => {
      const { key, value, type } = row;

      return [<code>{key}</code>, <code>{value}</code>, type];
    });

    if (!data) {
      return null;
    }

    return (
      <div style={containerStyle}>
        <p style={paragraphStyle}>These are the current configurations.</p>
        <Table
          headerData={["Key", "Value", "Type"]}
          rowData={rowData}
          columnWidths={[3, 5, 2]}
        />
      </div>
    );
  } else {
    return null;
  }
}

export default RuntimeConfig;
