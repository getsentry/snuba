import React, { useState, useEffect } from "react";
import { AutoReplacementsBypassProjectsData } from "SnubaAdmin/auto_replacements_bypass_projects/types";

import Client from "SnubaAdmin/api_client";
import { Table } from "../table";

function AutoReplacementsBypassProjects(props: { api: Client }) {
  const [data, setData] = useState<AutoReplacementsBypassProjectsData[] | null>(
    null
  );

  useEffect(() => {
    props.api.getAutoReplacementsBypassProjects().then((res) => {
      setData(res);
    });
  }, []);

  if (!data) {
    return null;
  }

  const rowData = data.map(({ projectID, expiry }) => {
    return [projectID, expiry];
  });

  return (
    <div>
      <div>
        <Table
          headerData={["Project ID", "Expiration Time"]}
          rowData={rowData}
        />
      </div>
    </div>
  );
}

export default AutoReplacementsBypassProjects;
