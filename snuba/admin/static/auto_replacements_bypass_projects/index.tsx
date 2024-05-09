import React, { useState, useEffect } from "react";
import { AutoReplacementsBypassProjectsData } from "SnubaAdmin/auto_replacements_bypass_projects/types";

import Client from "SnubaAdmin/api_client";

function AutoReplacementsBypassProjects(props: { api: Client }) {
  const [data, setData] = useState<AutoReplacementsBypassProjectsData[] | null>(
    null
  );

  useEffect(() => {
    props.api.getAutoReplacementsBypassProjects().then((res) => {
      setData(res);
    });
  }, []);

  return <div>{JSON.stringify(data)}</div>;
}

export default AutoReplacementsBypassProjects;
