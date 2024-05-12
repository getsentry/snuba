import React, { useState, useEffect } from "react";
import {
  AutoReplacementsBypassProjectsData,
  ExpiryWindow,
} from "SnubaAdmin/auto_replacements_bypass_projects/types";

import Client from "SnubaAdmin/api_client";

function AutoReplacementsBypassProjects(props: { api: Client }) {
  const [data, setData] = useState<AutoReplacementsBypassProjectsData[] | null>(
    null
  );

  const [expiryWindow, setExpiryWindow] = useState<ExpiryWindow | null>(null);

  useEffect(() => {
    props.api.getAutoReplacementsBypassProjects().then((res) => {
      setData(res);
    });
  }, []);

  useEffect(() => {
    props.api.getExpiryWindow().then((res) => {
      setExpiryWindow(res);
    });
  }, []);

  useEffect(() => {
    props.api.getExpiryWindow().then((res) => {
      setExpiryWindow(res);
    });
  }, []);

  return (
    <div>
      {JSON.stringify(data)}
      <div>
        <h2>Expiration window:</h2>
        <div>{JSON.stringify(expiryWindow)}</div>
      </div>
    </div>
  );
}

export default AutoReplacementsBypassProjects;
