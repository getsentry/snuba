import React from "react";
import { Title } from "@mantine/core";
import Client from "SnubaAdmin/api_client";
import SQLShell from "SnubaAdmin/sql_shell/shell";

interface SQLShellPageProps {
  api: Client;
}

function SQLShellPage({ api }: SQLShellPageProps) {
  return (
    <div>
      <Title order={2} style={{ marginBottom: "16px" }}>
        SQL Shell - ClickHouse Tracing
      </Title>
      <SQLShell api={api} />
    </div>
  );
}

export default SQLShellPage;
