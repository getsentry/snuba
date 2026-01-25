import React from "react";
import { Title } from "@mantine/core";
import Client from "SnubaAdmin/api_client";
import SQLShell from "SnubaAdmin/sql_shell/shell";

interface ShellPageProps {
  api: Client;
}

function SQLShellPage({ api }: ShellPageProps) {
  return (
    <div>
      <Title order={2} style={{ marginBottom: "16px" }}>
        Tracing Shell
      </Title>
      <SQLShell api={api} mode="tracing" />
    </div>
  );
}

function SystemShellPage({ api }: ShellPageProps) {
  return (
    <div>
      <Title order={2} style={{ marginBottom: "16px" }}>
        System Shell
      </Title>
      <SQLShell api={api} mode="system" />
    </div>
  );
}

export default SQLShellPage;
export { SystemShellPage };
