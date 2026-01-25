import React from "react";
import { Title } from "@mantine/core";
import Client from "SnubaAdmin/api_client";
import SQLShell from "SnubaAdmin/sql_shell/shell";

interface ShellPageProps {
  api: Client;
}

const shellWrapperStyle = {
  height: "calc(100vh - 180px)",
};

function SQLShellPage({ api }: ShellPageProps) {
  return (
    <div>
      <Title order={2} style={{ marginBottom: "16px" }}>
        Tracing Shell
      </Title>
      <div style={shellWrapperStyle}>
        <SQLShell api={api} mode="tracing" />
      </div>
    </div>
  );
}

function SystemShellPage({ api }: ShellPageProps) {
  return (
    <div>
      <Title order={2} style={{ marginBottom: "16px" }}>
        System Shell
      </Title>
      <div style={shellWrapperStyle}>
        <SQLShell api={api} mode="system" />
      </div>
    </div>
  );
}

export default SQLShellPage;
export { SystemShellPage };
