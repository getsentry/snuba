import React from "react";
import Client from "SnubaAdmin/api_client";
import SQLShell from "SnubaAdmin/sql_shell/shell";

interface ShellPageProps {
  api: Client;
}

const shellWrapperStyle = {
  height: "calc(100vh - 105px)",
};

function SQLShellPage({ api }: ShellPageProps) {
  return (
    <div style={shellWrapperStyle}>
      <SQLShell api={api} mode="tracing" />
    </div>
  );
}

function SystemShellPage({ api }: ShellPageProps) {
  return (
    <div style={shellWrapperStyle}>
      <SQLShell api={api} mode="system" />
    </div>
  );
}

export default SQLShellPage;
export { SystemShellPage };
