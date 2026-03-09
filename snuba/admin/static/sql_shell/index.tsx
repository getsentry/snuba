import React, { useEffect } from "react";
import Client from "SnubaAdmin/api_client";
import SQLShell from "SnubaAdmin/sql_shell/shell";

interface ShellPageProps {
  api: Client;
}

const shellWrapperStyle = {
  height: "calc(100vh - 75px)",
};

function useDisableWindowScroll() {
  useEffect(() => {
    const originalOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.body.style.overflow = originalOverflow;
    };
  }, []);
}

function SQLShellPage({ api }: ShellPageProps) {
  useDisableWindowScroll();
  return (
    <div style={shellWrapperStyle}>
      <SQLShell api={api} mode="tracing" />
    </div>
  );
}

function SystemShellPage({ api }: ShellPageProps) {
  useDisableWindowScroll();
  return (
    <div style={shellWrapperStyle}>
      <SQLShell api={api} mode="system" />
    </div>
  );
}

export default SQLShellPage;
export { SystemShellPage };
