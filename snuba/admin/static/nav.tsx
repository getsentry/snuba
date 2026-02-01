import React, { useState } from "react";
import { COLORS } from "SnubaAdmin/theme";
import { NAV_ITEMS } from "SnubaAdmin/data";
import Client from "SnubaAdmin/api_client";

type NavProps = {
  active: string | null;
  navigate: (nextTab: string) => void;
  api: Client;
};

function Nav(props: NavProps) {
  const { active, navigate, api } = props;

  const [allowedTools, setAllowedTools] = useState<string[] | null>(null);

  // Load data if it was not previously loaded
  if (allowedTools === null) {
    fetchData();
  }

  function fetchData() {
    api.getAllowedTools().then((res) => {
      setAllowedTools(res.tools);
    });
  }

  return (
    <nav style={navStyle}>
      <ul style={ulStyle}>
        {NAV_ITEMS.map((item) => {
          // Shell pages inherit permissions from their parent pages
          const permissionId = item.id === "tracing-shell" ? "tracing"
            : item.id === "system-shell" ? "system-queries"
            : item.id;
          return allowedTools?.includes(permissionId) || allowedTools?.includes("all") ? (
            item.id === active ? (
              <li key={item.id} >
                <a style={{ ...linkStyle, ...activeLinkStyle }} className="nav-link-active">
                  {item.display}
                </a>
              </li>
            ) : (
              <li key={item.id}>
                <a
                  style={{ color: COLORS.TEXT_INACTIVE, ...linkStyle }}
                  className="nav-link"
                  onClick={() => navigate(item.id)}
                >
                  {item.display}
                </a>
              </li>
            )
          ) : (
            <div key={item.id} />
          );
        })}
      </ul>
    </nav>
  );
}

const navStyle: React.CSSProperties = {
  borderRight: `1px solid ${COLORS.NAV_BORDER}`,
  width: 250,
  overflowY: "auto",
  flexShrink: 0,
};

const ulStyle = {
  listStyleType: "none",
  margin: 0,
  padding: 0,
};

const linkStyle = {
  display: "block",
  textDecoration: "none",
  cursor: "pointer",
  padding: 20,
};

const activeLinkStyle = {
  color: COLORS.TEXT_DEFAULT,
  backgroundColor: "rgba(59, 130, 246, 0.15)",
  borderLeft: "3px solid #3b82f6",
  fontWeight: "bold" as const,
};

export default Nav;
