import React, { useState } from "react";
import { COLORS } from "./theme";
import { NAV_ITEMS } from "./data";
import Client from "./api_client";

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
        {NAV_ITEMS.map((item) =>
          allowedTools?.includes(item.id) || allowedTools?.includes("all") ? (
            item.id === active ? (
              <li key={item.id} style={{ color: COLORS.TEXT_DEFAULT }}>
                <a className="nav-link-active" style={linkStyle}>
                  {item.display}
                </a>
              </li>
            ) : (
              <li key={item.id} style={{ color: COLORS.TEXT_INACTIVE }}>
                <a
                  className="nav-link"
                  style={linkStyle}
                  onClick={() => navigate(item.id)}
                >
                  {item.display}
                </a>
              </li>
            )
          ) : (
            <div key={item.id} />
          )
        )}
      </ul>
    </nav>
  );
}

const navStyle = {
  borderRight: `1px solid ${COLORS.NAV_BORDER}`,
  width: 250,
};

const ulStyle = {
  listStyleType: "none",
  margin: 0,
  padding: 0,
};

const linkStyle = {
  display: "block",
  cursor: "pointer",
  padding: 20,
};

export default Nav;
