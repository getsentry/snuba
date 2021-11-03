import React from "react";
import { COLORS } from "./theme";
import { NAV_ITEMS } from "./data";

type NavProps = {
  active: string | null;
  navigate: (nextTab: string) => void;
};

function Nav(props: NavProps) {
  const { active, navigate } = props;
  return (
    <nav style={navStyle}>
      <ul style={ulStyle}>
        {NAV_ITEMS.map((item) =>
          item.id === active ? (
            <li key={item.id} style={{ color: COLORS.NAV_ACTIVE_TEXT }}>
              <a className="nav-link-active" style={linkStyle}>
                {item.display}
              </a>
            </li>
          ) : (
            <li key={item.id} style={{ color: COLORS.NAV_INACTIVE_TEXT }}>
              <a
                className="nav-link"
                style={linkStyle}
                onClick={() => navigate(item.id)}
              >
                {item.display}
              </a>
            </li>
          )
        )}
      </ul>
    </nav>
  );
}

const navStyle = {
  borderRight: `1px solid ${COLORS.NAV_BORDER}`,
  width: "250px",
};

const ulStyle = {
  listStyleType: "none",
  margin: 0,
  padding: 0,
};

const linkStyle = {
  display: "block",
  cursor: "pointer",
  padding: "20px",
};

export default Nav;
