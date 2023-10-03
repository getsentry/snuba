import React from "react";
import { COLORS } from "./theme";

function Header() {
  const PROD_URL_START = "https://snuba-admin";
  const URL = window.location.origin;

  let current_region = "localhost";
  if (URL.startsWith(PROD_URL_START)) {
    current_region = currentRegion();
  }

  function currentRegion() {
    if (URL.startsWith(PROD_URL_START + ".getsentry.net")) {
      return "SaaS";
    }
    let regex = /https:\/\/snuba-admin\.(.*?)\.getsentry\.net/;
    let match = URL.match(regex);

    if (match && match[1]) {
      return match[1];
    }
    return "<unknown current region>";
  }

  return (
    <header style={headerStyle}>
      <img
        style={{ height: "100%" }}
        src="./static/snuba.svg"
        alt="Snuba admin"
      />
      <span style={regionTextStyle}>
        <strong>{current_region}</strong>
      </span>
      <span style={adminTextStyle}>ADMIN</span>
    </header>
  );
}

const headerStyle = {
  backgroundColor: COLORS.HEADER_BG,
  height: "45px",
  display: "flex",
  alignItems: "center",
  padding: "10px 20px",
  justifyContent: "space-between",
};

const adminTextStyle = {
  color: COLORS.HEADER_TEXT,
};
const regionTextStyle = {
  color: COLORS.RED,
  fontSize: "20px",
};

export default Header;
