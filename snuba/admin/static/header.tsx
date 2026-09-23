import React from "react";
import { COLORS } from "SnubaAdmin/theme";

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
      <div style={regionStripStyle}>
        {Array.from({ length: REGION_REPEAT_COUNT }, (_, i) => (
          <span key={i} style={regionBadgeStyle}>
            {current_region}
          </span>
        ))}
      </div>
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
// Repeat the region across the header so it's always in view when glancing up.
const REGION_REPEAT_COUNT = 4;

const regionStripStyle = {
  flex: 1,
  display: "flex",
  justifyContent: "space-around",
  alignItems: "center",
  margin: "0 20px",
  minWidth: 0,
};

const regionBadgeStyle = {
  backgroundColor: COLORS.REGION_BG,
  color: COLORS.REGION_TEXT,
  fontSize: "18px",
  fontWeight: 700,
  letterSpacing: "0.05em",
  textTransform: "uppercase" as const,
  padding: "4px 14px",
  borderRadius: "4px",
  whiteSpace: "nowrap" as const,
};

export default Header;
