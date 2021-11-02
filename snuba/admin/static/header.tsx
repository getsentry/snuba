import React from "react";
import { COLORS } from "./theme";

function Header() {
  return (
    <header style={headerStyle}>
      <img style={{ height: "100%" }} src="snuba.svg" alt="Snuba admin" />
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

export default Header;
