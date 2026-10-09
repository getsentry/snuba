import React, { useEffect, useRef, useState } from "react";
import Client from "SnubaAdmin/api_client";
import { NAV_ITEMS } from "SnubaAdmin/data";
import { COLORS } from "SnubaAdmin/theme";
import ToolGrid from "SnubaAdmin/tool_grid";
import { regionColor } from "SnubaAdmin/utils/region_color";

// Grace period so the menu doesn't close while the pointer crosses from the
// banner to the menu.
const MENU_CLOSE_DELAY_MS = 150;

function Header(props: { api: Client; active: string | null }) {
  const [allowedTools, setAllowedTools] = useState<string[] | null>(null);
  const [menuOpen, setMenuOpen] = useState(false);
  const closeTimer = useRef<number | undefined>(undefined);

  useEffect(() => {
    props.api
      .getAllowedTools()
      .then((res) => setAllowedTools(res.tools))
      .catch(() => {});
    return () => window.clearTimeout(closeTimer.current);
  }, []);

  function openMenu() {
    window.clearTimeout(closeTimer.current);
    setMenuOpen(true);
  }

  function closeMenuSoon() {
    window.clearTimeout(closeTimer.current);
    closeTimer.current = window.setTimeout(
      () => setMenuOpen(false),
      MENU_CLOSE_DELAY_MS,
    );
  }

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

  // The home page has no title of its own; the logo already says Snuba admin.
  const activeItem = NAV_ITEMS.find((item) => item.id === props.active);
  const pageTitle =
    activeItem && activeItem.id !== "overview" ? activeItem.display : null;

  return (
    <header style={headerStyle}>
      <a href="#overview" style={logoLinkStyle}>
        <img
          style={{ height: "100%" }}
          src="./static/snuba.svg"
          alt="Snuba admin"
        />
      </a>
      {/* The whole breadcrumb opens the tool menu, so the centered menu always
          sits under whatever the pointer is on. */}
      <div
        style={breadcrumbStyle}
        onMouseEnter={openMenu}
        onMouseLeave={closeMenuSoon}
        onFocus={openMenu}
        onBlur={closeMenuSoon}
      >
        <div
          tabIndex={0}
          aria-haspopup="true"
          aria-expanded={menuOpen}
          style={{
            ...regionBarStyle,
            backgroundColor: regionColor(current_region),
          }}
        >
          {current_region} ▾
        </div>
        {pageTitle && (
          <span style={pageTitleStyle}>
            <span style={separatorStyle}>/</span>
            {pageTitle}
          </span>
        )}
        {menuOpen && (
          <div style={menuPanelStyle}>
            <div style={menuCardStyle}>
              <ToolGrid
                allowedTools={allowedTools}
                compact
                onSelect={() => setMenuOpen(false)}
              />
            </div>
          </div>
        )}
      </div>
      <span style={adminTextStyle}>ADMIN</span>
    </header>
  );
}

// Equal outer columns keep the breadcrumb centered whatever its width.
const headerStyle = {
  backgroundColor: COLORS.HEADER_BG,
  height: "45px",
  display: "grid",
  gridTemplateColumns: "1fr auto 1fr",
  // A definite row height lets the logo size itself to the header.
  gridTemplateRows: "100%",
  alignItems: "center",
  gap: 16,
  padding: "10px 20px",
  borderBottom: `1px solid ${COLORS.NAV_BORDER}`,
  // The tool menu is positioned against the header.
  position: "relative" as const,
};

const logoLinkStyle = {
  height: "100%",
  justifySelf: "start" as const,
};

const breadcrumbStyle = {
  display: "flex",
  alignItems: "center",
  gap: 12,
  minWidth: 0,
};

const pageTitleStyle = {
  display: "flex",
  alignItems: "center",
  gap: 12,
  color: COLORS.HEADER_TEXT,
  fontSize: 18,
  fontWeight: 600,
  whiteSpace: "nowrap" as const,
  overflow: "hidden",
  textOverflow: "ellipsis",
};

const separatorStyle = {
  color: COLORS.TEXT_LIGHTER,
  fontWeight: 400,
};

const adminTextStyle = {
  color: COLORS.HEADER_TEXT,
  justifySelf: "end" as const,
};

const regionBarStyle = {
  cursor: "default",
  color: COLORS.REGION_TEXT,
  fontSize: "18px",
  fontWeight: 700,
  letterSpacing: "0.05em",
  textTransform: "uppercase" as const,
  textAlign: "center" as const,
  padding: "4px 14px",
  borderRadius: "4px",
  whiteSpace: "nowrap" as const,
};

const menuPanelStyle = {
  position: "absolute" as const,
  // Start just above the header's bottom edge; the transparent padding bridges
  // the gap below the breadcrumb so moving onto the menu never leaves the
  // hover area.
  top: "calc(100% - 8px)",
  left: "50%",
  transform: "translateX(-50%)",
  width: "min(960px, calc(100vw - 40px))",
  paddingTop: 12,
  zIndex: 1000,
};

const menuCardStyle = {
  backgroundColor: COLORS.PANEL_BG,
  border: `1px solid ${COLORS.NAV_BORDER}`,
  borderRadius: 8,
  boxShadow: "0 12px 32px rgba(0, 0, 0, 0.15)",
  padding: 12,
};

export default Header;
