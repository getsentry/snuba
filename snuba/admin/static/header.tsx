import React, { useEffect, useLayoutEffect, useRef, useState } from "react";
import Client from "SnubaAdmin/api_client";
import { NAV_ITEMS } from "SnubaAdmin/data";
import { COLORS } from "SnubaAdmin/theme";
import ToolGrid from "SnubaAdmin/tool_grid";
import { AdminRegion } from "SnubaAdmin/types";
import { regionColor } from "SnubaAdmin/utils/region_color";

// Grace period so a menu doesn't close while the pointer crosses from its
// trigger to the menu.
const MENU_CLOSE_DELAY_MS = 150;

const REGIONS_PER_ROW = 4;

// Opens on hover or keyboard focus. `menu` is rendered inside the wrapper, so
// moving the pointer onto it keeps the menu open.
function HoverMenu(props: {
  style: React.CSSProperties;
  trigger: (open: boolean) => React.ReactNode;
  menu: (close: () => void) => React.ReactNode;
}) {
  const [open, setOpen] = useState(false);
  const closeTimer = useRef<number | undefined>(undefined);

  useEffect(() => () => window.clearTimeout(closeTimer.current), []);

  function openMenu() {
    window.clearTimeout(closeTimer.current);
    setOpen(true);
  }

  function closeMenuSoon() {
    window.clearTimeout(closeTimer.current);
    closeTimer.current = window.setTimeout(
      () => setOpen(false),
      MENU_CLOSE_DELAY_MS,
    );
  }

  return (
    <div
      style={props.style}
      onMouseEnter={openMenu}
      onMouseLeave={closeMenuSoon}
      onFocus={openMenu}
      onBlur={closeMenuSoon}
    >
      {props.trigger(open)}
      {open && props.menu(() => setOpen(false))}
    </div>
  );
}

// Minimum gap between a menu and the edge of the window.
const MENU_EDGE_MARGIN_PX = 20;
// Space between the trigger and the menu card; it's transparent padding, so
// the pointer stays inside the hover area while crossing it.
const MENU_GAP_PX = 16;
// The caret is a rotated square straddling the card's top edge.
const CARET_SIZE_PX = 14;
// Keeps the caret clear of the card's rounded corners.
const CARET_INSET_PX = 20;

// A dropdown panel centered under its parent (the menu's trigger wrapper),
// nudged sideways only as far as needed to stay inside the window. A caret on
// top points back at the trigger, wherever the panel ends up.
function MenuPanel(props: {
  style?: React.CSSProperties;
  children: React.ReactNode;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const [shift, setShift] = useState(0);
  const [caretX, setCaretX] = useState<number | null>(null);

  useLayoutEffect(() => {
    function place() {
      const el = ref.current;
      if (!el || !el.parentElement) return;
      const anchor = el.parentElement.getBoundingClientRect();
      const width = el.offsetWidth;
      const centeredLeft = anchor.left + anchor.width / 2 - width / 2;
      const maxLeft =
        document.documentElement.clientWidth - MENU_EDGE_MARGIN_PX - width;
      const left = Math.max(
        MENU_EDGE_MARGIN_PX,
        Math.min(centeredLeft, maxLeft),
      );
      setShift(left - centeredLeft);
      const triggerCenter = anchor.left + anchor.width / 2 - left;
      setCaretX(
        Math.max(
          CARET_INSET_PX,
          Math.min(triggerCenter, width - CARET_INSET_PX),
        ),
      );
    }
    place();
    window.addEventListener("resize", place);
    return () => window.removeEventListener("resize", place);
  }, []);

  return (
    <div
      ref={ref}
      style={{
        ...menuPanelStyle,
        ...props.style,
        transform: `translateX(calc(-50% + ${shift}px))`,
      }}
    >
      {caretX !== null && (
        <div
          aria-hidden="true"
          style={{ ...caretStyle, left: caretX - CARET_SIZE_PX / 2 }}
        />
      )}
      {props.children}
    </div>
  );
}

function Header(props: { api: Client; active: string | null }) {
  const [allowedTools, setAllowedTools] = useState<string[] | null>(null);
  const [adminRegions, setAdminRegions] = useState<AdminRegion[]>([]);

  useEffect(() => {
    props.api
      .getAllowedTools()
      .then((res) => setAllowedTools(res.tools))
      .catch(() => {});
    props.api
      .getAdminRegions()
      .then((res) => setAdminRegions(res))
      .catch(() => {});
  }, []);

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

  // The header calls the main region "SaaS"; the regions list uses its name.
  const regionLabel = (region: AdminRegion) =>
    region.is_main ? `SaaS (${region.name})` : region.name;
  const regionKey = (region: AdminRegion) =>
    region.is_main ? "SaaS" : region.name;

  const regionPill = (
    <div
      tabIndex={adminRegions.length > 0 ? 0 : undefined}
      style={{
        ...regionBarStyle,
        backgroundColor: regionColor(current_region),
      }}
    >
      {current_region}
      {adminRegions.length > 0 && " ▾"}
    </div>
  );

  return (
    <header style={headerStyle}>
      <a href="#overview" style={logoLinkStyle}>
        <img
          style={{ height: "100%" }}
          src="./static/snuba.svg"
          alt="Snuba admin"
        />
      </a>
      <div style={breadcrumbStyle}>
        {adminRegions.length > 0 ? (
          <HoverMenu
            style={regionMenuStyle}
            trigger={(open) => (
              <div aria-haspopup="true" aria-expanded={open}>
                {regionPill}
              </div>
            )}
            menu={() => (
              <MenuPanel style={regionPanelStyle}>
                <div
                  style={{
                    ...menuCardStyle,
                    ...regionListStyle,
                    gridTemplateColumns: `repeat(${Math.min(
                      adminRegions.length,
                      REGIONS_PER_ROW,
                    )}, 1fr)`,
                  }}
                >
                  {adminRegions.map((region) => {
                    const isCurrent = regionKey(region) === current_region;
                    return (
                      <a
                        key={region.name}
                        // Land on the same page in the other region.
                        href={`${region.url}#${props.active ?? "overview"}`}
                        className="region-button"
                        aria-current={isCurrent ? "true" : undefined}
                        style={{
                          ...regionOptionStyle,
                          backgroundColor: regionColor(regionKey(region)),
                          ...(isCurrent ? currentRegionOptionStyle : {}),
                        }}
                      >
                        {regionLabel(region)}
                      </a>
                    );
                  })}
                </div>
              </MenuPanel>
            )}
          />
        ) : (
          regionPill
        )}
        {pageTitle && (
          <>
            <span style={separatorStyle}>/</span>
            <HoverMenu
              style={pageMenuStyle}
              trigger={(open) => (
                <span
                  tabIndex={0}
                  aria-haspopup="true"
                  aria-expanded={open}
                  className="page-menu-trigger"
                  style={pageTitleStyle}
                >
                  {pageTitle} ▾
                </span>
              )}
              menu={(close) => (
                <MenuPanel style={toolPanelStyle}>
                  <div style={menuCardStyle}>
                    <ToolGrid
                      allowedTools={allowedTools}
                      compact
                      activeId={props.active}
                      onSelect={close}
                    />
                  </div>
                </MenuPanel>
              )}
            />
          </>
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

// Menus hang from their trigger.
const pageMenuStyle = {
  position: "relative" as const,
  minWidth: 0,
};

// Bordered so it reads as a control. The padding (plus the 1px border) makes
// it as tall as the region pill.
const pageTitleStyle = {
  display: "block",
  cursor: "default",
  border: `1px solid ${COLORS.NAV_BORDER}`,
  borderRadius: 4,
  padding: "3px 12px",
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

const regionMenuStyle = {
  position: "relative" as const,
  flexShrink: 0,
};

const regionPanelStyle = {
  // At least as wide as the pill, so moving straight down always lands on it.
  minWidth: "100%",
};

const regionListStyle = {
  display: "grid",
  gap: 8,
  padding: 8,
};

const regionOptionStyle = {
  ...regionBarStyle,
  fontSize: 16,
  textDecoration: "none",
};

const currentRegionOptionStyle = {
  boxShadow: `inset 0 0 0 2px ${COLORS.HEADER_TEXT}`,
};

const menuPanelStyle = {
  position: "absolute" as const,
  // The transparent padding bridges the gap between the trigger and the
  // header's bottom edge, so moving onto the menu never leaves the hover area.
  top: "100%",
  left: "50%",
  paddingTop: MENU_GAP_PX,
  zIndex: 1000,
};

// Positioned, so it paints over the card's border where the two overlap.
const caretStyle = {
  position: "absolute" as const,
  top: MENU_GAP_PX - CARET_SIZE_PX / 2,
  width: CARET_SIZE_PX,
  height: CARET_SIZE_PX,
  backgroundColor: COLORS.PANEL_BG,
  borderTop: `1px solid ${COLORS.NAV_BORDER}`,
  borderLeft: `1px solid ${COLORS.NAV_BORDER}`,
  transform: "rotate(45deg)",
};

const toolPanelStyle = {
  width: `min(960px, calc(100vw - ${2 * MENU_EDGE_MARGIN_PX}px))`,
};

const menuCardStyle = {
  backgroundColor: COLORS.PANEL_BG,
  border: `1px solid ${COLORS.NAV_BORDER}`,
  borderRadius: 8,
  boxShadow: "0 12px 32px rgba(0, 0, 0, 0.15)",
  padding: 12,
};

export default Header;
