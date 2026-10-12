import React from "react";
import { NAV_ITEMS, isToolAllowed } from "SnubaAdmin/data";
import { COLORS } from "SnubaAdmin/theme";

type ToolGridProps = {
  allowedTools: string[] | null;
  // Compact tiles drop the description so more fit in a small space.
  compact?: boolean;
  // Highlights the current page's tile.
  activeId?: string | null;
  onSelect?: () => void;
};

function ToolGrid(props: ToolGridProps) {
  const { allowedTools, compact = false, activeId, onSelect } = props;

  const tools = NAV_ITEMS.filter(
    (item) => item.id !== "overview" && isToolAllowed(item.id, allowedTools)
  );

  return (
    <div style={compact ? compactGridStyle : gridStyle}>
      {tools.map((item) => {
        const [icon, ...title] = item.display.split(" ");
        return (
          <a
            key={item.id}
            href={`#${item.id}`}
            className="tool-tile"
            aria-current={item.id === activeId ? "page" : undefined}
            style={{
              ...(compact ? compactTileStyle : tileStyle),
              ...(item.id === activeId ? activeTileStyle : {}),
            }}
            onClick={onSelect}
          >
            <span style={compact ? compactIconStyle : iconStyle}>{icon}</span>
            <span style={compact ? compactTitleStyle : titleStyle}>
              {title.join(" ")}
            </span>
            {!compact && (
              <span style={descriptionStyle}>{item.description}</span>
            )}
          </a>
        );
      })}
    </div>
  );
}

const gridStyle = {
  display: "grid",
  gridTemplateColumns: "repeat(auto-fill, minmax(260px, 1fr))",
  gap: 16,
};

const compactGridStyle = {
  display: "grid",
  gridTemplateColumns: "repeat(auto-fill, minmax(210px, 1fr))",
  gap: 8,
};

const tileStyle = {
  display: "flex",
  flexDirection: "column" as const,
  gap: 6,
  padding: 18,
  borderRadius: 8,
  textDecoration: "none",
  color: COLORS.TEXT_DEFAULT,
};

const compactTileStyle = {
  display: "flex",
  alignItems: "center",
  gap: 10,
  padding: "10px 12px",
  borderRadius: 6,
  textDecoration: "none",
  color: COLORS.TEXT_DEFAULT,
};

const activeTileStyle = {
  borderColor: COLORS.SNUBA_BLUE,
  backgroundColor: "rgba(31, 111, 235, 0.08)",
};

const iconStyle = {
  fontSize: 28,
  lineHeight: 1,
};

const compactIconStyle = {
  fontSize: 20,
  lineHeight: 1,
};

const titleStyle = {
  fontSize: 18,
  fontWeight: 700,
};

const compactTitleStyle = {
  fontSize: 15,
  fontWeight: 600,
};

const descriptionStyle = {
  fontSize: 14,
  lineHeight: 1.4,
  color: COLORS.TEXT_LIGHTER,
};

export default ToolGrid;
