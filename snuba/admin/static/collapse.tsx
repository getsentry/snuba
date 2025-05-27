import React, { useState } from "react";
import { COLORS } from "SnubaAdmin/theme";

function Collapse(props: { text: string; children: React.ReactNode }) {
  const [collapsed, setCollapsed] = useState(false);
  const { text, children } = props;
  return (
    <div>
      <div style={header}>
        <span style={collapsed ? downArrowContainer : rightArrowContainer}>
          <a
            style={collapsed ? downArrow : rightArrow}
            onClick={() => setCollapsed((prev) => !prev)}
          ></a>
        </span>
        <span style={indent}>{text}</span>
      </div>
      {collapsed && <div style={indent}>{children}</div>}
    </div>
  );
}

const header = {
  fontSize: 16,
  lineHeight: 1,
  height: 20,
  display: "flex",
};

const downArrowContainer = {
  marginTop: -4,
};

const rightArrowContainer = {
  marginTop: 0,
};

const arrow = {
  border: `solid ${COLORS.TEXT_INACTIVE}`,
  borderWidth: "0 2px 2px 0",
  display: "inline-block",
  padding: 4,
  cursor: "pointer",
};

const rightArrow = {
  ...arrow,
  transform: "rotate(-45deg)",
};

const downArrow = {
  ...arrow,
  transform: "rotate(45deg)",
};

const indent = {
  marginLeft: 20,
};

export { Collapse };
