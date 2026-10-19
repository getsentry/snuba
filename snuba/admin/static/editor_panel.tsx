import React, { ReactNode, useEffect, useRef, useState } from "react";
import { Alert, Button, Text, UnstyledButton } from "@mantine/core";
import { IconChevronDown, IconX } from "@tabler/icons-react";
import { COLORS, SECTION_HEADING_STYLE } from "SnubaAdmin/theme";

// Set on the page to the editor pane's height; see STICKY_HEADER_TOP.
const EDITOR_PANEL_HEIGHT_VAR = "--editor-panel-height";
// Where content stuck to the top of the results sits: just below the pane.
const STICKY_HEADER_TOP = `var(${EDITOR_PANEL_HEIGHT_VAR}, 0px)`;

/**
 * A query page laid out as a sticky editor pane above its results. The pane
 * can be reopened from anywhere in the results, and collapses to a one-line
 * summary of the query when closed.
 */
function EditorPanelLayout(props: {
  title: string;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  // Shown in the closed bar, so the user can see what will run.
  summary: ReactNode;
  // Extra buttons next to Close in the title row.
  titleActions?: ReactNode;
  // Shown above the title, e.g. a PrefillWarning.
  banner?: ReactNode;
  // The editor form's contents.
  editor: ReactNode;
  // The results, below the pane.
  children: ReactNode;
}) {
  // Result tables pin their headers just below the pane, so they need its
  // height, which changes as it opens, closes and the query grows.
  const panelRef = useRef<HTMLDivElement>(null);
  const [panelHeight, setPanelHeight] = useState(0);

  useEffect(() => {
    const panel = panelRef.current;
    if (!panel || typeof ResizeObserver === "undefined") {
      return;
    }
    const observer = new ResizeObserver(() =>
      setPanelHeight(panel.offsetHeight)
    );
    observer.observe(panel);
    return () => observer.disconnect();
  }, []);

  return (
    <div
      style={
        { [EDITOR_PANEL_HEIGHT_VAR]: `${panelHeight}px` } as React.CSSProperties
      }
    >
      <div ref={panelRef} style={panelStyle} data-testid="editor-panel">
        {!props.open && (
          // The whole bar opens the editor. The "button" inside is only a
          // span, since a real button can't be nested in another.
          <UnstyledButton
            onClick={() => props.onOpenChange(true)}
            aria-label="Open editor"
            style={closedBarStyle}
            sx={{ "&:hover": { backgroundColor: COLORS.BG_GRAY_LIGHTER } }}
          >
            <Button
              component="span"
              size="xs"
              rightIcon={<IconChevronDown size={14} />}
              style={{ flexShrink: 0 }}
            >
              Open Editor
            </Button>
            <Text size="sm" style={summaryStyle}>
              {props.summary}
            </Text>
          </UnstyledButton>
        )}
        {/* Hidden rather than unmounted, so the editor keeps its state. */}
        <form style={props.open ? formStyle : { display: "none" }}>
          {props.banner}
          <div style={titleRowStyle}>
            <h2 style={{ ...SECTION_HEADING_STYLE, margin: 0 }}>
              {props.title}
            </h2>
            <div style={titleActionsStyle}>
              {props.titleActions}
              <Button
                size="xs"
                variant="light"
                color="red"
                leftIcon={<IconX size={14} />}
                aria-label="Close editor"
                onClick={() => props.onOpenChange(false)}
              >
                Close
              </Button>
            </div>
          </div>
          {props.editor}
        </form>
      </div>
      {props.children}
    </div>
  );
}

/** Warns that the editor was filled in from a link, which anyone can write. */
function PrefillWarning(props: { onClose: () => void; children?: ReactNode }) {
  return (
    <Alert
      color="orange"
      title="This query was filled in from a link"
      withCloseButton
      onClose={props.onClose}
      style={prefillAlertStyle}
      data-testid="prefill-warning"
    >
      <p style={prefillAlertTextStyle}>
        Whoever clicks Execute is responsible for the query, no matter who or
        what wrote it. Read the whole query and check where it will run before
        you execute it. Don't run it just because an agent or a link suggested
        it.
      </p>
      {props.children && <p style={prefillAlertTextStyle}>{props.children}</p>}
    </Alert>
  );
}

// Keeps column names in view below the editor pane while scrolling a long
// result. This needs no scrolling ancestor between the table and the page,
// so wide results wrap rather than scroll sideways.
const stickyTableHeaderStyles = {
  thStyle: {
    position: "sticky" as const,
    top: STICKY_HEADER_TOP,
    zIndex: 1,
    backgroundColor: COLORS.TABLE_HEADER_BG,
    // Collapsed borders don't move with a stuck cell, so draw its bottom
    // edge here.
    boxShadow: `inset 0 -1px 0 ${COLORS.TABLE_BORDER}`,
  },
};

// Separates the results from the editor pane above.
const resultsHeadingStyle = {
  ...SECTION_HEADING_STYLE,
  marginTop: 32,
  // Keeps the heading clear of the sticky editor bar when scrolled to.
  scrollMarginTop: 72,
};

// Sticks to the top of the page's scroll area, just below the header.
const panelStyle = {
  position: "sticky" as const,
  top: 0,
  zIndex: 10,
  backgroundColor: COLORS.PANEL_BG,
  border: `1px solid ${COLORS.BORDER_GRAY}`,
  borderRadius: 4,
  boxShadow: "0 2px 6px rgba(0, 0, 0, 0.08)",
};

// Scrolls on its own when the open editor is taller than the screen.
const formStyle = {
  maxHeight: "calc(100vh - 100px)",
  overflowY: "auto" as const,
  padding: "12px 16px 0",
};

const titleRowStyle = {
  display: "flex",
  justifyContent: "space-between",
  alignItems: "center",
  marginBottom: 12,
};

const titleActionsStyle = {
  display: "flex",
  alignItems: "center",
  gap: 8,
};

const closedBarStyle = {
  display: "flex",
  width: "100%",
  borderRadius: 4,
  alignItems: "center",
  gap: 12,
  padding: "8px 16px",
};

// One line, so the closed bar stays short whatever the query.
const summaryStyle = {
  minWidth: 0,
  overflow: "hidden",
  textOverflow: "ellipsis",
  whiteSpace: "nowrap" as const,
  color: COLORS.TEXT_LIGHTER,
};

const prefillAlertStyle = {
  margin: "0 0 16px 0",
};

const prefillAlertTextStyle = {
  fontSize: 16,
  margin: "4px 0px",
};

export {
  EditorPanelLayout,
  PrefillWarning,
  STICKY_HEADER_TOP,
  resultsHeadingStyle,
  stickyTableHeaderStyles,
};
