import React, { useState } from "react";
import QueryResultCopier from "SnubaAdmin/utils/query_result_copier";
import { COLORS } from "SnubaAdmin/theme";
import { QueryResult } from "SnubaAdmin/clickhouse_queries/types";

// History entries store the query and its target together as
// "<sql> (<storage>,<host>:<port>)"; split them back apart for display.
function parseInputQuery(inputQuery: string) {
  const match = inputQuery.match(/^([\s\S]*) \(([^,()]*),([^()]*)\)$/);
  if (!match) {
    return { sql: inputQuery, storage: null, host: null };
  }
  return { sql: match[1], storage: match[2], host: match[3] };
}

function QueryHistoryEntry(props: {
  queryResult: QueryResult;
  isLatest: boolean;
  defaultExpanded: boolean;
  csv: string;
  resultDataPopulator: (queryResult: QueryResult) => JSX.Element;
}) {
  const { queryResult, isLatest } = props;
  const [expanded, setExpanded] = useState(props.defaultExpanded);
  const { sql, storage, host } = parseInputQuery(queryResult.input_query);
  const rowCount = queryResult.rows.length;

  return (
    <div style={cardStyle}>
      <div
        role="button"
        tabIndex={0}
        aria-expanded={expanded}
        style={headerStyle}
        onClick={() => setExpanded((prev) => !prev)}
        onKeyDown={(evt) => {
          if (evt.key === "Enter" || evt.key === " ") {
            evt.preventDefault();
            setExpanded((prev) => !prev);
          }
        }}
      >
        <span style={chevronBoxStyle}>
          <span
            style={{
              ...chevronStyle,
              transform: expanded ? "rotate(45deg)" : "rotate(-45deg)",
            }}
          />
        </span>
        {isLatest && <span style={latestBadgeStyle}>Latest</span>}
        {storage && <span style={storageBadgeStyle}>{storage}</span>}
        {host && <span style={hostStyle}>{host}</span>}
        <span style={metaStyle}>
          {rowCount} {rowCount === 1 ? "row" : "rows"}
          {queryResult.timestamp
            ? ` · ${new Date(queryResult.timestamp * 1000).toLocaleTimeString()}`
            : ""}
        </span>
        {/* Copying shouldn't toggle the entry. */}
        <div style={copierStyle} onClick={(evt) => evt.stopPropagation()}>
          <QueryResultCopier
            rawInput={queryResult.trace_output || ""}
            jsonInput={JSON.stringify(queryResult)}
            csvInput={props.csv}
            compact
          />
        </div>
      </div>
      {expanded ? (
        <>
          <pre style={sqlStyle}>{sql}</pre>
          <div style={resultsStyle}>
            {props.resultDataPopulator(queryResult)}
          </div>
        </>
      ) : (
        <div style={sqlPreviewStyle} title={sql}>
          {sql.replace(/\s+/g, " ")}
        </div>
      )}
    </div>
  );
}

const cardStyle = {
  backgroundColor: COLORS.PANEL_BG,
  border: `1px solid ${COLORS.BORDER_GRAY_LIGHT}`,
  borderRadius: 8,
  marginBottom: 12,
  overflow: "hidden",
};

const headerStyle = {
  display: "flex",
  alignItems: "center",
  flexWrap: "wrap" as const,
  gap: 8,
  padding: "8px 12px",
  cursor: "pointer",
  fontSize: 14,
  userSelect: "none" as const,
};

const chevronBoxStyle = {
  display: "inline-flex",
  alignItems: "center",
  justifyContent: "center",
  width: 14,
  height: 14,
};

// Drawn from two borders; points right when collapsed, down when expanded.
const chevronStyle = {
  width: 7,
  height: 7,
  borderRight: `2px solid ${COLORS.TEXT_DEFAULT}`,
  borderBottom: `2px solid ${COLORS.TEXT_DEFAULT}`,
  transition: "transform 150ms ease",
};

const badgeStyle = {
  fontSize: 12,
  fontWeight: 600,
  padding: "2px 8px",
  borderRadius: 4,
  whiteSpace: "nowrap" as const,
};

const latestBadgeStyle = {
  ...badgeStyle,
  color: COLORS.SNUBA_BLUE,
  backgroundColor: "rgba(31, 111, 235, 0.1)",
};

const storageBadgeStyle = {
  ...badgeStyle,
  color: COLORS.TEXT_DEFAULT,
  backgroundColor: COLORS.BG_GRAY_LIGHT,
};

const hostStyle = {
  fontFamily: "monospace",
  fontSize: 13,
  color: COLORS.TEXT_LIGHTER,
  overflow: "hidden",
  textOverflow: "ellipsis",
  whiteSpace: "nowrap" as const,
  minWidth: 0,
};

const metaStyle = {
  color: COLORS.TEXT_LIGHTER,
  whiteSpace: "nowrap" as const,
};

const copierStyle = {
  marginLeft: "auto",
  cursor: "default",
};

const sqlBase = {
  fontFamily: "monospace",
  fontSize: 13,
  lineHeight: 1.5,
  margin: 0,
  padding: "8px 12px",
  backgroundColor: COLORS.BG_GRAY_LIGHTER,
  borderTop: `1px solid ${COLORS.BORDER_GRAY_LIGHT}`,
  color: COLORS.TEXT_DEFAULT,
};

const sqlStyle = {
  ...sqlBase,
  whiteSpace: "pre-wrap" as const,
  wordBreak: "break-word" as const,
  maxHeight: 240,
  overflowY: "auto" as const,
};

// One line, cut off with an ellipsis; the full query is in the tooltip.
const sqlPreviewStyle = {
  ...sqlBase,
  whiteSpace: "nowrap" as const,
  overflow: "hidden",
  textOverflow: "ellipsis",
  color: COLORS.TEXT_LIGHTER,
};

const resultsStyle = {
  padding: "12px 12px 0",
  borderTop: `1px solid ${COLORS.BORDER_GRAY_LIGHT}`,
};

export { QueryHistoryEntry, parseInputQuery };
