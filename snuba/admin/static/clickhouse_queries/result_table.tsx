import React, { useState } from "react";
import { Button } from "@mantine/core";
import { IconArrowsMaximize, IconArrowsMinimize } from "@tabler/icons-react";
import { COLORS } from "SnubaAdmin/theme";

// Past this many rows the table scrolls in a box of fixed height, unless the
// user expands it.
const COLLAPSED_ROW_LIMIT = 12;
const COLLAPSED_MAX_HEIGHT = 480;
// Longer values are cut off; clicking one shows all of it.
const CELL_MAX_CHARS = 40;

function displayText(value: unknown) {
  if (value === null || value === undefined) {
    return null;
  }
  return typeof value === "object" ? JSON.stringify(value) : String(value);
}

function isNumeric(value: unknown) {
  return (
    typeof value === "number" ||
    (typeof value === "string" && /^-?\d+(\.\d+)?$/.test(value))
  );
}

function ResultTable(props: { columnNames: string[]; rows: unknown[][] }) {
  const { columnNames, rows } = props;
  const [expanded, setExpanded] = useState(false);
  // "row:col" of each long value the user opened.
  const [openCells, setOpenCells] = useState<Set<string>>(new Set());
  const tall = rows.length > COLLAPSED_ROW_LIMIT;
  // A column lines up on the right only if every value in it is a number, so
  // one that mixes numbers and text doesn't zigzag.
  const numericColumns = columnNames.map(
    (_, c) =>
      rows.some((row) => row[c] !== null && row[c] !== undefined) &&
      rows.every(
        (row) => row[c] === null || row[c] === undefined || isNumeric(row[c])
      )
  );

  function toggleCell(key: string) {
    setOpenCells((prev) => {
      const next = new Set(prev);
      next.has(key) ? next.delete(key) : next.add(key);
      return next;
    });
  }

  return (
    <div>
      <div
        data-testid="result-scroll"
        style={{
          ...scrollBoxStyle,
          maxHeight: tall && !expanded ? COLLAPSED_MAX_HEIGHT : undefined,
        }}
      >
        <table style={tableStyle}>
          <thead>
            <tr>
              <th style={{ ...thStyle, ...rowNumberStyle, ...cornerStyle }}>
                #
              </th>
              {columnNames.map((name, col) => (
                <th key={col} style={thStyle}>
                  {name}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, r) => {
              const rowBg = r % 2 ? COLORS.BG_GRAY_LIGHTER : COLORS.TABLE_BG;
              return (
                <tr key={r}>
                  <td
                    style={{
                      ...tdStyle,
                      ...rowNumberStyle,
                      backgroundColor: rowBg,
                    }}
                  >
                    {r + 1}
                  </td>
                  {row.map((value, c) => {
                    const text = displayText(value);
                    const key = `${r}:${c}`;
                    const long = text !== null && text.length > CELL_MAX_CHARS;
                    const open = openCells.has(key);
                    return (
                      <td
                        key={c}
                        style={{
                          ...tdStyle,
                          backgroundColor: rowBg,
                          textAlign: numericColumns[c] ? "right" : "left",
                        }}
                      >
                        {text === null ? (
                          <span style={nullStyle}>NULL</span>
                        ) : long ? (
                          <div
                            role="button"
                            tabIndex={0}
                            aria-expanded={open}
                            title={open ? "Click to collapse" : text}
                            style={open ? openValueStyle : clippedValueStyle}
                            onClick={() => toggleCell(key)}
                            onKeyDown={(evt) => {
                              if (evt.key === "Enter" || evt.key === " ") {
                                evt.preventDefault();
                                toggleCell(key);
                              }
                            }}
                          >
                            {text}
                          </div>
                        ) : (
                          text
                        )}
                      </td>
                    );
                  })}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {tall && (
        <Button
          size="xs"
          variant="subtle"
          leftIcon={
            expanded ? (
              <IconArrowsMinimize size={14} />
            ) : (
              <IconArrowsMaximize size={14} />
            )
          }
          onClick={() => setExpanded((prev) => !prev)}
          style={toggleStyle}
        >
          {expanded ? "Collapse table" : `Expand to all ${rows.length} rows`}
        </Button>
      )}
    </div>
  );
}

const border = `1px solid ${COLORS.TABLE_BORDER}`;

// Scrolls both ways, so wide and long results stay inside the entry.
const scrollBoxStyle = {
  overflow: "auto",
  border,
  borderRadius: 4,
  marginBottom: 12,
};

// Separate borders, since collapsed ones don't move with stuck cells.
const tableStyle = {
  borderCollapse: "separate" as const,
  borderSpacing: 0,
  minWidth: "100%",
  fontSize: 13,
};

const thStyle = {
  position: "sticky" as const,
  top: 0,
  zIndex: 1,
  backgroundColor: COLORS.TABLE_HEADER_BG,
  color: COLORS.TABLE_HEADER_TEXT,
  fontWeight: 600,
  textAlign: "left" as const,
  whiteSpace: "nowrap" as const,
  padding: "6px 10px",
  borderBottom: border,
  borderRight: border,
};

const tdStyle = {
  fontFamily: "monospace",
  whiteSpace: "nowrap" as const,
  verticalAlign: "top" as const,
  padding: "4px 10px",
  borderBottom: `1px solid ${COLORS.BORDER_GRAY_LIGHT}`,
  borderRight: `1px solid ${COLORS.BORDER_GRAY_LIGHT}`,
};

// Row numbers stay in view while scrolling sideways. The tiny width shrinks
// the column to its widest number, so a table stretched to fill the box gives
// the spare room to the data columns.
const rowNumberStyle = {
  position: "sticky" as const,
  left: 0,
  width: 1,
  textAlign: "right" as const,
  color: COLORS.TEXT_LIGHTER,
  borderRight: border,
};

// Above both the header and the row numbers.
const cornerStyle = {
  zIndex: 2,
  backgroundColor: COLORS.TABLE_HEADER_BG,
};

// Opening a value keeps its width and only adds lines. A wrappable value with
// no set width would let the table squeeze its column to a sliver.
const longValueWidth = `${CELL_MAX_CHARS}ch`;

const clippedValueStyle = {
  width: longValueWidth,
  overflow: "hidden",
  textOverflow: "ellipsis",
  cursor: "pointer",
};

const openValueStyle = {
  width: longValueWidth,
  whiteSpace: "pre-wrap" as const,
  overflowWrap: "anywhere" as const,
  cursor: "pointer",
};

const nullStyle = {
  color: COLORS.TEXT_LIGHTER,
  fontStyle: "italic" as const,
  opacity: 0.7,
};

const toggleStyle = {
  marginBottom: 12,
};

export { ResultTable };
