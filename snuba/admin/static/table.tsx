import React, { ReactNode, CSSProperties } from "react";

import { COLORS } from "SnubaAdmin/theme";

type CustomTableStyles = {
  tableStyle?: CSSProperties;
  headerStyle?: CSSProperties;
  thStyle?: CSSProperties;
  tdStyle?: CSSProperties;
};

const EMPTY_CUSTOM_STYLES = {
  tableStyle: {},
  headerStyle: {},
  thStyle: {},
  tdStyle: {},
};

function createCustomTableStyles(
  styles: Partial<CustomTableStyles> = EMPTY_CUSTOM_STYLES
): CustomTableStyles {
  return { ...EMPTY_CUSTOM_STYLES, ...styles };
}

type TableProps = {
  headerData: ReactNode[];
  rowData: ReactNode[][];
  columnWidths?: number[];
  customStyles?: CustomTableStyles;
};

function Table(props: TableProps) {
  const { headerData, rowData, columnWidths } = props;

  const autoColumnWidths = Array(headerData.length).fill(1);
  const notEmptyColumnWidths = columnWidths ?? autoColumnWidths;
  const sumColumnWidths = notEmptyColumnWidths.reduce((acc, i) => acc + i, 0);
  const customStyles = props.customStyles
    ? props.customStyles
    : EMPTY_CUSTOM_STYLES;
  const thisTableStyle = {
    ...tableStyle,
    ...customStyles.tableStyle,
  };
  const thisHeaderStyle = {
    ...headerStyle,
    ...customStyles.headerStyle,
  };
  const thisThStyle = {
    ...thStyle,
    ...customStyles.thStyle,
  };
  const thisTdStyle = {
    ...tdStyle,
    ...customStyles.tdStyle,
  };

  return (
    <table style={thisTableStyle}>
      <thead style={thisHeaderStyle}>
        <tr>
          {headerData.map((col, idx) => (
            <th
              key={idx}
              style={{
                ...thisThStyle,
                width: `${
                  (notEmptyColumnWidths[idx] * 100) / sumColumnWidths
                }%`,
              }}
            >
              {col}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rowData.map((row, rowIdx) => (
          <tr key={rowIdx}>
            {row.map((col, colIdx) => (
              <td key={colIdx} style={thisTdStyle}>
                {col}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

const border = {
  border: `1px solid ${COLORS.TABLE_BORDER}`,
};

const tableStyle = {
  ...border,
  borderCollapse: "collapse" as const,
  width: "100%",
  fontSize: 16,
  marginBottom: 20,
};

const headerStyle = {
  backgroundColor: COLORS.SNUBA_BLUE,
  color: "white",
};

const thStyle = {
  ...border,
  fontWeight: 600,
  padding: 10,
  textAlign: "left" as const,
};

const tdStyle = {
  ...border,
  padding: 10,
  position: "relative" as const,
  wordBreak: "break-all" as const,
  "font-family": "monospace" as const,
};

function EditableTableCell(props: {
  value: string | number;
  onChange: (value: string) => void;
}) {
  const { value, onChange } = props;

  return (
    <textarea
      value={value}
      onChange={(evt) => onChange(evt.target.value)}
      spellCheck={false}
      style={textAreaStyle}
    />
  );
}

const textAreaStyle = {
  position: "absolute" as const,
  padding: 10,
  left: 0,
  right: 0,
  top: 0,
  bottom: 0,
  width: "calc(100% - 24px)",
};

export { Table, EditableTableCell, createCustomTableStyles };
