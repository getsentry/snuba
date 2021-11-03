import React, { ReactNode } from "react";

import { COLORS } from "./theme";

type TableProps = {
  headerData: ReactNode[];
  rowData: ReactNode[][];
  columnWidths: number[];
};

function Table(props: TableProps) {
  const { headerData, rowData, columnWidths } = props;

  const sumColumnWidths = columnWidths.reduce((acc, i) => acc + i, 0);

  return (
    <table style={tableStyle}>
      <thead style={headerStyle}>
        <tr>
          {headerData.map((col, idx) => (
            <th
              key={idx}
              style={{
                ...thStyle,
                width: `${(columnWidths[idx] * 100) / sumColumnWidths}%`,
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
              <td key={colIdx} style={tdStyle}>
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
};

function SelectableTableCell(props: {
  options: { value: any; label: string }[];
  selected: any;
  onChange: (value: any) => void;
}) {
  const { options, selected, onChange } = props;
  return (
    <div>
      <select value={selected} onChange={(evt) => onChange(evt.target.value)}>
        <option>Select an option</option>
        {options.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
    </div>
  );
}

function EditableTableCell(props: {
  multiline: boolean;
  value: string | number;
  onChange: (value: string) => void;
}) {
  const { multiline, value, onChange } = props;

  if (multiline) {
    return (
      <textarea value={value} onChange={(evt) => onChange(evt.target.value)} />
    );
  } else {
    return (
      <input
        type="text"
        value={value}
        onChange={(evt) => onChange(evt.target.value)}
      />
    );
  }
}

export { EditableTableCell, SelectableTableCell, Table };
