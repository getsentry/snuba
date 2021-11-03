import React from "react";

import { COLORS } from "./theme";

type Props = {
  headerData: any[];
  rowData: any[][];
};

function Table(props: Props) {
  const { headerData, rowData } = props;

  return (
    <table style={tableStyle}>
      <thead style={headerStyle}>
        <tr>
          {headerData.map((col, idx) => (
            <th key={idx} style={thStyle}>
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
  width: "800px",
  maxWidth: "100%",
  fontSize: "16px",
};

const headerStyle = {
  backgroundColor: COLORS.SNUBA_BLUE,
  color: "white",
};

const thStyle = {
  ...border,
  fontWeight: 600,
  padding: "10px",
  textAlign: "left" as const,
};

const tdStyle = {
  ...border,
  padding: "10px",
};

export default Table;
