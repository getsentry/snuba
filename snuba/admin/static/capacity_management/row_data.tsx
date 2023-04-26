import React, { ReactNode } from "react";

import { linkStyle } from "./styles";
import { RowData } from "./types";

function getReadonlyRow(
  key: string,
  value: string,
  description: string,
  type: string,
  showActions: boolean,
  edit: () => void
): RowData {
  return [
    <code style={{ wordBreak: "break-all" }}>{key}</code>,
    <code style={{ wordBreak: "break-all" }}>{value}</code>,
    description,
    type,
    showActions && (
      <a style={linkStyle} onClick={() => edit()}>
        edit
      </a>
    ),
  ];
}

export { getReadonlyRow };
