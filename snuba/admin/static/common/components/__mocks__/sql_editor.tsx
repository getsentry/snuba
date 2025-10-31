import React from "react";

import { Props } from "../sql_editor";

export function SQLEditor({ value, onChange }: Props) {
  return (
    <textarea
      data-testid="SQLEditor"
      value={value}
      onChange={(event) => onChange(event.target.value)}
    ></textarea>
  );
}
