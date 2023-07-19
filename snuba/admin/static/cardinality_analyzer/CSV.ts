export class CSV {
  static sheet(rows: Array<Array<unknown>>) {
    return rows.map(CSV.row).join("\n");
  }

  static row(values: unknown[]): string {
    return values.map(CSV.cell).join(",");
  }

  static cell(value: unknown): string {
    if (!value) return "";

    if (typeof value === "string") {
      if (value.includes(",")) {
        return `"${value}"`;
      }
    }

    return value.toString();
  }
}
