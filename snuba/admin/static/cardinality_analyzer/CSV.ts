export class CSV {
  static sheet(rows: Array<Array<unknown>>) {
    return rows.map(CSV.row).join("\n");
  }

  static row(values: unknown[]): string {
    return values.map(CSV.cell).join(",");
  }

  static cell(value: unknown): string {
    if (!value) return "";

    let sanitizedValue: string = "";

    if (typeof value === "string") {
      sanitizedValue = value.replace(/"/g, '\\"');

      if (value.includes(",")) {
        return `"${sanitizedValue}"`;
      }
    }

    return value.toString();
  }
}
