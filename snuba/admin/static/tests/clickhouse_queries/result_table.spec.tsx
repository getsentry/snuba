import React from "react";
import { it, expect, describe, afterEach } from "@jest/globals";
import { cleanup, render, screen } from "@testing-library/react";
import { ResultTable } from "SnubaAdmin/clickhouse_queries/result_table";

describe("ResultTable", () => {
  afterEach(cleanup);

  function alignOf(text: string) {
    return screen.getByText(text).closest("td")!.style.textAlign;
  }

  it("aligns a column by all of its values, not each value alone", () => {
    render(
      <ResultTable
        columnNames={["count()", "partition", "table"]}
        rows={[
          [30, "202604", "events"],
          [50, "(90,'2026-09-14')", "spans"],
          [80, null, "outcomes"],
        ]}
      />
    );

    expect(alignOf("30")).toBe("right");
    expect(alignOf("202604")).toBe("left");
    expect(alignOf("(90,'2026-09-14')")).toBe("left");
    expect(alignOf("events")).toBe("left");
  });

  it("right-aligns a column of numbers and nulls", () => {
    render(
      <ResultTable columnNames={["n"]} rows={[["12"], [null], ["3.5"]]} />
    );

    expect(alignOf("12")).toBe("right");
    expect(alignOf("3.5")).toBe("right");
  });
});
