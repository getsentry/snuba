import { CSV } from "../../cardinality_analyzer/CSV";

describe("CSV.sheet", () => {
  it("Should return a CSV value of a grid of values", () => {
    expect(
      CSV.sheet([
        ["name", "age"],
        ["George", 33],
      ])
    ).toEqual(`name,age\nGeorge,33`);
  });

  it("Should escape commas by wrapping in double quotes", () => {
    expect(
      CSV.sheet([
        ["name", "motto"],
        ["George", "do a good job, don't do a bad job"],
      ])
    ).toEqual(`name,motto\nGeorge,"do a good job, don't do a bad job"`);
  });

  it("Should escape quotes in comma-containing values", () => {
    expect(
      CSV.sheet([
        ["name", "motto"],
        ["George", 'hello, "world"'],
      ])
    ).toEqual(`name,motto\nGeorge,"hello, ""world"""`);
  });

  it("Should escape quotes by escaping with a backslash", () => {
    expect(
      CSV.sheet([
        ["name", "motto"],
        ["George", 'hello "world"'],
      ])
    ).toEqual(`name,motto\nGeorge,hello ""world""`);
  });
});
