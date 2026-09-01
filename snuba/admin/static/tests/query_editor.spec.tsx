import React from "react";
import { it, expect, describe, jest, afterEach } from "@jest/globals";
import { act, cleanup, render } from "@testing-library/react";
import {
  formatAbsoluteDateTime,
  generateQuery,
  mergeQueryParamValues,
} from "../query_editor";
import userEvent from "@testing-library/user-event";
import QueryEditor from "SnubaAdmin/query_editor";

jest.mock("SnubaAdmin/common/components/sql_editor");

describe("Query editor", () => {
  global.ResizeObserver = require("resize-observer-polyfill");
  afterEach(cleanup);
  describe("when generating queries", () => {
    it("should replace all instances of parameter name when it has a non-empty parameter value", () => {
      let queryTemplate = "{{key}}_{{value}}_{{key}}_{{value}}";
      let queryParamValues = {
        "{{key}}": "theActualKey",
        "{{value}}": "theActualValue",
      };

      expect(generateQuery(queryTemplate, queryParamValues)).toBe(
        "theActualKey_theActualValue_theActualKey_theActualValue"
      );
    });
    it("should not replace any instances of parameter name when it has an empty parameter value", () => {
      let queryTemplate = "{{key}}_{{value}}_{{key}}_{{value}}";
      let queryParamValues = {
        "{{key}}": "",
        "{{value}}": "theActualValue",
      };

      expect(generateQuery(queryTemplate, queryParamValues)).toBe(
        "{{key}}_theActualValue_{{key}}_theActualValue"
      );
    });
  });
  describe("when formatting absolute dates", () => {
    it("converts a local datetime value to a ClickHouse UTC expression", () => {
      const result = formatAbsoluteDateTime(
        new Date("2026-08-21T12:34:00.000Z")
      );

      expect(result).toBe("toDateTime('2026-08-21 12:34:00', 'UTC')");
    });

    it("leaves an empty datetime unresolved", () => {
      expect(formatAbsoluteDateTime(null)).toBe("");
    });
  });
  describe("when new parameters are given", () => {
    it("should keep existing values if parameter name already exist", () => {
      let newQueryParams = new Set(["a", "b", "c"]);
      let oldQueryParamValues = {
        a: "a_val",
        c: "c_val",
      };

      expect(
        mergeQueryParamValues(newQueryParams, oldQueryParamValues)
      ).toStrictEqual({
        a: "a_val",
        b: "",
        c: "c_val",
      });
    });

    it("should seed default values for known params", () => {
      expect(
        mergeQueryParamValues(new Set(["{{limit}}", "{{org_id}}"]), {})
      ).toStrictEqual({
        "{{limit}}": "100",
        "{{org_id}}": "",
      });
    });
  });
  describe("when rendered", () => {
    beforeEach(() => {
      // Reset query cache
      localStorage.setItem("-query-editor-query", "");
    });

    describe("with predefinedQueries", () => {
      const predefinedQueries = [
        {
          name: "query_1",
          sql: "query_1_sql {{label_1}} {{label_2}}",
          description: "descripton for query 1",
        },
        {
          name: "query_2",
          sql: "query_2_sql {{label_1}}",
          description: "descripton for query 2",
        },
      ];
      it("should show right number of predefined queries in drop down menu", async () => {
        let mockOnQueryUpdate = jest.fn<(query: string) => {}>();
        let { getAllByTestId, getByTestId } = render(
          <QueryEditor
            onQueryUpdate={mockOnQueryUpdate}
            predefinedQueryOptions={predefinedQueries}
          />
        );
        await act(async () => userEvent.click(getByTestId("select")));
        expect(getAllByTestId("select-option")).toHaveLength(
          predefinedQueries.length
        );
      });
      it("should invoke callback when predefined query is selected", async () => {
        let mockOnQueryUpdate = jest.fn<(query: string) => {}>();
        let { getByTestId, getByText } = render(
          <QueryEditor
            onQueryUpdate={mockOnQueryUpdate}
            predefinedQueryOptions={predefinedQueries}
          />
        );
        for (const predefinedQuery of predefinedQueries) {
          await act(async () => userEvent.click(getByTestId("select")));
          await act(async () =>
            userEvent.click(getByText(predefinedQuery.name))
          );
          expect(mockOnQueryUpdate).lastCalledWith(predefinedQuery.sql);
        }
      });
      it("should show query and description when predefined query selected", async () => {
        let mockOnQueryUpdate = jest.fn<(query: string) => {}>();
        let { getByTestId, getByText, getAllByText } = render(
          <QueryEditor
            onQueryUpdate={mockOnQueryUpdate}
            predefinedQueryOptions={predefinedQueries}
          />
        );
        for (const predefinedQuery of predefinedQueries) {
          await act(async () => userEvent.click(getByTestId("select")));
          await act(async () =>
            userEvent.click(getByText(predefinedQuery.name))
          );
          expect(getByText(predefinedQuery.description)).toBeTruthy();
        }
      });

      it("renders relative and absolute time range controls", async () => {
        const timeRangeQuery = {
          name: "time_range_query",
          sql: "timestamp >= {{start_time}} AND timestamp < {{end_time}}",
          description: "Query a time range",
        };
        const mockOnQueryUpdate = jest.fn<(query: string) => {}>();
        const { getByLabelText, getByTestId, getByText } = render(
          <QueryEditor
            onQueryUpdate={mockOnQueryUpdate}
            predefinedQueryOptions={[timeRangeQuery]}
          />
        );

        await act(async () => userEvent.click(getByTestId("select")));
        await act(async () => userEvent.click(getByText(timeRangeQuery.name)));
        expect(mockOnQueryUpdate).toHaveBeenLastCalledWith(
          "timestamp >= now() - INTERVAL 24 HOUR AND timestamp < now()"
        );

        await act(async () =>
          userEvent.click(getByLabelText("Absolute"))
        );
        expect(getByLabelText("Start date and time")).toBeTruthy();
        expect(getByLabelText("End date and time")).toBeTruthy();
        expect(mockOnQueryUpdate).toHaveBeenLastCalledWith(
          expect.stringMatching(
            /^timestamp >= toDateTime\(.+\) AND timestamp < toDateTime\(.+\)$/
          )
        );
        expect(mockOnQueryUpdate).not.toHaveBeenLastCalledWith(
          expect.stringContaining("{{")
        );
      });

      it("renders category and outcome dropdowns from backend options", async () => {
        const outcomesQuery = {
          name: "category_outcome_query",
          sql: "category = {{category}} AND outcome = {{outcome}}",
          description: "Filter by category and outcome",
        };
        const mockOnQueryUpdate = jest.fn<(query: string) => {}>();
        const { getByLabelText, getByTestId, getByText } = render(
          <QueryEditor
            onQueryUpdate={mockOnQueryUpdate}
            predefinedQueryOptions={[outcomesQuery]}
            categoryOptions={[{ value: "7", label: "7 — replay" }]}
            outcomeOptions={[{
              value: "2",
              label: "2 — rate_limited",
            }]}
          />
        );

        await act(async () => userEvent.click(getByTestId("select")));
        await act(async () => userEvent.click(getByText(outcomesQuery.name)));

        expect(getByLabelText("Category")).toBeTruthy();
        expect(getByLabelText("Outcome")).toBeTruthy();

        await act(async () => userEvent.click(getByLabelText("Category")));
        await act(async () => userEvent.click(getByText("7 — replay")));
        await act(async () => userEvent.click(getByLabelText("Outcome")));
        await act(async () =>
          userEvent.click(getByText("2 — rate_limited"))
        );

        expect(mockOnQueryUpdate).toHaveBeenLastCalledWith(
          "category = 7 AND outcome = 2"
        );
      });

      it("uses number inputs for org_id and limit", async () => {
        const numericQuery = {
          name: "numeric_query",
          sql: "org_id = {{org_id}} LIMIT {{limit}}",
          description: "Numeric params",
        };
        const mockOnQueryUpdate = jest.fn<(query: string) => {}>();
        const { getByLabelText, getByTestId, getByText } = render(
          <QueryEditor
            onQueryUpdate={mockOnQueryUpdate}
            predefinedQueryOptions={[numericQuery]}
          />
        );

        await act(async () => userEvent.click(getByTestId("select")));
        await act(async () => userEvent.click(getByText(numericQuery.name)));

        expect(mockOnQueryUpdate).toHaveBeenLastCalledWith(
          "org_id = {{org_id}} LIMIT 100"
        );

        const orgInput = getByLabelText("org id");
        await act(async () => userEvent.clear(orgInput));
        await act(async () => userEvent.type(orgInput, "42"));

        expect(mockOnQueryUpdate).toHaveBeenLastCalledWith(
          "org_id = 42 LIMIT 100"
        );
      });
    });
    describe("with text area input", () => {
      it("should invoke call back with text area value when no labels are present", async () => {
        const user = userEvent.setup();
        let mockOnQueryUpdate = jest.fn<(query: string) => {}>();
        let { getByTestId } = render(
          <QueryEditor onQueryUpdate={mockOnQueryUpdate} />
        );
        const input = "abcde";

        await act(async () => user.type(getByTestId("SQLEditor"), input));
        expect(mockOnQueryUpdate).toHaveBeenLastCalledWith(input);
      });
    });
  });
});
