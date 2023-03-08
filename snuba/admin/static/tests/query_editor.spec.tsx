import React from "react";
import { it, expect, describe, jest, afterEach } from "@jest/globals";
import { cleanup, render } from "@testing-library/react";
import { generateQuery, mergeQueryParamValues } from "../query_editor";
import userEvent from "@testing-library/user-event";
import QueryEditor from "../query_editor";

describe("Query editor", () => {
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
  });
  describe("when rendered", () => {
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
      let mockOnQueryUpdate = jest.fn<(query: string) => {}>();
      it("should show right number of predefined queries in drop down menu", () => {
        let { getAllByTestId } = render(
          <QueryEditor
            onQueryUpdate={mockOnQueryUpdate}
            predefinedQueryOptions={predefinedQueries}
          />
        );
        expect(getAllByTestId("select-option")).toHaveLength(
          predefinedQueries.length + 1
        );
      });
      it("should invoke callback when predefined query is selected", async () => {
        const user = userEvent.setup();
        let { getByTestId } = render(
          <QueryEditor
            onQueryUpdate={mockOnQueryUpdate}
            predefinedQueryOptions={predefinedQueries}
          />
        );
        predefinedQueries.forEach(async (predefinedQuery) => {
          await user.selectOptions(getByTestId("select"), predefinedQuery.name);
          expect(mockOnQueryUpdate).lastCalledWith(predefinedQuery.sql);
        });
      });
      it("should show query and description when predefined query selected", async () => {
        const user = userEvent.setup();
        let { getByTestId, getByText } = render(
          <QueryEditor
            onQueryUpdate={mockOnQueryUpdate}
            predefinedQueryOptions={predefinedQueries}
          />
        );
        predefinedQueries.forEach(async (predefinedQuery) => {
          await user.selectOptions(getByTestId("select"), predefinedQuery.name);
          expect(getByText(predefinedQuery.description)).toBeTruthy();
          expect(getByText(predefinedQuery.sql)).toBeTruthy();
        });
      });
    });
  });
});
