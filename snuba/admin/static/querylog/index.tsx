import React, { useState, useEffect } from "react";
import Client from "../api_client";
import { Table } from "../table";
import QueryDisplay from "./query_display";
import { QuerylogResult, PredefinedQuery } from "./types";
import QueryEditor from "../query_editor";

function QuerylogQueries(props: { api: Client }) {
  const [predefinedQuery, setPredefinedQuery] =
    useState<PredefinedQuery | null>(null);
  const [predefinedQueryOptions, setPredefinedQueryOptions] = useState<
    PredefinedQuery[]
  >([]);

  useEffect(() => {
    props.api.getPredefinedQuerylogOptions().then((res) => {
      res.forEach(
        (queryOption) => (queryOption.sql = formatSQL(queryOption.sql))
      );
      setPredefinedQueryOptions(res);
    });
  }, []);

  function tablePopulator(queryResult: QuerylogResult) {
    return (
      <div style={scroll}>
        <Table
          headerData={queryResult.column_names}
          rowData={queryResult.rows}
        />
      </div>
    );
  }

  function updatePredefinedQuery(queryName: string) {
    const selectedQuery = predefinedQueryOptions.find(
      (query) => query.name === queryName
    );
    if (selectedQuery) {
      setPredefinedQuery(() => {
        return {
          ...selectedQuery,
        };
      });
    }
  }

  function formatSQL(sql: string) {
    const formatted = sql
      .split("\n")
      .map((line) => line.substring(4, line.length))
      .join("\n");
    return formatted.trim();
  }

  return (
    <div>
      <QueryEditor
        onQueryUpdate={function (query: string): void {}}
        predefinedQueries={[
          {
            name: "a",
            queryTemplate: `
SELECT x as {{{alias}}}
FROM {{{table}}}
WHERE {{{alias}}} < {{{value}}}`,
            description: "Description for a, what does a do?",
          },
          {
            name: "b",
            queryTemplate: `
I'm too lazy to come up with more queries {{{alias}}} {{{hello}}}
{{{alias}}} {{{qwert}}}`,
            description: "Description for b, same as a",
          },
        ]}
      ></QueryEditor>
      <div>
        <form>
          <select
            value={predefinedQuery?.name || ""}
            onChange={(evt) => updatePredefinedQuery(evt.target.value)}
            style={selectStyle}
          >
            <option disabled value="">
              Select a predefined query
            </option>
            {predefinedQueryOptions.map((option: PredefinedQuery) => (
              <option key={option.name} value={option.name}>
                {option.name}
              </option>
            ))}
          </select>
        </form>
      </div>
    </div>
  );
}

const selectStyle = {
  marginBottom: 8,
  height: 30,
};

const scroll = {
  overflowX: "scroll" as const,
  width: "100%",
};

export default QuerylogQueries;
