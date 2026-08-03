import React, { useEffect, useState } from "react";
import Client from "SnubaAdmin/api_client";
import { Table } from "SnubaAdmin/table";
import QueryDisplay from "SnubaAdmin/outcomes_analyzer/query_display";
import {
  OutcomesQueryResult,
  PredefinedQuery,
} from "SnubaAdmin/outcomes_analyzer/types";

function formatSQL(sql: string): string {
  return sql
    .split("\n")
    .map((line) => line.substring(4))
    .join("\n")
    .trim();
}

function OutcomesAnalyzer(props: { api: Client }) {
  const [predefinedQueryOptions, setPredefinedQueryOptions] = useState<
    PredefinedQuery[]
  >([]);

  useEffect(() => {
    props.api.getPredefinedOutcomesQueryOptions().then((res) => {
      setPredefinedQueryOptions(
        res.map((queryOption) => ({
          ...queryOption,
          sql: formatSQL(queryOption.sql),
        }))
      );
    });
  }, []);

  function tablePopulator(queryResult: OutcomesQueryResult) {
    return (
      <div style={scroll}>
        <Table
          headerData={queryResult.column_names}
          rowData={queryResult.rows}
        />
      </div>
    );
  }

  return (
    <QueryDisplay
      api={props.api}
      resultDataPopulator={tablePopulator}
      predefinedQueryOptions={predefinedQueryOptions}
    />
  );
}

const scroll = {
  overflowX: "scroll" as const,
  width: "100%",
};

export default OutcomesAnalyzer;
