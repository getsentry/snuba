import React, { useState, useEffect } from "react";
import Client from "SnubaAdmin/api_client";
import { Table } from "SnubaAdmin/table";
import QueryDisplay from "SnubaAdmin/outcomes_analyzer/query_display";
import {
  OutcomesQueryResult,
  PredefinedQuery,
} from "SnubaAdmin/outcomes_analyzer/types";

function OutcomesAnalyzer(props: { api: Client }) {
  const [predefinedQueryOptions, setPredefinedQueryOptions] = useState<
    PredefinedQuery[]
  >([]);

  useEffect(() => {
    props.api.getPredefinedOutcomesQueryOptions().then((res) => {
      res.forEach((queryOption) => (queryOption.sql = formatSQL(queryOption.sql)));
      setPredefinedQueryOptions(res);
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

  function formatSQL(sql: string) {
    const formatted = sql
      .split("\n")
      .map((line) => line.substring(4, line.length))
      .join("\n");
    return formatted.trim();
  }

  return (
    <div>
      {QueryDisplay({
        api: props.api,
        resultDataPopulator: tablePopulator,
        predefinedQueryOptions: predefinedQueryOptions,
      })}
    </div>
  );
}

const scroll = {
  overflowX: "scroll" as const,
  width: "100%",
};

export default OutcomesAnalyzer;
