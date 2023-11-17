import React, { useState, useEffect } from "react";
import Client from "../api_client";
import { Table } from "../table";
import QueryDisplay from "./query_display";
import { CardinalityQueryResult, PredefinedQuery } from "./types";

function CardinalityQueries(props: { api: Client }) {
  const [predefinedQueryOptions, setPredefinedQueryOptions] = useState<
    PredefinedQuery[]
  >([]);

  useEffect(() => {
    props.api.getPredefinedCardinalityQueryOptions().then((res) => {
      res.forEach(
          (queryOption) => (queryOption.sql = formatSQL(queryOption.sql))
      );
      setPredefinedQueryOptions(res);
    });
  }, []);

  function tablePopulator(queryResult: CardinalityQueryResult) {
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

const selectStyle = {
  marginBottom: 8,
  height: 30,
};

const scroll = {
  overflowX: "scroll" as const,
  width: "100%",
};

export default CardinalityQueries;
