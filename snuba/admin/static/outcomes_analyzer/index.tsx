import React, { useEffect, useState } from "react";
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
    // Backend already strips class-body indentation from predefined SQL.
    props.api.getPredefinedOutcomesQueryOptions().then((res) => {
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
