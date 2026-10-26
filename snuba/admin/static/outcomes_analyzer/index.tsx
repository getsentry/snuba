import React, { useEffect, useState } from "react";
import Client from "SnubaAdmin/api_client";
import { Table } from "SnubaAdmin/table";
import QueryDisplay from "SnubaAdmin/outcomes_analyzer/query_display";
import {
  EnumOption,
  OutcomesQueryResult,
  PredefinedQuery,
} from "SnubaAdmin/outcomes_analyzer/types";

function OutcomesAnalyzer(props: { api: Client }) {
  const [predefinedQueryOptions, setPredefinedQueryOptions] = useState<
    PredefinedQuery[]
  >([]);
  const [categoryOptions, setCategoryOptions] = useState<EnumOption[]>([]);
  const [outcomeOptions, setOutcomeOptions] = useState<EnumOption[]>([]);
  const [enumOptionsError, setEnumOptionsError] = useState<string | null>(null);

  useEffect(() => {
    // Backend already strips class-body indentation from predefined SQL.
    props.api.getPredefinedOutcomesQueryOptions().then((res) => {
      setPredefinedQueryOptions(res);
    });
    props.api
      .getOutcomesEnumOptions()
      .then((res) => {
        setEnumOptionsError(null);
        setCategoryOptions(res.categories ?? []);
        setOutcomeOptions(res.outcomes ?? []);
      })
      .catch((err) => {
        setCategoryOptions([]);
        setOutcomeOptions([]);
        setEnumOptionsError(
          err?.message ||
            "Could not load category/outcome dropdown options; falling back to free-text fields."
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
      categoryOptions={categoryOptions}
      outcomeOptions={outcomeOptions}
      enumOptionsError={enumOptionsError}
    />
  );
}

const scroll = {
  overflowX: "scroll" as const,
  width: "100%",
};

export default OutcomesAnalyzer;
