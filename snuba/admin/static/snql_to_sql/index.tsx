import React, { useEffect, useState } from "react";
import Client from "SnubaAdmin/api_client";
import { Table } from "SnubaAdmin/table";

import {
  executeActionsStyle,
  selectStyle,
  executeButtonStyle,
} from "SnubaAdmin/snql_to_sql/styles";
import { TextArea } from "SnubaAdmin/snql_to_sql/utils";
import ExecuteButton from "SnubaAdmin/utils/execute_button";
import {
  SnQLRequest,
  SnQLResult,
  SnubaDatasetName,
  SnQLQueryState,
} from "./types";

function SnQLToSQL(props: { api: Client }) {
  const [datasets, setDatasets] = useState<SnubaDatasetName[]>([]);
  const [snql_query, setQuery] = useState<SnQLQueryState>({});
  const [queryResultHistory, setQueryResultHistory] = useState<SnQLResult[]>(
    []
  );

  useEffect(() => {
    props.api.getSnubaDatasetNames().then((res) => {
      setDatasets(res);
    });
  }, []);

  function selectDataset(dataset: string) {
    setQuery((prevQuery) => {
      return {
        ...prevQuery,
        dataset,
      };
    });
  }

  function updateQuerySql(query: string) {
    setQuery((prevQuery) => {
      return {
        ...prevQuery,
        query,
      };
    });
  }

  function convertQuery() {
    return props.api
      .debugSnQLQuery(snql_query as SnQLRequest)
      .then((result) => {
        const query_result = {
          input_query: snql_query.query,
          sql: result.sql,
        };
        setQueryResultHistory((prevHistory) => [query_result, ...prevHistory]);
      });
  }

  return (
    <div>
      <form>
        <h2>Construct a SnQL Query</h2>
        <div>
          <TextArea value={snql_query.query || ""} onChange={updateQuerySql} />
        </div>
        <div style={executeActionsStyle}>
          <div>
            <select
              value={snql_query.dataset || ""}
              onChange={(evt) => selectDataset(evt.target.value)}
              style={selectStyle}
            >
              <option disabled value="">
                Select a dataset
              </option>
              {datasets.map((dataset) => (
                <option key={dataset} value={dataset}>
                  {dataset}
                </option>
              ))}
            </select>
          </div>
          <div>
            <ExecuteButton
              onClick={convertQuery}
              disabled={
                snql_query.dataset == undefined || snql_query.query == undefined
              }
              label="Convert Query"
            />
          </div>
        </div>
      </form>
      <div>
        <h2>Query results</h2>
        <Table
          headerData={["SnQL", "Generated SQL"]}
          rowData={queryResultHistory.map((queryResult) => [
            <span>{queryResult.input_query}</span>,
            <span>{queryResult.sql}</span>,
          ])}
          columnWidths={[2, 5]}
        />
      </div>
    </div>
  );
}

export default SnQLToSQL;
