import React, { useEffect, useState } from "react";
import Client from "../api_client";
import { Table } from "../table";
import {
  SnQLQueryState,
  SnQLRequest,
  SnQLResult,
  SnubaDatasetName,
} from "../snql_to_sql/types";
import { TextArea } from "../snql_to_sql/utils";
import {
  executeActionsStyle,
  executeButtonStyle,
  selectStyle,
} from "../snql_to_sql/styles";

function ProductionQueries(props: { api: Client }) {
  const [datasets, setDatasets] = useState<SnubaDatasetName[]>([]);
  const [snql_query, setQuery] = useState<SnQLQueryState>({});
  const [queryResultHistory, setQueryResultHistory] = useState<SnQLResult[]>(
    []
  );
  const [isExecuting, setIsExecuting] = useState<boolean>(false);

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
    if (isExecuting) {
      window.alert("A query is already running");
    }
    setIsExecuting(true);
    props.api
      .executeSnQLQuery(snql_query as SnQLRequest)
      .then((result) => {
        console.log(result);
        const query_result = {
          input_query: snql_query.query,
          sql: result.sql,
        };
        setQueryResultHistory((prevHistory) => [query_result, ...prevHistory]);
      })
      .catch((err) => {
        console.log("ERROR", err);
        window.alert("An error occurred: " + err.message);
      })
      .finally(() => {
        setIsExecuting(false);
      });
  }

  return (
    <div>
      <form>
        <h2>Run a SnQL Query</h2>
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
            <button
              onClick={(_) => convertQuery()}
              style={executeButtonStyle}
              disabled={
                isExecuting ||
                snql_query.dataset == undefined ||
                snql_query.query == undefined
              }
            >
              Convert Query
            </button>
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

export default ProductionQueries;
