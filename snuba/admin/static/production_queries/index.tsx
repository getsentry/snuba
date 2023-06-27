import React, { useEffect, useState } from "react";
import Client from "../api_client";
import { Table } from "../table";
import { QueryResult, QueryResultColumnMeta, SnQLRequest } from "./types";
import { executeActionsStyle, executeButtonStyle, selectStyle } from "./styles";

function ProductionQueries(props: { api: Client }) {
  const [datasets, setDatasets] = useState<string[]>([]);
  const [snql_query, setQuery] = useState<Partial<SnQLRequest>>({});
  const [queryResultHistory, setQueryResultHistory] = useState<QueryResult[]>(
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

  function executeQuery() {
    if (isExecuting) {
      window.alert("A query is already running");
    }
    setIsExecuting(true);
    props.api
      .executeSnQLQuery(snql_query as SnQLRequest)
      .then((result) => {
        const result_columns = result.meta.map(
          (col: QueryResultColumnMeta) => col.name
        );
        const query_result: QueryResult = {
          input_query: snql_query.query,
          columns: result_columns,
          rows: result.data.map((obj: object) =>
            result_columns.map(
              (col_name: string) => obj[col_name as keyof typeof obj]
            )
          ),
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
          <textarea
            spellCheck={false}
            value={snql_query.query || ""}
            onChange={(evt) => updateQuerySql(evt.target.value)}
            style={{ width: "100%", height: 100 }}
            placeholder={"Write your query here"}
          />
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
              onClick={(_) => executeQuery()}
              style={executeButtonStyle}
              disabled={
                isExecuting ||
                snql_query.dataset == undefined ||
                snql_query.query == undefined
              }
            >
              Execute Query
            </button>
          </div>
        </div>
      </form>
      <div>
        <h2>Query results</h2>
        {queryResultHistory.map((queryResult, idx) => {
          if (idx === 0) {
            console.log(queryResult);
            return (
              <div>
                <Table
                  headerData={queryResult.columns}
                  rowData={queryResult.rows}
                />
              </div>
            );
          }
        })}
      </div>
    </div>
  );
}

export default ProductionQueries;
