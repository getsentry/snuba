import React, { useEffect, useState } from "react";
import { Prism } from "@mantine/prism";

import Client from "SnubaAdmin/api_client";
import QueryEditor from "SnubaAdmin/query_editor";
import { Collapse } from "SnubaAdmin/collapse";
import { SnQLRequest, SnQLResult, ExplainResult, ExplainStep } from "SnubaAdmin/snuba_explain/types";
import { Step } from "SnubaAdmin/snuba_explain/step_render";
import { CustomSelect, getDatasetFromUrl } from "SnubaAdmin/components";

import {
  executeActionsStyle,
  selectStyle,
  executeButtonStyle,
  collapsibleStyle,
} from "SnubaAdmin/snuba_explain/styles";
import { SnubaDatasetName, SnQLQueryState } from "SnubaAdmin/snql_to_sql/types";

function SnubaExplain(props: { api: Client }) {
  const [datasets, setDatasets] = useState<SnubaDatasetName[]>([]);
  const [snql_query, setQuery] = useState<SnQLQueryState>({dataset: getDatasetFromUrl("dataset")});
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

  function updateQuerySnQL(query: string) {
    setQuery((prevQuery) => {
      return {
        ...prevQuery,
        query,
      };
    });
  }

  function explainQuery() {
    if (isExecuting) {
      window.alert("A query is already running");
    }
    setIsExecuting(true);
    props.api
      .debugSnQLQuery(snql_query as SnQLRequest)
      .then((result) => {
        const query_result = {
          input_query: snql_query.query,
          sql: result.sql,
          explain: result.explain as ExplainResult,
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

  let currentExplain, currentRow, groupedSteps;
  if (queryResultHistory.length > 0) {
    currentRow = queryResultHistory[0];
    currentExplain = currentRow.explain;
    if (currentExplain != null) {
      // Group the steps by their category
      groupedSteps = currentExplain.steps.reduce(
        (acc: ExplainStep[][], step: ExplainStep) => {
          if (acc.length == 0) {
            acc.push([step]);
          } else if (acc.slice(-1)[0].slice(-1)[0].category != step.category) {
            acc.push([step]);
          } else {
            acc[acc.length - 1].push(step);
          }
          return acc;
        },
        []
      );
    }
  }

  return (
    <div>
      <h2>Construct a SnQL Query</h2>
      <QueryEditor
        onQueryUpdate={(sql) => {
          updateQuerySnQL(sql);
        }}
      />
      <div style={executeActionsStyle}>
        <div>
          <CustomSelect
            value={snql_query.dataset || ""}
            onChange={selectDataset}
            options={datasets}
            name="dataset"
          />
        </div>
        <div style={executeActionsStyle}>
          <div>
            <button
              onClick={(evt) => {
                evt.preventDefault();
                explainQuery();
              }}
              style={executeButtonStyle}
              disabled={
                isExecuting ||
                snql_query.dataset == undefined ||
                snql_query.query == undefined
              }
            >
              Explain Query
            </button>
          </div>
        </div>
      </div>
      {currentExplain != null && currentRow != null && groupedSteps != null && (
        <div>
          <h2>SNUBSPLAIN</h2>
          <Collapse key="orig_ast" text="Original AST">
            <span>{currentExplain.original_ast}</span>
          </Collapse>
          <h3>Steps</h3>
          <ol style={collapsibleStyle}>
            {groupedSteps.map((steps, i) => (
              <div>
                <Collapse key={i} text={steps[0].category}>
                  {steps.map((step, i) => (
                    <li>
                      <Step key={i} step={step} />
                    </li>
                  ))}
                </Collapse>
              </div>
            ))}
          </ol>
          <Collapse key="final_sql" text="Final SQL">
            <Prism withLineNumbers language="sql">
              {currentRow.sql}
            </Prism>
          </Collapse>
        </div>
      )}
    </div>
  );
}

export default SnubaExplain;
