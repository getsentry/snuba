import React, { useEffect, useState } from "react";
import Client from "../api_client";
import { Table } from "../table";
import { QueryResult, QueryResultColumnMeta, SnQLRequest } from "./types";
import { executeActionsStyle } from "./styles";
import {
  Box,
  Button,
  Collapse,
  Group,
  Loader,
  Select,
  Space,
  Text,
  Textarea,
} from "@mantine/core";
import { useDisclosure } from "@mantine/hooks";
import { CSV } from "../cardinality_analyzer/CSV";

function ProductionQueries(props: { api: Client }) {
  const [datasets, setDatasets] = useState<string[]>([]);
  const [allowedProjects, setAllowedProjects] = useState<string[]>([]);
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

  useEffect(() => {
    props.api.getAllowedProjects().then((res) => {
      setAllowedProjects(res);
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
          duration_ms: result.timing.duration_ms,
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
        <ProjectsList projects={allowedProjects} />
        <div>
          <Textarea
            value={snql_query.query || ""}
            onChange={(evt) => updateQuerySql(evt.target.value)}
            placeholder="Write your query here"
            autosize
            minRows={2}
            maxRows={8}
            data-testid="text-area-input"
          />
        </div>
        <div style={executeActionsStyle}>
          <div>
            <Select
              placeholder="Select a dataset"
              value={snql_query.dataset || ""}
              onChange={selectDataset}
              data={datasets}
            />
          </div>
          <div>
            <Button
              onClick={executeQuery}
              disabled={
                isExecuting ||
                snql_query.dataset == undefined ||
                snql_query.query == undefined
              }
            >
              Execute Query
            </Button>
          </div>
        </div>
      </form>
      <div>
        <h2>Query results</h2>
        {isExecuting ? (
          <Loader />
        ) : (
          queryResultHistory.map((queryResult, idx) => {
            if (idx === 0) {
              return (
                <div>
                  <p>Execution Duration (ms): {queryResult.duration_ms}</p>
                  <Button.Group>
                    <Button
                      variant="outline"
                      onClick={() =>
                        window.navigator.clipboard.writeText(
                          JSON.stringify(queryResult)
                        )
                      }
                    >
                      Copy to clipboard (JSON)
                    </Button>
                    <Button
                      variant="outline"
                      onClick={() =>
                        window.navigator.clipboard.writeText(
                          CSV.sheet([queryResult.columns, ...queryResult.rows])
                        )
                      }
                    >
                      Copy to clipboard (CSV)
                    </Button>
                  </Button.Group>
                  <Space h="md" />
                  <Table
                    headerData={queryResult.columns}
                    rowData={queryResult.rows}
                  />
                </div>
              );
            }
          })
        )}
      </div>
    </div>
  );
}

function ProjectsList(props: { projects: string[] }) {
  const [opened, { toggle }] = useDisclosure(false);

  return (
    <Box mb="xs" mx="auto">
      <Group position="left" mb={5}>
        <Button onClick={toggle}>
          {opened ? "Hide" : "View"} Allowed Projects
        </Button>
      </Group>

      <Collapse in={opened}>
        <Text>{props.projects.join(", ")}</Text>
      </Collapse>
    </Box>
  );
}

// {queryResultHistory.length > 1 && <h2>Query History</h2>}
//         {queryResultHistory.map((queryResult, idx) => {
//           if (idx) {
//             <QueryResultHistoryItem queryResult={queryResult} />;
//           }
//         })}
//       </div>
//     </div>
//   );
// }

// function QueryResultHistoryItem(props: { queryResult: QueryResult }) {
//   const [opened, { toggle }] = useDisclosure(false);
//   return (
//     <Collapse in={opened}>
//       <div>
//         <p>Execution Duration (ms): {queryResult.duration_ms}</p>
//         <Table headerData={queryResult.columns} rowData={queryResult.rows} />
//       </div>
//     </Collapse>
//   );
// }

export default ProductionQueries;
