import React, { useEffect, useState } from "react";
import {
  Accordion,
  Switch,
  Code,
  Stack,
  Title,
  Group,
  Button,
} from "@mantine/core";
import Client from "SnubaAdmin/api_client";
import QueryEditor from "SnubaAdmin/query_editor";
import { Table } from "SnubaAdmin/table";
import ExecuteButton from "SnubaAdmin/utils/execute_button";
import { getRecentHistory, setRecentHistory } from "SnubaAdmin/query_history";

import { TracingRequest, TracingResult, PredefinedQuery } from "./types";

type QueryState = Partial<TracingRequest>;

const HISTORY_KEY = "tracing_queries";
function QueryDisplay(props: {
  api: Client;
  resultDataPopulator: (
    queryResult: TracingResult,
    showFormatted: boolean
  ) => JSX.Element;
  predefinedQueryOptions: Array<PredefinedQuery>;
}) {
  const [storages, setStorages] = useState<string[]>([]);
  const [query, setQuery] = useState<QueryState>({});
  const [queryResultHistory, setQueryResultHistory] = useState<TracingResult[]>(
    getRecentHistory(HISTORY_KEY)
  );
  const [showFormatted, setShowFormatted] = useState<boolean>(true);

  useEffect(() => {
    props.api.getClickhouseNodes().then((res) => {
      setStorages(res.map((n) => n.storage_name));
    });
  }, []);

  function updateQuerySql(sql: string) {
    setQuery((prevQuery) => {
      return {
        ...prevQuery,
        sql,
      };
    });
  }

  function executeQuery() {
    return props.api
      .executeTracingQuery(query as TracingRequest)
      .then((result) => {
        const tracing_result = {
          input_query: `${query.sql}`,
          timestamp: result.timestamp,
          num_rows_result: result.num_rows_result,
          cols: result.cols,
          trace_output: result.trace_output,
          summarized_trace_output: result.summarized_trace_output,
          error: result.error,
        };
        setRecentHistory(HISTORY_KEY, tracing_result)
        setQueryResultHistory((prevHistory) => [
          tracing_result,
          ...prevHistory,
        ]);
      });
  }

  function selectStorage(storage: string) {
    setQuery((prevQuery) => {
      return {
        ...prevQuery,
        storage: storage,
      };
    });
    console.log(query);
  }

  function copyText(text: string) {
    window.navigator.clipboard.writeText(text);
  }

  return (
    <div>
      <h2>Construct a ClickHouse Query</h2>
      <QueryEditor
        onQueryUpdate={(sql) => {
          updateQuerySql(sql);
        }}
      />
      <div style={executeActionsStyle}>
        <div>
          <select
            value={query.storage || ""}
            onChange={(evt) => selectStorage(evt.target.value)}
            style={selectStyle}
          >
            <option disabled value="">
              Select a storage
            </option>
            {storages.map((storage) => (
              <option key={storage} value={storage}>
                {storage}
              </option>
            ))}
          </select>
        </div>
        <ExecuteButton
          onClick={executeQuery}
          disabled={!query.storage || !query.sql}
        />
      </div>
      <div>
        <h2>Query results</h2>
        <Switch
          checked={showFormatted}
          onChange={(evt: React.ChangeEvent<HTMLInputElement>) =>
            setShowFormatted(evt.currentTarget.checked)
          }
          onLabel="FORMATTED"
          offLabel="RAW"
          size="xl"
        />
        <br />
        <Table
          headerData={["Response"]}
          rowData={queryResultHistory.map((queryResult) => [
            <Stack>
              <Group>
                <Button
                  onClick={() => copyText(queryResult.trace_output || "")}
                >
                  Copy to clipboard (Raw)
                </Button>
                <Button onClick={() => copyText(JSON.stringify(queryResult))}>
                  Copy to clipboard (JSON)
                </Button>
              </Group>
              <Title order={3}>Tracing Data</Title>
              {props.resultDataPopulator(queryResult, showFormatted)}
              <Accordion chevronPosition="left">
                <Accordion.Item value="input-query" key="input-query">
                  <Accordion.Control>
                    <Title order={3}>Input Query</Title>
                  </Accordion.Control>
                  <Accordion.Panel>
                    <Code block>{queryResult.input_query}</Code>
                  </Accordion.Panel>
                </Accordion.Item>
              </Accordion>
            </Stack>,
          ])}
          columnWidths={[1, 5]}
        />
      </div>
    </div>
  );
}

const executeActionsStyle = {
  display: "flex",
  justifyContent: "space-between",
  marginTop: 8,
};

const selectStyle = {
  marginRight: 8,
  height: 30,
};
export default QueryDisplay;
