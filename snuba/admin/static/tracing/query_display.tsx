import React, { useEffect, useState } from "react";
import {
  Accordion,
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
import QueryResultCopier from "SnubaAdmin/utils/query_result_copier";
import { getRecentHistory, setRecentHistory } from "SnubaAdmin/query_history";
import { CustomSelect, getParamFromStorage } from "SnubaAdmin/select";
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
  const [query, setQuery] = useState<QueryState>({
    storage: getParamFromStorage("storage"),
  });
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
          storage: query.storage,
          num_rows_result: result.num_rows_result,
          result: result.result,
          cols: result.cols,
          trace_output: result.trace_output,
          summarized_trace_output: result.summarized_trace_output,
          profile_events_results: result.profile_events_results,
          profile_events_meta: result.profile_events_meta,
          profile_events_profile: result.profile_events_profile,
          error: result.error,
        };
        setRecentHistory(HISTORY_KEY, tracing_result);
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
          <CustomSelect
            value={query.storage || ""}
            onChange={selectStorage}
            name="storage"
            options={storages}
          />
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: "1rem" }}>
          <ExecuteButton
            onClick={executeQuery}
            disabled={!query.storage || !query.sql}
          />
        </div>
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
              <QueryResultCopier
                rawInput={queryResult.trace_output || ""}
                jsonInput={JSON.stringify(queryResult)}
              />
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
              <Accordion chevronPosition="left">
                <Accordion.Item value="query-result" key="query-result">
                  <Accordion.Control>
                    <Title order={3}>Query Result</Title>
                  </Accordion.Control>
                  <Accordion.Panel>
                    <Table
                      headerData={
                        (queryResult.cols || []).map(([name, ty]) => <>{name} <small>({ty})</small></>)
                      }
                      rowData={queryResult.result || []}
                    />
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
