import React, { useEffect, useState } from "react";
import Client from "SnubaAdmin/api_client";
import ExecuteButton from "SnubaAdmin/utils/execute_button";
import { Table } from "SnubaAdmin/table";
import {
  QueryResult,
  QueryResultColumnMeta,
  MQLRequest,
} from "SnubaAdmin/mql_queries/types";
import { executeActionsStyle, spacing } from "SnubaAdmin/mql_queries/styles";
import {
  Accordion,
  Box,
  Button,
  Code,
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
import QueryResultCopier from "SnubaAdmin/utils/query_result_copier";

const MQLQueryExample = `(sum(d:transactions/duration@millisecond{status_code: 200}) by transaction + sum(d:transactions/duration@millisecond) by transaction) * 100.0`;

const MQLContextExample = `{
  "start": "2024-01-01T08:00:00+00:00",
  "end": "2024-01-01T10:30:00+00:00",
  "rollup": {
    "orderby": "DESC",
    "granularity": 60,
    "interval": null,
    "with_totals": "True"
  },
  "scope": {
    "org_ids": [101],
    "project_ids": [1],
    "use_case_id": "performance"
  },
  "indexer_mappings": {
    "transaction.duration": "d:transactions/duration@millisecond",
    "d:transactions/duration@millisecond": 1068,
    "transaction": 12345,
    "status_code": 67890
  },
  "limit": null,
  "offset": null
}`;

function MQLQueries(props: { api: Client }) {
  const [mql_query, setQuery] = useState<Partial<MQLRequest>>({});
  const [raw_mql_context, setMQLContext] = useState<string>();
  const [queryResultHistory, setQueryResultHistory] = useState<QueryResult[]>(
    []
  );
  const [allowedProjects, setAllowedProjects] = useState<string[]>([]);
  const [isExecuting, setIsExecuting] = useState<boolean>(false);

  useEffect(() => {
    props.api.getAllowedProjects().then((res) => {
      setAllowedProjects(res);
    });
  }, []);

  function updateMQLQuery(query: string) {
    setQuery((prevQuery) => {
      return {
        ...prevQuery,
        query,
      };
    });
  }

  function executeQuery() {
    try {
      let mql_context_obj = JSON.parse(raw_mql_context!);
      mql_query.mql_context = mql_context_obj;
      mql_query.query = mql_query.query!.trim();
    } catch (err) {
      if (err instanceof Error) {
        console.log("ERROR", err);
        window.alert("An error occurred: " + err.message);
      }
    }

    return props.api.executeMQLQuery(mql_query as MQLRequest).then((result) => {
      const result_columns = result.meta.map(
        (col: QueryResultColumnMeta) => col.name
      );
      const query_result: QueryResult = {
        input_query: mql_query.query,
        input_mql_context: mql_query.mql_context,
        columns: result_columns,
        rows: result.data.map((obj: object) =>
          result_columns.map(
            (col_name: string) => obj[col_name as keyof typeof obj]
          )
        ),
        duration_ms: result.timing.duration_ms,
        quota_allowance: result.quota_allowance,
      };
      setQueryResultHistory((prevHistory) => [query_result, ...prevHistory]);
    });
  }

  return (
    <div>
      <form>
        <h2>Run a MQL Query</h2>
        <ProjectsList projects={allowedProjects} />
        <div style={spacing}>
          <MQLExample />
        </div>
        <div>
          <Textarea
            value={mql_query.query || ""}
            onChange={(evt) => updateMQLQuery(evt.target.value)}
            placeholder="Write your MQL query here"
            autosize
            minRows={1}
            maxRows={8}
            data-testid="text-area-input"
          />
        </div>
        <div style={spacing}>
          <Textarea
            value={raw_mql_context || ""}
            onChange={(evt) => setMQLContext(evt.target.value)}
            placeholder="Write the MQL context dictionary here"
            autosize
            minRows={4}
            maxRows={22}
            data-testid="text-area-input"
          />
        </div>
        <div style={executeActionsStyle}>
          <div>
            <ExecuteButton
              onClick={executeQuery}
              disabled={
                mql_query.query == undefined || raw_mql_context == undefined
              }
            />
          </div>
        </div>
      </form>
      <div>
        {queryResultHistory.length > 0 && (
          <>
            <h2>Query results</h2>
            <div>
              <div id="queryResultInfo">
                Execution Duration (ms): {queryResultHistory[0].duration_ms}
                {QueryResultQuotaAllowance({
                  queryResult: queryResultHistory[0],
                })}
              </div>
              <QueryResultCopier
                jsonInput={JSON.stringify(queryResultHistory[0])}
                csvInput={CSV.sheet([
                  queryResultHistory[0].columns,
                  ...queryResultHistory[0].rows,
                ])}
              />
              <Space h="md" />
              <Table
                headerData={queryResultHistory[0].columns}
                rowData={queryResultHistory[0].rows}
              />
            </div>
          </>
        )}
      </div>
      <div>
        {queryResultHistory.length > 1 && (
          <>
            <h2>Query History</h2>
            <Accordion multiple transitionDuration={0} chevronPosition="left">
              {queryResultHistory.slice(1).map((queryResult, idx) => {
                return (
                  <Accordion.Item
                    value={(queryResultHistory.length - idx).toString()}
                  >
                    <Accordion.Control>
                      query: {queryResult.input_query}, mql_context:{" "}
                      {JSON.stringify(queryResult.input_mql_context)}
                    </Accordion.Control>
                    <Accordion.Panel>
                      <QueryResultHistoryItem queryResult={queryResult} />
                    </Accordion.Panel>
                  </Accordion.Item>
                );
              })}
            </Accordion>
          </>
        )}
      </div>
    </div >
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

function MQLExample() {
  const [opened, { toggle }] = useDisclosure(false);

  return (
    <Box mb="xs" mx="auto">
      <Group position="left" mb={5}>
        <Button onClick={toggle}>{opened ? "Hide" : "View"} MQL Example</Button>
      </Group>

      <Collapse in={opened}>
        MQL Query:
        <Code block>{MQLQueryExample}</Code>
        MQL Context:
        <Code block>{MQLContextExample}</Code>
      </Collapse>
    </Box>
  );
}

function renderThrottleStatus(isThrottled: boolean, reasonHeader: string[]) {
  return isThrottled ? (
    <Text
      style={{ color: "darkorange", fontFamily: "Arial", fontSize: "medium" }}
    >
      Quota Allowance - Throttled <br />
      <ol>
        {reasonHeader.map((line, index) => (
          <li key={index}>{line}</li>
        ))}
      </ol>
    </Text>
  ) : (
    <Text style={{ color: "green", fontFamily: "Arial", fontSize: "medium" }}>
      Quota Allowance - Not throttled <br />
      MQL Query executed with 10 threads.
    </Text>
  );
}

function renderPolicyDetails(props: { queryResult: QueryResult }) {
  return (
    <>
      {props.queryResult.quota_allowance &&
        Object.keys(props.queryResult.quota_allowance).map((policy, index) => {
          const { can_run, ...policyDetails } =
            props.queryResult.quota_allowance![policy]; // remove can_run from policyDetails
          const policyDetailsString = JSON.stringify(policyDetails);
          return (
            <React.Fragment key={index}>
              <Text size="xs">{policy}</Text>
              <Text size="xs">{policyDetailsString}</Text>
            </React.Fragment>
          );
        })}
    </>
  );
}

function QueryResultQuotaAllowance(props: { queryResult: QueryResult }) {
  const isThrottled: boolean =
    (props.queryResult.quota_allowance &&
      Object.values(props.queryResult.quota_allowance).some(
        (policy) => policy.max_threads < 10
      )) ||
    false;
  let reasonHeader: string[] = [];
  if (isThrottled) {
    props.queryResult.quota_allowance &&
      Object.keys(props.queryResult.quota_allowance).forEach((policyName) => {
        const policy = props.queryResult.quota_allowance![policyName];
        if (policy.max_threads < 10 && policy.explanation.reason != null) {
          reasonHeader.push(
            policyName +
            ": " +
            policy.explanation.reason +
            ". MQL Query executed with " +
            policy.max_threads +
            " threads."
          );
        }
      });
  }
  return (
    <Accordion multiple transitionDuration={0} chevronPosition="left">
      <Accordion.Item value="0">
        <Accordion.Control>
          {renderThrottleStatus(isThrottled, reasonHeader)}
        </Accordion.Control>
        <Accordion.Panel>{renderPolicyDetails(props)}</Accordion.Panel>
      </Accordion.Item>
    </Accordion>
  );
}

function QueryResultHistoryItem(props: { queryResult: QueryResult }) {
  return (
    <div>
      <div id="queryResultInfo">
        Execution Duration (ms): {props.queryResult.duration_ms}
        {QueryResultQuotaAllowance({ queryResult: props.queryResult })}
      </div>
      <QueryResultCopier
        jsonInput={JSON.stringify(props.queryResult)}
        csvInput={CSV.sheet([props.queryResult.columns, ...props.queryResult.rows])}
      />
      <Space h="md" />
      <Table
        headerData={props.queryResult.columns}
        rowData={props.queryResult.rows}
      />
    </div>
  );
}

export default MQLQueries;
