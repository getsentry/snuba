import React, { useEffect, useState } from "react";
import Client from "SnubaAdmin/api_client";
import { Table } from "SnubaAdmin/table";
import {
  QueryResult,
  QueryResultColumnMeta,
  SnQLRequest,
} from "SnubaAdmin/production_queries/types";
import ExecuteButton from "SnubaAdmin/utils/execute_button";
import { executeActionsStyle } from "SnubaAdmin/production_queries/styles";
import {
  Accordion,
  Box,
  Button,
  Collapse,
  Group,
  Space,
  Text,
  Textarea,
} from "@mantine/core";
import { CustomSelect, getParamFromStorage } from "SnubaAdmin/select";
import { useDisclosure } from "@mantine/hooks";
import { CSV } from "SnubaAdmin/cardinality_analyzer/CSV";
import { getRecentHistory, setRecentHistory } from "SnubaAdmin/query_history";
import QueryResultCopier from "SnubaAdmin/utils/query_result_copier";

const HISTORY_KEY = "production_queries";
function ProductionQueries(props: { api: Client }) {
  const [datasets, setDatasets] = useState<string[]>([]);
  const [allowedProjects, setAllowedProjects] = useState<string[]>([]);
  const [snql_query, setQuery] = useState<Partial<SnQLRequest>>({
    dataset: getParamFromStorage("dataset"),
  });
  const [queryResultHistory, setQueryResultHistory] = useState<QueryResult[]>(
    getRecentHistory(HISTORY_KEY)
  );

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
    return props.api
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
          quota_allowance: result.quota_allowance,
        };
        setRecentHistory(HISTORY_KEY, query_result);
        setQueryResultHistory((prevHistory) => [query_result, ...prevHistory]);
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
            <CustomSelect
              value={snql_query.dataset || ""}
              onChange={selectDataset}
              options={datasets}
              name="dataset"
            />
          </div>
          <div>
            <ExecuteButton
              onClick={executeQuery}
              disabled={
                snql_query.dataset == undefined || snql_query.query == undefined
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
                      {queryResult.input_query}
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
      SnQL Query executed with 10 threads.
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
            ". SnQL Query executed with " +
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
        csvInput={CSV.sheet([
          props.queryResult.columns,
          ...props.queryResult.rows,
        ])}
      />
      <Space h="md" />
      <Table
        headerData={props.queryResult.columns}
        rowData={props.queryResult.rows}
      />
    </div>
  );
}

export default ProductionQueries;
