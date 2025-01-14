import React, { useEffect, useState } from "react";
import Client from "SnubaAdmin/api_client";
import { Collapse } from "SnubaAdmin/collapse";
import QueryEditor from "SnubaAdmin/query_editor";
import ExecuteButton from "SnubaAdmin/utils/execute_button";

import { SelectItem, Switch } from "@mantine/core";
import { Prism } from "@mantine/prism";
import { RichTextEditor } from "@mantine/tiptap";
import { useEditor } from "@tiptap/react";
import HardBreak from "@tiptap/extension-hard-break";
import Placeholder from "@tiptap/extension-placeholder";
import StarterKit from "@tiptap/starter-kit";
import { getRecentHistory, setRecentHistory } from "SnubaAdmin/query_history";
import { CustomSelect, getParamFromStorage } from "SnubaAdmin/select";

import {
  ClickhouseNodeData,
  QueryRequest,
  QueryResult,
  PredefinedQuery,
} from "SnubaAdmin/clickhouse_queries/types";

type QueryState = Partial<QueryRequest>;

const HISTORY_KEY = "clickhouse_queries";
function QueryDisplay(props: {
  api: Client;
  resultDataPopulator: (queryResult: QueryResult) => JSX.Element;
  predefinedQueryOptions: Array<PredefinedQuery>;
}) {
  const [nodeData, setNodeData] = useState<ClickhouseNodeData[]>([]);
  const [query, setQuery] = useState<QueryState>({
    storage: getParamFromStorage("storage"),
  });
  const [queryResultHistory, setQueryResultHistory] = useState<QueryResult[]>(
    getRecentHistory(HISTORY_KEY)
  );

  useEffect(() => {
    props.api.getClickhouseNodes().then((res) => {
      setNodeData(res);
    });
  }, []);

  function selectStorage(storage: string) {
    setQuery((prevQuery) => {
      // clear old host port
      delete prevQuery.host;
      delete prevQuery.port;

      return {
        ...prevQuery,
        storage: storage,
      };
    });
  }

  function setSudo(value: boolean) {
    setQuery((prevQuery) => {
      return {
        ...prevQuery,
        sudo: value,
      };
    });
  }

  function selectHost(hostString: string) {
    const [host, portAsString] = hostString.split(":");

    setQuery((prevQuery) => {
      return {
        ...prevQuery,
        host: host,
        port: parseInt(portAsString, 10),
      };
    });
  }

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
      .executeSystemQuery(query as QueryRequest)
      .then((result) => {
        result.input_query = `${query.sql} (${query.storage},${query.host}:${query.port})`;
        setRecentHistory(HISTORY_KEY, result);
        setQueryResultHistory((prevHistory) => [result, ...prevHistory]);
      })

  }

  function copyText(text: string) {
    window.navigator.clipboard.writeText(text);
  }

  function getHosts(nodeData: ClickhouseNodeData[]): SelectItem[] {
    let node_info = nodeData.find((el) => el.storage_name === query.storage)!;
    // populate the hosts entries marking distributed hosts that are not also local
    if (node_info) {
      let local_hosts = node_info.local_nodes.map((node) => ({
        value: `${node.host}:${node.port}`,
        label: `${node.host}:${node.port}`,
      }));
      let dist_hosts = node_info.dist_nodes
        .filter((node) => !node_info.local_nodes.includes(node))
        .map((node) => ({
          value: `${node.host}:${node.port}`,
          label: `${node.host}:${node.port} (distributed)`,
        }));
      let hosts = local_hosts.concat(dist_hosts);
      let query_node = node_info.query_node;
      if (query_node) {
        hosts.push({
          value: `${query_node.host}:${query_node.port}`,
          label: `${query_node.host}:${query_node.port} (query node)`,
        });
      }
      return hosts;
    }
    return [];
  }

  return (
    <div>
      <form style={query.sudo ? sudoForm : standardForm}>
        <h2>Construct a ClickHouse System Query</h2>
        <div>
          <Switch
            checked={query.sudo}
            onChange={(evt: React.ChangeEvent<HTMLInputElement>) =>
              setSudo(evt.currentTarget.checked)
            }
            onLabel="Sudo mode"
            offLabel="User mode"
            size="xl"
          />
        </div>
        <QueryEditor
          onQueryUpdate={(sql) => {
            updateQuerySql(sql);
          }}
          predefinedQueryOptions={props.predefinedQueryOptions}
        />
        <div style={executeActionsStyle}>
          <div>
            <CustomSelect
              value={query.storage || ""}
              onChange={selectStorage}
              name="storage"
              options={nodeData.map((storage) => storage.storage_name)}
            />
            <CustomSelect
              disabled={!query.storage}
              value={
                query.host && query.port ? `${query.host}:${query.port}` : ""
              }
              onChange={selectHost}
              name="Host"
              options={getHosts(nodeData)}
            />
          </div>
          <div>
            <ExecuteButton
              onClick={executeQuery}
              onError={(error) => {
                const errorResult: QueryResult = {
                  input_query: `${query.sql} (${query.storage},${query.host}:${query.port})`,
                  timestamp: Math.floor(new Date().getTime() / 1000),
                  column_names: ["Error"],
                  rows: [[JSON.stringify(error)]],
                  trace_output: error.trace_output,
                  error: error.message || "An unknown error occurred",
                };
                setRecentHistory(HISTORY_KEY, errorResult);
                setQueryResultHistory((prevHistory) => [errorResult, ...prevHistory]);
              }}
              disabled={
                !query.storage || !query.host || !query.port || !query.sql
              }
            />
          </div>
        </div>
      </form>
      <div>
        <h2>Query results</h2>
        {queryResultHistory.map((queryResult, idx) => {
          if (idx === 0) {
            return (
              <div key={idx}>
                <p>{queryResult.input_query}</p>
                <p>
                  <button
                    style={executeButtonStyle}
                    onClick={() => copyText(JSON.stringify(queryResult))}
                  >
                    Copy to clipboard
                  </button>
                </p>
                {props.resultDataPopulator(queryResult)}
              </div>
            );
          }

          return (
            <Collapse key={idx} text={queryResult.input_query}>
              <button
                style={executeButtonStyle}
                onClick={() => copyText(JSON.stringify(queryResult))}
              >
                Copy to clipboard
              </button>
              {props.resultDataPopulator(queryResult)}
            </Collapse>
          );
        })}
      </div>
    </div>
  );
}

const sudoForm = {
  border: "8px solid red",
};

const standardForm = {};

const executeActionsStyle = {
  display: "flex",
  justifyContent: "space-between",
  marginTop: 8,
};

const executeButtonStyle = {
  height: 30,
  border: 0,
  padding: "4px 20px",
};

const selectStyle = {
  marginRight: 8,
  height: 30,
};

function TextArea(props: {
  value: string;
  onChange: (nextValue: string) => void;
}) {
  const { value, onChange } = props;
  const editor = useEditor({
    extensions: [
      StarterKit,
      Placeholder.configure({
        placeholder: "Write your query here.",
      }),
      HardBreak.extend({
        addKeyboardShortcuts() {
          return {
            Enter: () => this.editor.commands.setHardBreak(),
          };
        },
      }),
    ],
    content: `${value}`,
    onUpdate({ editor }) {
      onChange(editor.getText());
    },
  });
  return (
    <div>
      <RichTextEditor editor={editor}>
        <RichTextEditor.Content />
      </RichTextEditor>
      <Prism withLineNumbers language="sql">
        {value || ""}
      </Prism>
    </div>
  );
}

const queryDescription = {
  minHeight: 10,
  width: "auto",
  fontSize: 16,
  padding: "10px 5px",
};

export default QueryDisplay;
