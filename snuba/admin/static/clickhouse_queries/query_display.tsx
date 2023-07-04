import React, { useEffect, useState } from "react";
import Client from "../api_client";
import { Collapse } from "../collapse";

import { Prism } from "@mantine/prism";
import { RichTextEditor } from "@mantine/tiptap";
import { useEditor } from "@tiptap/react";
import HardBreak from "@tiptap/extension-hard-break";
import Placeholder from "@tiptap/extension-placeholder";
import StarterKit from "@tiptap/starter-kit";

import {
  ClickhouseNodeData,
  QueryRequest,
  QueryResult,
  PredefinedQuery,
} from "./types";

type QueryState = Partial<QueryRequest>;

function QueryDisplay(props: {
  api: Client;
  resultDataPopulator: (queryResult: QueryResult) => JSX.Element;
  predefinedQuery: PredefinedQuery | null;
}) {
  const [nodeData, setNodeData] = useState<ClickhouseNodeData[]>([]);
  const [query, setQuery] = useState<QueryState>({});
  const [queryResultHistory, setQueryResultHistory] = useState<QueryResult[]>(
    []
  );

  useEffect(() => {
    props.api.getClickhouseNodes().then((res) => {
      setNodeData(res);
    });
  }, []);

  useEffect(() => {
    if (props.predefinedQuery) {
      setQuery({ sql: props.predefinedQuery.sql });
    }
  }, [props.predefinedQuery]);

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
    props.api
      .executeSystemQuery(query as QueryRequest)
      .then((result) => {
        result.input_query = `${query.sql} (${query.storage},${query.host}:${query.port})`;
        setQueryResultHistory((prevHistory) => [result, ...prevHistory]);
      })
      .catch((err) => {
        console.log("ERROR", err);
        window.alert("An error occurred: " + err.error);
      });
  }

  function copyText(text: string) {
    window.navigator.clipboard.writeText(text);
  }

  function getHosts(nodeData: ClickhouseNodeData[]): JSX.Element[] {
    let node_info = nodeData.find((el) => el.storage_name === query.storage)!;
    // populate the hosts entries marking distributed hosts that are not also local
    if (node_info) {
      let local_hosts = node_info.local_nodes.map((node) => (
        <option
          key={`${node.host}:${node.port}`}
          value={`${node.host}:${node.port}`}
        >
          {node.host}:{node.port}
        </option>
      ));
      let dist_hosts = node_info.dist_nodes
        .filter((node) => !node_info.local_nodes.includes(node))
        .map((node) => (
          <option
            key={`${node.host}:${node.port} dist`}
            value={`${node.host}:${node.port}`}
          >
            {node.host}:{node.port} (distributed)
          </option>
        ));
      let hosts = local_hosts.concat(dist_hosts);
      let query_node = node_info.query_node;
      if (query_node) {
        hosts.push(
          <option
            key={`${query_node.host}:${query_node.port} query`}
            value={`${query_node.host}:${query_node.port}`}
          >
            {query_node.host}:{query_node.port} (query node)
          </option>
        );
      }
      return hosts;
    }
    return [];
  }

  return (
    <div>
      <form>
        <h2>Construct a ClickHouse System Query</h2>
        <div style={queryDescription}>{props.predefinedQuery?.description}</div>
        <div>
          <TextArea value={query.sql || ""} onChange={updateQuerySql} />
        </div>
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
              {nodeData.map((storage) => (
                <option key={storage.storage_name} value={storage.storage_name}>
                  {storage.storage_name}
                </option>
              ))}
            </select>
            <select
              disabled={!query.storage}
              value={
                query.host && query.port ? `${query.host}:${query.port}` : ""
              }
              onChange={(evt) => selectHost(evt.target.value)}
              style={selectStyle}
            >
              <option disabled value="">
                Select a host
              </option>
              {getHosts(nodeData)}
            </select>
          </div>
          <div>
            <button
              onClick={(evt) => {
                evt.preventDefault();
                executeQuery();
              }}
              style={executeButtonStyle}
              disabled={
                !query.storage || !query.host || !query.port || !query.sql
              }
            >
              Execute query
            </button>
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
