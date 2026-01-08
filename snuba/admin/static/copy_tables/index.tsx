import React, { useEffect, useState } from "react";
import Client from "SnubaAdmin/api_client";
import ExecuteButton from "SnubaAdmin/utils/execute_button";
import { Box, Switch } from "@mantine/core";
import { SQLEditor } from "SnubaAdmin/common/components/sql_editor";
import { CustomSelect, getParamFromStorage } from "SnubaAdmin/select";
import { getHostsForStorage, getErrorDomElement } from "SnubaAdmin/utils/clickhouse_node_utils";

import {
  ClickhouseNodeData,
  QueryRequest,
  CopyTableRequest,
  CopyTable,
  CopyTableResult,
  ShowTablesQueryState,
  ShowTablesQueryResult,
} from "SnubaAdmin/copy_tables/types";


enum HostType {
  source = "sourceHost",
  target = "targetHost"
}

const COPY_TABLE_DEFAULT = {
  table: "",
  onCluster: false,
  previewStatement: ""
}

function CopyTables(props: {
  api: Client;
}) {
  const [nodeData, setNodeData] = useState<ClickhouseNodeData[]>([]);
  const [tableQueries, setShowTablesQueries] = useState<ShowTablesQueryState>({
    sourceHost: {},
    targetHost: {}
  });
  const [tableResult, setTableResults] = useState<ShowTablesQueryResult>({
    sourceHost: {},
    targetHost: {}
  });
  const [copyTable, setCopyTable] = useState<CopyTable>(COPY_TABLE_DEFAULT);
  const [copyTableResult, setCopyTableResult] = useState<CopyTableResult | null>(null);

  const [queryError, setQueryError] = useState<Error | null>(null);

  const [collapseOpened, setCollapseOpened] = useState(false);

  useEffect(() => {
    props.api.getClickhouseNodes().then((res) => {
      setNodeData(res);
    });
  }, []);

  function selectStorage(storage: string) {
    setShowTablesQueries((showTablesQueries) => {
      // clear old host port
      delete showTablesQueries.sourceHost.host
      delete showTablesQueries.sourceHost.port
      delete showTablesQueries.targetHost.host
      delete showTablesQueries.targetHost.port

      showTablesQueries.sourceHost.storage = storage
      showTablesQueries.targetHost.storage = storage

      return {
        ...showTablesQueries,
      };
    });
  }

  function selectTargetHost(value: string) {
    setShowTablesQueries((showTablesQueries) => {

      showTablesQueries.targetHost.sql = "SHOW tables"
      showTablesQueries.targetHost.host = value
      showTablesQueries.targetHost.port = 9000


      return {
        ...showTablesQueries,
      };
    });
  }

  function selectSourceHost(hostString: string) {
    const [host, portAsString] = hostString.split(":");

    setShowTablesQueries((showTablesQueries) => {
      showTablesQueries.sourceHost.sql = "SHOW tables"
      showTablesQueries.sourceHost.host = host
      showTablesQueries.sourceHost.port = parseInt(portAsString, 10)


      return {
        ...showTablesQueries,

      };
    });
  }

  function selectTable(table: string) {
    setCopyTable((prevCopyTable) => {
      return {
        ...prevCopyTable,
        table: table
      }
    })
  }

  function setOnCluster(onCluster: boolean) {
    setCopyTable((prevCopyTable) => {
      return {
        ...prevCopyTable,
        onCluster: onCluster
      }
    })
  }

  function executeShowTable(hostType: HostType) {
    const query = tableQueries[hostType]
    return props.api
      .executeSystemQuery(query as QueryRequest)
      .then((result) => {
        setQueryError(null);
        setTableResults((previousResult) => {
          previousResult[hostType] = result
          return {
            ...previousResult,
          };
        })
      });
  }

  function executeCopyTableQuery(dryRun: boolean) {
    const query = {
      storage: tableQueries.sourceHost.storage,
      source_host: tableQueries.sourceHost.host,
      source_port: tableQueries.sourceHost.port,
      target_host: tableQueries.targetHost.host,
      target_port: tableQueries.targetHost.port,
      table_name: copyTable.table || "",
      dry_run: dryRun,
      on_cluster: copyTable.onCluster,
    }
    return props.api
      .executeCopyTable(query as CopyTableRequest)
      .then((result) => {
        setQueryError(null);
        if (dryRun === true) {
          setCopyTable({ ...copyTable, previewStatement: result.create_statement });
          setCopyTableResult(null);
        } else {
          setCopyTable(COPY_TABLE_DEFAULT);
          setCopyTableResult(result);

        }
      });
  }

  function handleQueryError(error: Error) {
    setQueryError(error);
  }


  function showTableButton(hostType: HostType) {
    return (
      <div style={showTablesButton}>
        <ExecuteButton
          label="Show Tables"
          onError={handleQueryError}
          onClick={() => executeShowTable(hostType)}
          disabled={
            !tableQueries[hostType].host ||
            !tableQueries[hostType].port
          }
        />
      </div>)
  }

  return (
    <div>
      <form style={standardForm}>
        <div style={executeActionsStyle}>
          <div>
            <h3>Source Host</h3>
            <div style={hostSelectStyle}>
              <CustomSelect
                value={tableQueries.sourceHost.storage || ""}
                onChange={selectStorage}
                name="storage"
                options={nodeData.map((storage) => storage.storage_name)}
              />
              <CustomSelect
                disabled={!tableQueries.sourceHost.storage}
                value={
                  tableQueries.sourceHost.host && tableQueries.sourceHost.port ? `${tableQueries.sourceHost.host}:${tableQueries.sourceHost.port}` : ""
                }
                onChange={selectSourceHost}
                name="host"
                options={getHostsForStorage(nodeData, tableQueries.sourceHost.storage)}
              />
              {showTableButton(HostType.source)}
            </div>

            {tableResult.sourceHost && (
              <div style={tableResultStyle}>
                <Box my="md" id="source-host">
                  <SQLEditor
                    value={tableResult.sourceHost.rows?.join("\n") || ""}
                    onChange={() => { }}
                  />
                </Box>
              </div>
            )}
          </div>

          <div>
            <h3>Target Host</h3>
            <div style={hostSelectStyle}>
              <input
                style={inputStyle}
                id="clusterless"
                value={tableQueries.targetHost.host || ""}
                onChange={(evt) => selectTargetHost(evt.target.value)}
                name="host"
                placeholder="target host"
                type="text"
              />
              {showTableButton(HostType.target)}
            </div>
            {tableResult.targetHost && (
              <div style={tableResultStyle}>
                <Box my="md" id="target-host">
                  <SQLEditor
                    value={tableResult.targetHost.rows?.join("\n") || ""}
                    onChange={() => { }}
                  />
                </Box>
              </div>
            )}
          </div>
        </div>
      </form >
      <div>
        {getErrorDomElement(queryError, collapseOpened, setCollapseOpened)}
      </div>
      <div style={executeActionsStyle}>
        <div>
          <h3>Enter Table</h3>
          <div style={hostSelectStyle}>
            <input
              style={inputStyle}
              id="tableinput"
              value={copyTable.table || ""}
              onChange={(evt) => selectTable(evt.target.value)}
              name="host"
              placeholder="table name"
              type="text"
            />
            <Switch
              checked={copyTable.onCluster}
              onChange={(evt: React.ChangeEvent<HTMLInputElement>) =>
                setOnCluster(evt.currentTarget.checked)
              }
              onLabel="ON CLUSTER"
              offLabel="SINGLE HOST"
              size="xl"
            />
            <ExecuteButton
              label="Preview Create Statement"
              onError={handleQueryError}
              onClick={() => executeCopyTableQuery(true)}
              disabled={copyTable.table ? false : true}
            />
          </div>
          <div style={hostSelectStyle}>
            <ExecuteButton
              label=" ‼️ COPY TABLE ‼️ "
              onError={handleQueryError}
              onClick={() => executeCopyTableQuery(false)}
              disabled={copyTable.table ? false : true}
            />
          </div>
        </div>
        <div>
          <div style={copyTableResultStyle}>
            {copyTableResult && (
              <div>
                <h2>Ran with</h2>
                <p>Source Host: <code>{copyTableResult.source_host}</code></p>
                <p>Target Host: <code>{copyTableResult.target_host}</code></p>
                <p>Table: <code>{copyTableResult.table}</code></p>
                <p>On Cluster: <code>{copyTableResult.on_cluster ? "True" : "False"}</code></p>
                <p>Create Statement: <code>{copyTableResult.create_statement}</code></p>
                <code>{copyTableResult.create_statement}</code>
              </div>
            )}
            {copyTable?.previewStatement && (
              <div>
                <h2>Preview Create Statement:</h2>
                <Box my="md">
                  <SQLEditor
                    value={copyTable?.previewStatement || ""}
                    onChange={() => { }}
                  />
                </Box>
              </div>
            )}
          </div>
        </div>
      </div>
    </div >
  );
}


const standardForm = {};

const hostSelectStyle = {
  width: '30em',
  height: '8em',
  margin: '8px 0px',
  display: 'flex',
  flexDirection: 'column' as const,
  justifyContent: 'space-between',
  marginRight: 40,
}

const showTablesButton = {
  margin: '8px 0px',
}

const tableResultStyle = {
  display: 'flex',
  justifyContent: 'justify-start',
  width: '30em',
  height: '10em',
  marginRight: 40,
  overflowY: "scroll" as const,

}

const copyTableResultStyle = {
  display: 'flex',
  justifyContent: 'justify-start',
  width: '50em',
  height: '40em',
  marginRight: 40,
  marginTop: 20,
  overflowY: "scroll" as const,

}

const inputStyle = {
  fontSize: 16,
  minHeight: '2.25rem',
}

const executeActionsStyle = {
  display: "flex",
  // justifyContent: "space-around",
  marginTop: 8,
};

export default CopyTables;
