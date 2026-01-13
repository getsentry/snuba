import React, { useEffect, useState } from "react";
import Client from "SnubaAdmin/api_client";
import ExecuteButton from "SnubaAdmin/utils/execute_button";
import { CustomSelect } from "SnubaAdmin/select";
import { getHostsForStorage, getErrorDomElement } from "SnubaAdmin/utils/clickhouse_node_utils";

import {
  ClickhouseNodeData,
  CopyTableRequest,
  CopyTableResult,
  CopyTableHostsState,
} from "SnubaAdmin/copy_tables/types";


function CopyTables(props: {
  api: Client;
}) {
  const [nodeData, setNodeData] = useState<ClickhouseNodeData[]>([]);
  const [copyTableHosts, setCopyTableHosts] = useState<CopyTableHostsState>({
    sourceHost: {},
    targetHost: {}
  });
  const [copyTableResult, setCopyTableResult] = useState<CopyTableResult | null>(null);
  const [queryError, setQueryError] = useState<Error | null>(null);
  const [collapseOpened, setCollapseOpened] = useState(false);

  useEffect(() => {
    props.api.getClickhouseNodes().then((res) => {
      setNodeData(res);
    });
  }, []);

  function selectStorage(storage: string) {
    setCopyTableHosts((copyTableHosts) => {
      // clear old host port
      delete copyTableHosts.sourceHost.host
      delete copyTableHosts.sourceHost.port
      delete copyTableHosts.targetHost.host
      delete copyTableHosts.targetHost.port

      copyTableHosts.sourceHost.storage = storage
      copyTableHosts.targetHost.storage = storage

      return {
        ...copyTableHosts,
      };
    });
  }

  function selectTargetHost(value: string) {
    setCopyTableHosts((copyTableHosts) => {

      copyTableHosts.targetHost.host = value
      copyTableHosts.targetHost.port = 9000


      return {
        ...copyTableHosts,
      };
    });
  }

  function selectSourceHost(hostString: string) {
    const [host, portAsString] = hostString.split(":");

    setCopyTableHosts((copyTableHosts) => {
      copyTableHosts.sourceHost.host = host
      copyTableHosts.sourceHost.port = parseInt(portAsString, 10)


      return {
        ...copyTableHosts,

      };
    });
  }

  function executeCopyTableQuery(shouldExecute: boolean) {
    const query = {
      storage: copyTableHosts.sourceHost.storage,
      source_host: copyTableHosts.sourceHost.host,
      source_port: copyTableHosts.sourceHost.port,
      target_host: copyTableHosts.targetHost.host,
      target_port: copyTableHosts.targetHost.port,
      dry_run: !shouldExecute,
    }
    return props.api
      .executeCopyTable(query as CopyTableRequest)
      .then((result) => {
        setQueryError(null);
        setCopyTableResult(result);
      });
  }

  function handleQueryError(error: Error) {
    setQueryError(error);
  }

  return (
    <div>
      <form>
        <div style={sectionStyle}>
          <div>
            <h3>Source Host</h3>
            <div style={hostSelectStyle}>
              <CustomSelect
                value={copyTableHosts.sourceHost.storage || ""}
                onChange={selectStorage}
                name="storage"
                options={nodeData.map((storage) => storage.storage_name)}
              />
              <CustomSelect
                disabled={!copyTableHosts.sourceHost.storage}
                value={
                  copyTableHosts.sourceHost.host && copyTableHosts.sourceHost.port ? `${copyTableHosts.sourceHost.host}:${copyTableHosts.sourceHost.port}` : ""
                }
                onChange={selectSourceHost}
                name="host"
                options={getHostsForStorage(nodeData, copyTableHosts.sourceHost.storage)}
              />
            </div>
          </div>
          <div>
            <h3>Target Host</h3>
            <div style={hostSelectStyle}>
              <input
                style={inputStyle}
                id="clusterless"
                value={copyTableHosts.targetHost.host || ""}
                onChange={(evt) => selectTargetHost(evt.target.value)}
                name="target-host"
                placeholder="target host"
                type="text"
              />
            </div>
          </div>
        </div>
      </form >

      <div style={sectionStyle}>
        <div>
          <div style={hostSelectStyle}>
            <ExecuteButton
              label=" DRY RUN"
              onError={handleQueryError}
              onClick={() => executeCopyTableQuery(false)}
              disabled={copyTableHosts.targetHost.host ? false : true}
            />
            <ExecuteButton
              label=" ‼️ COPY TABLE ‼️ "
              onError={handleQueryError}
              onClick={() => executeCopyTableQuery(true)}
              disabled={copyTableHosts.targetHost.host ? false : true}
            />
          </div>
          <div>
            {getErrorDomElement(queryError, collapseOpened, setCollapseOpened)}
          </div>
          {copyTableResult && (
            <div style={copyTableResultStyle}>
              <h2>{copyTableResult.dry_run ? "Dry Ran with" : "Executed with"}</h2>
              <div>
                <p>Source Host: <code>{copyTableResult.source_host}</code></p>
                <p>Target Host: <code>{copyTableResult.target_host}</code></p>
                <p>Cluster Name: <code>{copyTableResult.cluster_name}</code></p>
              </div>
              <p>Tables:</p>
              <div style={tableListStyle}>
                {copyTableResult.tables.split(',')?.map((val, inx) => (
                  <div key={inx}><code>{val}</code></div>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
    </div >
  );
}


const sectionStyle = {
  display: "flex",
  marginTop: 8,
};

const hostSelectStyle = {
  width: '30em',
  height: '4em',
  margin: '8px 0px',
  display: 'flex',
  flexDirection: 'column' as const,
  justifyContent: 'space-between',
  marginRight: 40,
}

const inputStyle = {
  fontSize: 16,
  minHeight: '2.25rem',
}

const tableListStyle = {
  marginLeft: 5,
  overflowY: "scroll" as const,
}

const copyTableResultStyle = {
  display: 'flex',
  justifyContent: 'space-between',
  width: '60em',
  height: '40em',
  marginRight: 40,
  marginTop: 20,
  overflowY: "scroll" as const,
}

export default CopyTables;
