import React, { useEffect, useState } from "react";
import Client from "SnubaAdmin/api_client";
import ExecuteButton from "SnubaAdmin/utils/execute_button";
import { CustomSelect } from "SnubaAdmin/select";
import { getHostsForStorage, getErrorDomElement } from "SnubaAdmin/utils/clickhouse_node_utils";
import { COLORS } from "SnubaAdmin/theme";

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

      copyTableHosts.sourceHost.storage = storage

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

  const [targetHostInput, setTargetHostInput] = useState("");

  function executeCopyTableQuery(shouldExecute: boolean) {
    const query: CopyTableRequest = {
      storage: copyTableHosts.sourceHost.storage!,
      source_host: copyTableHosts.sourceHost.host!,
      source_port: copyTableHosts.sourceHost.port!,
      dry_run: !shouldExecute,
    }
    const trimmed = targetHostInput.trim();
    if (trimmed) {
      query.target_host = trimmed;
    }
    return props.api
      .executeCopyTable(query)
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
        <div style={formRowStyle}>
          <div style={sectionContainerStyle}>
            <h3>Select source host:</h3>
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
          <div style={sectionContainerStyle}>
            <h3>Target host (optional):</h3>
            <p style={targetHelpTextStyle}>
              If specified, CREATE statements will be run on this host instead of the source host.
            </p>
            <div style={{ marginTop: 10 }}>
              <input
                type="text"
                style={textInputStyle}
                placeholder="e.g. my-clickhouse-node.example.com"
                value={targetHostInput}
                onChange={(e) => setTargetHostInput(e.target.value)}
              />
            </div>
          </div>
        </div>
        <div style={buttonRowStyle}>
          <ExecuteButton
            label=" DRY RUN"
            onError={handleQueryError}
            onClick={() => executeCopyTableQuery(false)}
            disabled={copyTableHosts.sourceHost.host ? false : true}
          />
          <ExecuteButton
            label=" ‼️ COPY TABLES FOR CLUSTER ‼️ "
            onError={handleQueryError}
            onClick={() => executeCopyTableQuery(true)}
            disabled={copyTableHosts.sourceHost.host ? false : true}
          />
        </div>
      </form >

      <div>
        {getErrorDomElement(queryError, collapseOpened, setCollapseOpened)}
      </div>

      {copyTableResult && (
        <div style={copyTableResultStyle}>
          <h4>
            {copyTableResult.dry_run ? "DRY RUN" : <>Executed <code>CREATE TABLE</code></>}
            {" "}with <strong>Source Host:</strong> <code>{copyTableResult.source_host}</code>
            {copyTableResult.cluster_name && (
              <> and <strong>Cluster Name:</strong> <code>{copyTableResult.cluster_name}</code></>
            )}
          </h4>

          {(copyTableResult.incomplete_hosts || copyTableResult.verified) && (
            <div style={resultSectionStyle}>
              <h3>Table Verification</h3>
              {(() => {
                const verifiedHosts = copyTableResult.verified ?? 0;
                const incompleteHosts = Object.keys(copyTableResult.incomplete_hosts ?? {}).length;
                const totalHosts = verifiedHosts + incompleteHosts;
                const allVerified = incompleteHosts === 0;

                return (
                  <>
                    <p style={allVerified ? successStyle : missingTablesStyle}>
                      <strong>{verifiedHosts} of {totalHosts} hosts verified</strong>
                      {allVerified ? " ✓" : ""}
                    </p>
                    {incompleteHosts > 0 && (
                      <div style={unverifiedHostsContainerStyle}>
                        {Object.entries(copyTableResult.incomplete_hosts ?? {}).map(([host, missingTables]) => (
                          <div key={host} style={hostItemStyle}>
                            <p><strong>Host:</strong> <code>{host}</code></p>
                            <p style={missingTablesStyle}>
                              <strong>Missing Tables:</strong> <code>{missingTables}</code>
                            </p>
                          </div>
                        ))}
                      </div>
                    )}
                  </>
                );
              })()}
            </div>
          )}

          <div style={resultSectionStyle}>
            <h3>Tables Created</h3>
            <div style={tableListStyle}>
              {copyTableResult.tables.split(',')?.map((val, inx) => (
                <div key={inx}><code>{val}</code></div>
              ))}
            </div>
          </div>


        </div>
      )}
    </div>
  );
}


const formRowStyle = {
  display: "flex",
  alignItems: 'flex-start',
  gap: 20,
  marginBottom: 20,
};

const hostSelectStyle = {
  width: '100%',
  minWidth: '25em',
  display: 'flex',
  flexDirection: 'column' as const,
  gap: 10,
  marginTop: 10,
}

const buttonRowStyle = {
  display: 'flex',
  gap: 10,
  marginTop: 10,
}

const targetHelpTextStyle = {
  fontSize: '0.85em',
  color: COLORS.TEXT_DEFAULT,
  marginTop: 4,
  marginBottom: 0,
}

const textInputStyle = {
  width: '100%',
  padding: '6px 10px',
  border: `1px solid ${COLORS.BORDER_GRAY}`,
  borderRadius: 4,
  fontSize: '14px',
  boxSizing: 'border-box' as const,
}

const tableListStyle = {
  marginTop: 10,
  marginLeft: 5,
  maxHeight: '200px',
  overflowY: "auto" as const,
}

const copyTableResultStyle = {
  display: 'flex',
  flexDirection: 'column' as const,
  width: '60em',
  maxHeight: '60em',
  marginRight: 40,
  marginTop: 20,
  overflowY: "scroll" as const,
  padding: 15,
  border: `1px solid ${COLORS.BORDER_GRAY_LIGHT}`,
  borderRadius: 8,
  backgroundColor: COLORS.BG_GRAY_LIGHTER,
}

const sectionContainerStyle = {
  padding: 15,
  backgroundColor: COLORS.WHITE,
  border: `1px solid ${COLORS.BORDER_GRAY}`,
  borderRadius: 5,
  marginTop: 0,
}

const resultSectionStyle = {
  ...sectionContainerStyle,
  marginTop: 20,
}

const hostItemStyle = {
  marginBottom: 15,
  padding: 10,
  backgroundColor: COLORS.BG_GRAY_LIGHT,
  borderRadius: 4,
}

const missingTablesStyle = {
  color: COLORS.ERROR,
  marginTop: 5,
}

const successStyle = {
  color: COLORS.SUCCESS,
  marginTop: 5,
}

const unverifiedHostsContainerStyle = {
  marginTop: 15,
}

export default CopyTables;
