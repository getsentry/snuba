import React, { useEffect, useState } from "react";
import Client from "../api_client";

import { ClickhouseNodeData } from "./types";

type QueryState = {
  storage: string | null;
  host: string | null;
  port: number | null;
};

function ClickhouseQueries(props: { api: Client }) {
  const [nodeData, setNodeData] = useState<ClickhouseNodeData[]>([]);

  const [query, setQuery] = useState<QueryState>({
    storage: null,
    host: null,
    port: null,
  });

  useEffect(() => {
    props.api.getClickhouseNodes().then((res) => {
      setNodeData(res);
    });
  }, []);

  function selectStorage(storage: string) {
    setQuery({
      storage,
      host: null,
      port: null,
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

  return (
    <div>
      <form>
        <select
          value={query.storage || ""}
          onChange={(evt) => selectStorage(evt.target.value)}
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
        {query.storage && (
          <select
            value={
              query.host && query.port ? `${query.host}:${query.port}` : ""
            }
            onChange={(evt) => selectHost(evt.target.value)}
          >
            <option disabled value="">
              Select a host
            </option>
            {nodeData
              .find((el) => el.storage_name === query.storage)
              ?.local_nodes.map((node, nodeIndex) => (
                <option
                  key={`${node.host}:${node.port}`}
                  value={`${node.host}:${node.port}`}
                >
                  {node.host}:{node.port}
                </option>
              ))}
          </select>
        )}
      </form>
    </div>
  );
}

export default ClickhouseQueries;
