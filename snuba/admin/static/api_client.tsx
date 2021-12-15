import {
  Config,
  ConfigKey,
  ConfigValue,
  ConfigChange,
} from "./runtime_config/types";

import {
  ClickhouseCannedQuery,
  ClickhouseNodeData,
  QueryRequest,
  QueryResult,
} from "./clickhouse_queries/types";

interface Client {
  getConfigs: () => Promise<Config[]>;
  createNewConfig: (key: ConfigKey, value: ConfigValue) => Promise<Config>;
  getAuditlog: () => Promise<ConfigChange[]>;
  getClickhouseNodes: () => Promise<[ClickhouseNodeData]>;
  getClickhouseCannedQueries: () => Promise<[ClickhouseCannedQuery]>;
  executeQuery: (req: QueryRequest) => Promise<QueryResult>;
}

function Client() {
  const baseUrl = "/";

  return {
    getConfigs: () => {
      const url = baseUrl + "configs";
      return fetch(url).then((resp) => resp.json());
    },
    createNewConfig: (key: ConfigKey, value: ConfigValue) => {
      const url = baseUrl + "configs";
      const params = { key, value };

      return fetch(url, {
        headers: { "Content-Type": "application/json" },
        method: "POST",
        body: JSON.stringify(params),
      }).then((res) => {
        if (res.ok) {
          return Promise.resolve(res.json());
        } else {
          return res.json().then((err) => {
            let errMsg = err?.error || "Could not create config";
            throw new Error(errMsg);
          });
        }
      });
    },
    getAuditlog: () => {
      const url = baseUrl + "config_auditlog";
      return fetch(url).then((resp) => resp.json());
    },
    getClickhouseNodes: () => {
      const url = baseUrl + "clickhouse_nodes";
      return (
        fetch(url)
          .then((resp) => resp.json())
          // If the cluster_name was not defined in the Snuba installation,
          // no local nodes can be found so let's filter these out
          .then((res) => {
            return res.filter((storage: any) => storage.local_nodes.length > 0);
          })
      );
    },
    getClickhouseCannedQueries: () => {
      const url = baseUrl + "clickhouse_queries";
      return fetch(url).then((resp) => resp.json());
    },
    executeQuery: (query: QueryRequest) => {
      const url = baseUrl + "run_clickhouse_system_query";
      return fetch(
        new Request(url, {
          method: "POST",
          body: new Blob([JSON.stringify(query)], { type: "application/json" }),
        })
      )
        .then((resp) => resp.json())
        .then((resp: QueryResult) => {
          // used for keying in history
          resp.timestamp = Date.now();
          return resp;
        });
    },
  };
}

export default Client;
