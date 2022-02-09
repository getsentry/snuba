import {
  Config,
  ConfigKey,
  ConfigValue,
  ConfigChange,
} from "./runtime_config/types";

import {
  ClickhouseNodeData,
  QueryRequest,
  QueryResult,
} from "./clickhouse_queries/types";
import { TracingRequest, TracingResult } from "./tracing/types";

interface Client {
  getConfigs: () => Promise<Config[]>;
  createNewConfig: (key: ConfigKey, value: ConfigValue) => Promise<Config>;
  deleteConfig: (key: ConfigKey) => Promise<void>;
  editConfig: (key: ConfigKey, value: ConfigValue) => Promise<Config>;
  getAuditlog: () => Promise<ConfigChange[]>;
  getClickhouseNodes: () => Promise<[ClickhouseNodeData]>;
  executeSystemQuery: (req: QueryRequest) => Promise<QueryResult>;
  executeTracingQuery: (req: TracingRequest) => Promise<TracingResult>;
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
    deleteConfig: (key: ConfigKey) => {
      const url = baseUrl + "configs/" + encodeURIComponent(key);
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
        method: "DELETE",
      }).then((res) => {
        if (res.ok) {
          return;
        } else {
          throw new Error("Could not delete config");
        }
      });
    },
    editConfig: (key: ConfigKey, value: ConfigValue) => {
      const url = baseUrl + "configs/" + encodeURIComponent(key);
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
        method: "PUT",
        body: JSON.stringify({ value }),
      }).then((res) => {
        if (res.ok) {
          return Promise.resolve(res.json());
        } else {
          throw new Error("Could not edit config");
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

    executeSystemQuery: (query: QueryRequest) => {
      const url = baseUrl + "run_clickhouse_system_query";
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
        method: "POST",
        body: JSON.stringify(query),
      }).then((resp) => {
        if (resp.ok) {
          return resp.json();
        } else {
          return resp.json().then(Promise.reject.bind(Promise));
        }
      });
    },
    executeTracingQuery: (query: TracingRequest) => {
      const url = baseUrl + "clickhouse_trace_query";
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
        method: "POST",
        body: JSON.stringify(query),
      }).then((resp) => {
        if (resp.ok) {
          return resp.json();
        } else {
          return resp.json().then(Promise.reject.bind(Promise));
        }
      });
    },
  };
}

export default Client;
