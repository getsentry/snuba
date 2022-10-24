import {
  Config,
  ConfigKey,
  ConfigValue,
  ConfigChange,
  ConfigDescription,
  ConfigDescriptions,
} from "./runtime_config/types";

import {
  ClickhouseNodeData,
  QueryRequest,
  QueryResult,
  PredefinedQuery,
} from "./clickhouse_queries/types";
import { TracingRequest, TracingResult } from "./tracing/types";
import { SnQLRequest, SnQLResult, SnubaDatasetName } from "./snql_to_sql/types";

import { KafkaTopicData } from "./kafka/types";

interface Client {
  getConfigs: () => Promise<Config[]>;
  createNewConfig: (
    key: ConfigKey,
    value: ConfigValue,
    description: ConfigDescription
  ) => Promise<Config>;
  deleteConfig: (key: ConfigKey, keepDescription: boolean) => Promise<void>;
  editConfig: (
    key: ConfigKey,
    value: ConfigValue,
    description: ConfigDescription
  ) => Promise<Config>;
  getDescriptions: () => Promise<ConfigDescriptions>;
  getAuditlog: () => Promise<ConfigChange[]>;
  getClickhouseNodes: () => Promise<[ClickhouseNodeData]>;
  getSnubaDatasetNames: () => Promise<SnubaDatasetName[]>;
  convertSnQLQuery: (query: SnQLRequest) => Promise<SnQLResult>;
  getPredefinedQueryOptions: () => Promise<[PredefinedQuery]>;
  executeSystemQuery: (req: QueryRequest) => Promise<QueryResult>;
  executeTracingQuery: (req: TracingRequest) => Promise<TracingResult>;
  getKafkaData: () => Promise<KafkaTopicData[]>;
}

function Client() {
  const baseUrl = "/";

  return {
    getConfigs: () => {
      const url = baseUrl + "configs";
      return fetch(url).then((resp) => resp.json());
    },
    createNewConfig: (
      key: ConfigKey,
      value: ConfigValue,
      description: ConfigDescription
    ) => {
      const url = baseUrl + "configs";
      const params = { key, value, description };

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
    deleteConfig: (key: ConfigKey, keepDescription: boolean) => {
      const url =
        baseUrl +
        "configs/" +
        encodeURIComponent(key) +
        (keepDescription ? "?keepDescription=true" : "");
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
    editConfig: (
      key: ConfigKey,
      value: ConfigValue,
      description: ConfigDescription
    ) => {
      const url = baseUrl + "configs/" + encodeURIComponent(key);
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
        method: "PUT",
        body: JSON.stringify({ value, description }),
      }).then((res) => {
        if (res.ok) {
          return Promise.resolve(res.json());
        } else {
          throw new Error("Could not edit config");
        }
      });
    },
    getDescriptions: () => {
      const url = baseUrl + "all_config_descriptions";
      return fetch(url).then((resp) => resp.json());
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

    getSnubaDatasetNames: () => {
      const url = baseUrl + "snuba_datasets";
      return fetch(url).then((resp) => resp.json());
    },

    convertSnQLQuery: (query: SnQLRequest) => {
      const url = baseUrl + "snql_to_sql";
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
        method: "POST",
        body: JSON.stringify(query),
      }).then((res) => {
        if (res.ok) {
          return Promise.resolve(res.json());
        } else {
          return res.json().then((err) => {
            let errMsg = err?.error.message || "Could not convert SnQL";
            throw new Error(errMsg);
          });
        }
      });
    },

    getPredefinedQueryOptions: () => {
      const url = baseUrl + "clickhouse_queries";
      return fetch(url).then((resp) => resp.json());
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
    getKafkaData: () => {
      const url = baseUrl + "kafka";
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
      }).then((resp) => resp.json());
    },
  };
}

export default Client;
