import { AllowedTools } from "./types";

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
import {
  MigrationGroupResult,
  RunMigrationRequest,
  RunMigrationResult,
} from "./clickhouse_migrations/types";
import { TracingRequest, TracingResult } from "./tracing/types";
import { SnQLRequest, SnQLResult, SnubaDatasetName } from "./snql_to_sql/types";

import { KafkaTopicData } from "./kafka/types";
import { QuerylogRequest, QuerylogResult } from "./querylog/types";

import {
  AllocationPolicy,
  AllocationPolicyConfig,
  AllocationPolicyOptionalConfigDefinition,
} from "./capacity_management/types";

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
  getPredefinedQuerylogOptions: () => Promise<[PredefinedQuery]>;
  getQuerylogSchema: () => Promise<QuerylogResult>;
  executeQuerylogQuery: (req: QuerylogRequest) => Promise<QuerylogResult>;
  getAllMigrationGroups: () => Promise<MigrationGroupResult[]>;
  runMigration: (req: RunMigrationRequest) => Promise<RunMigrationResult>;
  getAllowedTools: () => Promise<AllowedTools>;
  getAllocationPolicies: () => Promise<AllocationPolicy[]>;
  getAllocationPolicyConfigs: (
    storage: string
  ) => Promise<AllocationPolicyConfig[]>;
  getAllocationPolicyOptionalConfigDefinitions: (
    storage: string
  ) => Promise<AllocationPolicyOptionalConfigDefinition[]>;
  setAllocationPolicyConfig: (
    storage: string,
    key: string,
    value: string,
    params: object
  ) => Promise<AllocationPolicyConfig>;
  deleteAllocationPolicyConfig: (
    storage: string,
    key: string,
    params: object
  ) => Promise<void>;
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
      return fetch(url)
        .then((resp) => resp.json())
        .then((res) => {
          return res.filter(
            (storage: any) =>
              storage.local_nodes.length > 0 ||
              storage.dist_nodes.length > 0 ||
              storage.query_node
          );
        });
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

    getPredefinedQuerylogOptions: () => {
      const url = baseUrl + "querylog_queries";
      return fetch(url).then((resp) => resp.json());
    },
    getQuerylogSchema: () => {
      const url = baseUrl + "clickhouse_querylog_schema";
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
      }).then((resp) => {
        if (resp.ok) {
          return resp.json();
        } else {
          return resp.json().then(Promise.reject.bind(Promise));
        }
      });
    },
    executeQuerylogQuery: (query: QuerylogRequest) => {
      const url = baseUrl + "clickhouse_querylog_query";
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
    getAllMigrationGroups: () => {
      const url = baseUrl + "migrations/groups";
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
      }).then((resp) => resp.json());
    },

    runMigration: (req: RunMigrationRequest) => {
      const params = new URLSearchParams({
        force: (req.force || false).toString(),
        fake: (req.fake || false).toString(),
        dry_run: (req.dry_run || false).toString(),
      });
      const url: string = `/migrations/${req.group}/${req.action}/${req.migration_id}?`;
      return fetch(url + params, {
        headers: { "Content-Type": "application/json" },
        method: "POST",
        body: JSON.stringify(params),
      }).then((resp) => {
        if (resp.ok) {
          return resp.json();
        } else {
          return resp.json().then(Promise.reject.bind(Promise));
        }
      });
    },

    getAllowedTools: () => {
      const url = baseUrl + "tools";
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
      }).then((resp) => resp.json());
    },

    getAllocationPolicies: () => {
      const url = baseUrl + "allocation_policies";
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
      }).then((resp) => resp.json());
    },
    getAllocationPolicyConfigs: (storage: string) => {
      const url =
        baseUrl + "allocation_policy_configs/" + encodeURIComponent(storage);
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
      }).then((resp) => resp.json());
    },
    getAllocationPolicyOptionalConfigDefinitions: (storage: string) => {
      const url =
        baseUrl +
        "allocation_policy_optional_config_definitions/" +
        encodeURIComponent(storage);
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
      }).then((resp) => resp.json());
    },
    setAllocationPolicyConfig: (
      storage: string,
      key: string,
      value: string,
      params: object
    ) => {
      const url = baseUrl + "allocation_policy_config";
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
        method: "POST",
        body: JSON.stringify({ storage, key, value, params }),
      }).then((res) => {
        if (res.ok) {
          return Promise.resolve(res.json());
        } else {
          return res.json().then((err) => {
            let errMsg = err?.error || "Could not set config";
            throw new Error(errMsg);
          });
        }
      });
    },
    deleteAllocationPolicyConfig: (
      storage: string,
      key: string,
      params: object
    ) => {
      const url = baseUrl + "allocation_policy_config";
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
        method: "DELETE",
        body: JSON.stringify({ storage, key, params }),
      }).then((res) => {
        if (res.ok) {
          return;
        } else {
          throw new Error("Could not delete config");
        }
      });
    },
  };
}

export default Client;
