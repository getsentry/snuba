import { AllowedTools, Settings } from "SnubaAdmin/types";

import {
  Config,
  ConfigKey,
  ConfigValue,
  ConfigChange,
  ConfigDescription,
  ConfigDescriptions,
} from "SnubaAdmin/runtime_config/types";

import {
  ClickhouseNodeData,
  QueryRequest,
  QueryResult,
  PredefinedQuery,
} from "SnubaAdmin/clickhouse_queries/types";
import {
  MigrationGroupResult,
  RunMigrationRequest,
  RunMigrationResult,
} from "SnubaAdmin/clickhouse_migrations/types";
import { TracingRequest, TracingResult } from "SnubaAdmin/tracing/types";
import { MQLRequest } from "SnubaAdmin/mql_queries/types";
import {
  SnQLRequest,
  SnQLResult,
  SnubaDatasetName,
} from "SnubaAdmin/snql_to_sql/types";

import { KafkaTopicData } from "SnubaAdmin/kafka/types";
import { QuerylogRequest, QuerylogResult } from "SnubaAdmin/querylog/types";
import {
  CardinalityQueryRequest,
  CardinalityQueryResult,
} from "SnubaAdmin/cardinality_analyzer/types";

import { AllocationPolicy } from "SnubaAdmin/capacity_management/types";

import { ReplayInstruction, Topic } from "SnubaAdmin/dead_letter_queue/types";
import { AutoReplacementsBypassProjectsData } from "SnubaAdmin/auto_replacements_bypass_projects/types";
import { ClickhouseNodeInfo, ClickhouseSystemSetting } from "SnubaAdmin/database_clusters/types";

interface Client {
  getSettings: () => Promise<Settings>;
  getConfigs: () => Promise<Config[]>;
  getAutoReplacementsBypassProjects: () => Promise<
    AutoReplacementsBypassProjectsData[]
  >;
  createNewConfig: (
    key: ConfigKey,
    value: ConfigValue,
    description: ConfigDescription,
  ) => Promise<Config>;
  deleteConfig: (key: ConfigKey, keepDescription: boolean) => Promise<void>;
  editConfig: (
    key: ConfigKey,
    value: ConfigValue,
    description: ConfigDescription,
  ) => Promise<Config>;
  getDescriptions: () => Promise<ConfigDescriptions>;
  getAuditlog: () => Promise<ConfigChange[]>;
  getClickhouseNodes: () => Promise<[ClickhouseNodeData]>;
  getClickhouseNodeInfo: () => Promise<[ClickhouseNodeInfo]>;
  getSnubaDatasetNames: () => Promise<SnubaDatasetName[]>;
  getAllowedProjects: () => Promise<string[]>;
  executeSnQLQuery: (query: SnQLRequest) => Promise<any>;
  executeMQLQuery: (query: MQLRequest) => Promise<any>;
  debugSnQLQuery: (query: SnQLRequest) => Promise<SnQLResult>;
  getPredefinedQueryOptions: () => Promise<[PredefinedQuery]>;
  executeSystemQuery: (req: QueryRequest) => Promise<QueryResult>;
  executeTracingQuery: (req: TracingRequest) => Promise<TracingResult>;
  getKafkaData: () => Promise<KafkaTopicData[]>;
  getRpcEndpoints: () => Promise<Array<[string, string]>>;
  executeRpcEndpoint: (endpointName: string, version: string, requestBody: any, signal?: AbortSignal) => Promise<any>;
  getPredefinedQuerylogOptions: () => Promise<[PredefinedQuery]>;
  getQuerylogSchema: () => Promise<QuerylogResult>;
  executeQuerylogQuery: (req: QuerylogRequest) => Promise<QuerylogResult>;
  getPredefinedCardinalityQueryOptions: () => Promise<[PredefinedQuery]>;
  executeCardinalityQuery: (
    req: CardinalityQueryRequest,
  ) => Promise<CardinalityQueryResult>;
  getAllMigrationGroups: () => Promise<MigrationGroupResult[]>;
  runMigration: (req: RunMigrationRequest) => Promise<RunMigrationResult>;
  getAllowedTools: () => Promise<AllowedTools>;
  getStoragesWithAllocationPolicies: () => Promise<string[]>;
  getAllocationPolicies: (storage: string) => Promise<AllocationPolicy[]>;
  setAllocationPolicyConfig: (
    configurable_component_namespace: string,
    configurable_component_class_name: string,
    resource_name: string,
    key: string,
    value: string,
    params: object,
  ) => Promise<void>;
  deleteAllocationPolicyConfig: (
    configurable_component_namespace: string,
    configurable_component_class_name: string,
    resource_name: string,
    key: string,
    params: object,
  ) => Promise<void>;
  getDlqTopics: () => Promise<Topic[]>;
  getDlqInstruction: () => Promise<ReplayInstruction | null>;
  setDlqInstruction: (
    topic: Topic,
    instruction: ReplayInstruction,
  ) => Promise<ReplayInstruction | null>;
  clearDlqInstruction: () => Promise<ReplayInstruction | null>;
  getAdminRegions: () => Promise<string[]>;
  runLightweightDelete: (
    storage_name: string,
    column_conditions: object,
  ) => Promise<Response>;
  listJobSpecs: () => Promise<JobSpecMap>;
  runJob(job_id: string): Promise<String>;
  getJobLogs(job_id: string): Promise<string[]>;
  getClickhouseSystemSettings: (host: string, port: number, storage: string) => Promise<ClickhouseSystemSetting[]>;
  summarizeTraceWithProfile: (traceLogs: string, spanType: string, signal?: AbortSignal) => Promise<any>;
}

function Client(): Client {
  const baseUrl = "/";

  return {
    getSettings: () => {
      const url = baseUrl + "settings";
      return fetch(url).then((resp) => resp.json());
    },
    getConfigs: () => {
      const url = baseUrl + "configs";
      return fetch(url).then((resp) => resp.json());
    },
    getAutoReplacementsBypassProjects: () => {
      const url = baseUrl + "auto-replacements-bypass-projects";
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
      }).then((resp) => resp.json());
    },
    createNewConfig: (
      key: ConfigKey,
      value: ConfigValue,
      description: ConfigDescription,
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
      description: ConfigDescription,
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
              storage.query_node,
          );
        });
    },

    getClickhouseNodeInfo: () => {
      const url = baseUrl + "clickhouse_node_info";
      return fetch(url).then((resp) => resp.json());
    },

    getSnubaDatasetNames: () => {
      const url = baseUrl + "snuba_datasets";
      return fetch(url).then((resp) => resp.json());
    },

    getAllowedProjects: () => {
      const url = baseUrl + "allowed_projects";
      return fetch(url).then((resp) => resp.json());
    },

    getAdminRegions: () => {
      const url = baseUrl + "admin_regions";
      return fetch(url).then((resp) => resp.json());
    },

    debugSnQLQuery: (query: SnQLRequest) => {
      const url = baseUrl + "snuba_debug";
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

    executeSnQLQuery: (query: SnQLRequest) => {
      const url = baseUrl + "production_snql_query";
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
        method: "POST",
        body: JSON.stringify(query),
      }).then((res) => {
        if (res.ok) {
          return Promise.resolve(res.json());
        } else {
          return res.json().then((err) => {
            let errMsg = err?.error.message || "Could not execute SnQL";
            throw new Error(errMsg);
          });
        }
      });
    },

    executeMQLQuery: (query: MQLRequest) => {
      const url = baseUrl + "production_mql_query";
      query.dataset = "generic_metrics";
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
        method: "POST",
        body: JSON.stringify(query),
      }).then((res) => {
        if (res.ok) {
          return Promise.resolve(res.json());
        } else {
          return res.json().then((err) => {
            let errMsg = err?.error.message || "Could not execute SnQL";
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

    getRpcEndpoints: () => {
      const url = baseUrl + "rpc_endpoints";
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
      }).then((resp) => resp.json()) as Promise<Array<[string, string]>>;
    },

    executeRpcEndpoint: async (endpointName: string, version: string, requestBody: any, signal?: AbortSignal) => {
      try {
        const url = `${baseUrl}rpc_execute/${endpointName}/${version}`;
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Accept: "application/json"
          },
          body: JSON.stringify(requestBody),
          signal,
        });
        if (!response.ok) {
          const errorData = await response.json();
          throw new Error(errorData.error || `Error! status: ${response.status}`);
        }
        const result = await response.json();
        return result;
      } catch (error) {
        if (error instanceof Error) {
          throw error;
        } else {
          throw new Error('An unexpected error occurred');
        }
      }
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
    getPredefinedCardinalityQueryOptions: () => {
      const url = baseUrl + "cardinality_queries";
      return fetch(url).then((resp) => resp.json());
    },
    executeCardinalityQuery: (query: CardinalityQueryRequest) => {
      const url = baseUrl + "cardinality_query";
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

    getStoragesWithAllocationPolicies: () => {
      const url = baseUrl + "storages_with_allocation_policies";
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
      }).then((resp) => resp.json());
    },
    getAllocationPolicies: (storage: string) => {
      const url =
        baseUrl + "allocation_policy_configs/" + encodeURIComponent(storage);
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
      }).then((resp) => resp.json());
    },
    setAllocationPolicyConfig: (
      configurable_component_namespace: string,
      configurable_component_class_name: string,
      resource_name: string,
      key: string,
      value: string,
      params: object,
    ) => {
      const url = baseUrl + "set_configurable_component_configuration";
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
        method: "POST",
        body: JSON.stringify({ configurable_component_namespace, configurable_component_class_name, resource_name, key, value, params }),
      }).then((res) => {
        if (res.ok) {
          return;
        } else {
          return res.json().then((err) => {
            let errMsg = err?.error || "Could not set config";
            throw new Error(errMsg);
          });
        }
      });
    },
    deleteAllocationPolicyConfig: (
      configurable_component_namespace: string,
      configurable_component_class_name: string,
      resource_name: string,
      key: string,
      params: object,
    ) => {
      const url = baseUrl + "set_configurable_component_configuration";
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
        method: "DELETE",
        body: JSON.stringify({ configurable_component_namespace, configurable_component_class_name, resource_name, key, params }),
      }).then((res) => {
        if (res.ok) {
          return;
        } else {
          return res.json().then((err) => {
            let errMsg = err?.error || "Could not delete config";
            throw new Error(errMsg);
          });
        }
      });
    },
    getDlqTopics: () => {
      const url = baseUrl + "dead_letter_queue";
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
      }).then((resp) => resp.json());
    },
    getDlqInstruction: () => {
      const url = baseUrl + "dead_letter_queue/replay";
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
      }).then((resp) => resp.json());
    },
    setDlqInstruction: (topic: Topic, instruction: ReplayInstruction) => {
      const url = baseUrl + "dead_letter_queue/replay";
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
        method: "POST",
        body: JSON.stringify({
          logicalName: topic.logicalName,
          physicalName: topic.physicalName,
          storage: topic.storage,
          slice: topic.slice,
          maxMessages: instruction.messagesToProcess,
          policy: instruction.policy,
        }),
      }).then((res) => {
        if (res.ok) {
          return res.json();
        } else {
          return res.json().then((err) => {
            let errMsg = err?.error || "Could not replay";
            throw new Error(errMsg);
          });
        }
      });
    },
    clearDlqInstruction: () => {
      const url = baseUrl + "dead_letter_queue/replay";
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
        method: "DELETE",
      }).then((resp) => resp.json());
    },
    runLightweightDelete: (storage_name: string, column_conditions: object) => {
      const url = baseUrl + "delete";
      return fetch(url, {
        method: "DELETE",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          storage: storage_name,
          query: { columns: column_conditions },
        }),
      });
    },
    listJobSpecs: () => {
      const url = baseUrl + "job-specs";
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
        method: "GET",
      }).then((resp) => resp.json());
    },
    runJob: (job_id: string) => {
      const url = baseUrl + "job-specs/" + job_id;
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
        method: "POST",
      }).then((resp) => resp.text());
    },
    getJobLogs: (job_id: string) => {
      const url = baseUrl + "job-specs/" + job_id + "/logs";
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
        method: "GET",
      }).then((resp) => resp.json());
    },
    getClickhouseSystemSettings: (host: string, port: number, storage: string) => {
      const url = `${baseUrl}clickhouse_system_settings?host=${encodeURIComponent(host)}&port=${encodeURIComponent(port)}&storage=${encodeURIComponent(storage)}`;
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
        method: "GET",
      }).then((resp) => {
        if (resp.ok) {
          return resp.json();
        } else {
          return resp.json().then((err) => {
            let errMsg = err?.error || "Could not get Clickhouse system settings";
            throw new Error(errMsg);
          });
        }
      });
    },
    summarizeTraceWithProfile: (traceLogs: string, storage: string, signal?: AbortSignal) => {
      const url = baseUrl + "rpc_summarize_trace_with_profile";
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
        method: "POST",
        body: JSON.stringify({
          trace_logs: traceLogs,
          storage: storage
        }),
        signal,
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
