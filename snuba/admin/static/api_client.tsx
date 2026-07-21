import { AllowedTools, Settings } from "SnubaAdmin/types";

import {
  ClickhouseNodeData,
  QueryRequest,
  QueryResult,
  PredefinedQuery,
} from "SnubaAdmin/clickhouse_queries/types";

import {
  CopyTableRequest,
} from "SnubaAdmin/copy_tables/types";
import {
  MigrationGroupResult,
  RunMigrationRequest,
  RunMigrationResult,
} from "SnubaAdmin/clickhouse_migrations/types";
import { TracingRequest, TracingResult } from "SnubaAdmin/tracing/types";
import {
  SnQLRequest,
  SnQLResult,
  SnubaDatasetName,
} from "SnubaAdmin/snql_to_sql/types";

import { QuerylogRequest, QuerylogResult } from "SnubaAdmin/querylog/types";

import { AutoReplacementsBypassProjectsData } from "SnubaAdmin/auto_replacements_bypass_projects/types";

interface Client {
  getSettings: () => Promise<Settings>;
  getAutoReplacementsBypassProjects: () => Promise<
    AutoReplacementsBypassProjectsData[]
  >;
  getClickhouseNodes: () => Promise<[ClickhouseNodeData]>;
  getSnubaDatasetNames: () => Promise<SnubaDatasetName[]>;
  getAllowedProjects: () => Promise<string[]>;
  executeSnQLQuery: (query: SnQLRequest) => Promise<any>;
  debugSnQLQuery: (query: SnQLRequest) => Promise<SnQLResult>;
  getPredefinedQueryOptions: () => Promise<[PredefinedQuery]>;
  executeSystemQuery: (req: QueryRequest) => Promise<QueryResult>;
  executeCopyTable: (req: CopyTableRequest) => Promise<any>;
  executeTracingQuery: (req: TracingRequest) => Promise<TracingResult>;
  getRpcEndpoints: () => Promise<Array<[string, string]>>;
  executeRpcEndpoint: (endpointName: string, version: string, requestBody: any, signal?: AbortSignal) => Promise<any>;
  getPredefinedQuerylogOptions: () => Promise<[PredefinedQuery]>;
  getQuerylogSchema: () => Promise<QuerylogResult>;
  executeQuerylogQuery: (req: QuerylogRequest) => Promise<QuerylogResult>;
  getAllMigrationGroups: () => Promise<MigrationGroupResult[]>;
  runMigration: (req: RunMigrationRequest) => Promise<RunMigrationResult>;
  getAllowedTools: () => Promise<AllowedTools>;
  getAdminRegions: () => Promise<string[]>;
  listJobSpecs: () => Promise<JobSpecMap>;
  runJob(job_id: string): Promise<String>;
  listJobTypes: () => Promise<string[]>;
  runJobByType(
    job_type: string,
    params: { [key: string]: any },
  ): Promise<{ job_id: string; status: string }>;
  getJobLogs(job_id: string): Promise<string[]>;
  summarizeTraceWithProfile: (traceLogs: string, spanType: string, signal?: AbortSignal) => Promise<any>;
}

function Client(): Client {
  const baseUrl = "/";

  return {
    getSettings: () => {
      const url = baseUrl + "settings";
      return fetch(url).then((resp) => resp.json());
    },
    getAutoReplacementsBypassProjects: () => {
      const url = baseUrl + "auto-replacements-bypass-projects";
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
      }).then((resp) => resp.json());
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
    executeCopyTable: (query: CopyTableRequest) => {
      const url = baseUrl + "run_copy_table_query";
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
    listJobTypes: () => {
      const url = baseUrl + "job-types";
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
        method: "GET",
      }).then((resp) => resp.json());
    },
    runJobByType: (job_type: string, params: { [key: string]: any }) => {
      const url = baseUrl + "job-types/" + job_type + "/run";
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
        method: "POST",
        body: JSON.stringify({ params }),
      }).then((resp) => {
        if (resp.ok) {
          return resp.json();
        }
        return resp.json().then((err) => {
          const error = new Error(err?.error || "Could not run job");
          (error as any).jobId = err?.job_id;
          throw error;
        });
      });
    },
    getJobLogs: (job_id: string) => {
      const url = baseUrl + "job-specs/" + job_id + "/logs";
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
        method: "GET",
      }).then((resp) => resp.json());
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
