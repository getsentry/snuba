local gocdtasks = import 'github.com/getsentry/gocd-jsonnet/libs/gocd-tasks.libsonnet';

local pipeline_group = 'snuba';
local pipeline_name = 'clickhouse-query-fetcher';

local generate_fetch_job() =
  {
    environment_variables: {
      SENTRY_REGION: 's4s',
      SNUBA_CMD_TYPE: 'fetcher',
    },
    elastic_profile_id: pipeline_group,
    tasks: [
      gocdtasks.script(importstr './bash/s4s-clickhouse-queries.sh'),
    ],
  };

local pipeline = {
  environment_variables: {
    GOOGLE_CLOUD_PROJECT: 'mattrobenolt-kube',
    QUERYLOG_HOST: 'snuba-query-legacy-1-1.c.mattrobenolt-kube.internal',
    QUERYLOG_PORT: '9000',
    WINDOW_HOURS: '24',
    TABLES: 'errors_dist,transactions_dist',
    GCS_BUCKET: 'clickhouse-query-comparisons-s4s',

  },
  group: pipeline_group,
  display_order: 100,  // Ensure it's last pipeline in UI
  lock_behavior: 'unlockWhenFinished',
  materials: {
    snuba_repo: {
      git: 'git@github.com:getsentry/snuba.git',
      shallow_clone: true,
      branch: 'master',
      destination: 'snuba',
    },
  },
  stages: [
    {
      'fetch-queries': {
        approval: {
          type: 'manual',
        },
        jobs: {
          'query-fetcher': generate_fetch_job(),
        },
      },
    },
  ],
};

// We can output two variances of these pipelines.
if std.extVar('output-files') then
  // 1. We output each pipeline in a seperate file, this makes debugging easier.
  {
    [pipeline_name + '.yaml']: {
      format_version: 10,
      pipelines: {
        [pipeline_name]: pipeline,
      },
    },
  }
else
  // 2. Output all pipelines in a single file, needed for validation or passing
  //    pipelines directly to GoCD.
  {
    format_version: 10,
    pipelines: {
      [pipeline_name]: pipeline,
    },
  }
