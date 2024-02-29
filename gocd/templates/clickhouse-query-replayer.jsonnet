local gocdtasks = import 'github.com/getsentry/gocd-jsonnet/libs/gocd-tasks.libsonnet';

local pipeline_group = 'snuba';
local pipeline_name = 'clickhouse-query-replayer';

local generate_replay_job(component) =
  {
    environment_variables: {
      SENTRY_REGION: 's4s',
      SNUBA_SERVICE_NAME: 'query-replayer-gocd',
      GOOGLE_CLOUD_PROJECT: 'search-and-storage',
      REPLAYER_ARGS: 'your args here (e.g --gcs-bucket abcd)',
    },
    elastic_profile_id: pipeline_group,
    tasks: [
      gocdtasks.script(importstr './bash/s4s-replay-queries.sh'),
    ],
  };

local pipeline = {
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
      'replay-queries': {
        approval: {
          type: 'manual',
        },
        jobs: {
          'query-replayer': generate_replay_job('query-replayer'),
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
