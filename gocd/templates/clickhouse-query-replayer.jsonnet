local gocdtasks = import 'github.com/getsentry/gocd-jsonnet/libs/gocd-tasks.libsonnet';
local regions = getsentry.prod_regions + getsentry.test_regions;

local pipeline_group = 'snuba';
local pipeline_name = 'clickhouse-query-replayer';

local generate_replay_job(component) =
  {
    environment_variables: {
      SENTRY_REGION: 's4s',
      SNUBA_SERVICE_NAME: 'snuba-admin',
      GOOGLE_CLOUD_PROJECT: 'search-and-storage',
      REPLAYER_ARGS: 'your args here (e.g --gcs-bucket abcd)'
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
      replay-queries: {
        approval: {
          type: 'manual',
        },
        jobs: {
          query-replayer: generate_replay_job('query-replayer)
        },
      },
    },
  ],
};
