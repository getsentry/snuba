local gocdtasks = import 'github.com/getsentry/gocd-jsonnet/libs/gocd-tasks.libsonnet';
local regions = getsentry.prod_regions + getsentry.test_regions;

local pipeline_group = 'snuba';
local pipeline_name = 'clickhouse-query-replayer';

local generate_replay_job(component) =
  {
    environment_variables: {
      // TODO - what env vars do we need ?
      SENTRY_REGION='s4s',
      // TODO do correct interpolation
      LABEL_SELECTOR: 'service=snuba,' + 'component=' + component
    },
    elastic_profile_id: pipeline_group,
    tasks: [
      gocdtasks.script(importstr './bash/replay-ch-queries.sh'),
    ],
  };

local pipeline = {
  group: pipeline_group,
  display_order: 100,  // Ensure it's last pipeline in UI
  // TODO: for parameters, use parameters as parameters
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
          ['replay-' + component]: generate_replay_job(component)
          for component in ['query-replayer-1', 'query-replayer-2']
        },
      },
    },
  ],
};
