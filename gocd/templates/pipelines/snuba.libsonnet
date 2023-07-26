local getsentry = import 'github.com/getsentry/gocd-jsonnet/libs/getsentry.libsonnet';
local gocdtasks = import 'github.com/getsentry/gocd-jsonnet/libs/gocd-tasks.libsonnet';

// The return value of this function is the body of a GoCD pipeline.
// More information on gocd-flavor YAML this is producing can be found here:
// - https://github.com/tomzo/gocd-yaml-config-plugin#pipeline
// - https://www.notion.so/sentry/GoCD-New-Service-Quickstart-6d8db7a6964049b3b0e78b8a4b52e25d

local migrate_stage(stage_name, region) = [
  {
    [stage_name]: {
      fetch_materials: true,
      jobs: {
        migrate: {
          timeout: 1200,
          elastic_profile_id: 'snuba',
          environment_variables: {
            // ST deployments use 'snuba' for container and label selectors
            // in migrations, whereas the US region deployment uses snuba-admin.
            SNUBA_SERVICE_NAME: if getsentry.is_st(region) then 'snuba' else 'snuba-admin',
          },
          tasks: [
            if getsentry.is_st(region) then
              gocdtasks.script(importstr '../bash/migrate-st.sh')
            else
              gocdtasks.script(importstr '../bash/migrate.sh'),
            {
              plugin: {
                options: gocdtasks.script(importstr '../bash/migrate-reverse.sh'),
                run_if: 'failed',
                configuration: {
                  id: 'script-executor',
                  version: 1,
                },
              },
            },
          ],
        },
      },
    },
  },
];

// Snuba relies on checks to prevent folks from writing migrations and code
// at the same time, this means there is a requirement that folks MUST deploy
// the migration before merge code changes relying on that migration.
// This doesn't hold true for ST deployments today, so temporarily run an
// early migration stage for ST deployments.
local early_migrate(region) =
  if getsentry.is_st(region) then
    migrate_stage('st_migrate', region)
  else
    [];

function(region) {
  environment_variables: {
    SENTRY_REGION: region,
    // Required for checkruns.
    GITHUB_TOKEN: '{{SECRET:[devinfra-github][token]}}',
    GOCD_ACCESS_TOKEN: '{{SECRET:[devinfra][gocd_access_token]}}',
  },
  group: 'snuba-next',
  lock_behavior: 'unlockWhenFinished',
  materials: {
    snuba_repo: {
      git: 'git@github.com:getsentry/snuba.git',
      shallow_clone: false,
      branch: 'master',
      destination: 'snuba',
    },
  },
  stages: [
    {
      checks: {
        jobs: {
          checks: {
            timeout: 1800,
            elastic_profile_id: 'snuba',
            tasks: [
              gocdtasks.script(importstr '../bash/check-github.sh'),
              gocdtasks.script(importstr '../bash/check-cloud-build.sh'),
              gocdtasks.script(importstr '../bash/check-migrations.sh'),
            ],
          },
        },
      },
    },

  ] + early_migrate(region) + [

    {
      'deploy-canary': {
        fetch_materials: true,
        jobs: {
          'create-sentry-release': {
            environment_variables: {
              SENTRY_ORG: 'sentry',
              SENTRY_PROJECT: 'snuba',
              SENTRY_AUTH_TOKEN: '{{SECRET:[devinfra-sentryio][token]}}',
            },
            timeout: 300,
            elastic_profile_id: 'snuba',
            tasks: [
              gocdtasks.script(importstr '../bash/sentry-release-canary.sh'),
            ],
          },
          'deploy-canary': {
            timeout: 1200,
            elastic_profile_id: 'snuba',
            environment_variables: {
              LABEL_SELECTOR: 'service=snuba,is_canary=true',
            },
            tasks: [
              gocdtasks.script(importstr '../bash/deploy.sh'),
            ],
          },
        },
      },
    },

    {
      'deploy-primary': {
        fetch_materials: true,
        jobs: {
          'create-sentry-release': {
            environment_variables: {
              SENTRY_ORG: 'sentry',
              SENTRY_PROJECT: 'snuba',
              SENTRY_AUTH_TOKEN: '{{SECRET:[devinfra-sentryio][token]}}',
            },
            timeout: 300,
            elastic_profile_id: 'snuba',
            tasks: [
              gocdtasks.script(importstr '../bash/sentry-release-primary.sh'),
            ],
          },
          'deploy-primary': {
            timeout: 1200,
            elastic_profile_id: 'snuba',
            environment_variables: {
              LABEL_SELECTOR: 'service=snuba',
            },
            tasks: [
              if getsentry.is_st(region) then
                gocdtasks.script(importstr '../bash/deploy-st.sh')
              else
                gocdtasks.script(importstr '../bash/deploy.sh'),
            ],
          },
        },
      },
    },

  ] + migrate_stage('migrate', region),
}
