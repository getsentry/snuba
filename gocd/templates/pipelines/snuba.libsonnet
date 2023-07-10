local gocdtasks = import 'github.com/getsentry/gocd-jsonnet/v1.0.0/gocd-tasks.libsonnet';

// The return value of this function is the body of a GoCD pipeline.
// More information on gocd-flavor YAML this is producing can be found here:
// - https://github.com/tomzo/gocd-yaml-config-plugin#pipeline
// - https://www.notion.so/sentry/GoCD-New-Service-Quickstart-6d8db7a6964049b3b0e78b8a4b52e25d

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
              gocdtasks.script(importstr '../bash/deploy.sh'),
            ],
          },
        },
      },
    },
    {
      migrate: {
        fetch_materials: true,
        jobs: {
          migrate: {
            timeout: 1200,
            elastic_profile_id: 'snuba',
            environment_variables: {
              SNUBA_SERVICE_NAME: if region == 'monitor' || std.startsWith(region, 'customer-') then 'snuba' else 'snuba-admin',
            },
            tasks: [
              if region == 'monitor' || std.startsWith(region, 'customer-') then
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
  ],
}
