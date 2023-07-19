local snuba = import './pipelines/snuba.libsonnet';
local pipedream = import 'github.com/getsentry/gocd-jsonnet/libs/pipedream.libsonnet';

local pipedream_config = {
  name: 'snuba',
  materials: {
    snuba_repo: {
      git: 'git@github.com:getsentry/snuba.git',
      shallow_clone: true,
      branch: 'master',
      destination: 'snuba',
    },
  },
  rollback: {
    material_name: 'snuba_repo',
    stage: 'deploy-primary',
    elastic_profile_id: 'snuba',
  },

  // Set to true to auto-deploy changes (defaults to true)
  auto_deploy: false,
};

pipedream.render(pipedream_config, snuba)
