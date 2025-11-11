local snuba = import './pipelines/snuba-rs.libsonnet';
local pipedream = import 'github.com/getsentry/gocd-jsonnet/libs/pipedream.libsonnet';

local rs_pipedream_config = {
  name: 'snuba-rs',
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
  auto_deploy: true,
};

pipedream.render(rs_pipedream_config, snuba)
