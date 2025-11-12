local snuba = import './pipelines/snuba-py.libsonnet';
local pipedream = import 'github.com/getsentry/gocd-jsonnet/libs/pipedream.libsonnet';

local py_pipedream_config = {
  name: 'snuba-py',
  materials: {
    snuba_repo: {
      git: 'git@github.com:getsentry/snuba.git',
      shallow_clone: true,
      branch: 'master',
      destination: 'snuba',
      ignore: ['rust_snuba/**'],
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

pipedream.render(py_pipedream_config, snuba)
