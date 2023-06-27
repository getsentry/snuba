local snuba = import './pipelines/snuba.libsonnet';
local pipedream = import 'github.com/getsentry/gocd-jsonnet/v1.0.0/pipedream.libsonnet';

local pipedream_config = {
  name: 'snuba-next',
  auto_deploy: false,
  materials: {
    snuba_repo: {
      git: 'git@github.com:getsentry/snuba.git',
      shallow_clone: true,
      branch: 'master',
      destination: 'snuba',
    },
  },
};

pipedream.render(pipedream_config, snuba)
