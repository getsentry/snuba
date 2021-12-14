import { Config, ConfigChange } from "./runtime_config/types";

interface Client {
  getConfigs: () => Promise<Config[]>;
  getAuditlog: () => Promise<ConfigChange[]>;
}

function Client() {
  const baseUrl = "/";

  return {
    getConfigs: () => {
      const url = baseUrl + "configs";
      return fetch(url).then((resp) => resp.json());
    },
    getAuditlog: () => {
      const url = baseUrl + "config_auditlog";
      return fetch(url).then((resp) => resp.json());
    },
  };
}

export default Client;
