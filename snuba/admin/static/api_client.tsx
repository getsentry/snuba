import {
  Config,
  ConfigKey,
  ConfigValue,
  ConfigChange,
} from "./runtime_config/types";

interface Client {
  getConfigs: () => Promise<Config[]>;
  createNewConfig: (key: ConfigKey, value: ConfigValue) => Promise<Config>;
  getAuditlog: () => Promise<ConfigChange[]>;
}

function Client() {
  const baseUrl = "/";

  return {
    getConfigs: () => {
      const url = baseUrl + "configs";
      return fetch(url).then((resp) => resp.json());
    },
    createNewConfig: (key: ConfigKey, value: ConfigValue) => {
      const url = baseUrl + "configs";
      const params = { key, value };

      return fetch(url, {
        headers: { "Content-Type": "application/json" },
        method: "POST",
        body: JSON.stringify(params),
      }).then((res) => {
        if (res.ok) {
          return Promise.resolve(res.json());
        } else {
          return res.json().then((err) => {
            let errMsg = err?.error || "Could not create config";
            throw new Error(errMsg);
          });
        }
      });
    },
    getAuditlog: () => {
      const url = baseUrl + "config_auditlog";
      return fetch(url).then((resp) => resp.json());
    },
  };
}

export default Client;
