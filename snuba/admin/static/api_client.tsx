import { Config, ConfigKey, ConfigValue } from "./runtime_config/types";

interface Client {
  getConfigs: () => Promise<Config[]>;
  createNewConfig: (key: ConfigKey, value: ConfigValue) => Promise<Config>;
  deleteConfig: (key: ConfigKey) => Promise<void>;
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
          return res.json();
        } else {
          throw new Error("Could not create config");
        }
      });
    },
    deleteConfig: (key: ConfigKey) => {
      const url = baseUrl + "configs/" + key;
      return fetch(url, {
        headers: { "Content-Type": "application/json" },
        method: "DELETE",
      }).then((res) => {
        if (res.ok) {
          return;
        } else {
          throw new Error("Could not delete config");
        }
      });
    },
  };
}

export default Client;
