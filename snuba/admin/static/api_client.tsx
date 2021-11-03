import { ConfigType } from "./types";

type Config = { key: string; value: string | number; type: ConfigType };

interface Client {
  getConfigs: () => Promise<Config[]>;
  createNewConfig: (
    key: string,
    value: string | number,
    type: ConfigType
  ) => Promise<Config>;
}

function Client() {
  const baseUrl = "/";

  return {
    getConfigs: () => {
      const url = baseUrl + "configs";
      return fetch(url).then((resp) => resp.json());
    },
    createNewConfig: (
      key: string,
      value: string | number,
      type: ConfigType
    ) => {
      const url = baseUrl + "configs";
      const params = { key, value, type };

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
  };
}

export default Client;
