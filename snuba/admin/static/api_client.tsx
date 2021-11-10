import { Config } from "./runtime_config/types";

interface Client {
  getConfigs: () => Promise<Config[]>;
}

function Client() {
  const baseUrl = "/";

  return {
    getConfigs: () => {
      const url = baseUrl + "configs";
      return fetch(url).then((resp) => resp.json());
    },
  };
}

export default Client;
