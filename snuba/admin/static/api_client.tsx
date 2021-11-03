interface Client {
  getConfigs: () => Promise<Map<string, string | number>>;
}

function Client() {
  const baseUrl = "/";

  return {
    getConfigs: async () => {
      const url = baseUrl + "configs";
      return fetch(url).then((resp) => resp.json());
    },
  };
}

export default Client;
