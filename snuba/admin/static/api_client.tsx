interface Client {
  getConfigs: () => Promise<
    Map<string, { value: string | number; type: "string" | "int" | "float" }>
  >;
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
