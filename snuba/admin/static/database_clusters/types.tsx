type ClickhouseNodeInfo = {
    cluster: string,
    host_name: string,
    host_address: string,
    port: number,
    shard: number,
    replica: number,
    version: string,
    storage_name: string,
    is_query_node: boolean,
    is_distributed: boolean,
};

type ClickhouseSystemSetting = {
    name: string,
    value: string,
    default: string,
    changed: number,
    description: string,
    type: string,
};

export { ClickhouseNodeInfo, ClickhouseSystemSetting };
