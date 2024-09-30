type ClickhouseNodeInfo = {
    cluster: string,
    host_name: string,
    host_address: string,
    shard: number,
    replica: number,
    version: string,
};

export { ClickhouseNodeInfo };
