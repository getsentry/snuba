import { MantineReactTable, MRT_ColumnDef, useMantineReactTable } from "mantine-react-table";
import React, { useMemo } from "react";
import { useEffect, useState } from "react";
import Client from "SnubaAdmin/api_client";
import { ClickhouseNodeInfo } from "./types";

function DatabaseClusters(props: { api: Client }) {
    const [nodeData, setNodeData] = useState<ClickhouseNodeInfo[]>([]);

    useEffect(() => {
        props.api.getClickhouseNodeInfo().then((res) => {
            setNodeData(res);
        });
    }, []);

    const columns = useMemo<MRT_ColumnDef<ClickhouseNodeInfo>[]>(
        () => [
            {
                accessorKey: 'cluster',
                header: 'Cluster',
            },
            {
                accessorFn: (row) => `${row.host_name} (${row.host_address})`,
                header: 'Host',
            },
            {
                accessorKey: 'shard',
                header: 'Shard',
            },
            {
                accessorKey: 'replica',
                header: 'Replica',
            },
            {
                accessorKey: 'version',
                header: 'Version',
            },
        ],
        [],
    );

    const table = useMantineReactTable({
        columns,
        data: nodeData,
    });

    return <MantineReactTable table={table} />;
}

export default DatabaseClusters
