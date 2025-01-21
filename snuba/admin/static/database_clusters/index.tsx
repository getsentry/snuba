import { MantineReactTable, MRT_ColumnDef, useMantineReactTable } from "mantine-react-table";
import React, { useMemo } from "react";
import { useEffect, useState } from "react";
import Client from "SnubaAdmin/api_client";
import { ClickhouseNodeInfo, ClickhouseSystemSetting } from "SnubaAdmin/database_clusters/types";
import { Button } from "@mantine/core";


function DatabaseClusters(props: { api: Client }) {
    const [nodeData, setNodeData] = useState<ClickhouseNodeInfo[]>([]);
    const [systemSettings, setSystemSettings] = useState<ClickhouseSystemSetting[]>([]);
    const [selectedNode, setSelectedNode] = useState<ClickhouseNodeInfo | null>(null);

    useEffect(() => {
        props.api.getClickhouseNodeInfo().then((res) => {
            setNodeData(res);
        });
    }, []);

    const handleNodeClick = (node: ClickhouseNodeInfo) => {
        setSelectedNode(node);
        props.api.getClickhouseSystemSettings(node.host_name, node.port, node.storage_name).then((res) => {
            setSystemSettings(res);
        });
    };

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
                accessorKey: 'port',
                header: 'Port',
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
                accessorKey: 'storage_name',
                header: 'Storage Name',
            },
            {
                accessorFn: (row) => row.is_distributed ? 'Query Node' : 'Storage Node',
                header: 'Node Type',
            },
            {
                accessorKey: 'version',
                header: 'Version',
                filterVariant: 'multi-select',
            },
        ],
        [],
    );

    const mainTable = useMantineReactTable({
        columns,
        data: nodeData,
        initialState: { showColumnFilters: true },
        enableFacetedValues: true,
        mantineTableBodyRowProps: ({ row }) => ({
            onClick: () => handleNodeClick(row.original as ClickhouseNodeInfo),
            sx: { cursor: 'pointer' },
        }),
    });

    const detailColumns = useMemo<MRT_ColumnDef<ClickhouseSystemSetting>[]>(
        () => [
            { accessorKey: 'name', header: 'Name' },
            { accessorKey: 'value', header: 'Value' },
            { accessorKey: 'default', header: 'Default' },
            { accessorKey: 'changed', header: 'Changed' },
            { accessorKey: 'description', header: 'Description' },
            { accessorKey: 'type', header: 'Type' },
        ],
        []
    );

    const detailTable = useMantineReactTable({
        columns: detailColumns,
        data: selectedNode ? systemSettings : [],
    });

    return (
        <div>
            {selectedNode ? (
                <>
                    <Button onClick={() => setSelectedNode(null)} variant="outline">Back</Button>
                    <h2>Node ({selectedNode.host_name}) Details</h2>
                    <MantineReactTable table={detailTable} />
                </>
            ) : (
                <MantineReactTable table={mainTable} />
            )}
        </div>
    );
}

export default DatabaseClusters
