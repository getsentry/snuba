import React, { useState, useCallback, useEffect } from 'react';
import { Select, Button, Code, Space, Textarea, Accordion, createStyles, Loader, Checkbox, Text, Table } from '@mantine/core';
import useApi from 'SnubaAdmin/api_client';

const DEBUG_SUPPORTED_VERSIONS = ['v1'];

function RpcEndpoints() {
    const api = useApi();
    const [endpoints, setEndpoints] = useState<Array<{ name: string, version: string }>>([]);
    const [selectedEndpoint, setSelectedEndpoint] = useState<string | null>(null);
    const [selectedVersion, setSelectedVersion] = useState<string | null>(null);
    const [requestBody, setRequestBody] = useState('');
    const [response, setResponse] = useState<any | null>(null);
    const exampleRequestTemplates: Record<string, Record<string, any>> = require('SnubaAdmin/rpc_endpoints/exampleRequestTemplates.json');
    const [accordionOpened, setAccordionOpened] = useState(false);
    const [isLoading, setIsLoading] = useState(false);
    const [debugMode, setDebugMode] = useState(false);

    const fetchEndpoints = useCallback(async () => {
        try {
            const fetchedEndpoints: [string, string][] = await api.getRpcEndpoints();
            const formattedEndpoints = fetchedEndpoints.map((endpoint: [string, string]) => ({
                name: endpoint[0],
                version: endpoint[1]
            }));
            setEndpoints(formattedEndpoints);
        } catch (error) {
            console.error("Error fetching endpoints:", error);
        }
    }, []);

    useEffect(() => {
        fetchEndpoints();
    }, []);

    const handleEndpointSelect = (value: string | null) => {
        setSelectedEndpoint(value);
        const selectedEndpointData = endpoints.find(e => e.name === value);
        setSelectedVersion(selectedEndpointData?.version || null);
        setRequestBody('');
        setResponse(null);
    };

    const handleExecute = async () => {
        if (!selectedEndpoint || !selectedVersion) return;
        setIsLoading(true);
        try {
            const parsedBody = JSON.parse(requestBody);
            if (debugMode && DEBUG_SUPPORTED_VERSIONS.includes(selectedVersion)) {
                parsedBody.meta = parsedBody.meta || {};
                parsedBody.meta.debug = true;
            }
            const result = await api.executeRpcEndpoint(selectedEndpoint, selectedVersion, parsedBody);
            setResponse(result);
        } catch (error: any) {
            alert(`Error: ${error.message}`);
            setResponse({ error: error.message });
        } finally {
            setIsLoading(false);
        }
    };

    const useStyles = createStyles((theme) => ({
        accordion: {
            '& .mantine-Accordion-control': {
                backgroundColor: theme.colors.blue[1],
                color: theme.colors.blue[7],
                fontSize: theme.fontSizes.xs,
                padding: '2px 4px',
                lineHeight: 1.2,
                borderBottom: `1px solid ${theme.colors.gray[3]}`,
                cursor: 'pointer',
                '&:hover': {
                    backgroundColor: theme.colors.blue[2],
                },
            },
        },
        table: {
            border: `1px solid ${theme.colors.gray[3]}`,
            '& th, & td': {
                border: `1px solid ${theme.colors.gray[3]}`,
                padding: theme.spacing.xs,
            },
            '& th': {
                backgroundColor: theme.colors.gray[1],
                fontWeight: 'bold',
            },
            '& td:first-of-type': {
                width: '20%',
                fontWeight: 'bold',
            },
            '& td:last-of-type': {
                width: '80%',
            },
        },
        debugCheckbox: {
            marginBottom: theme.spacing.md,
        },
    }));

    const { classes } = useStyles();

    return (
        <div>
            <h2>RPC Endpoints</h2>
            <Select
                label="Select an endpoint"
                placeholder="Choose an endpoint"
                data={endpoints.map(endpoint => ({ value: endpoint.name, label: `${endpoint.name} (${endpoint.version})` }))}
                value={selectedEndpoint}
                onChange={handleEndpointSelect}
                style={{ width: '100%', marginBottom: '1rem' }}
            />
            <Space h="md" />
            <Accordion
                classNames={{ item: classes.accordion }}
                variant="filled"
                radius="sm"
                value={accordionOpened ? 'example' : null}
                onChange={(value) => setAccordionOpened(value === 'example')}
            >
                <Accordion.Item value="example">
                    <Accordion.Control>Example Request Payload</Accordion.Control>
                    <Accordion.Panel>
                        <Code block style={{ color: 'green' }}>
                            <pre>
                                {JSON.stringify(
                                    selectedEndpoint && selectedVersion
                                        ? exampleRequestTemplates[selectedEndpoint]?.[selectedVersion] || exampleRequestTemplates.default
                                        : exampleRequestTemplates.default,
                                    null,
                                    2
                                )}
                            </pre>
                        </Code>
                        <Button
                            onClick={() => {
                                setRequestBody(
                                    JSON.stringify(
                                        selectedEndpoint && selectedVersion
                                            ? exampleRequestTemplates[selectedEndpoint]?.[selectedVersion] || exampleRequestTemplates.default
                                            : exampleRequestTemplates.default,
                                        null,
                                        2
                                    )
                                );
                                setAccordionOpened(false);
                            }}
                            style={{ marginTop: '1rem' }}
                        >
                            Copy to Request Body
                        </Button>
                    </Accordion.Panel>
                </Accordion.Item>
            </Accordion>
            <Space h="md" />
            <Textarea
                label="Request Body (JSON)"
                placeholder="Enter request body"
                value={requestBody}
                onChange={(event) => setRequestBody(event.currentTarget.value)}
                style={{ width: '100%' }}
                autosize
                minRows={5}
            />
            <Space h="md" />
            <Checkbox
                label="Enable Debug Mode"
                checked={debugMode}
                onChange={(event) => setDebugMode(event.currentTarget.checked)}
                disabled={!DEBUG_SUPPORTED_VERSIONS.includes(selectedVersion || '')}
                className={classes.debugCheckbox}
            />
            <Button
                onClick={handleExecute}
                disabled={!selectedEndpoint || !requestBody || isLoading}
                leftIcon={isLoading ? <Loader size="xs" /> : null}
            >
                {isLoading ? 'Executing...' : 'Execute'}
            </Button>
            {response && (
                <>
                    <Space h="md" />
                    <h3>Response:</h3>
                    <Accordion classNames={{ item: classes.accordion }}>
                        <Accordion.Item value="query-info">
                            <Accordion.Control>Query Metadata</Accordion.Control>
                            <Accordion.Panel>
                                {response.meta?.queryInfo ? (
                                    response.meta.queryInfo.map((queryInfo: any, index: number) => (
                                        <div key={index}>
                                            <h4>Query {index + 1}</h4>
                                            <Table className={classes.table}>
                                                <thead>
                                                    <tr>
                                                        <th>Attribute</th>
                                                        <th>Value</th>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    {Object.entries({ ...queryInfo.stats, ...queryInfo.metadata }).map(([key, value]) => (
                                                        <tr key={key}>
                                                            <td>{key}</td>
                                                            <td>
                                                                {typeof value === 'object' ? (
                                                                    <Code block>{JSON.stringify(value, null, 2)}</Code>
                                                                ) : (
                                                                    String(value)
                                                                )}
                                                            </td>
                                                        </tr>
                                                    ))}
                                                </tbody>
                                            </Table>
                                        </div>
                                    ))
                                ) : (
                                    <Text>No query info available</Text>
                                )}
                            </Accordion.Panel>
                        </Accordion.Item>
                    </Accordion>
                    <Space h="md" />
                    <h4>Response Data:</h4>
                    <Code block>
                        <pre>
                            {JSON.stringify(
                                (({ meta, ...rest }) => rest)(response),
                                null,
                                2
                            )}
                        </pre>
                    </Code>
                </>
            )}
        </div>
    );
}

export default RpcEndpoints;
