import React, { useState, useCallback, useEffect } from 'react';
import { Select, Button, Code, Space, Textarea, Accordion, createStyles } from '@mantine/core';
import useApi from 'SnubaAdmin/api_client';


function RpcEndpoints() {
    const api = useApi();
    const [endpoints, setEndpoints] = useState<Array<{ name: string, version: string }>>([]);
    const [selectedEndpoint, setSelectedEndpoint] = useState<string | null>(null);
    const [selectedVersion, setSelectedVersion] = useState<string | null>(null);
    const [requestBody, setRequestBody] = useState('');
    const [response, setResponse] = useState<any | null>(null);
    const exampleRequestTemplates = require('./exampleRequestTemplates.json');
    const [accordionOpened, setAccordionOpened] = useState(false);

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
        try {
            const parsedBody = JSON.parse(requestBody);
            const result = await api.executeRpcEndpoint(selectedEndpoint, selectedVersion, parsedBody);
            setResponse(result);
        } catch (error: any) {
            console.error('Error details:', error.message);
            setResponse({ error: error.message });
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
    }));

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
                classNames={{ item: useStyles().classes.accordion }}
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
                                    exampleRequestTemplates[selectedEndpoint as keyof typeof exampleRequestTemplates] || exampleRequestTemplates.default,
                                    null,
                                    2
                                )}
                            </pre>
                        </Code>
                        <Button
                            onClick={() => {
                                setRequestBody(
                                    JSON.stringify(
                                        exampleRequestTemplates[selectedEndpoint as keyof typeof exampleRequestTemplates] || exampleRequestTemplates.default,
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
            <Button onClick={handleExecute} disabled={!selectedEndpoint || !requestBody}>
                Execute
            </Button>
            {response && (
                <>
                    <Space h="md" />
                    <h3>Response:</h3>
                    <Code block>
                        {Object.keys(response).length === 0 ? (
                            <div>Empty response</div>
                        ) : (
                            <pre>{JSON.stringify(response, null, 2)}</pre>
                        )}
                    </Code>
                </>
            )}
        </div>
    );
}

export default RpcEndpoints;
