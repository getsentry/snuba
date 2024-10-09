import React, { useState, useCallback, useEffect } from 'react';
import { Select, TextInput, Button, Code, Space, Textarea, Accordion, createStyles } from '@mantine/core';
import useApi from '../api_client';


function RpcEndpoints() {
    const api = useApi();
    const [endpoints, setEndpoints] = useState<string[]>([]);
    const [selectedEndpoint, setSelectedEndpoint] = useState<string | null>(null);
    const [requestBody, setRequestBody] = useState('');
    const [response, setResponse] = useState<any | null>(null);
    const exampleRequestTemplates = require('./exampleRequestTemplates.json');
    const [accordionOpened, setAccordionOpened] = useState(false);

    const fetchEndpoints = useCallback(async () => {
        if (endpoints.length === 0) {
            const fetchedEndpoints = await api.getRpcEndpoints();
            setEndpoints(fetchedEndpoints);
        }
    }, [api, endpoints]);

    useEffect(() => {
        if (endpoints.length === 0) {
            fetchEndpoints();
        }
    }, [endpoints, fetchEndpoints]);

    const handleEndpointSelect = (value: string | null) => {
        setSelectedEndpoint(value);
    };

    const handleExecute = async () => {
        if (!selectedEndpoint) return;
        try {
            const parsedBody = JSON.parse(requestBody);
            const result = await api.executeRpcEndpoint(selectedEndpoint, parsedBody);
            setResponse(result);
        } catch (error: any) {
            console.error('Error details:', error.message);
            setResponse({ tags: [] });
        }
    };

    const useStyles = createStyles((theme) => ({
        accordion: {
            '& .mantine-Accordion-item': {
                padding: '2px',
                margin: '0',
            },
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
            '& .mantine-Accordion-content': {
                padding: '2px 4px',
                fontSize: theme.fontSizes.xs,
                lineHeight: 1.2,
            },
        },
        code: {
            fontSize: theme.fontSizes.xxs || '10px',
        },
    }));




    return (
        <div>
            <h2>RPC Endpoints</h2>
            <Select
                label="Select an endpoint"
                placeholder="Choose an endpoint"
                data={endpoints.map(endpoint => ({ value: endpoint, label: endpoint }))}
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
