import React, { useState, useCallback, useEffect } from 'react';
import { Select, Button, Code, Space, Textarea, Accordion, createStyles, Loader, Checkbox, Text, Table, Switch } from '@mantine/core';
import useApi from 'SnubaAdmin/api_client';
import { TraceLog } from 'SnubaAdmin/rpc_endpoints/trace_formatter';
import { ExampleRequestAccordionProps, QueryInfo } from 'SnubaAdmin/rpc_endpoints/types';
import { useStyles } from 'SnubaAdmin/rpc_endpoints/styles';
import { fetchEndpointsList, executeEndpoint, getEndpointData } from 'SnubaAdmin/rpc_endpoints/utils';

const DEBUG_SUPPORTED_VERSIONS = ['v1'];

function ExampleRequestAccordion({
  selectedEndpoint,
  selectedVersion,
  exampleRequestTemplates,
  setRequestBody,
  classes,
}: ExampleRequestAccordionProps) {
  const [isOpened, setIsOpened] = useState(false);

  return (
    <Accordion
      classNames={{ item: classes.accordion }}
      variant="filled"
      radius="sm"
      value={isOpened ? 'example' : null}
      onChange={(value) => setIsOpened(value === 'example')}
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
              setIsOpened(false);
            }}
            style={{ marginTop: '1rem' }}
          >
            Copy to Request Body
          </Button>
        </Accordion.Panel>
      </Accordion.Item>
    </Accordion>
  );
}

function RpcEndpoints() {
  const api = useApi();
  const [endpoints, setEndpoints] = useState<Array<{ name: string, version: string }>>([]);
  const [selectedEndpoint, setSelectedEndpoint] = useState<string | null>(null);
  const [selectedVersion, setSelectedVersion] = useState<string | null>(null);
  const [requestBody, setRequestBody] = useState('');
  const [response, setResponse] = useState<any | null>(null);
  const exampleRequestTemplates: Record<string, Record<string, any>> = require('SnubaAdmin/rpc_endpoints/exampleRequestTemplates.json');
  const [isLoading, setIsLoading] = useState(false);
  const [debugMode, setDebugMode] = useState(false);
  const [showTraceLogs, setShowTraceLogs] = useState(false);

  const { classes } = useStyles();

  const fetchEndpoints = useCallback(async () => {
    const formattedEndpoints = await fetchEndpointsList(api);
    setEndpoints(formattedEndpoints);
  }, []);

  useEffect(() => {
    fetchEndpoints();
  }, []);

  const handleEndpointSelect = (value: string | null) => {
    setSelectedEndpoint(value);
    const selectedEndpointData = getEndpointData(endpoints, value || '');
    setSelectedVersion(selectedEndpointData?.version || null);
    setRequestBody('');
    setResponse(null);
    setDebugMode(false);
  };

  const handleExecute = async () => {
    setIsLoading(true);
    try {
      const result = await executeEndpoint(
        api,
        selectedEndpoint,
        selectedVersion,
        requestBody,
        debugMode
      );
      setResponse(result);
    } catch (error: any) {
      alert(`Error: ${error.message}`);
      setResponse({ error: error.message });
    } finally {
      setIsLoading(false);
    }
  };

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
      <ExampleRequestAccordion
        selectedEndpoint={selectedEndpoint}
        selectedVersion={selectedVersion}
        exampleRequestTemplates={exampleRequestTemplates}
        setRequestBody={setRequestBody}
        classes={classes}
      />
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
          <Switch
            className={classes.viewToggle}
            label="Show Trace Logs"
            checked={showTraceLogs}
            onChange={(event) => setShowTraceLogs(event.currentTarget.checked)}
          />
          <h4>Response Metadata:</h4>
          <Accordion classNames={{ item: classes.accordion }}>
            <Accordion.Item value="query-info">
              <Accordion.Control>
                <Text fw={700}>
                  {showTraceLogs ? 'Query Trace Logs' : 'Query Metadata'}
                </Text>
              </Accordion.Control>
              <Accordion.Panel>
                {response.meta?.queryInfo ? (
                  response.meta.queryInfo.map((queryInfo: QueryInfo, index: number) => (
                    <div key={index}>
                      {showTraceLogs ? (
                        <>
                          <h4>Trace Logs</h4>
                          {queryInfo.traceLogs ? (
                            <TraceLog log={queryInfo.traceLogs} />
                          ) : (
                            <Text>No trace logs available</Text>
                          )}
                        </>
                      ) : (
                        <>
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
                        </>
                      )}
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
          <div className={classes.responseDataContainer}>
            <Code block>
              <pre>
                {JSON.stringify(
                  (({ meta, ...rest }) => rest)(response),
                  null,
                  2
                )}
              </pre>
            </Code>
          </div>
        </>
      )}
    </div>
  );
}

export default RpcEndpoints;
