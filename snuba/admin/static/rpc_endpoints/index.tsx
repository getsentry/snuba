import React, { useState, useCallback, useEffect } from 'react';
import { Space } from '@mantine/core';
import useApi from 'SnubaAdmin/api_client';
import { EndpointSelector } from 'SnubaAdmin/rpc_endpoints/endpoint_selector';
import { RequestInput } from 'SnubaAdmin/rpc_endpoints/request_input';
import { ResponseDisplay } from 'SnubaAdmin/rpc_endpoints/response_display';
import { useStyles } from 'SnubaAdmin/rpc_endpoints/styles';
import { fetchEndpointsList, executeEndpoint, getEndpointData, processTraceResults } from 'SnubaAdmin/rpc_endpoints/utils';
import { TracingSummary, HostProfileEvents } from 'SnubaAdmin/rpc_endpoints/types';

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
  const [showSummarizedView, setShowSummarizedView] = useState(false);
  const [showProfileEvents, setShowProfileEvents] = useState(false);
  const [summarizedTraceOutput, setSummarizedTraceOutput] = useState<TracingSummary | null>(null);
  const [profileEvents, setProfileEvents] = useState<HostProfileEvents[] | null>(null);

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
    setShowTraceLogs(false);
    setShowSummarizedView(false);
    setShowProfileEvents(false);
    try {
      const result = await executeEndpoint(
        api,
        selectedEndpoint,
        selectedVersion,
        requestBody,
        debugMode
      );
      setResponse(result);
      await processTraceResults(result, api, setProfileEvents, setSummarizedTraceOutput);
      setIsLoading(false);
    } catch (error: any) {
      alert(`Error: ${error.message}`);
      setResponse({ error: error.message });
      setIsLoading(false);
    }
  };

  return (
    <div>
      <EndpointSelector
        endpoints={endpoints}
        selectedEndpoint={selectedEndpoint}
        handleEndpointSelect={handleEndpointSelect}
      />
      <Space h="md" />
      <RequestInput
        selectedEndpoint={selectedEndpoint}
        selectedVersion={selectedVersion}
        exampleRequestTemplates={exampleRequestTemplates}
        requestBody={requestBody}
        setRequestBody={setRequestBody}
        debugMode={debugMode}
        setDebugMode={setDebugMode}
        isLoading={isLoading}
        handleExecute={handleExecute}
        classes={classes}
      />
      {response && (
        <ResponseDisplay
          response={response}
          showTraceLogs={showTraceLogs}
          setShowTraceLogs={setShowTraceLogs}
          showSummarizedView={showSummarizedView}
          setShowSummarizedView={setShowSummarizedView}
          summarizedTraceOutput={summarizedTraceOutput}
          showProfileEvents={showProfileEvents}
          setShowProfileEvents={setShowProfileEvents}
          profileEvents={profileEvents}
          classes={classes}
        />
      )}
    </div>
  );
}

export default RpcEndpoints;
