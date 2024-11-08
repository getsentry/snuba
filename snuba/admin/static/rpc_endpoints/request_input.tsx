import React from 'react';
import { Space, Textarea, Checkbox, Button, Loader } from '@mantine/core';
import { ExampleRequestAccordion } from 'SnubaAdmin/rpc_endpoints/example_request_accordion';
import { RequestInputProps } from 'SnubaAdmin/rpc_endpoints/types';
const DEBUG_SUPPORTED_VERSIONS = ['v1'];

export const RequestInput = ({
  selectedEndpoint,
  selectedVersion,
  exampleRequestTemplates,
  requestBody,
  setRequestBody,
  debugMode,
  setDebugMode,
  isLoading,
  handleExecute,
  classes
}: RequestInputProps) => (
  <>
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
      maxRows={15}
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
  </>
);
