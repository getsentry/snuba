import React from 'react';
import { Select } from '@mantine/core';
import { EndpointSelectorProps } from 'SnubaAdmin/rpc_endpoints/types';

export const EndpointSelector = ({
  endpoints,
  selectedEndpoint,
  selectedVersion,
  handleEndpointSelect
}: EndpointSelectorProps) => (
  <>
    <h2>RPC Endpoints</h2>
    <Select
      label="Select an endpoint"
      placeholder="Choose an endpoint"
      data={endpoints.map(endpoint => ({
        value: `${endpoint.name}_${endpoint.version}`,
        label: `${endpoint.name} (${endpoint.version})`
      }))}
      value={selectedEndpoint + "_" + selectedVersion}
      onChange={handleEndpointSelect}
      style={{ width: '100%', marginBottom: '1rem' }}
    />
  </>
);
