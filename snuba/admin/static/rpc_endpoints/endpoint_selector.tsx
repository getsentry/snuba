import React from 'react';
import { Select } from '@mantine/core';
import { EndpointSelectorProps } from 'SnubaAdmin/rpc_endpoints/types';

export const EndpointSelector = ({
  endpoints,
  selectedEndpoint,
  handleEndpointSelect
}: EndpointSelectorProps) => (
  <>
    <h2>RPC Endpoints</h2>
    <Select
      label="Select an endpoint"
      placeholder="Choose an endpoint"
      data={endpoints.map(endpoint => ({
        value: endpoint.name,
        label: `${endpoint.name} (${endpoint.version})`
      }))}
      value={selectedEndpoint}
      onChange={handleEndpointSelect}
      style={{ width: '100%', marginBottom: '1rem' }}
    />
  </>
);
