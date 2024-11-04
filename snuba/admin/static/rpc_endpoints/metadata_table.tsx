import React from 'react';
import { Table, Code } from '@mantine/core';
import { MetadataTableProps } from 'SnubaAdmin/rpc_endpoints/types';

export const MetadataTable = ({ queryInfo, classes }: MetadataTableProps) => (
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
);
