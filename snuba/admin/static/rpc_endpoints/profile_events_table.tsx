import React from 'react';
import { Stack, Title, Table } from '@mantine/core';
import { ProfileEventsTableProps } from 'SnubaAdmin/rpc_endpoints/types';

export const ProfileEventsTable = ({ profileEvents, classes }: ProfileEventsTableProps) => (
  <div className={classes.scrollableDataContainer}>
    <Stack spacing="md">
      {profileEvents.map(({ hostName, events }) => (
        <div key={hostName}>
          <Title order={4}>Host: {hostName}</Title>
          <Table>
            <thead>
              <tr>
                <th>Event Name</th>
                <th>Count</th>
              </tr>
            </thead>
            <tbody>
              {events.map(event => (
                <tr key={event.name}>
                  <td>{event.name}</td>
                  <td>{event.count}</td>
                </tr>
              ))}
            </tbody>
          </Table>
        </div>
      ))}
    </Stack>
  </div>
);
