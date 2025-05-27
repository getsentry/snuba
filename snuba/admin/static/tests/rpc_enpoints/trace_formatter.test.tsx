import React from 'react';
import { render } from '@testing-library/react';
import { TraceLog } from 'SnubaAdmin/rpc_endpoints/trace_formatter';

describe('TraceLog', () => {
  it('renders empty state message when log is empty', () => {
    const { queryByText } = render(React.createElement(TraceLog, { log: "" }));
    expect(queryByText('No trace logs available')).toBeTruthy();
  });

  it('renders formatted log lines correctly', () => {
    const sampleLog = '[ host1 ] [ 1087365 ] {2a1a1deb-fa59-4b07-918d-1b7558e50fd5} <Trace> component: Test message';
    const { queryByText } = render(React.createElement(TraceLog, { log: sampleLog }));

    expect(queryByText(/host1/)).toBeTruthy();
    expect(queryByText(/1087365/)).toBeTruthy();
    expect(queryByText(/2a1a1deb-fa59-4b07-918d-1b7558e50fd5/)).toBeTruthy();
    expect(queryByText(/component/)).toBeTruthy();
    expect(queryByText(/Test message/)).toBeTruthy();
  });

  it('handles multiple log lines', () => {
    const multipleLines = [
      '[ host1 ] [ 1111 ] {id-1} <Trace> first component: First message',
      '[ host2 ] [ 2222 ] {id-2} <Debug> second component: Second message'
    ].join('\n');

    const { queryByText } = render(React.createElement(TraceLog, { log: multipleLines }));

    expect(queryByText(/First message/)).toBeTruthy();
    expect(queryByText(/Second message/)).toBeTruthy();
    expect(queryByText(/first component/)).toBeTruthy();
    expect(queryByText(/second component/)).toBeTruthy();
    expect(queryByText(/host1/)).toBeTruthy();
    expect(queryByText(/host2/)).toBeTruthy();
  });

  it('handles invalid log lines', () => {
    const invalidLog = 'Invalid log line format\n[ host1 ] [ 1111 ] {id-1} <Trace> component: Valid message';
    const { queryByText } = render(React.createElement(TraceLog, { log: invalidLog }));

    expect(queryByText(/Invalid log line format/)).toBeFalsy();
    expect(queryByText(/Valid message/)).toBeTruthy();
  });
});
