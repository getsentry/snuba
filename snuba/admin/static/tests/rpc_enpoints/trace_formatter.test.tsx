import React from 'react';
import { render } from '@testing-library/react';
import '@testing-library/jest-dom';
import { TraceLog } from 'SnubaAdmin/rpc_endpoints/trace_formatter';

describe('TraceLog', () => {
  it('renders empty state message when log is empty', () => {
    const { getByText } = render(React.createElement(TraceLog, { log: "" }));
    expect(getByText('No trace logs available')).toBeInTheDocument();
  });

  it('renders formatted log lines correctly', () => {
    const sampleLog = '[ host1 ] [ 1087365 ] {2a1a1deb-fa59-4b07-918d-1b7558e50fd5} <Trace> component: Test message';
    const { getByText } = render(React.createElement(TraceLog, { log: sampleLog }));

    expect(getByText(/host1/)).toBeInTheDocument();
    expect(getByText(/1087365/)).toBeInTheDocument();
    expect(getByText(/2a1a1deb-fa59-4b07-918d-1b7558e50fd5/)).toBeInTheDocument();
    expect(getByText(/component/)).toBeInTheDocument();
    expect(getByText(/Test message/)).toBeInTheDocument();
  });

  it('handles multiple log lines', () => {
    const multipleLines = [
      '[ host1 ] [ 1111 ] {id-1} <Trace> first component: First message',
      '[ host2 ] [ 2222 ] {id-2} <Debug> second component: Second message'
    ].join('\n');

    const { getByText } = render(React.createElement(TraceLog, { log: multipleLines }));

    expect(getByText(/First message/)).toBeInTheDocument();
    expect(getByText(/Second message/)).toBeInTheDocument();
    expect(getByText(/first component/)).toBeInTheDocument();
    expect(getByText(/second component/)).toBeInTheDocument();
    expect(getByText(/host1/)).toBeInTheDocument();
    expect(getByText(/host2/)).toBeInTheDocument();
  });

  it('handles invalid log lines', () => {
    const invalidLog = 'Invalid log line format\n[ host1 ] [ 1111 ] {id-1} <Trace> component: Valid message';
    const { getByText, queryByText } = render(React.createElement(TraceLog, { log: invalidLog }));

    expect(queryByText(/Invalid log line format/)).not.toBeInTheDocument();
    expect(getByText(/Valid message/)).toBeInTheDocument();
  });
});
