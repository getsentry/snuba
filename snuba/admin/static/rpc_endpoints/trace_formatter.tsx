import React from 'react';
import { parseLogLine } from 'SnubaAdmin/tracing/util';
import { StyledSpanProps, TraceLogProps } from 'SnubaAdmin/rpc_endpoints/types';
import { useStyles } from 'SnubaAdmin/rpc_endpoints/styles';

const COLOR_HOST = '#00ffff';
const COLOR_THREAD_ID = '#6666ff';
const COLOR_QUERYID = '#ff66ff';
const COLOR_DEBUG = '#ffff00';
const COLOR_TRACE = '#ff6666';
const COLOR_MESSAGE = '#ffffff';

const StyledSpan: React.FC<StyledSpanProps> = ({ color, children, className, style }) => (
    <span style={{ color, ...style }} className={className}>
        {children}
    </span>
);

export const TraceLog: React.FC<TraceLogProps> = ({ log }) => {
    const { classes } = useStyles();

    if (!log?.trim()) {
        return <span className="text-muted">No trace logs available</span>;
    }

    const lines = log.split(/\n/);
    const formattedLines = lines.map((line, index) => {
        if (!line.trim()) {
            return null;
        }

        const parsedLine = parseLogLine(line);
        if (!parsedLine) {
            return null;
        }

        const { host, pid, query_id, log_level, message, component } = parsedLine;
        const levelColor = log_level === 'Debug' ? COLOR_DEBUG : COLOR_TRACE;

        const header = (
            <div key={`header-${index}`}>
                <StyledSpan color={COLOR_HOST}>[{host}]</StyledSpan>{' '}
                <StyledSpan color={COLOR_THREAD_ID}>[{pid}]</StyledSpan>{' '}
                <StyledSpan color={COLOR_QUERYID}>{'{' + query_id + '}'}</StyledSpan>{' '}
                <StyledSpan color={levelColor}>&lt;{log_level}&gt;</StyledSpan>{' '}
                <StyledSpan color={COLOR_MESSAGE}>{component}:</StyledSpan>
            </div>
        );

        const messageComponent = (
            <StyledSpan color="#ffffff" className="trace-message">
                {message}
            </StyledSpan>
        );

        return (
            <div key={index} style={{ marginBottom: '1em' }}>
                {header}
                {messageComponent}
            </div>
        );
    });

    return <div className={classes.traceLogsContainer}>{formattedLines}</div>;
};
