import { parseLogLine } from 'SnubaAdmin/tracing/util';

const COLOR_HOST = '#00ffff';
const COLOR_THREAD_ID = '#6666ff';
const COLOR_QUERYID = '#ff66ff';
const COLOR_DEBUG = '#ffff00';
const COLOR_TRACE = '#ff6666';
const COLOR_MESSAGE = '#ffffff';

function format_trace_log(log: string, width: number = 140): string {
    if (!log?.trim()) {
        return '<span class="text-muted">No trace logs available</span>';
    }

    const output: string[] = [];
    const lines = log.split(/\n/);

    for (const line of lines) {
        if (!line.trim()) {
            continue;
        }

        const parsedLine = parseLogLine(line);
        if (!parsedLine) {
            continue;
        }

        const { host, pid, query_id, log_level, message } = parsedLine;
        const levelColor = log_level === 'Debug' ? COLOR_DEBUG : COLOR_TRACE;

        const header = [
            `<span style="color: ${COLOR_HOST}">[${host}]</span>`,
            `<span style="color: ${COLOR_THREAD_ID}">[${pid}]</span>`,
            `<span style="color: ${COLOR_QUERYID}">{${query_id}}</span>`,
            `<span style="color: ${levelColor}">&lt;${log_level}&gt;</span>`,
        ].join(' ');

        const wrappedMessage = wrapText(
            `<span style="color: ${COLOR_MESSAGE}; display: inline-block; font-weight: bold" class="trace-message">${escapeHtml(message)}</span>`,
            width,
            4
        );

        output.push(`${header}\n${wrappedMessage}`);
    }

    return output.join('\n\n');
}

function wrapText(text: string, width: number, indent: number = 0): string {
    const words = text.split(' ');
    const lines: string[] = [];
    let currentLine = '';
    const indentStr = ' '.repeat(indent);

    for (const word of words) {
        if (stripHtml(currentLine + ' ' + word).length > width) {
            lines.push(currentLine);
            currentLine = indentStr + word;
        } else {
            currentLine += (currentLine ? ' ' : '') + word;
        }
    }

    if (currentLine) {
        lines.push(currentLine);
    }

    return lines.join('\n');
}

function stripHtml(html: string): string {
    return html.replace(/<[^>]*>/g, '');
}

function escapeHtml(text: string): string {
    const htmlEntities: Record<string, string> = {
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#39;'
    };

    return text.replace(/[&<>"']/g, char => htmlEntities[char]);
}

export { format_trace_log, stripHtml, escapeHtml, wrapText };
