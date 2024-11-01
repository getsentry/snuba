import { escapeHtml, format_trace_log, stripHtml, wrapText } from 'SnubaAdmin/rpc_endpoints/trace_formatter';

describe('trace_formatter', () => {
  describe('format_trace_log', () => {
    it('returns message for empty input', () => {
      expect(format_trace_log('')).toBe('<span class="text-muted">No trace logs available</span>');
      expect(format_trace_log('   ')).toBe('<span class="text-muted">No trace logs available</span>');
    });

    it('formats a single log line correctly', () => {
      const input = '[ snuba-events-analytics-platform-z3-3-1 ] [ 1087365 ] {2a1a1deb-fa59-4b07-918d-1b7558e50fd5} <Trace> default.eap_spans_local (13c49dff-2011-4143-b8c9-3c5f732799e7) (SelectExecutor): Used generic exclusion search over index for part 20241007_444453_445838_6 with 507 steps';
      const expected = [
        '<span style="color: #00ffff">[snuba-events-analytics-platform-z3-3-1]</span>',
        '<span style="color: #6666ff">[1087365]</span>',
        '<span style="color: #ff66ff">{2a1a1deb-fa59-4b07-918d-1b7558e50fd5}</span>',
        '<span style="color: #ff6666">&lt;Trace&gt;</span>'
      ].join(' ') + '\n' +
        '<span style="color: #ffffff; display: inline-block; font-weight: bold" class="trace-message">Used generic exclusion search over index for part 20241007_444453_445838_6 with 507 steps</span>';

      expect(format_trace_log(input)).toBe(expected);
    });

    it('handles multiple log lines', () => {
      const input = [
        '[ snuba-events-analytics-platform-z3-3-1 ] [ 1087365 ] {2a1a1deb-fa59-4b07-918d-1b7558e50fd5} <Trace> default.eap_spans_local (13c49dff-2011-4143-b8c9-3c5f732799e7) (SelectExecutor): First message',
        '[ snuba-events-analytics-platform-z3-3-1 ] [ 1786792 ] {2a1a1deb-fa59-4b07-918d-1b7558e50fd5} <Trace> default.eap_spans_local (13c49dff-2011-4143-b8c9-3c5f732799e7) (SelectExecutor): Second message'
      ].join('\n');

      const result = format_trace_log(input);
      expect(result).toContain('[1087365]');
      expect(result).toContain('[1786792]');
      expect(result).toContain('First message');
      expect(result).toContain('Second message');
      expect(result).toContain('#ff6666');
    });

    it('respects width parameter for message wrapping', () => {
      const input = '[ snuba-events-analytics-platform-z3-3-1 ] [ 1087365 ] {2a1a1deb-fa59-4b07-918d-1b7558e50fd5} <Trace> default.eap_spans_local (13c49dff-2011-4143-b8c9-3c5f732799e7) (SelectExecutor): ' + 'very long message '.repeat(10);
      const result = format_trace_log(input, 40);
      expect(result.split('\n').length).toBeGreaterThan(1);
    });
  });

  describe('stripHtml', () => {
    it('removes HTML tags', () => {
      const input = '<span>test</span> <div>content</div>';
      expect(stripHtml(input)).toBe('test content');
    });
  });

  describe('escapeHtml', () => {
    it('escapes special characters', () => {
      const input = '< > & " \'';
      expect(escapeHtml(input)).toBe('&lt; &gt; &amp; &quot; &#39;');
    });
  });

  describe('wrapText', () => {
    it('wraps text at specified width', () => {
      const input = '<span>This is a long text that should wrap</span>';
      const result = wrapText(input, 20, 2);
      const lines = result.split('\n');
      expect(lines.length).toBeGreaterThan(1);
      expect(stripHtml(lines[1]).startsWith('  ')).toBe(true);
    });
  });
});
