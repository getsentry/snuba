import { LogLine } from "SnubaAdmin/tracing/types";

const logLineMatcher =
  /\[ (?<hostname>\S+) \] \[ (?<pid>\d+) \] \{(?<local_query_id>[^}]+)\} <(?<log_level>[^>]+)> (?<component>[^:]+): (?<message>.*)/;

function parseLogLine(logLine: string): LogLine | null {
  const logLineRegexMatch = logLine.match(logLineMatcher);
  const host = logLineRegexMatch?.groups?.hostname;
  const pid = logLineRegexMatch?.groups?.pid;
  const local_query_id = logLineRegexMatch?.groups?.local_query_id;
  const log_level = logLineRegexMatch?.groups?.log_level;
  const message = logLineRegexMatch?.groups?.message;
  const component = logLineRegexMatch?.groups?.component;

  if (host && pid && local_query_id && log_level && message) {
    const logLineParsed: LogLine = {
      host: host,
      pid: pid!,
      query_id: local_query_id,
      log_level: log_level!,
      component: component!,
      message: message!,
    };
    return logLineParsed;
  } else {
    return null;
  }
}

export { parseLogLine };
