import { ParsedCommand, ShellMode } from "./types";

/**
 * Command definition for the shell parser.
 * Each command specifies a pattern to match and how to parse it.
 */
interface CommandDefinition {
  /** Regex pattern to match the command (case-insensitive) */
  pattern: RegExp;
  /** Parse the matched input and return a ParsedCommand */
  parse: (match: RegExpMatchArray, input: string) => ParsedCommand;
  /** Which modes this command is available in (default: both) */
  modes?: ShellMode[];
}

/**
 * Registry of all shell commands.
 * Commands are matched in order, so more specific patterns should come first.
 * Add new commands here to extend the shell.
 */
const COMMANDS: CommandDefinition[] = [
  // USE <storage> - Select a storage
  {
    pattern: /^USE\s+(.+)$/i,
    parse: (match) => ({
      type: "use",
      storage: match[1].trim(),
    }),
  },

  // HOST <host:port> - Select target host (system mode only)
  {
    pattern: /^HOST\s+(.+)$/i,
    parse: (match) => {
      const hostStr = match[1].trim();
      const hostMatch = hostStr.match(/^([^:]+):(\d+)$/);
      if (hostMatch) {
        return {
          type: "host",
          host: hostMatch[1],
          port: parseInt(hostMatch[2], 10),
        };
      }
      // Default port if not specified
      return { type: "host", host: hostStr, port: 9000 };
    },
    modes: ["system"],
  },

  // SHOW STORAGES - List available storages
  {
    pattern: /^SHOW\s+STORAGES$/i,
    parse: () => ({ type: "show_storages" }),
  },

  // SHOW HOSTS - List available hosts (system mode only)
  {
    pattern: /^SHOW\s+HOSTS$/i,
    parse: () => ({ type: "show_hosts" }),
    modes: ["system"],
  },

  // PROFILE ON/OFF - Toggle profile event collection (tracing mode only)
  {
    pattern: /^PROFILE\s+(ON|OFF)$/i,
    parse: (match) => ({
      type: "profile",
      enabled: match[1].toUpperCase() === "ON",
    }),
    modes: ["tracing"],
  },

  // TRACE RAW/FORMATTED - Toggle trace output format (tracing mode only)
  {
    pattern: /^TRACE\s+(RAW|FORMATTED)$/i,
    parse: (match) => ({
      type: "trace_mode",
      formatted: match[1].toUpperCase() === "FORMATTED",
    }),
    modes: ["tracing"],
  },

  // SUDO ON/OFF - Toggle sudo mode (system mode only)
  {
    pattern: /^SUDO\s+(ON|OFF)$/i,
    parse: (match) => ({
      type: "sudo",
      enabled: match[1].toUpperCase() === "ON",
    }),
    modes: ["system"],
  },

  // HELP - Show help message
  {
    pattern: /^HELP$/i,
    parse: () => ({ type: "help" }),
  },

  // CLEAR - Clear terminal output
  {
    pattern: /^CLEAR$/i,
    parse: () => ({ type: "clear" }),
  },
];

/**
 * Parse user input into a structured command.
 * Commands are matched against the registry in order.
 * If no command matches, the input is treated as a SQL query.
 *
 * @param input - The raw user input
 * @param mode - The current shell mode (tracing or system)
 * @returns The parsed command
 */
export function parseCommand(input: string, mode: ShellMode): ParsedCommand {
  const trimmed = input.trim();

  for (const cmd of COMMANDS) {
    // Skip commands not available in current mode
    if (cmd.modes && !cmd.modes.includes(mode)) {
      continue;
    }

    const match = trimmed.match(cmd.pattern);
    if (match) {
      return cmd.parse(match, trimmed);
    }
  }

  // Default: treat as SQL query
  return { type: "sql", query: trimmed };
}

/**
 * Get list of commands available in a given mode.
 * Useful for generating help text or autocomplete suggestions.
 *
 * @param mode - The shell mode
 * @returns Array of pattern strings for available commands
 */
export function getAvailableCommands(mode: ShellMode): string[] {
  return COMMANDS
    .filter((cmd) => !cmd.modes || cmd.modes.includes(mode))
    .map((cmd) => cmd.pattern.source.replace(/\^|\$|\\/g, "").replace(/\(\.\+\)/g, "<arg>"));
}
