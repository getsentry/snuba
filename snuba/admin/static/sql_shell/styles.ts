import { createStyles } from "@mantine/core";

export const useShellStyles = createStyles((theme) => ({
  shellContainer: {
    height: "100%",
    minHeight: 0,
    display: "flex",
    flexDirection: "column",
    backgroundColor: theme.colors.dark[9],
    borderRadius: theme.radius.sm,
    fontFamily:
      '"JetBrains Mono", "Fira Code", "Source Code Pro", "IBM Plex Mono", "Roboto Mono", "Cascadia Code", Consolas, Monaco, "Courier New", monospace',
    overflow: "hidden",
  },
  shellContainerSudo: {
    height: "100%",
    minHeight: 0,
    display: "flex",
    flexDirection: "column",
    backgroundColor: theme.colors.dark[9],
    borderRadius: theme.radius.sm,
    fontFamily:
      '"JetBrains Mono", "Fira Code", "Source Code Pro", "IBM Plex Mono", "Roboto Mono", "Cascadia Code", Consolas, Monaco, "Courier New", monospace',
    overflow: "hidden",
    border: "3px solid #ff4444",
  },
  outputArea: {
    flex: 1,
    overflowY: "auto",
    padding: theme.spacing.md,
    fontSize: "14px",
    color: "#ffffff",
  },
  inputArea: {
    borderTop: `1px solid ${theme.colors.dark[6]}`,
    padding: theme.spacing.sm,
    display: "flex",
    alignItems: "center",
  },
  prompt: {
    color: "#00ff00",
    marginRight: theme.spacing.xs,
    fontSize: "14px",
    fontWeight: 600,
  },
  input: {
    flex: 1,
    backgroundColor: "transparent",
    border: "none",
    color: "#ffffff",
    fontFamily: "inherit",
    fontSize: "14px",
    outline: "none",
    caretColor: "#00ff00",
    "&::placeholder": {
      color: theme.colors.dark[4],
    },
  },
  statusBar: {
    borderTop: `1px solid ${theme.colors.dark[6]}`,
    padding: `${theme.spacing.xs}px ${theme.spacing.sm}px`,
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
    fontSize: "12px",
    color: theme.colors.dark[2],
    backgroundColor: theme.colors.dark[8],
  },
  statusItem: {
    display: "flex",
    alignItems: "center",
    gap: "4px",
  },
  statusActive: {
    color: "#00ff00",
  },
  statusInactive: {
    color: theme.colors.dark[4],
  },
  statusWarn: {
    color: "#ff4444",
    fontWeight: "bold" as const,
  },
  commandLine: {
    marginBottom: "8px",
  },
  promptDisplay: {
    color: "#00ff00",
    marginRight: "8px",
  },
  commandText: {
    color: "#ffffff",
  },
  infoText: {
    color: "#00ffff",
  },
  errorText: {
    color: "#ff6666",
  },
  resultBox: {
    backgroundColor: theme.colors.dark[8],
    borderRadius: theme.radius.sm,
    padding: theme.spacing.sm,
    marginTop: "8px",
    marginBottom: "8px",
    border: `1px solid ${theme.colors.dark[6]}`,
  },
  resultHeader: {
    color: "#888888",
    fontSize: "12px",
    marginBottom: "8px",
    borderBottom: `1px solid ${theme.colors.dark[6]}`,
    paddingBottom: "4px",
  },
  traceBox: {
    backgroundColor: theme.colors.dark[8],
    borderRadius: theme.radius.sm,
    padding: theme.spacing.sm,
    marginTop: "8px",
    marginBottom: "8px",
    border: `1px solid ${theme.colors.dark[6]}`,
    maxHeight: "300px",
    overflowY: "auto",
  },
  storageList: {
    color: "#ffffff",
    listStyleType: "none",
    padding: 0,
    margin: 0,
  },
  storageItem: {
    padding: "2px 0",
    "&:before": {
      content: '"  - "',
      color: "#888888",
    },
  },
  helpCommand: {
    color: "#ffff00",
    minWidth: "200px",
    display: "inline-block",
  },
  helpDescription: {
    color: "#888888",
  },
  tableContainer: {
    overflowX: "auto",
    marginTop: "8px",
  },
  resultTable: {
    borderCollapse: "collapse",
    width: "100%",
    fontSize: "13px",
    "& th": {
      backgroundColor: theme.colors.dark[7],
      color: "#00ffff",
      padding: "6px 12px",
      textAlign: "left",
      borderBottom: `1px solid ${theme.colors.dark[5]}`,
      fontWeight: 600,
    },
    "& td": {
      padding: "4px 12px",
      borderBottom: `1px solid ${theme.colors.dark[7]}`,
      color: "#ffffff",
    },
    "& tr:hover td": {
      backgroundColor: theme.colors.dark[7],
    },
  },
  nodeHeader: {
    color: "#00ffff",
    fontWeight: 600,
    marginTop: "8px",
    marginBottom: "4px",
  },
  summaryText: {
    color: "#cccccc",
    marginLeft: "16px",
  },
  executingIndicator: {
    color: "#ffff00",
    animation: "blink 1s infinite",
  },
}));
