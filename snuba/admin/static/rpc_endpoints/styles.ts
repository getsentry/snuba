import { createStyles } from '@mantine/core';

export const useStyles = createStyles((theme) => ({
  mainAccordion: {
    '& .mantine-Accordion-control': {
      backgroundColor: theme.colors.blue[1],
      color: theme.colors.blue[7],
      fontSize: theme.fontSizes.xs,
      padding: '2px 4px',
      lineHeight: 1.2,
      borderBottom: `1px solid ${theme.colors.gray[3]}`,
      cursor: 'pointer',
      fontWeight: 'bold',
      '&:hover': {
        backgroundColor: theme.colors.blue[2],
      },
    },
    '& .mantine-Accordion-item': {
      borderBottom: `1px solid ${theme.colors.gray[3]}`,
      '&[data-active]': {
        backgroundColor: theme.colors.blue[0],
      },
    },
  },
  traceAccordion: {
    '& .mantine-Accordion-control': {
      color: theme.colors.green[7],
      fontSize: '6px',
      padding: '1px 2px',
      lineHeight: 1,
      borderBottom: `1px solid ${theme.colors.gray[3]}`,
      cursor: 'pointer',
      '&:hover': {
        backgroundColor: theme.colors.green[2],
      },
    },
    '& .mantine-Accordion-item': {
      borderBottom: `1px solid ${theme.colors.gray[3]}`,
      '&[data-active]': {
        backgroundColor: theme.colors.green[0],
      },
    },
    '& .mantine-Accordion-chevron': {
      color: theme.colors.green[6],
    },
  },
  table: {
    border: `1px solid ${theme.colors.gray[3]}`,
    '& th, & td': {
      border: `1px solid ${theme.colors.gray[3]}`,
      padding: theme.spacing.xs,
    },
    '& th': {
      backgroundColor: theme.colors.gray[1],
      fontWeight: 'bold',
    },
    '& td:first-of-type': {
      width: '20%',
      fontWeight: 'bold',
    },
    '& td:last-of-type': {
      width: '80%',
    },
  },
  debugCheckbox: {
    marginBottom: theme.spacing.md,
  },
  traceLogsContainer: {
    maxHeight: '500px',
    overflowY: 'auto',
    backgroundColor: theme.colors.dark[9],
    padding: theme.spacing.md,
    borderRadius: theme.radius.sm,
    fontFamily: '"JetBrains Mono", "Fira Code", "Source Code Pro", "IBM Plex Mono", "Roboto Mono", "Cascadia Code", Consolas, Monaco, "Courier New", monospace',
    whiteSpace: 'pre-wrap',
    fontSize: '16px',
    '& span': {
      display: 'inline'
    },
    '& .trace-message': {
      color: '#ffffff',
    }
  },
  viewToggle: {
    marginBottom: theme.spacing.sm,
  },
  scrollableDataContainer: {
    maxHeight: '500px',
    overflowY: 'auto',
    backgroundColor: 'white',
    padding: theme.spacing.sm,
    borderRadius: theme.radius.sm,
    border: `1px solid ${theme.colors.gray[3]}`,
  },
  toggleOption: {
    fontSize: theme.fontSizes.sm,
    transition: 'all 0.2s ease',

    '&[data-active="true"]': {
      fontWeight: 700,
      color: theme.colors.blue[6]
    },

    '&[data-active="false"]': {
      fontWeight: 700,
      color: theme.colors.gray[6]
    }
  }
}));
