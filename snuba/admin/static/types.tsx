type AllowedTools = {
  tools: string[];
};

type Settings = {
  dsn: string;
  tracesSampleRate: number;
  replaysSessionSampleRate: number;
  replaysOnErrorSampleRate: number;
};

export { AllowedTools, Settings };
