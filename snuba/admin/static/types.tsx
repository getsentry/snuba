type AllowedTools = {
  tools: string[];
};

type Settings = {
  dsn: string;
  tracesSampleRate: number;
  profilesSampleRate: number;
  replaysSessionSampleRate: number;
  replaysOnErrorSampleRate: number;
  userEmail: string;
};

export { AllowedTools, Settings };
