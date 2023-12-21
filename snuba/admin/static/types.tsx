type AllowedTools = {
  tools: string[];
};

type Settings = {
  dsn: string;
  tracesSampleRate: number;
  profilesSampleRate: number;
  tracePropagationTargets: string[] | null;
  replaysSessionSampleRate: number;
  replaysOnErrorSampleRate: number;
  userEmail: string;
};

export { AllowedTools, Settings };
