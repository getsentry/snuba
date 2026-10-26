type AllowedTools = {
  tools: string[];
};

type AdminRegion = {
  name: string;
  url: string;
  is_main: boolean;
};

type Settings = {
  dsn: string;
  tracesSampleRate: number;
  profileSessionSampleRate: number;
  tracePropagationTargets: string[] | null;
  replaysSessionSampleRate: number;
  replaysOnErrorSampleRate: number;
  userEmail: string;
};

export { AdminRegion, AllowedTools, Settings };
