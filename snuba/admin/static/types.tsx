type AllowedTools = {
  tools: string[];
};

type Settings = {
  dsn: string;
  traces_sample_rate: number;
};

export { AllowedTools, Settings };
