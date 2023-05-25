const SETTINGS = {
  sentryIntegration: {
    // Performance Monitoring
    tracesSampleRate: 1.0, // Capture 100% of the transactions, reduce in production!
    // Session Replay
    replaysSessionSampleRate: 0.1, // This sets the sample rate at 10%
    replaysOnErrorSampleRate: 1.0,
  },
};

export { SETTINGS };
