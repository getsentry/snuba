import * as esbuild from 'esbuild';
import { sentryEsbuildPlugin } from '@sentry/esbuild-plugin';

await esbuild.build({
  entryPoints: ['./static/index.tsx'],
  bundle: true,
  outfile: 'dist/bundle.js',
  sourcemap: true, // Source map generation must be turned on
  plugins: [
    // Put the Sentry esbuild plugin after all other plugins
    sentryEsbuildPlugin({
      org: process.env.SENTRY_ORGANIZATION,
      project: process.env.SENTRY_PROJECT,

      // Auth tokens can be obtained from https://sentry.io/orgredirect/organizations/:orgslug/settings/auth-tokens/
      authToken: process.env.SENTRY_AUTH_TOKEN,
      telemetry: false,
    }),
  ],
});
