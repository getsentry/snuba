const path = require('path');
const { sentryWebpackPlugin } = require("@sentry/webpack-plugin");

module.exports = (env) => ({
    entry: './static/index.tsx',
    module: {
        rules: [
            {
                test: /\.tsx?$/,
                use: 'ts-loader',
                exclude: /node_modules/,
            },
        ],
    },
    resolve: {
        extensions: ['.tsx', '.ts', '.js'],
    },
    output: {
        filename: 'bundle.js',
        path: path.resolve(__dirname, 'dist'),
    },
    devtool: "source-map", // Source map generation must be turned on
    plugins: [
        sentryWebpackPlugin({
            org: process.env.SENTRY_ORGANIZATION,
            project: process.env.SENTRY_PROJECT,
            // Auth tokens can be obtained from https://sentry.io/settings/account/api/auth-tokens/
            // and need `project:releases` and `org:read` scopes
            authToken: process.env.SENTRY_AUTH_TOKEN,
        }),
    ],
})
