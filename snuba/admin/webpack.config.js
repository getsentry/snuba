const path = require('path');
const webpack = require('webpack');


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
    plugins: [
        new webpack.DefinePlugin({
            'process.env': {
                ADMIN_HOST: JSON.stringify(env.ADMIN_HOST || "0.0.0.0"),
                ADMIN_PORT: JSON.stringify(env.ADMIN_PORT || "1219"),
            },
        }),
    ]
})
