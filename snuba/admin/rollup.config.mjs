import nodeResolve from "@rollup/plugin-node-resolve";
import resolve from "@rollup/plugin-node-resolve";
import typescript from "@rollup/plugin-typescript";
import replace from "@rollup/plugin-replace";
import commonjs from "@rollup/plugin-commonjs";
import babel from "@rollup/plugin-babel";
import { sentryRollupPlugin } from "@sentry/rollup-plugin";
// import { terser } from "@rollup/plugin-terser";

const RESOLVABLE_EXTENSIONS = [".js", ".jsx", ".ts", ".tsx"];

// `npm run build` -> `production` is true
// `npm run dev` -> `production` is false
const production = !process.env.ROLLUP_WATCH;

export default {
  input: "./static/index.tsx",
  output: {
    file: "./dist/bundle.js",
    sourcemap: true,
  },
  plugins: [
    replace({
      "process.env.NODE_ENV": JSON.stringify("production"),
    }),
    nodeResolve(),
    resolve(),
    typescript({ compilerOptions: { jsx: "preserve" } }),
    sentryRollupPlugin({}),
    babel({
      babelHelpers: "bundled",
      presets: ["@babel/preset-react"],
      extensions: RESOLVABLE_EXTENSIONS,
    }),
    commonjs(),
    // production && terser(),
  ],
};
