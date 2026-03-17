/**
 * Build script: bundles src/index.ts + @modelcontextprotocol/sdk into
 * a single dist/index.js that OpenClaw's jiti loader can consume.
 */
import { build } from "esbuild";

await build({
  entryPoints: ["src/index.ts"],
  bundle: true,
  outfile: "dist/index.js",
  platform: "node",
  target: "node22",
  format: "esm",
  // Keep node builtins external — they're available at runtime
  external: [
    "child_process",
    "node:child_process",
    "node:path",
    "node:fs",
    "node:*",
    "fs",
    "path",
    "events",
    "stream",
    "util",
    "net",
    "tls",
    "http",
    "https",
    "os",
    "crypto",
    "buffer",
    "url",
    "string_decoder",
    "querystring",
    "zlib",
    "assert",
    "tty",
    "worker_threads",
    "perf_hooks",
    "async_hooks",
    "diagnostics_channel",
  ],
  // Banner for source identification
  banner: {
    js: "// keep — OpenClaw context engine plugin (built)",
  },
  minify: false, // Keep readable for debugging
  sourcemap: false,
});

console.log("Built dist/index.js");
