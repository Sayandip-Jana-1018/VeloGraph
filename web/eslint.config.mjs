import { defineConfig, globalIgnores } from "eslint/config";
import nextVitals from "eslint-config-next/core-web-vitals";
import nextTs from "eslint-config-next/typescript";

const eslintConfig = defineConfig([
  ...nextVitals,
  ...nextTs,
  // Override default ignores of eslint-config-next.
  globalIgnores([
    // Default ignores of eslint-config-next:
    ".next/**",
    "out/**",
    "build/**",
    "next-env.d.ts",
    // Copied in by `pnpm sync`: serving data, screenshots and MapLibre's prebuilt worker, none of it our source.
    "public/data/**",
    "public/spark-ui/**",
    "public/maplibre/**",
    "src/generated/**",
  ]),
]);

export default eslintConfig;
