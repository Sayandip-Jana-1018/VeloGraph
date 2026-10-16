import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // Loaders are 'use cache' functions, so every page prerenders from the static JSON at build time.
  cacheComponents: true,
  partialPrefetching: true,
  turbopack: {
    rules: {
      "*.css": {
        loaders: ["@tailwindcss/turbopack"],
        as: "*.css",
      },
    },
  },
};

export default nextConfig;
