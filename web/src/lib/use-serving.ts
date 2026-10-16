"use client";

import { useEffect, useState } from "react";
import type { z } from "zod";

export type Loaded<T> =
  | { state: "loading" }
  | { state: "error"; message: string }
  | { state: "ready"; data: T };

const requests = new Map<string, Promise<unknown>>();

/**
 * Client-side loader for the large exports (network nodes, flows, paths), fetched when a portal opens rather than
 * embedded in the page. One request per file per session; the result is validated with the same zod contract.
 */
export function useServing<T extends z.ZodType>(file: string, schema: T): Loaded<z.infer<T>> {
  const [result, setResult] = useState<Loaded<z.infer<T>>>({ state: "loading" });

  useEffect(() => {
    let alive = true;
    const request =
      requests.get(file) ??
      fetch(`/data/${file}`).then((response) => {
        if (!response.ok) throw new Error(`${file}: HTTP ${response.status}`);
        return response.json();
      });
    requests.set(file, request);
    request
      .then((json) => {
        if (!alive) return;
        const parsed = schema.safeParse(json);
        setResult(
          parsed.success
            ? { state: "ready", data: parsed.data }
            : { state: "error", message: `${file} does not match its contract` },
        );
      })
      .catch((error: unknown) => {
        requests.delete(file);
        if (!alive) return;
        const offline = typeof navigator !== "undefined" && !navigator.onLine;
        setResult({
          state: "error",
          message: offline ? "You are offline, and this view has not been loaded yet." : String(error),
        });
      });
    return () => {
      alive = false;
    };
  }, [file, schema]);

  return result;
}
