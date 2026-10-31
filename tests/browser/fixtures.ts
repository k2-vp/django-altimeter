import { test as base, expect } from "@playwright/test";
import { spawn } from "node:child_process";
import { createInterface } from "node:readline";

export interface Snapshot {
  events: { event: string; properties: Record<string, unknown> }[];
  foreignRequests: { method: string; headers: Record<string, string> }[];
  requests: { method: string; path: string; query: string; headers: Record<string, string> }[];
  spans: { traceId: string; spanId: string; parentSpanId: string; kind: number; name: string; durationMs: number; attributes: Record<string, unknown> }[];
  wire: string[];
}
export interface Reference {
  frontendOrigin: string;
  restrictedOrigin: string;
  apiOrigin: string;
  controlOrigin: string;
  wheelSha256: string;
  snapshot(): Promise<Snapshot>;
  reset(): Promise<void>;
}

export const test = base.extend<{}, { reference: Reference }>({
  reference: [async ({}, use) => {
    const child = spawn("bun", ["server.ts"], { cwd: import.meta.dirname, stdio: ["ignore", "pipe", "inherit"] });
    const lines = createInterface({ input: child.stdout });
    try {
      const ready = await new Promise<Omit<Reference, "snapshot" | "reset">>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error("Browser stack startup exceeded 180 seconds")), 180_000);
        child.once("error", reject);
        child.once("exit", code => { clearTimeout(timer); reject(new Error(`Browser stack exited ${code}`)); });
        lines.on("line", line => {
          if (line.startsWith("BROWSER_READY ")) {
            clearTimeout(timer);
            resolve(JSON.parse(line.slice(14)));
          } else console.log(line);
        });
      });
      await use({ ...ready,
        async snapshot() { const response = await fetch(`${ready.controlOrigin}/snapshot`); expect(response.ok).toBe(true); return await response.json() as Snapshot; },
        async reset() { const response = await fetch(`${ready.controlOrigin}/reset`, { method: "POST" }); expect(response.ok).toBe(true); },
      });
    } finally {
      child.kill("SIGTERM");
      await new Promise<void>((resolve, reject) => {
        if (child.exitCode !== null) { resolve(); return; }
        const timer = setTimeout(() => { child.kill("SIGKILL"); reject(new Error("Browser stack failed to stop")); }, 12_000);
        child.once("exit", code => { clearTimeout(timer); code === 0 ? resolve() : reject(new Error(`Browser stack shutdown ${code}`)); });
      });
      lines.close();
    }
  }, { scope: "worker", timeout: 200_000 }],
});
export { expect };
