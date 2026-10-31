import { resolve } from "node:path";

const root = resolve(import.meta.dir, "../..");
const events: unknown[] = [];
const foreignRequests: unknown[] = [];
let apiOrigin = "";
let controlOrigin = "";
let scripts = new Map<string, Blob>();
let stopping = false;
const SERVICE_WORKER = `
self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', event => event.waitUntil(self.clients.claim()));
self.addEventListener('fetch', event => {
  if (new URL(event.request.url).pathname === '/sw-data') {
    event.respondWith(Promise.resolve(new Response(JSON.stringify({items: []}), {
      headers: {'Content-Type': 'application/json', 'Cache-Control': 'no-store'},
    })));
  }
});
`;

async function serve(request: Request): Promise<Response> {
  const url = new URL(request.url);
  if (url.pathname === "/snapshot") {
    const response = await fetch(`${controlOrigin}/snapshot`);
    return Response.json({ ...await response.json(), events, foreignRequests });
  }
  if (url.pathname === "/reset" && request.method === "POST") {
    events.length = 0;
    foreignRequests.length = 0;
    return Response.json({ ok: true });
  }
  if (["/e/", "/i/v0/e/", "/batch/"].includes(url.pathname)) {
    const text = await request.text();
    const encoded = new URLSearchParams(text).get("data");
    let payload: unknown;
    try {
      payload = JSON.parse(encoded ?? text);
    } catch {
      payload = JSON.parse(Buffer.from(encoded ?? text, "base64").toString());
    }
    const batch = payload && typeof payload === 'object' && 'batch' in payload
      ? payload.batch : payload;
    events.push(...(Array.isArray(batch) ? batch : [batch]));
    return Response.json({ status: 1 });
  }
  if (url.pathname.startsWith("/flags") || url.pathname.startsWith("/array/")) {
    return Response.json({ featureFlags: {}, errorsWhileComputingFlags: false });
  }
  if (url.pathname === "/foreign") {
    foreignRequests.push({ method: request.method, headers: Object.fromEntries(request.headers) });
    return Response.json({ items: [] }, { headers: {
      "Access-Control-Allow-Origin": request.headers.get("Origin") ?? "*",
      "Access-Control-Allow-Headers": "content-type",
      "Cache-Control": "no-store",
    } });
  }
  if (url.pathname === "/pad") {
    return new Response("padding", { headers: { "Cache-Control": "no-store" } });
  }
  if (url.pathname === '/cached') {
    return Response.json({ items: [] }, { headers: { 'Cache-Control': 'max-age=60' } });
  }
  if (url.pathname === '/service-worker.js') {
    return new Response(SERVICE_WORKER, { headers: { 'Content-Type': 'text/javascript', 'Cache-Control': 'no-store' } });
  }
  if (url.pathname === '/sw-data') {
    return Response.json({ items: [] }, { headers: { 'Cache-Control': 'no-store' } });
  }
  if (url.pathname === "/away") return new Response("<!doctype html><title>Done</title><p>Done</p>", { headers: { "Content-Type": "text/html" } });
  if (url.pathname === "/styles.css") return new Response(Bun.file(resolve(root, "reference/browser/styles.css")), { headers: { "Content-Type": "text/css" } });
  const script = scripts.get(url.pathname);
  if (script) return new Response(script, { headers: { "Content-Type": "text/javascript" } });
  const vitalScript = /^\/static\/(?:1\.444\.0\/)?(web-vitals[\w-]*\.js)$/.exec(url.pathname);
  if (vitalScript) {
    return new Response(Bun.file(resolve(import.meta.dir, "node_modules/posthog-js/dist", vitalScript[1])), { headers: { "Content-Type": "text/javascript" } });
  }
  if (url.pathname !== "/") return new Response("Not found", { status: 404 });
  const html = (await Bun.file(resolve(root, "reference/browser/index.html")).text())
    .replace("__API_ORIGIN__", apiOrigin);
  return new Response(html, { headers: { "Content-Type": "text/html", "Cache-Control": "no-store" } });
}

const frontend = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: serve, maxRequestBodySize: 128 * 1024 });
const restricted = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: serve, maxRequestBodySize: 128 * 1024 });
const frontendOrigin = `http://127.0.0.1:${frontend.port}`;
const restrictedOrigin = `http://127.0.0.1:${restricted.port}`;
const child = Bun.spawn([resolve(root, ".venv/bin/python"), resolve(import.meta.dir, "server.py"), JSON.stringify([frontendOrigin, restrictedOrigin])], {
  cwd: root, stdout: "pipe", stderr: "inherit",
});

async function stop() {
  if (stopping) return;
  stopping = true;
  child.kill("SIGTERM");
  await child.exited;
  frontend.stop(true);
  restricted.stop(true);
}
process.on("SIGTERM", () => { void stop().then(() => process.exit(0)); });
process.on("SIGINT", () => { void stop().then(() => process.exit(0)); });

try {
  const reader = child.stdout.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  while (true) {
    const { value, done } = await reader.read();
    if (done) {
      if (stopping) break;
      throw new Error(`Python browser stack exited ${await child.exited}`);
    }
    buffer += decoder.decode(value, { stream: true });
    let newline: number;
    while ((newline = buffer.indexOf("\n")) >= 0) {
      const line = buffer.slice(0, newline);
      buffer = buffer.slice(newline + 1);
      if (!line.startsWith("BROWSER_STACK ")) { console.log(line); continue; }
      const stack = JSON.parse(line.slice(14)) as { apiOrigin: string; controlOrigin: string; assetRoot: string; wheelSha256: string };
      apiOrigin = stack.apiOrigin;
      controlOrigin = stack.controlOrigin;
      const built = await Bun.build({
        entrypoints: [resolve(root, "reference/browser/app.ts")],
        target: "browser", format: "esm", tsconfig: resolve(import.meta.dir, "tsconfig.json"),
        plugins: [{ name: "installed-browser-assets", setup(build) {
          build.onResolve({ filter: /^(axios|posthog-js)$/ }, args => ({
            path: resolve(import.meta.dir, args.path === "axios"
              ? "node_modules/axios/dist/esm/axios.js"
              : "node_modules/posthog-js/dist/module.mjs"),
          }));
          build.onResolve({ filter: /django_altimeter\/browser\// }, args => {
            const suffix = args.path.split("django_altimeter/browser/")[1];
            return { path: resolve(stack.assetRoot, suffix.endsWith('.ts') ? suffix : suffix + '.ts') };
          });
        } }],
      });
      if (!built.success) throw new Error(built.logs.map(String).join("\n"));
      scripts = new Map(built.outputs.map(output => [`/${output.path.split("/").at(-1)}`, output]));
      console.log("BROWSER_READY " + JSON.stringify({ frontendOrigin, restrictedOrigin, apiOrigin, controlOrigin: frontendOrigin, wheelSha256: stack.wheelSha256 }));
    }
  }
} catch (error) {
  console.error(error);
  await stop();
  process.exit(1);
}
