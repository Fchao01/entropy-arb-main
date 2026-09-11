#!/usr/bin/env node
/** Read-only Chrome DevTools Protocol probe for Variational Omni.
 *
 * Starts from an already-running Chrome instance launched with
 * --remote-debugging-port. It never clicks, signs, or submits anything.
 */

const port = Number(process.env.VARIATIONAL_CDP_PORT || 9222);
const targetUrl = process.env.VARIATIONAL_URL || "https://omni.variational.io";
const timeoutMs = Number(process.env.VARIATIONAL_PROBE_TIMEOUT_MS || 15000);

async function json(path, init) {
  const response = await fetch(`http://127.0.0.1:${port}${path}`, init);
  if (!response.ok) throw new Error(`${path}: HTTP ${response.status}`);
  return response.json();
}

let target;
try {
  target = await json(`/json/new?${encodeURIComponent(targetUrl)}`, { method: "PUT" });
} catch {
  target = await json("/json/new", { method: "PUT" });
}

const ws = new WebSocket(target.webSocketDebuggerUrl);
const pending = new Map();
let nextId = 1;
const hosts = new Map();
const sockets = new Set();
const socketUrls = new Map();
const frames = [];

function send(method, params = {}) {
  const id = nextId++;
  ws.send(JSON.stringify({ id, method, params }));
  return new Promise((resolve, reject) => pending.set(id, { resolve, reject }));
}

ws.addEventListener("message", (event) => {
  const message = JSON.parse(event.data);
  if (message.id) {
    const waiter = pending.get(message.id);
    if (!waiter) return;
    pending.delete(message.id);
    if (message.error) waiter.reject(new Error(message.error.message));
    else waiter.resolve(message.result);
    return;
  }
  if (message.method === "Network.requestWillBeSent") {
    try {
      const url = new URL(message.params.request.url);
      if (url.protocol === "http:" || url.protocol === "https:") {
        hosts.set(url.host, (hosts.get(url.host) || 0) + 1);
      }
    } catch {}
  }
  if (message.method === "Network.webSocketCreated") {
    sockets.add(message.params.url);
    socketUrls.set(message.params.requestId, message.params.url);
  }
  if (message.method === "Network.webSocketFrameReceived") {
    const url = socketUrls.get(message.params.requestId) || "";
    if (url.includes("variational.io") && frames.length < 12) {
      frames.push({ url, payload: message.params.response.payloadData.slice(0, 1000) });
    }
  }
});

await new Promise((resolve, reject) => {
  ws.addEventListener("open", resolve, { once: true });
  ws.addEventListener("error", reject, { once: true });
});

await send("Network.enable");
await send("Page.enable");
await send("Runtime.enable");
await send("Page.navigate", { url: targetUrl });
await new Promise((resolve) => setTimeout(resolve, timeoutMs));

const evaluated = await send("Runtime.evaluate", {
  expression: `JSON.stringify({
    title: document.title,
    url: location.href,
    body: (document.body?.innerText || "").slice(0, 1200)
  })`,
  returnByValue: true,
});
const page = JSON.parse(evaluated.result.value || "{}");

console.log(JSON.stringify({
  ok: true,
  chrome: (await json("/json/version")).Browser,
  page,
  requestHosts: [...hosts.entries()].sort((a, b) => b[1] - a[1]),
  webSockets: [...sockets],
  sampleFrames: frames,
}, null, 2));

ws.close();
await fetch(`http://127.0.0.1:${port}/json/close/${target.id}`);
