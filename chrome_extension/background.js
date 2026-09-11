const VERSION = "1.3";
const REST_ALLOWED = "https://omni.variational.io/api/quotes/indicative";
const WS_ALLOWED = new Set([
  "wss://omni-ws-server.prod.ap-northeast-1.variational.io/events",
  "wss://omni-ws-server.prod.ap-northeast-1.variational.io/portfolio"
]);
const state = {active: false, tabId: null, pending: new Map(), sockets: new Map(),
  executionArmed: false};

class LocalForwarder {
  constructor(url) { this.url = url; this.socket = null; this.queue = []; }
  start() {
    if (!state.active || this.socket) return;
    const socket = new WebSocket(this.url);
    this.socket = socket;
    socket.onopen = () => {
      while (this.queue.length) socket.send(this.queue.shift());
      notifyStatus();
    };
    socket.onclose = () => {
      if (this.socket === socket) this.socket = null;
      notifyStatus();
      if (state.active) setTimeout(() => this.start(), 1000);
    };
  }
  send(value) {
    const text = JSON.stringify(value);
    if (this.socket?.readyState === WebSocket.OPEN) this.socket.send(text);
    else { this.queue.push(text); this.queue = this.queue.slice(-1000); this.start(); }
  }
  stop() { this.socket?.close(); this.socket = null; this.queue = []; }
  get label() { return this.socket?.readyState === WebSocket.OPEN ? "connected" : "disconnected"; }
}
const wsOut = new LocalForwarder("ws://127.0.0.1:8766");
const restOut = new LocalForwarder("ws://127.0.0.1:8767");

class CommandSocket {
  constructor(url) { this.url = url; this.socket = null; this.timer = null; }
  start() {
    if (!state.active || this.socket) return;
    const socket = new WebSocket(this.url);
    this.socket = socket;
    socket.onmessage = event => this.handle(event.data);
    socket.onclose = () => {
      if (this.socket === socket) this.socket = null;
      notifyStatus();
      if (state.active) this.timer = setTimeout(() => this.start(), 1000);
    };
    socket.onopen = notifyStatus;
  }
  async handle(raw) {
    let request;
    try { request = JSON.parse(raw); }
    catch { return; }
    if (request?.type !== "PLACE_ORDER") return;
    const response = {type: "ORDER_RESULT", requestId: request.requestId,
      timestamp: new Date().toISOString()};
    try {
      if (!state.active || state.tabId == null) throw new Error("forwarder is not active");
      if (request.submit && !state.executionArmed)
        throw new Error("live execution is not armed in the extension");
      const expression = `(${pageOrder.toString()})(${JSON.stringify(request)})`;
      const result = await command(state.tabId, "Runtime.evaluate", {
        expression, awaitPromise: true, returnByValue: true,
        userGesture: true, timeout: 10000
      });
      if (result.exceptionDetails) throw new Error("page script exception");
      const value = result.result?.value;
      if (!value?.ok) throw new Error(value?.error || "page rejected command");
      Object.assign(response, value, {ok: true});
    } catch (error) {
      Object.assign(response, {ok: false, error: error.message});
    }
    if (this.socket?.readyState === WebSocket.OPEN)
      this.socket.send(JSON.stringify(response));
  }
  stop() {
    if (this.timer) clearTimeout(this.timer);
    this.timer = null; this.socket?.close(); this.socket = null;
  }
  get label() { return this.socket?.readyState === WebSocket.OPEN ? "connected" : "disconnected"; }
}

/* Runs in the Variational page's main world through CDP Runtime.evaluate. */
async function pageOrder(request) {
  const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
  const text = node => String(node?.textContent || "").replace(/\s+/g, " ").trim();
  try {
    if (!location.hostname.endsWith("variational.io"))
      throw new Error("wrong browser tab");
    const pageText = document.body?.innerText || "";
    if (request.symbol && !pageText.toUpperCase().includes(request.symbol.toUpperCase()))
      throw new Error(`symbol ${request.symbol} is not visible on the page`);
    const sideWords = request.side === "BUY" ? ["买", "BUY", "LONG"] : ["卖", "SELL", "SHORT"];
    const buttons = [...document.querySelectorAll("button")];
    let submits = [...document.querySelectorAll('button[data-testid="submit-button"]')];
    let submit = submits.find(button => sideWords.some(word =>
      text(button).toUpperCase().includes(word)));
    if (!submit) {
      const sideButton = buttons.find(button => button.dataset.testid !== "submit-button" &&
        text(button).length < 50 && sideWords.some(word =>
          text(button).toUpperCase().includes(word)));
      if (!sideButton) {
        const visible = buttons.map(text).filter(Boolean).slice(0, 30).join(" | ");
        throw new Error(`cannot find ${request.side} control; buttons: ${visible}`);
      }
      sideButton.click();
      await sleep(350);
    }

    const container = document.querySelector('[data-testid="quantity-input-container"]');
    let input = container?.querySelector("input");
    if (!input) throw new Error("cannot find Variational quantity input");
    const inputMode = String(localStorage.getItem("vr-input-mode") || "").toLowerCase();
    if (inputMode.includes("amount")) {
      const toggle = container.querySelector("button");
      if (!toggle) throw new Error("quantity input is in USD mode and its mode toggle was not found");
      toggle.click();
      await sleep(250);
      input = container.querySelector("input");
    }
    const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")?.set;
    if (!setter) throw new Error("cannot set quantity input");
    setter.call(input, String(request.quantity));
    input.dispatchEvent(new Event("input", {bubbles: true}));
    input.dispatchEvent(new Event("change", {bubbles: true}));
    input.blur();
    submit = null;
    for (let attempt = 0; attempt < 20; attempt++) {
      await sleep(250);
      submits = [...document.querySelectorAll('button[data-testid="submit-button"]')];
      submit = submits.find(button => !button.disabled &&
        sideWords.some(word => text(button).toUpperCase().includes(word)));
      if (submit) break;
    }
    if (!submit) {
      const visible = submits.map(button => `${text(button)} disabled=${button.disabled}`).join(" | ");
      throw new Error(`enabled ${request.side} submit button not found; input=${input.value}; submits: ${visible}`);
    }
    if (!request.submit)
      return {ok: true, submitted: false, buttonText: text(submit), quantity: input.value};
    submit.click();
    return {ok: true, submitted: true, buttonText: text(submit), quantity: input.value};
  } catch (error) {
    return {ok: false, error: String(error?.message || error)};
  }
}

const commandIn = new CommandSocket("ws://127.0.0.1:8768");

function command(tabId, method, params = {}) {
  return new Promise((resolve, reject) => chrome.debugger.sendCommand(
    {tabId}, method, params, result => chrome.runtime.lastError
      ? reject(new Error(chrome.runtime.lastError.message)) : resolve(result || {})));
}
function attach(tabId) {
  return new Promise((resolve, reject) => chrome.debugger.attach(
    {tabId}, VERSION, () => chrome.runtime.lastError
      ? reject(new Error(chrome.runtime.lastError.message)) : resolve()));
}
function detach(tabId) {
  return new Promise(resolve => chrome.debugger.detach({tabId}, resolve));
}
function normalized(url) {
  try { const u = new URL(url); return `${u.origin}${u.pathname}`; }
  catch { return ""; }
}
function status() {
  return {active: state.active, ws: wsOut.label, rest: restOut.label,
    command: commandIn.label, executionArmed: state.executionArmed};
}
function notifyStatus() {
  chrome.runtime.sendMessage({event: "status", status: status()}).catch(() => {});
}

async function start() {
  if (state.active) return status();
  const [tab] = await chrome.tabs.query({active: true, currentWindow: true});
  if (!tab?.id || !String(tab.url || "").startsWith("https://omni.variational.io/"))
    throw new Error("Open and select a Variational Omni tab first.");
  await attach(tab.id);
  try { await command(tab.id, "Network.enable"); }
  catch (error) { await detach(tab.id); throw error; }
  state.active = true; state.tabId = tab.id;
  state.executionArmed = false;
  wsOut.start(); restOut.start(); commandIn.start();
  await chrome.tabs.reload(tab.id);
  notifyStatus();
  return status();
}
async function stop() {
  const tabId = state.tabId;
  state.active = false; state.tabId = null; state.executionArmed = false;
  state.pending.clear(); state.sockets.clear(); wsOut.stop(); restOut.stop(); commandIn.stop();
  if (tabId != null) await detach(tabId);
  notifyStatus();
  return status();
}

chrome.debugger.onEvent.addListener(async (source, method, p) => {
  if (!state.active || source.tabId !== state.tabId) return;
  if (method === "Network.responseReceived" &&
      ["XHR", "Fetch"].includes(p.type) && normalized(p.response?.url) === REST_ALLOWED) {
    state.pending.set(p.requestId, {url: p.response.url, status: p.response.status});
  } else if (method === "Network.loadingFinished" && state.pending.has(p.requestId)) {
    const meta = state.pending.get(p.requestId); state.pending.delete(p.requestId);
    try {
      const body = await command(state.tabId, "Network.getResponseBody", {requestId: p.requestId});
      restOut.send({kind: "rest_response", ...meta, ...body, timestamp: new Date().toISOString()});
    } catch {}
  } else if (method === "Network.webSocketCreated" && WS_ALLOWED.has(normalized(p.url))) {
    state.sockets.set(p.requestId, p.url);
  } else if (method === "Network.webSocketClosed" && state.sockets.has(p.requestId)) {
    wsOut.send({kind: "ws_closed", url: state.sockets.get(p.requestId), timestamp: new Date().toISOString()});
    state.sockets.delete(p.requestId);
  } else if (["Network.webSocketFrameReceived", "Network.webSocketFrameSent"].includes(method)
             && state.sockets.has(p.requestId)) {
    wsOut.send({kind: "ws_frame",
      direction: method.endsWith("Received") ? "received" : "sent",
      url: state.sockets.get(p.requestId), opcode: p.response?.opcode,
      payloadData: p.response?.payloadData || "", timestamp: new Date().toISOString()});
  }
});
chrome.debugger.onDetach.addListener(source => {
  if (source.tabId === state.tabId) {
    state.active = false; state.tabId = null; state.executionArmed = false;
    wsOut.stop(); restOut.stop(); commandIn.stop();
    notifyStatus();
  }
});
chrome.runtime.onMessage.addListener((msg, _sender, reply) => {
  const task = msg.action === "start" ? start() : msg.action === "stop" ? stop() :
    msg.action === "arm" ? Promise.resolve((state.executionArmed = Boolean(msg.armed), status())) :
    Promise.resolve(status());
  task.then(value => reply({ok: true, status: value}))
      .catch(error => reply({ok: false, error: error.message}));
  return true;
});
