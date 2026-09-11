const ui = {
  badge: document.querySelector("#badge"), badgeText: document.querySelector("#badgeText"),
  wsCard: document.querySelector("#wsCard"), restCard: document.querySelector("#restCard"),
  wsState: document.querySelector("#wsState"), restState: document.querySelector("#restState"),
  commandCard: document.querySelector("#commandCard"), commandState: document.querySelector("#commandState"),
  start: document.querySelector("#start"), stop: document.querySelector("#stop"), arm: document.querySelector("#arm"),
  notice: document.querySelector("#notice")
};

function render(state, message = "") {
  const active = Boolean(state.active);
  ui.badge.classList.toggle("active", active);
  ui.badgeText.textContent = active ? "Live" : "Idle";
  ui.start.disabled = active;
  ui.stop.disabled = !active;
  ui.arm.disabled = !active || state.command !== "connected";
  ui.arm.classList.toggle("armed", Boolean(state.executionArmed));
  ui.arm.textContent = state.executionArmed ? "实盘已武装" : "实盘未武装";
  for (const [card, label, value] of [[ui.wsCard, ui.wsState, state.ws], [ui.restCard, ui.restState, state.rest], [ui.commandCard, ui.commandState, state.command]]) {
    const online = value === "connected";
    card.classList.toggle("online", online);
    label.textContent = online ? "Connected" : "Disconnected";
  }
  ui.notice.classList.remove("error");
  ui.notice.textContent = message || (active
    ? "正在监听当前 Variational 标签页，并向本机安全转发行情与账户事件。"
    : "请先启动本地接收器，再打开 Variational 交易页并启动转发。");
}

async function call(action) {
  ui.start.disabled = ui.stop.disabled = true;
  ui.notice.classList.remove("error");
  ui.notice.textContent = action === "start" ? "正在连接 Variational 标签页…" : action === "stop" ? "正在停止转发…" : "正在读取状态…";
  try {
    const reply = await chrome.runtime.sendMessage({action});
    if (!reply?.ok) throw new Error(reply?.error || "扩展后台没有响应");
    render(reply.status);
  } catch (error) {
    render({active:false, ws:"disconnected", rest:"disconnected"});
    ui.notice.classList.add("error");
    ui.notice.textContent = error.message;
  }
}

ui.start.addEventListener("click", () => call("start"));
ui.stop.addEventListener("click", () => call("stop"));
ui.arm.addEventListener("click", async () => {
  const reply = await chrome.runtime.sendMessage({action: "arm", armed: !ui.arm.classList.contains("armed")});
  if (reply?.ok) render(reply.status);
});
chrome.runtime.onMessage.addListener(message => {
  if (message?.event === "status" && message.status) render(message.status);
});
call("status");
