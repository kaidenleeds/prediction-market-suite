const el = (id) => document.getElementById(id);
const money = (value) => `${value < 0 ? "-" : ""}$${Math.abs(value).toFixed(2)}`;
const cents = (value) => `${Math.round(Number(value || 0) * 100)}¢`;
const signedCents = (value) => `${value >= 0 ? "+" : ""}${(value * 100).toFixed(1)}¢`;
const volume = (value) => value >= 1000000 ? `$${(value / 1000000).toFixed(1)}m` : `$${Math.round(value / 1000)}k`;
const clock = (value) => {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "now" : date.toLocaleTimeString([], { hour: "numeric", minute: "2-digit", second: "2-digit" });
};

let lastTick = -1;

function render(state) {
  el("paperPnl").textContent = money(state.paper_pnl || 0);
  el("paperPnl").className = (state.paper_pnl || 0) >= 0 ? "positive" : "negative";
  el("openTrades").textContent = state.open_trades || 0;
  el("signalCount").textContent = (state.signals || []).length;
  el("tickCount").textContent = state.tick || 0;
  el("uptime").textContent = `${state.uptime || "0s"} uptime`;
  el("databasePath").textContent = `Local database: ${state.database || "demo-data/showcase.db"}`;

  const pulse = el("marketPulse");
  pulse.textContent = state.paused ? "paused" : "updating";
  pulse.classList.toggle("paused", Boolean(state.paused));
  el("pauseButton").textContent = state.paused ? "Resume" : "Pause";

  el("marketRows").innerHTML = (state.markets || []).map((row) => {
    const gap = Number(row.reference || 0) - Number(row.yes || 0);
    return `<tr>
      <td><b>${escapeText(row.question)}</b><small>${escapeText(row.id)}</small></td>
      <td>${escapeText(row.venue)}</td>
      <td class="num"><b>${cents(row.yes)}</b></td>
      <td class="num">${cents(row.reference)}</td>
      <td class="num ${gap >= 0 ? "positive" : "negative"}">${signedCents(gap)}</td>
      <td class="num">${volume(row.volume || 0)}</td>
    </tr>`;
  }).join("");

  el("eventList").innerHTML = (state.events || []).map((row) => `<div class="event" data-stage="${escapeText(row.stage)}">
    <span class="event-dot"></span><div><p>${escapeText(row.message)}</p><small>${escapeText(row.stage)} · ${clock(row.created)}</small></div>
  </div>`).join("");

  el("signalList").innerHTML = (state.signals || []).slice(0, 6).map((row) => `<div class="list-card">
    <div><h3>${escapeText(row.market)}</h3><span class="trade-price">${escapeText(row.family)} · ${Math.round((row.strength || 0) * 100)}% strength</span></div>
    <span class="tag ${String(row.side).toLowerCase()}">${escapeText(row.side)}</span>
    <p>${escapeText(row.summary)}</p>
  </div>`).join("");

  el("tradeList").innerHTML = (state.trades || []).slice(0, 6).map((row) => `<div class="list-card">
    <div><h3>${escapeText(row.market)}</h3><span class="trade-price">${row.qty} contracts · ${cents(row.entry)} entry → ${cents(row.current)} now</span></div>
    <span class="tag ${(row.pnl || 0) >= 0 ? "yes" : "no"}">${money(row.pnl || 0)}</span>
    <p>${escapeText(row.status)} paper position · created ${clock(row.created)}</p>
  </div>`).join("");

  el("researchGrid").innerHTML = (state.research || []).map((row) => {
    const width = Math.max(8, Math.min(100, row.sample / 7));
    return `<div class="research-card"><h3>${escapeText(row.name)}</h3><strong class="${row.edge >= 0 ? "positive" : "negative"}">${row.edge >= 0 ? "+" : ""}${row.edge.toFixed(1)}¢</strong><p>${escapeText(row.holdout)}</p><div class="bar"><i style="width:${width}%"></i></div><small>${row.sample} settled · ${escapeText(row.status)}</small></div>`;
  }).join("");

  if (lastTick !== -1 && lastTick !== state.tick) {
    pulse.animate([{ opacity: .45 }, { opacity: 1 }], { duration: 350 });
  }
  lastTick = state.tick;
}

function escapeText(value) {
  return String(value ?? "").replace(/[&<>"']/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[char]);
}

async function load() {
  try {
    const response = await fetch("/api/demo/state", { cache: "no-store" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    render(await response.json());
    el("connectionState").textContent = "Connected to the local demo";
  } catch (error) {
    el("connectionState").textContent = `Waiting for the local demo: ${error.message}`;
  }
}

async function post(path) {
  const response = await fetch(path, { method: "POST" });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  await load();
}

el("pauseButton").addEventListener("click", () => post("/api/demo/toggle"));
el("stepButton").addEventListener("click", () => post("/api/demo/step"));
load();
setInterval(load, 1000);
