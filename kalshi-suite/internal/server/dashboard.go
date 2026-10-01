package server

// dashboardHTML is a self-contained dashboard (no build step) served at "/".
// It reads /api/status, /api/orderbook etc. and auto-refreshes. R75: /api/markets is the
// exception — the MARKETS tab loads on demand (button/search/next-page) and its poll only
// re-fetches rows already loaded, only while a markets surface is visible.
const dashboardHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Prediction Market Suite</title>
<style>
  /* R60 GRID skin (operator-approved variant 6 "GRID" — Fidelity ATP style): dense multi-pane
     widget grid on flat #141619, 2px colored left edge per pane, 10.5px type on 17px rows with
     1px column rules, #24272d header strips. Same CSS variable NAMES as before (mapped onto the
     v6 palette) so every existing renderer keeps working untouched. */
  :root{--bg:#141619;--card:#1e2126;--card2:#101214;--line:#26292f;--gut:#0c0d0f;--hd:#a5adb8;--muted:#7f8590;--text:#d8dbe0;--accent:#3b82f6;--good:#16c784;--bad:#ea3943;--warn:#f59e0b;}
  *{box-sizing:border-box;}
  html,body{overflow-x:hidden;} /* R59 (operator): NO horizontal scrolling, ever — panels wrap or scroll vertically */
  body{margin:0;color:var(--text);font-family:"Cascadia Code","JetBrains Mono",Consolas,ui-monospace,"Segoe UI",monospace;font-size:10.5px;line-height:15px;font-variant-numeric:tabular-nums;background:var(--bg);}
  /* R60 SHELL: [bar1 ≤30px status][bar2 flat icon nav][WIDGET GRID = everything else]. The page
     never scrolls; the widget grid scrolls vertically and every widget body scrolls internally. */
  .wrap{max-width:none;margin:0;padding:0;height:100vh;display:grid;grid-template-rows:28px 24px minmax(0,1fr);gap:0;box-sizing:border-box;overflow:hidden;}
  @media(min-width:1101px){html,body{overflow:hidden;}}
  /* R60 BAR1 (≤30px, variant-6 top bar): wordmark · PAPER net chip · Kalshi conn · live P&L chip ·
     readiness dots · RESET SESSION · kill switch · local + ET clock. env badge REMOVED (operator). */
  #bar1{display:flex;align-items:center;gap:8px;padding:0 8px;background:var(--card2);border-bottom:1px solid var(--gut);font-size:10.5px;flex-wrap:nowrap;min-height:0;overflow:hidden;white-space:nowrap;grid-row:1;position:relative;z-index:130;} /* R62 item 1: bar1 sits ABOVE any overlay — nothing may cover the bars */
  .wm{font-weight:700;letter-spacing:.1em;white-space:nowrap;}
  .clk{font-variant-numeric:tabular-nums;letter-spacing:.05em;color:var(--text);white-space:nowrap;}
  /* R60 BAR2: variant-5 flat icon nav restyled to variant-6 + ＋Widgets menu + PAPER|LIVE layout
     switcher (P / L keys). One flat row — same onclicks the old dropdowns carried. */
  /* R63 4e: 19 tabs on one row — tighter tab padding first; horizontal scroll ONLY as the fallback
     when even that can't fit (scrollbar hidden — wheel/drag still scrolls). */
  #bar2{display:flex;align-items:stretch;background:var(--card2);border-bottom:1px solid var(--gut);padding:0 2px;position:relative;z-index:120;min-height:0;grid-row:2;overflow-x:auto;overflow-y:hidden;scrollbar-width:none;}
  #bar2::-webkit-scrollbar{display:none;}
  .ni{padding:0 8px;display:inline-flex;align-items:center;gap:5px;color:var(--muted);cursor:pointer;font-size:10px;white-space:nowrap;background:transparent;border:none;letter-spacing:.03em;}
  .ni:hover{color:var(--text);}
  .ni .ic{color:var(--accent);font-size:10px;}
  /* R62 item 10: ALL destinations are top-level tabs with a PERMANENT accent color — colored text +
     underline always on (dimmed when inactive); the active tab is filled/brighter. --tc per tab. */
  .mtab{display:inline-flex;align-items:center;justify-content:center;cursor:pointer;font-weight:700;letter-spacing:.06em;font-size:10px;color:var(--tc,var(--muted));border-left:1px solid var(--line);border-right:1px solid var(--line);padding:0 5px;user-select:none;box-shadow:inset 0 -2px 0 var(--tc,transparent);opacity:.68;white-space:nowrap;flex:none;} /* R63 4e: denser tabs so one row fits ≤1500px */
  .mtab:hover{opacity:1;}
  /* R145: research navigation separates order-producing Systems from their immutable validation tests.
     Non-clickable dividers; every tab keeps its own permanent accent. */
  .tgrp{display:inline-flex;align-items:center;font-size:8.5px;font-weight:800;letter-spacing:.14em;color:var(--muted);opacity:.55;padding:0 4px 0 10px;border-left:2px solid #33373e;user-select:none;white-space:nowrap;flex:none;}
  .mtab.on{opacity:1;background:#1a1d21;box-shadow:inset 0 -2px 0 var(--tc,var(--good)),inset 0 40px 0 rgba(255,255,255,.035);}
  .ddgrp{position:relative;flex:none;display:inline-flex;align-items:stretch;}
  .ddmenu{display:none;position:absolute;top:100%;left:0;z-index:200;background:var(--card);border:1px solid #33373e;border-radius:1px;box-shadow:0 14px 40px rgba(0,0,0,.65);padding:2px;min-width:196px;}
  .ddmenu button{display:block;width:100%;text-align:left;background:transparent;border:none;padding:4px 8px;font-size:10.5px;font-weight:600;border-radius:1px;white-space:nowrap;color:var(--text);}
  .ddmenu button:hover{background:#24272d;box-shadow:none;filter:none;}
  .ddmenu button:disabled{opacity:.4;cursor:default;}
  .ddmenu label{display:block;padding:4px 8px;font-size:10px;color:var(--muted);cursor:pointer;white-space:nowrap;user-select:none;}
  .ddmenu .ddver{display:block;padding:4px 8px;font-size:9.5px;color:var(--muted);border-top:1px solid var(--line);margin-top:2px;white-space:nowrap;}
  .spacer{flex:1;}
  .dot{width:6px;height:6px;border-radius:50%;background:var(--good);display:inline-block;}
  @keyframes pulse{0%{opacity:1;}50%{opacity:.45;}100%{opacity:1;}}
  .badge{font-size:9.5px;padding:1px 6px;border-radius:1px;background:transparent;color:var(--text);font-weight:600;border:1px solid #33373e;white-space:nowrap;letter-spacing:.04em;}
  .badge.good{color:var(--good);border-color:rgba(22,199,132,.4);}
  .badge.bad{color:var(--bad);border-color:rgba(234,57,67,.45);}
  .badge.warn{color:var(--warn);border-color:rgba(245,158,11,.4);}
  button{background:transparent;color:#6ba3f8;border:1px solid var(--accent);padding:1px 7px;border-radius:1px;cursor:pointer;font-size:9.5px;font-weight:700;font-family:inherit;letter-spacing:.06em;}
  button:hover{background:rgba(59,130,246,.15);filter:none;box-shadow:none;}
  button:active{transform:translateY(1px);}
  button.danger{background:#c93131;color:#fff;border-color:#e05555;}
  button.danger:hover{background:#e04040;box-shadow:none;}
  .updated{color:var(--muted);font-size:9.5px;display:inline-flex;align-items:center;gap:6px;white-space:nowrap;}
  .card{background:var(--card);border:1px solid var(--gut);border-left:2px solid var(--edge,#3b82f6);border-radius:0;padding:6px;margin-top:0;}
  .cardhead{display:flex;align-items:center;gap:8px;min-height:20px;margin-bottom:3px;}
  .cardhead strong{font-size:10.5px;text-transform:uppercase;letter-spacing:.12em;color:var(--hd);}
  .muted{color:var(--muted);font-size:10.5px;}
  table{width:100%;border-collapse:collapse;margin-top:2px;}
  /* R60 density (variant 6): 16px sticky 9px headers on #24272d, 17px data rows of 10.5px type,
     1px COLUMN RULES between cells (the Fidelity ATP look). Row P&L tint (C2) rides inline. */
  thead th{position:sticky;top:0;background:#24272d;color:var(--hd);font-weight:600;font-size:9px;height:16px;line-height:12px;text-transform:uppercase;letter-spacing:.08em;text-align:left;padding:1px 4px;border-bottom:1px solid var(--line);border-right:1px solid var(--line);z-index:1;}
  thead th:last-child{border-right:0;}
  tbody td{padding:1px 4px;height:17px;border-bottom:1px solid var(--line);border-right:1px solid var(--line);font-size:10.5px;line-height:14px;vertical-align:middle;}
  tbody td:last-child{border-right:0;}
  tbody tr:hover{box-shadow:inset 2px 0 0 var(--accent);}
  td.num{text-align:right;font-variant-numeric:tabular-nums;}
  td.name a{color:var(--text);text-decoration:none;font-weight:500;}
  td.name a:hover{color:var(--accent);text-decoration:underline;}
  td.ell{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;} /* R63 1b/4g: one-line market cells, full title in the tooltip */
  .pill{display:inline-block;min-width:38px;text-align:center;padding:0 5px;border-radius:1px;font-weight:700;font-variant-numeric:tabular-nums;}
  .vol{color:#c7d2e6;}
  .when{color:var(--muted);font-size:10.5px;white-space:nowrap;}
  table.mkt{table-layout:fixed;}
  th.c,td.c{text-align:center;}
  th.r,td.r{text-align:right;}
  /* R59: the old 3-panel top region (.grid FEEDS|FLOW|CROSS-VENUE) is GONE — those panels are
     toggled full-viewport surfaces now (FEEDS nav dropdown) and the freed shell row is the PAPER
     zone. The .grid container + all its CSS were deleted with it. */
  @media(max-width:1100px){.wrap{display:block;height:auto;overflow:visible;}#wgrid{grid-auto-rows:80px;}.vsplit{grid-template-columns:1fr!important;}}
  /* R70 (audit §a): dead-dialog selectors dropped (#evcard/#buycard/#gatecard had no elements or
     no openers); z-index raised 50→140 so a centered dialog's top edge can no longer slide UNDER
     bar2 (z120) / bar1 (z130) on short viewports. */
  #book,#stopscard,#combocard,#betscard{position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);width:min(900px,94vw);max-height:86vh;overflow:auto;z-index:140;box-shadow:0 24px 70px rgba(0,0,0,.7);} /* R67f: betscard is a centered market popup again */
  /* R63 4e: EVERY former ⋯More overlay (combos/signals/orders/history/edge/curves/replay/bets) is a
     real top-level TAB now — same static shell-row-3 panels as the R62 tabs. Only genuine dialogs
     (book / buy / stops / combo-place / gate) stay fixed overlays. */
  #feedscard,#flowcard,#xvenuecard,#researchcard,#mlcard,#marketscard,#statscard,#settingscard,#logscard,
  #parlaycard,#signalscard,#orderscard,#historycard,#edgecard,#curvescard,#replaycard,#horizoncard,
  #overviewcard,#systemscard,#experimentscard,#evidencecard,#datacard,#operationscard{
    grid-row:3;grid-column:1;position:static;width:auto;max-width:none;height:auto;max-height:none;min-height:0;overflow:auto;margin:0;box-shadow:none;animation:surfIn .12s ease-out;}
  @keyframes surfIn{from{opacity:.55;transform:translateY(10px);}to{opacity:1;transform:translateY(0);}}
  /* R60 WIDGET GRID (the old fixed paper/live zones are GONE): 12 columns × 90px rows, 1px
     gutters (variant 6). Widgets are user-arranged panes — ＋Widgets adds instantly, ✕ removes,
     drag the title bar to MOVE, drag the corner handle to RESIZE (snaps to grid, min 2×1);
     layout persists per tab in localStorage pms_layout_v1 (+ pms_layout_paper / pms_layout_live). */
  #wgrid{display:grid;grid-template-columns:repeat(12,minmax(0,1fr));grid-auto-rows:90px;gap:1px;background:var(--gut);overflow-y:auto;overflow-x:hidden;min-height:0;grid-row:3;grid-column:1;}
  .wdg{position:relative;display:flex;flex-direction:column;min-width:0;min-height:0;background:var(--card);border-left:2px solid var(--edge,#3b82f6);overflow:hidden;}
  .wdg.drag{opacity:.8;z-index:9;}
  .wh{flex:none;height:20px;display:flex;align-items:center;gap:6px;padding:0 3px 0 6px;background:#24272d;border-bottom:1px solid var(--line);white-space:nowrap;overflow:hidden;cursor:grab;user-select:none;}
  .wh .wt{font-size:10.5px;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:var(--hd);}
  .wbtn{background:transparent;border:none;color:var(--muted);font-size:9.5px;font-weight:700;padding:0 4px;height:16px;letter-spacing:.04em;cursor:pointer;}
  .wbtn:hover{color:var(--text);background:rgba(255,255,255,.06);}
  .wx:hover{color:var(--bad);}
  .wb{flex:1;min-height:0;overflow:auto;position:relative;}
  /* R63 6b: resize handles on ALL FOUR corners (se stays the marker-visible default). */
  .wrs{position:absolute;right:0;bottom:0;width:14px;height:14px;cursor:nwse-resize;z-index:5;}
  .wrs::after{content:"";position:absolute;right:2px;bottom:2px;width:7px;height:7px;border-right:2px solid var(--muted);border-bottom:2px solid var(--muted);}
  .wrs:hover::after{border-color:var(--accent);}
  .wrs.nw{left:0;top:0;right:auto;bottom:auto;cursor:nwse-resize;}
  .wrs.nw::after{left:2px;top:2px;right:auto;bottom:auto;border-right:0;border-bottom:0;border-left:2px solid var(--muted);border-top:2px solid var(--muted);}
  .wrs.ne{right:0;top:0;bottom:auto;cursor:nesw-resize;}
  .wrs.ne::after{right:2px;top:2px;bottom:auto;border-bottom:0;border-top:2px solid var(--muted);}
  .wrs.sw{left:0;bottom:0;right:auto;cursor:nesw-resize;}
  .wrs.sw::after{left:2px;bottom:2px;right:auto;border-right:0;border-left:2px solid var(--muted);}
  .wadopt{min-width:0;min-height:0;} /* R63 6e: slot that ADOPTS a single-instance tab panel into a widget */
  .wsum{font-size:10px;color:var(--muted);padding:2px 4px;border-bottom:1px solid var(--line);}
  .wsec{font-size:9px;font-weight:700;letter-spacing:.14em;color:var(--hd);text-transform:uppercase;padding:2px 4px;}
  .echip{border-bottom:1px dashed #4a5058;cursor:pointer;}
  .echip:hover{color:var(--text);border-bottom-color:var(--accent);}
  .gatepop{position:absolute;top:21px;right:2px;z-index:40;background:var(--card);border:1px solid #33373e;padding:8px;width:330px;box-shadow:0 10px 30px rgba(0,0,0,.6);font-size:10.5px;}
  /* R59 FEEDS/FLOW toggled surfaces are flex columns so the 3 venue columns fill the viewport
     height and scroll independently (same internals as the old grid panels, just full-size). */
  #feedscard,#flowcard{flex-direction:column;overflow:hidden;}
  /* R55a pipeline funnel — now INLINE inside the blotter status strip (R57); PLACE pulses on AUTO */
  .pipestrip{display:flex;align-items:center;gap:4px;font-size:10px;color:var(--muted);flex:none;letter-spacing:.05em;white-space:nowrap;}
  .pseg{border:1px solid var(--line);border-radius:1px;padding:1px 5px;background:var(--card2);white-space:nowrap;}
  .pseg b{color:var(--text);font-variant-numeric:tabular-nums;}
  .pseg.on{color:var(--good);border-color:rgba(47,230,168,.35);}
  .pseg.on b{color:var(--good);text-shadow:0 0 8px rgba(47,230,168,.4);}
  .pseg.hot{color:var(--warn);border-color:rgba(255,194,75,.45);animation:pulse 2s infinite;}
  .parrow{color:#33405f;}
  /* R139 research-first shell: ten human-oriented destinations. Dense evidence remains available,
     but each page starts with a plain-language decision strip and explicit authority state. */
  .r138grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(310px,1fr));gap:6px;margin-top:6px;}
  .r138pane{background:var(--card2);border:1px solid var(--line);border-left:2px solid var(--edge,var(--accent));padding:7px;min-height:72px;overflow:auto;}
  .r138pane h3{font-size:10px;letter-spacing:.12em;text-transform:uppercase;color:var(--hd);margin:0 0 5px;}
  .r138hero{font-size:12px;line-height:17px;padding:8px;border:1px solid var(--line);background:#171a1f;}
  .r138hero b{color:var(--text);}
  .r138state{display:inline-block;padding:0 5px;border:1px solid var(--line);font-size:9px;font-weight:800;letter-spacing:.05em;}
  .r138state.ok{color:var(--good);border-color:rgba(22,199,132,.45)}
  .r138state.block{color:var(--warn);border-color:rgba(245,158,11,.45)}
  .r138state.bad{color:var(--bad);border-color:rgba(234,57,67,.5)}
  .r138links{display:flex;gap:5px;flex-wrap:wrap;margin-top:7px;}
  /* R141 operator view: decision-sized summaries first; dense ledgers are collapsed diagnostics. */
  .r141lead{font-size:13.5px;line-height:19px;padding:9px 11px;border:1px solid var(--line);background:#171a1f;margin-bottom:8px;}
  .r141grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:8px;}
  .r141card{background:var(--card2);border:1px solid var(--line);border-left:3px solid var(--edge,var(--accent));padding:10px 12px;min-height:66px;font-size:13px;line-height:18px;}
  .r141card h3{font-size:10px;letter-spacing:.11em;text-transform:uppercase;color:var(--hd);margin:0 0 5px;}
  .r141big{font-size:17px;font-weight:800;color:var(--text);font-variant-numeric:tabular-nums;}
  .r141row{display:flex;gap:8px;align-items:baseline;padding:4px 0;border-bottom:1px solid rgba(255,255,255,.04);}
  .r141row:last-child{border-bottom:0;}
  .r141row .name{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
  .r141diag{margin-top:10px;border:1px solid var(--line);background:#14171b;padding:7px 9px;}
  .r141diag>summary{cursor:pointer;color:var(--muted);font-size:11.5px;font-weight:700;}
  .r141warn{color:var(--warn);font-size:12px;margin-top:5px;}
  .r142combo{border:1px solid var(--line);border-left:3px solid #c084fc;background:var(--card2);padding:8px 10px;margin-top:8px;font-size:12px;line-height:16px;}
  .r142combohead{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:6px;}
  .r142combohead .title{font-weight:800;color:var(--hd);}
  .r142combohead .count{font-weight:750;}
  .r142combolegs{display:grid;grid-template-columns:repeat(5,minmax(92px,1fr));gap:5px;}
  .r142combochip{background:#171a1f;border:1px solid var(--line);padding:5px 7px;min-width:0;}
  .r142combochip b{display:block;font-size:11px;}
  @media(max-width:900px){.r142combolegs{grid-template-columns:repeat(2,minmax(110px,1fr));}}
  /* ?win= popout (R60): bars hidden, the ONE requested widget fills the window (initWinMode). */
  body.win .wrap{display:grid;grid-template-rows:0 0 minmax(0,1fr);height:100vh;overflow:hidden;}
  body.win #wgrid{grid-auto-rows:minmax(70px,1fr);}
  #stopscard{width:min(460px,92vw);}
  #combocard{width:min(640px,94vw);}
  /* R63 4c: #settingscard width cap REMOVED — the settings form uses the full panel width (the
     flex-wrapped field groups become a natural multi-column grid at wide widths). */
  button.mini{padding:0 5px;font-size:9px;border-radius:1px;font-weight:700;height:16px;line-height:14px;}
  .modesel{display:inline-flex;align-items:center;gap:3px;font-size:10px;}
  .modebtn{padding:1px 7px;border-radius:1px;border:1px solid #33373e;background:transparent;color:var(--muted);font-weight:700;font-size:9.5px;}
  .modebtn.on{color:var(--text);background:rgba(59,130,246,.18);border-color:rgba(59,130,246,.5);}
  .modebtn.on.live{color:var(--bad);background:rgba(234,57,67,.18);border-color:rgba(234,57,67,.5);}
  .go{color:var(--accent);text-decoration:none;font-weight:600;}
  .books{display:flex;gap:18px;margin-top:10px;flex-wrap:wrap;}
  .books table{flex:1;min-width:240px;}
  .bid td:first-child{color:var(--good);font-weight:600;}
  .ask td:first-child{color:var(--bad);font-weight:600;}
  .fld{display:flex;flex-direction:column;font-size:10.5px;color:var(--muted);gap:4px;}
  .fld input,.fld select{background:var(--card2);border:1px solid var(--line);color:var(--text);border-radius:6px;padding:1px 6px;height:20px;font-size:10.5px;width:140px;}
  .fld input:focus,.fld select:focus{outline:none;border-color:var(--accent);box-shadow:0 0 0 2px rgba(91,157,255,.18);}
  /* R59 density v2: every text/number input + select lands on the 20px control height, 10.5px type
     (checkboxes/radios excluded). Inline font-size styles on a few legacy inputs still win — fine. */
  input:not([type=checkbox]):not([type=radio]),select{height:20px;font-size:10.5px;font-family:inherit;}
  /* --- visual polish --- */
  *{scrollbar-width:thin;scrollbar-color:#33373e transparent;}
  *::-webkit-scrollbar{width:8px;height:8px;}
  *::-webkit-scrollbar-thumb{background:#33373e;border-radius:0;border:2px solid var(--card);}
  *::-webkit-scrollbar-thumb:hover{background:#454b55;}
  *::-webkit-scrollbar-track{background:transparent;}
  td.r,td.num{font-variant-numeric:tabular-nums;}
  .card{box-shadow:0 1px 2px rgba(0,0,0,.25);transition:border-color .15s;}
  button{transition:filter .15s,background .15s,transform .05s;}
  /* R70 (audit §a): byte-identical button:active dup removed (defined once above). */
  button.mini{background:transparent;border-color:#33373e;color:#9aa4b2;}
  button.mini:hover{background:rgba(255,255,255,.06);border-color:#454b55;filter:none;}
  .muted a,a.go{color:var(--accent);text-decoration:none;}
  .muted a:hover,a.go:hover,td.name a:hover{text-decoration:underline;}
  tbody tr{transition:background .1s;}
  .pill{box-shadow:inset 0 0 0 1px rgba(255,255,255,.04);}
  /* R57 UNIFIED FEED SCHEMA: TIME 9% | SIDE 7% | MARKET 51% | PRICE 11% | SIZE 14% | Δ 8% — the
     same 6-column tape on every venue; venue sub-headers are one standardized 28px row. */
  table.uf{table-layout:fixed;}
  .uf col.c-time{width:9%;} .uf col.c-side{width:7%;} .uf col.c-mkt{width:51%;} .uf col.c-px{width:11%;} .uf col.c-sz{width:14%;} .uf col.c-d{width:8%;}
  .uf td{overflow:hidden;}
  .uf td.mkt .mrow{display:flex;align-items:center;gap:4px;min-width:0;}
  .uf td.mkt .mtxt{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
  .uf td.mkt .mrow .mini{flex:none;padding:0 4px;font-size:10px;}
  .sidetag{display:inline-block;min-width:26px;text-align:center;padding:1px 4px;border-radius:2px;font-size:10px;font-weight:700;}
  .sidetag.b{background:rgba(47,230,168,.18);color:var(--good);}
  .sidetag.s{background:rgba(255,107,107,.18);color:var(--bad);}
  .sidetag.n{background:rgba(148,163,184,.15);color:#cbd5e1;}
  .vhead{display:flex;align-items:center;gap:6px;height:28px;min-width:0;overflow:hidden;white-space:nowrap;}
  .vchip{flex:none;width:44px;text-align:center;font-size:10px;font-weight:700;letter-spacing:.5px;text-transform:uppercase;border:1px solid var(--line);border-radius:4px;padding:2px 0;background:var(--card2);color:var(--text);}
  .vhead .vstat{font-size:10px;color:var(--muted);overflow:hidden;text-overflow:ellipsis;min-width:0;}
  .vhead .vctl{margin-left:auto;display:inline-flex;align-items:center;gap:4px;font-size:10px;color:var(--muted);flex:none;height:20px;}
  .vhead .vctl input{height:20px;padding:0 4px;font-size:10px;width:56px;background:var(--card2);border:1px solid var(--line);color:var(--text);border-radius:4px;}
  .vhead .vctl a{color:var(--muted);text-decoration:none;}
  .vhead .vctl a:hover{color:var(--text);}
  /* R57 cross-venue aggregator: EVENT 32% | KAL 11% | PUS 11% | INT 11% | EDGE 10% | VOL 12% | ACT 6% */
  table.xv{table-layout:fixed;}
  .xv td.ev{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
  .xv td.best{background:rgba(47,230,168,.14);font-weight:700;color:var(--good);}
  .xv tr.xvd td{background:var(--card2);font-size:11px;color:var(--muted);white-space:normal;}
  /* R56 ⓘ info bubble — the explainer paragraphs moved out of flow into hover tooltips */
  .inf{cursor:help;border:1px solid var(--line);border-radius:50%;width:14px;height:14px;display:inline-flex;align-items:center;justify-content:center;font-size:10px;color:var(--muted);flex:none;}
  /* compact mode REMOVED (operator report: the class flip re-laid-out the whole ~10k-node DOM and
     near-hung Chrome on the big tabs). Full view is the only view. */
  .rdot{display:inline-block;width:8px;height:8px;border-radius:50%;margin:0 2px;vertical-align:middle;}
  /* R63 4d: logs tab right-edge cutoff — the body + rows are hard-capped to the panel width and
     long unbroken payloads wrap instead of pushing past the clipped card edge. */
  #logsBody{width:100%;max-width:100%;overflow-x:hidden;min-width:0;}
  #logsBody div{max-width:100%;overflow-wrap:anywhere;white-space:normal;}
</style>
</head>
<body>
<div class="wrap">
  <!-- R62 BAR1 (item 11): wordmark + "Kalshi connected" badge REMOVED (max space). PAPER net chip ·
       live P&L chip · readiness dots · feeds-live + Live-updated indicators (moved from bar2) ·
       RESET SESSION (two-step confirm) · kill switch · 12h local + ET clock. One row. -->
  <div id="bar1">
    <span id="networth" class="badge" title="THE Paper simulation portfolio (one book): modeled systems + combos + RFQ comparisons — simulated settled net, fee-inclusive; not exchange profit evidence">PAPER …</span>
    <span id="livechip" class="badge" title="live unrealized P&amp;L at venue marks, split by Kalshi and PolyUS">LIVE …</span>
    <span id="readydots" class="badge" title="system readiness — hover each dot">…</span>
    <!-- R86: persistent chip while Kalshi credentials are LOCKED (the passphrase env var is
         gone — e.g. a PC crash killed the shell that exported it). Tooltip = the exact fix text
         from /api/ready kalshi_auth; hidden when auth is green or nothing is stored.
         R87: same chip also covers the key-FILE red states (file unreadable / kalshi_key_id
         missing) — both fixes are named: key file (Settings) OR passphrase + restart. -->
    <span id="authchip" class="badge bad" style="display:none;cursor:default" title="Kalshi credentials locked — fix: point kalshi_key_file at your PEM key + set kalshi_key_id (Settings), OR re-set KALSHI_SUITE_PASSPHRASE and restart">🔑 KALSHI AUTH LOCKED</span>
    <span id="bookws" class="badge" style="display:none;cursor:default" title="orderbook delta WS health">…</span>
    <span id="mktschip" class="badge" style="display:none;cursor:default" title="market coverage — live vs tracked">…</span>
    <!-- R107 Part 5: latency health chip — 🟢/🟡/🔴 from /api/latency (30s probes; hover = why + numbers) -->
    <span id="latchip" class="badge" style="cursor:default" title="connection/latency health — warming up">📡 …</span>
    <span id="modenote" class="muted" style="font-size:10px;white-space:nowrap;" title="AUTO/AI only place bets once the live feeds are connected"></span>
    <span class="updated"><span class="dot"></span><span id="updated">Loading…</span></span>
    <span id="expbar" class="updated" style="display:none;color:#6ba3f8;font-variant-numeric:tabular-nums" title="export build progress (R67m — no longer replaces the feeds-live indicator)"></span>
    <span class="spacer"></span>
    <!-- R72-A #2 VERSION CHIP: deterministic color+animal build name (dot = the actual CSS color);
         hover = full sha@time. Populated by loadStatus from /api/status build_name/build_color. -->
    <span id="bldchip" class="badge" style="display:none;cursor:default"></span>
    <!-- R73 (operator): the arm/confirm two-step is GONE — a plain ON/OFF-styled switch that fires
         the reset POST IMMEDIATELY on click (one click = reset; paper-safe by design). Brief
         RESETTING… state (~2s) while it runs; the broken CONFIRM handler is removed entirely. -->
    <!-- ROS chip (operator: "toggle right by the reset on top right") — the R30 reset-on-start
         switch, moved here from the ⚙ Settings card. Two-state; lit = boots start from a clean slate. -->
    <span class="modesel" title="Reset P&amp;L on start — ON: every suite boot FIRST closes ALL open paper/ML positions at live marks and zeroes the session P&amp;L + graphs, THEN trading begins. OFF: the book carries across restarts."><button id="rosBtn" class="modebtn" onclick="toggleROS()">↺RST:—</button></span>
    <span class="modesel" title="RESET SESSION — one click fires immediately (paper-safe): closes open paper/ML positions at live marks, zeroes the session P&amp;L graph + header baselines. Closed-bet history is KEPT."><button id="resetBtn" class="modebtn" onclick="resetNow()">RESET</button></span>
    <button id="ksbtn" onclick="toggleKill()">Kill switch</button>
    <button class="mini" onclick="toggleFullscreen()" title="fullscreen on/off (F11 works too)">⛶</button>
    <span id="clk" class="clk"></span>
  </div>
  <!-- R139: the executable is now a research cockpit, not a directory of implementation parts.
       Each destination answers a human question. Settings sits beside Operations; legacy tools remain linked there;
       Paper and Live keep their established widget layouts and safety controls. -->
  <div id="bar2">
    <span class="tgrp" title="what is working, proving, or blocked">RESEARCH</span>
    <span class="mtab" id="tab_overview" style="--tc:#60a5fa" onclick="uiTab('overview')" title="plain-language state of the whole research factory">🏠 OVERVIEW</span>
    <span class="mtab" id="tab_systems" style="--tc:#f59e0b" onclick="uiTab('systems')" title="all registered systems and their evidence state">⚙ SYSTEMS</span>
    <span class="mtab" id="tab_experiments" style="--tc:#c084fc" onclick="uiTab('experiments')" title="immutable tests, cohorts, holdouts, and results">🧪 VALIDATION</span>
    <span class="mtab" id="tab_evidence" style="--tc:#22d3ee" onclick="uiTab('evidence')" title="collector health, exact routes, outcomes, and portfolio causes">📐 EVIDENCE</span>
    <span class="mtab" id="tab_data" style="--tc:#38bdf8" onclick="uiTab('data')" title="source clocks, replay integrity, and venue notices">🗃 DATA</span>
    <span class="mtab" id="tab_markets" style="--tc:#84cc16" onclick="uiTab('markets')" title="live venue books and canonical market tree">📈 MARKETS</span>
    <span class="tgrp" title="simulated and real-money ledgers">PORTFOLIOS</span>
    <span class="mtab" id="tab_paper" style="--tc:var(--good)" onclick="setTab('paper')" title="PAPER widget layout (key: P)">📝 PAPER</span>
    <span class="mtab" id="tab_live" style="--tc:var(--bad)" onclick="setTab('live')" title="LIVE widget layout (key: L)">🔴 LIVE</span>
    <span class="mtab" id="tab_operations" style="--tc:#94a3b8" onclick="uiTab('operations')" title="readiness, governance, recovery, logs, settings, and legacy diagnostic tools">🛠 OPERATIONS</span>
    <span class="mtab" id="tab_settings" style="--tc:#a3a3a3" onclick="uiTab('settings')" title="suite configuration and safe runtime controls">⚙ SETTINGS</span>
    <span class="ni" onclick="exportData()" title="full data export (one JSON)"><span class="ic">⇩</span>Export</span>
    <span class="ddgrp"><span class="ni" onclick="buildWidgetMenu();toggleDD('dd_widgets',event)" title="add a widget to this layout — click adds it instantly"><span class="ic">＋</span>Widgets</span>
      <div id="dd_widgets" class="ddmenu"></div></span>
    <span class="modesel" title="PAPER auto-pilot. OFF = idle · AUTO = executes the paper venue books"><button id="modeOff" class="modebtn" onclick="setMode('off')">OFF</button><button id="modeAuto" class="modebtn" onclick="setMode('autobet')">AUTO</button></span>
    <span class="modesel" title="REAL-MONEY auto. Visible beside Paper Auto, but it cannot turn on until this session is armed."><button id="modeLiveAuto" class="modebtn" onclick="toggleLiveAutoQuick()">LIVE AUTO</button></span>
    <span class="spacer"></span>
  </div>

  <!-- R138 RESEARCH-FIRST PAGES. These are read-only digest surfaces. The evidence pages display
       missing/blocked truth instead of turning sample count or a point estimate into authority. -->
  <div id="overviewcard" class="card" style="display:none;">
    <div class="cardhead"><strong>🏠 Research overview</strong><span class="spacer"></span><span class="muted">what is working, proving, or blocked right now</span><button class="mini" onclick="loadResearchOverview()">Refresh</button></div>
    <div class="r141lead"><b>Goal:</b> find fee-net systems that survive untouched Paper testing and transfer unchanged to armed LIVE. This page shows only readiness, measurable progress, and the next blocker.</div>
    <div id="r138OverviewBody" class="r138grid"><div class="r138pane">Loading research truth…</div></div>
  </div>

  <div id="systemscard" class="card" style="display:none;">
    <div class="cardhead"><strong>⚙ Systems</strong><span class="spacer"></span><span class="muted">complete stack: model + signal + route + strategy</span><button class="mini" onclick="loadResearchSystemsPage()">Refresh</button></div>
    <div class="r141lead"><b>Read samples simply:</b> <b>n</b> is distinct settled venue+ticker contracts; repeated fills, shares, snapshots, and re-entries count once. Different tickers can still be related, so n measures coverage—not independence. 🔴0–10 · 🟠11–40 · 🟡41–120 · 🟢121–500 · 🔵501–1,000 · 🟣1,001+ contracts. <b>sealed</b> means an untouched multiplicity-controlled holdout passed. <b>RFQ-proved</b> means the exact combo received an authenticated venue quote. <b>$-days</b> means entry dollars × days held. Paper promotion is never LIVE proof.</div>
    <div id="r138SystemsBody">Loading compact systems digest…</div>
    <details class="r141diag" ontoggle="if(this.open)loadResearchSystemDetails()"><summary>Show every system collector and the complete Systems Leaderboard</summary>
      <div id="r138SystemsDetail" style="margin-top:8px">Open to load detailed diagnostics.</div>
      <div class="cardhead" style="margin-top:12px"><strong>📊 Complete Systems Leaderboard</strong><span class="spacer"></span><span class="muted">detailed rows load only on request</span></div>
      <div id="r139SystemsLeaderboard" style="margin-top:7px">Not loaded.</div>
    </details>
  </div>

  <div id="experimentscard" class="card" style="display:none;">
    <div class="cardhead"><strong>🧪 Validation</strong><span class="spacer"></span><span class="muted">tests for Systems · cohorts · holdouts · negative results retained</span><button class="mini" onclick="loadResearchExperimentsPage()">Refresh</button></div>
    <div class="r141lead"><b>Only the decision matters:</b> what is being tested, how many exact outcomes exist, and what evidence is still needed before promotion.</div>
    <div id="r138ExperimentsBody">Loading compact validation digest…</div>
    <details class="r141diag" ontoggle="if(this.open)loadResearchExperimentDetails()"><summary>Show immutable test registry, exclusions, state funnel, and proper-score diagnostics</summary><div id="r138ExperimentsDetail" class="r138grid" style="margin-top:8px">Not loaded.</div></details>
  </div>

  <div id="evidencecard" class="card" style="display:none;">
    <div class="cardhead"><strong>📐 Evidence</strong><span class="spacer"></span><span class="muted">collector funnels · exact routes · fills/cancels · causes · capacity</span><button class="mini" onclick="loadResearchEvidencePage()">Refresh</button></div>
    <div class="r141lead"><b>Default view:</b> real alerts, exact route coverage, and promotion blockers. Maker and taker are different experiments. Full funnels and controls remain below.</div>
    <div id="r138EvidenceBody">Loading compact evidence digest…</div>
    <details class="r141diag" ontoggle="if(this.open)loadResearchEvidenceDetails()"><summary>Show collector funnels, route ledger, cross-venue rules, EVI, and cause graph</summary><div id="r138EvidenceDetail" class="r138grid" style="margin-top:8px">Not loaded.</div></details>
  </div>

  <div id="datacard" class="card" style="display:none;">
    <div class="cardhead"><strong>🗃 Data</strong><span class="spacer"></span><span class="muted">source clocks · schema drift · replay chain · official venue notices</span><button class="mini" onclick="loadResearchDataPage()">Refresh</button></div>
    <div class="r141lead"><b>Default view:</b> unhealthy or stale feeds first. Healthy source clocks stay hidden unless you expand diagnostics.</div>
    <div id="r138DataBody">Loading compact data-health digest…</div>
    <details class="r141diag" ontoggle="if(this.open)loadResearchDataDetails()"><summary>Show every source clock, replay receipt, and official venue notice</summary><div id="r138DataDetail" class="r138grid" style="margin-top:8px">Not loaded.</div></details>
  </div>

  <div id="operationscard" class="card" style="display:none;">
    <div class="cardhead"><strong>🛠 Operations</strong><span class="spacer"></span><span class="muted">readiness · governance · security · recovery</span><button class="mini" onclick="loadResearchOperationsPage()">Refresh</button></div>
    <div class="r141lead"><b>Default view:</b> ready or not, ARM/AUTO state, kill switch, and the current operational blocker.</div>
    <div id="r138OperationsBody">Loading compact operations digest…</div>
    <details class="r141diag" title="restore has no automatic or HTTP action" ontoggle="if(this.open)loadResearchOperationsDetails()"><summary>Show governance, compliance, and recovery diagnostics</summary><div id="r138OperationsDetail" class="r138grid" style="margin-top:8px">Not loaded.</div></details>
    <div class="r138links">
      <button class="mini" onclick="uiTab('logs')">📜 Logs</button><button class="mini" onclick="uiTab('settings')">⚙ Settings</button>
      <button class="mini" onclick="uiTab('ml')">🤖 ML research</button><button class="mini" onclick="uiTab('combos')">🧩 Combo lab</button>
      <button class="mini" onclick="uiTab('signals')">🎛 Model controls</button>
      <button class="mini" onclick="uiTab('whales')">🐋 Feeds</button><button class="mini" onclick="uiTab('flow')">🌊 Flow</button>
      <button class="mini" onclick="uiTab('xvenue')">🌉 Cross-venue</button>
      <button class="mini" onclick="uiTab('history')">🗂 History</button><span class="muted">🛟 Snapshot creation is offline CLI-only with the suite stopped, preventing a heavy live-DB copy.</span>
    </div>
  </div>

  <!-- R59 (operator: "put feeds, flow, and smart money in their own dropdown list"): the FEEDS /
       FLOW / CROSS-VENUE panels left the main grid and are toggled FULL-VIEWPORT surfaces now,
       opened from the FEEDS nav dropdown (statscard pattern). Renderer target IDs unchanged; the
       global refresh loop keeps polling them exactly as before, open or not. The 3 venue columns
       now get ~a third of the viewport each — no more squeezed/overlapping text. -->
  <!-- R56 FEEDS panel (operator: "for feeds just make a single window for feeds - the whale feeds
       (kalshi, polyus, polyint), single window that splits into 3 for the 3 venues"). The three whale
       cards became venue COLUMNS; every loader target ID (whales / pmuswhales / pwhales / pwfeed) is
       a dedicated element unchanged, so the renderers keep working untouched. Explainer paragraphs
       moved verbatim into the ⓘ tooltips (operator: "mini window descriptions take up all the space"). -->
  <div id="feedscard" class="card split" style="display:none;">
    <div class="cardhead"><strong>📡 Whales</strong><span class="spacer"></span><span class="muted" style="font-size:11px;">large aggressive prints · 3 venues</span></div>
    <!-- R63 1d: LEFT half = Kalshi (top) + Poly US (bottom); RIGHT half = Poly-int with the rich
         trader columns (rank ★ / trader / all-time PnL restored as real columns). -->
    <div class="vsplit" style="display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;min-height:0;flex:1;grid-auto-rows:minmax(0,1fr);">
      <div style="display:flex;flex-direction:column;gap:8px;min-width:0;min-height:0;max-height:100%;overflow:hidden;">
        <div id="fcKal" style="min-width:0;overflow:auto;flex:1;min-height:0;">
          <div class="vhead"><span class="vchip" title="Kalshi">🟩 KAL</span><span class="inf" title="Large aggressive prints on the most active markets — what the big money is hitting right now. Green = bought YES, red = bought NO.">i</span><span class="vstat" id="kwCount"></span><span class="vctl">min $<input id="minK" type="number" min="0" step="100" value="0" oninput="loadWhales()"> sort <a href="#" id="kSortAmt" onclick="setKSort('amt');return false;">$</a> <a href="#" id="kSortRec" onclick="setKSort('recent');return false;">new</a></span></div>
          <div id="whales" style="margin-top:4px;">Scanning…</div>
        </div>
        <div id="fcPus" style="min-width:0;overflow:auto;flex:1;min-height:0;">
          <div class="vhead"><span class="vchip" title="Polymarket US">🇺🇸 PUS</span><span class="inf" title="Recent large aggressive (taker) prints on Poly US — anonymous sizes (it's a regulated exchange), newest first.">i</span><span class="vstat" id="puswCount"></span><span class="vctl">min $<input id="minPus" type="number" min="0" step="100" value="0" oninput="loadPolyUS()"> sort <a href="#" id="pusSortAmt" onclick="setPusSort('amt');return false;">$</a> <a href="#" id="pusSortRec" onclick="setPusSort('recent');return false;">new</a></span></div>
          <div id="pmuswhales" style="margin-top:4px;">Scanning…</div>
        </div>
      </div>
      <div id="fcInt" style="min-width:0;overflow:auto;max-height:100%;">
        <div class="vhead"><span class="vchip" title="Polymarket international (on-chain)">🟦 INT</span><span class="inf" title="Largest aggressive Polymarket trades right now, ranked by dollars. Polymarket is on-chain, so each whale shows a wallet/name.">i</span><span class="vstat" id="pwfeed"></span><span class="vctl">min $<input id="minP" type="number" min="0" step="500" value="0" oninput="loadPoly()"> sort <a href="#" id="pSortAmt" onclick="setPSort('amt');return false;">$</a> <a href="#" id="pSortRec" onclick="setPSort('recent');return false;">new</a></span></div>
        <div id="pwhales" style="margin-top:4px;">Scanning…</div>
      </div>
    </div>
  </div>

  <!-- R56 FLOW panel ("a single window for flow for the 3 venues"): Kalshi whale consensus +
       Poly US own-data taker flow + Poly-int leaderboard consensus, split by venue. -->
  <div id="flowcard" class="card split" style="display:none;">
    <div class="cardhead"><strong>🌊 Flow — where aggressive money leans</strong><span class="spacer"></span><span class="muted" style="font-size:11px;">one-sided money · 3 venues</span></div>
    <div class="vsplit" style="display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;min-height:0;flex:1;grid-auto-rows:minmax(0,1fr);">
      <div id="flKal" style="min-width:0;overflow:auto;max-height:100%;">
        <div class="vhead"><span class="vchip" title="Kalshi">🟩 KAL</span><span class="inf" title="Kalshi markets where big aggressive trades lean one way. Kalshi is anonymous, so this is trade flow — not a verified win-rate (no way to rank Kalshi traders by skill).">i</span><span class="vstat">aggressive money</span></div>
        <div id="ksmart" style="margin-top:4px;">Scanning…</div>
      </div>
      <div id="flPus" style="min-width:0;overflow:auto;max-height:100%;">
        <div class="vhead"><span class="vchip" title="Polymarket US">🇺🇸 PUS</span><span class="inf" title="Poly US is a regulated exchange (anonymous — no wallets / leaderboard), so this is its OWN executed taker tape: which side the aggressive money is hitting. Live games first.">i</span><span class="vstat">aggressive money</span></div>
        <div id="pmusflow" style="margin-top:4px;">Scanning…</div>
      </div>
      <div id="flInt" style="min-width:0;overflow:auto;max-height:100%;">
        <div class="vhead"><span class="vchip" title="Polymarket international (on-chain)">🟦 INT</span><span class="inf" title="Markets where multiple Polymarket profit-leaderboard traders hold the same side (live on-chain positions). Rep = a 0–100 signal score from how many agree, how elite the top-ranked one is, and their combined all-time P&amp;L. Higher = stronger copy signal.">i</span><span class="vstat">profit-leaderboard wallets</span></div>
        <div id="poly" style="margin-top:4px;">Scanning…</div>
      </div>
    </div>
  </div>

  <!-- R57 CROSS-VENUE panel: renderBoth draws the OddsJam-style aggregator from /api/arb rows —
       KAL/PUS/INT YES-¢ columns per matched event (PolyUS via has_polyus/polyus_pct), EDGE-sorted,
       ▸ expands the lock/opposite detail; the smart-money agreement sub-list sits below it. -->
  <div id="xvenuecard" class="card" style="display:none;">
    <div class="cardhead"><strong>🌉 Cross-venue — same event, all venues</strong><span class="spacer"></span><span class="muted" style="font-size:11px;">same event · side-aligned</span><span class="inf" title="Events priced on multiple venues, compared on the SAME outcome (shown as &quot;→ side&quot;). Green cell = the cheapest YES for that side; Edge = the widest venue gap in points; ▸ shows the tradeable-lock detail. Settlement rules can still differ — verify each before trading.">i</span></div>
    <div id="both" style="margin-top:6px;">Scanning…</div>
  </div>

  <div id="book" class="card" style="display:none;">
    <div class="cardhead"><strong id="bookTitle">Order book</strong><span class="spacer"></span><button onclick="hideBook()">Close</button></div>
    <div class="muted">Left = buyers (YES bid). Right = sellers (YES ask). Price is the implied chance; size is contracts. Updates live.</div>
    <div id="bookFee" class="muted" style="margin-top:2px;"></div><!-- R70-B #2: the series' EFFECTIVE venue fee schedule (per-series multiplier, /series/fee_changes) -->
    <div class="books">
      <table><thead><tr><th>Buy YES @</th><th class="num">size</th></tr></thead><tbody id="bids"></tbody></table>
      <table><thead><tr><th>Sell YES @</th><th class="num">size</th></tr></thead><tbody id="asks"></tbody></table>
    </div>
    <div id="bookBets" style="margin-top:8px;"></div><!-- R67f: this market's whale/bets log rides in the same popup -->
  </div>

  <!-- R60 WIDGET GRID (replaces the fixed R59 paper zone + live blotter): shell row 3. Widgets are
       built by initWidgets() from the WREG registry — every legacy renderer target ID
       (paperSummary / riskBox / portSessChart / paperPositions / pipestrip / liveHead / liveProps /
       liveCombos / livePosGrid / liveOrdersGrid / liveHistGrid / liveLogGrid …) lives INSIDE a
       widget body now, so the existing loaders keep painting regardless of which tab is visible. -->
  <div id="wgrid"></div>

  <div id="parlaycard" class="card" style="display:none;width:min(1560px,98vw);max-width:none;max-height:92vh;">
    <div class="cardhead"><strong>🎰 Combos</strong><span class="spacer"></span><span class="muted">paper — per-platform, own portfolio</span><button onclick="resetParlayPnL()" style="margin-left:10px;color:var(--warn)" title="wipe the combo portfolio's realized P&amp;L (deletes settled combos; keeps open ones)">↺ Reset P&amp;L</button><button onclick="loadParlay()" style="margin-left:6px;">Refresh</button><button onclick="closeParlay()" style="margin-left:6px;">Close</button></div>
    <div class="muted" style="font-size:12.5px;margin:4px 0 10px;">A combo is one <b>2-6 leg</b>, all-or-nothing bet: it pays out only if <b>every</b> leg hits. Combo price = product of the leg prices, so the payout multiplies (e.g. two 0.70 legs → 0.49 cost, ~2.04× if both win). Legs must be on the <b>same</b> venue — Kalshi with Kalshi, Poly US with Poly US. The six-leg ceiling applies to suggestions, Paper AUTO, Combo Lab, and LIVE Kalshi RFQs. ML-only and positive-single-leg combinations remain research until an exact typed conjunction earns its own authenticated RFQ quote, identical Paper acceptance, untouched proof, and current promotion governance.</div>
    <div id="parlaySummary" class="muted" style="margin:8px 0;">Loading…</div>
    <div class="cardhead" style="margin-top:8px;"><strong>Open combos</strong></div>
    <div id="parlayOpen" style="margin-top:4px;"></div>
    <div class="cardhead" style="margin-top:16px;"><strong>💡 AUTO suggestions</strong><span class="spacer"></span><span class="muted">read-only here · Paper AUTO owns placement</span></div>
    <div id="parlaySuggest" style="margin-top:4px;">Loading…</div>
    <div class="cardhead" style="margin-top:16px;"><strong>🗂 Settled</strong></div>
    <div id="parlaySettled" style="margin-top:4px;"></div>
    <div class="cardhead" style="margin-top:18px;"><strong>🤖🎲 New-ML Combo Paper</strong><span class="spacer"></span><span class="muted">separate $600 portfolio · current book-native predictions only</span><button onclick="resetMLComboPaper()" style="margin-left:10px;color:var(--warn)">↺ Reset</button></div>
    <div class="muted" style="font-size:12px;margin:2px 0 5px">Kalshi rows must fit a current RFQ collection, but remain Paper/unproved. PolyUS rows collect Paper evidence only and are not LIVE-transferable.</div>
    <div id="mlComboPaperSummary" class="muted">Loading…</div>
    <div id="mlComboPaperRows" style="margin-top:4px"></div>
    <div class="cardhead" style="margin-top:16px;"><strong>🧪 Combo Lab grading</strong><span class="spacer"></span><span class="muted">did historical same-venue combinations survive joint settlement?</span><button onclick="loadParlayBacktest()" style="margin-left:10px;">Run</button></div>
    <div class="muted" style="font-size:12px;margin:2px 0 4px">Historical resolved-market snapshot plus prospective 2–6-leg Combo Lab cohorts. Settlement-graded synthetic net/$1 is a counterfactual result, not placed profit. The legacy nominated-system cohort remains research-only unless an identical combo earns separate authenticated route, Paper, and untouched holdout proof.</div>
    <div id="parlayBacktest" class="muted" style="margin-top:2px;">Click Run to grade the research sample.</div>
    <div id="parlayMsg" class="muted" style="margin-top:8px;"></div>
  </div>

  <div id="stopscard" class="card" style="display:none;">
    <div class="cardhead"><strong>🎯 Edit take-profit / stop-loss</strong><span class="spacer"></span><button onclick="closeStops()">Cancel</button></div>
    <div class="muted" id="stopsSub" style="margin:6px 0 10px;"></div>
    <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end;">
      <label class="fld">Take-profit ¢<input id="stopsTP" type="number" min="1" max="99" placeholder="none"></label>
      <label class="fld">Stop-loss ¢<input id="stopsSL" type="number" min="1" max="99" placeholder="none"></label>
    </div>
    <button onclick="saveStops()" style="margin-top:12px;">Save</button>
    <div class="muted" style="margin-top:8px;font-size:12px;">Leave a box blank (or 0) to clear that stop. Prices are the side's price in cents.</div>
    <div id="stopsMsg" class="muted" style="margin-top:8px;"></div>
  </div>

  <div id="statscard" class="card" style="display:none;">
    <div class="cardhead"><strong>📈 Stats</strong><span class="spacer"></span><span class="muted">Paper simulation — not exchange profit</span><button onclick="resetPnL()" style="margin-left:10px;color:var(--warn)" title="resets the modeled Paper P&amp;L graph only — open simulations and history are kept">↺ Reset P&amp;L</button></div>
    <div id="statsSummary" class="muted" style="margin:8px 0;">Loading…</div>
    <div class="muted" style="font-size:12px;margin:2px 0 4px">Net P&amp;L over time (after fees) <span style="font-size:11px;margin-left:8px">· <a href="#" id="pnlR_all" onclick="setPnlRange('all');return false;">All</a> <a href="#" id="pnlR_week" onclick="setPnlRange('week');return false;">Week</a> <a href="#" id="pnlR_day" onclick="setPnlRange('day');return false;">Today</a></span></div>
    <div id="pnlchart" style="margin-bottom:10px;"></div>
    <div id="statsBySource"></div>
    <div class="cardhead" style="margin-top:16px;"><strong>🗃 Legacy signal diagnostics</strong><span class="spacer"></span><span class="muted">archival signal-price discovery only · cannot rank, prove, or authorize a trade</span></div>
    <div id="statsBacktest" style="margin-top:6px;"></div>
    <div class="muted" style="margin-top:12px;font-size:12.5px;">These are closed <b>Paper simulations</b>. A row counts as a modeled win when its simulated position closes — manually, by TP/SL, or at resolution. “By strategy” is a research comparison only: it is not exchange profit evidence and cannot promote, size, or authorize LIVE.</div>
  </div>

  <!-- R56 (operator: "livemarkets as a tab"): the old wide grid card, verbatim, as a toggled surface.
       openMarkets()/closeMarkets() follow the statscard pattern; loaders keep polling it in refresh(). -->
  <div id="marketscard" class="card" style="display:none;">
    <div class="cardhead"><strong>📈 Live markets</strong> <span class="inf" title="&quot;Chance&quot; is what the market implies right now (its price). Higher 24h volume = more people trading it. Click a market for its order book.">i</span> <button class="mini" onclick="document.getElementById('rows').scrollIntoView({block:'start'})">Kalshi ↑</button> <button class="mini" onclick="document.getElementById('pmhead').scrollIntoView({block:'start'})">Poly ↑</button> <button class="mini" onclick="document.getElementById('pmushead').scrollIntoView({block:'start'})">Poly US ↑</button> <input id="lmsearch" oninput="applyLiveFilter();mktSearchDeb()" onkeydown="if(event.key==='Enter'){applyLiveFilter();mktSearchGo();}" placeholder="🔎 search all markets…" title="Kalshi: server-side search over the FULL universe (title/ticker) — fetches on demand. Poly lists: filters the rows below." style="margin-left:8px;padding:3px 8px;border-radius:6px;border:1px solid rgba(255,255,255,.15);background:rgba(255,255,255,.05);color:var(--text);font-size:12px;width:200px;"><button class="mini" onclick="mktLoadFirst()" style="margin-left:6px;font-weight:700">Load markets</button><span class="spacer"></span></div>
    <div id="note"></div>
    <!-- R102 STRUCTURAL GAME TREE (operator ask): games anchored by the canonical cross-venue join
         (gameident.go) render hierarchically — game row → submarkets grouped by type, each row
         showing BOTH venues' prices side-by-side where both list it. Non-sports markets keep the
         flat venue lists below. Lazy per R98: fetches on Markets-tab open only. -->
    <div id="mkSecG">
    <div class="cardhead"><strong>🧩 Games — both venues</strong> <span class="inf" title="Every sports market is anchored to its game using the venues' own metadata (teams, start time, market type) — no name guessing. A row shows Kalshi and Poly US prices side-by-side when both list the same submarket. ↔NO marks a Poly US spread quoted from the other team (its YES = the Kalshi NO).">i</span><span class="spacer"></span><span class="muted" id="gtcount"></span></div>
    <div id="gametree"><span class="muted">Loading games…</span></div>
    </div>
    <!-- R106 UNIVERSAL TREE (operator: every genre incl. unknown): genre → event → member markets,
         genres from VENUE series/category metadata only. Sports' anchored games stay in the 🧩
         section above; unanchored sports + every other genre live here. Fully lazy. -->
    <div id="mkSecT">
    <div class="cardhead" style="margin-top:14px;"><strong>🌳 All markets — by genre</strong> <span class="inf" title="Every market grouped by the venue's own category metadata (series category on Kalshi, league/category on Poly US, event tags on Poly-int) — never guessed from titles. Unknown/new venue categories get their own node; anything without category metadata sits under Other/Unknown, still grouped by event.">i</span><span class="spacer"></span><span class="muted" id="mtcount"></span></div>
    <div id="markettree"><span class="muted">Loading genres…</span></div>
    </div>
    <!-- R63 1b: the three venue lists share ONE column set — MARKET | CHANCE | SIGNAL | BID/ASK |
         24H VOL | RESOLVES | LIVE | ≣ | ↗ (blank cell when a venue lacks a field). 1c: +Y/+N gone. -->
    <div id="mkSecK">
    <div class="cardhead"><strong>🟩 Kalshi — live</strong><span class="spacer"></span><span class="muted" id="count"></span></div>
    <table class="mkt">
      <colgroup><col><col style="width:40px"><col style="width:40px"><col style="width:56px"><col style="width:74px"><col style="width:44px"><col style="width:64px"><col style="width:100px"><col style="width:36px"><col style="width:30px"><col style="width:30px"></colgroup>
      <thead><tr><th>Market</th><th class="r">Bid</th><th class="r">Ask</th><th class="c">Chance</th><th class="c">Strategy</th><th class="c" title="24h price move ¢">Δ</th><th class="r">24h Vol</th><th class="r">Resolves</th><th class="c" title="actively trading right now">Live</th><th class="c" title="order book + this market's whale prints (popup)">≣</th><th class="c" title="open on venue">↗</th></tr></thead>
      <tbody id="rows"><tr><td colspan="11" class="muted" style="padding:10px 6px"><button class="mini" onclick="mktLoadFirst()" style="padding:3px 12px;font-weight:700">Load markets</button><span style="margin-left:10px">Markets load on demand now (R75) — press Load, or search the whole universe above.</span></td></tr></tbody>
    </table>
    </div>
    <div id="mkSecI">
    <div class="cardhead" id="pmhead" style="margin-top:14px;"><strong>🟦 Poly-int — live</strong><span class="spacer"></span><span class="muted" id="pmcount"></span></div>
    <table class="mkt">
      <colgroup><col><col style="width:40px"><col style="width:40px"><col style="width:56px"><col style="width:74px"><col style="width:44px"><col style="width:64px"><col style="width:100px"><col style="width:36px"><col style="width:30px"><col style="width:30px"></colgroup>
      <thead><tr><th>Market</th><th class="r">Bid</th><th class="r">Ask</th><th class="c">Chance</th><th class="c">Strategy</th><th class="c" title="24h price move ¢">Δ</th><th class="r">24h Vol</th><th class="r">Resolves</th><th class="c" title="a trade printed in the last ~2 min">Live</th><th class="c" title="who bet this — ★ smart-money bets popup (no public order-book endpoint)">≣</th><th class="c" title="open on venue">↗</th></tr></thead>
      <tbody id="pmrows"><tr><td colspan="11" class="muted">Loading…</td></tr></tbody>
    </table>
    </div>
    <div id="mkSecP">
    <div class="cardhead" id="pmushead" style="margin-top:14px;"><strong>🇺🇸 Poly US — live</strong><span class="spacer"></span><span class="muted" id="pmuscount"></span></div>
    <table class="mkt">
      <colgroup><col><col style="width:40px"><col style="width:40px"><col style="width:56px"><col style="width:74px"><col style="width:44px"><col style="width:64px"><col style="width:100px"><col style="width:36px"><col style="width:30px"><col style="width:30px"></colgroup>
      <thead><tr><th>Market</th><th class="r">Bid</th><th class="r">Ask</th><th class="c">Chance</th><th class="c">Strategy</th><th class="c" title="24h price move ¢">Δ</th><th class="r" title="R67m: Poly US publishes no 24h window — this is the TOTAL notionalTraded snapshot from the last book fetch (≤40 books sampled per 45s pass); blank = not sampled, not zero. NOT comparable to the Poly-int 24h $ column.">Vol $ (bk)</th><th class="r" title="Poly US does not publish a resolve time in its public market feed — blank by design">Resolves</th><th class="c" title="game live — score in the tooltip">Live</th><th class="c" title="whale prints popup (Poly US has no public order book)">≣</th><th class="c" title="open on venue">↗</th></tr></thead>
      <tbody id="pmusrows"><tr><td colspan="11" class="muted">Loading…</td></tr></tbody>
    </table>
    </div>
  </div>

  <div id="edgecard" class="card" style="display:none;">
    <div class="cardhead"><strong>🔬 Edge finder</strong><span class="spacer"></span><span class="muted">which conditions actually win</span><button onclick="closeEdge()" style="margin-left:10px;">Close</button></div>
    <div class="muted" style="font-size:12.5px;margin:4px 0 10px;">Win-rate of each strategy broken down by every logged feature — entry price, conviction, momentum, flow strength, book imbalance, spread, time-to-resolve. Resolved signals only; color-scaled red→green. Small buckets (low n) are noisy — weigh by count. This is the evaluate→adjust loop: find the buckets that win, then tighten the strategy to them.</div>
    <div id="edgeBody">Loading…</div>
  </div>

  <div id="mlcard" class="card" style="display:none;">
    <div class="cardhead"><strong>🤖 New ML</strong><span class="spacer"></span><span class="muted">book-native probabilities and Paper results</span><span class="inf" title="Book-native-v2 can take exploratory Paper samples once its three-day chronological fit is valid. LIVE remains independently locked until a later untouched replication passes. Old signal-price models and P&L are archived and never appear as New ML.">i</span></div>
    <details id="mlWhatNow" style="background:#0d1117;border:1px solid #30363d;border-radius:8px;padding:7px 10px;margin:6px 0 10px;font-size:12px;line-height:1.5;">
      <summary style="cursor:pointer"><b>What this means</b></summary>
      <div style="margin-top:6px">The displayed AUC, Brier, log loss, calibration, picks, and P&amp;L belong only to <b>book-native-v2</b>. Model-holdout scores and settled Paper-simulation scores are shown separately. A three-day event-separated score is labeled <b>Paper provisional</b>; it is research-only, not exchange profit evidence, and cannot authorize LIVE. LIVE stays locked until its separate untouched replication passes. Legacy ML is retained in exports only.</div>
    </details>
    <div id="mlBody">Loading…</div>
    <!-- R63 3e: the Top-scored list renders into its OWN container so the ml-top-scored widget can
         adopt it (single-instance DOM, renderer targets by id wherever it lives). -->
    <div id="mlTopScored" style="margin-top:6px;"></div>
  </div>

  <div id="curvescard" class="card" style="display:none;">
    <div class="cardhead"><strong>📈 Curves</strong><span class="spacer"></span><select id="curvesPlat" onchange="loadCurves()" style="font-size:12px"><option value="">All venues</option><option value="kalshi">Kalshi</option><option value="polyus">Poly US</option><option value="polymarket">Poly-int</option></select><button onclick="closeCurves()" style="margin-left:10px;">Close</button></div>
    <div class="muted" style="font-size:12.5px;margin:4px 0 10px;">Research views from Paper simulations and signal replay: <b>modeled net by signal</b>, the <b>modeled EV profile</b>, ML <b>calibration</b>, a <b>Monte Carlo</b> re-deal, and <b>stop / sizing simulations</b>. Paper placement and visible-book assumed fills are not exchange profit evidence and cannot promote, size, or authorize LIVE. Authenticated LIVE results remain separate.</div>
    <div id="curvesInfo" style="display:none;background:#0d1117;border:1px solid #30363d;border-radius:8px;padding:10px 12px;margin:0 0 10px;font-size:12.5px;line-height:1.5;"></div>
    <!-- R77 item 4: curvesBody is a PERMANENT scaffold of section divs (ids stable, .rsec = research
         section). loadCurves fills each section; the Research groups toggle section VISIBILITY only
         (Performance: src+mc · Edge: ev+cal · Labs: stop+kelly). The 'curves' grid widget still
         adopts #curvesBody whole — wAdoptSync un-hides every .rsec when a widget owns it. -->
    <div id="curvesBody"><div id="cvStatus">Loading…</div><div id="cvSecSrc" class="rsec"></div><div id="cvSecEV" class="rsec"></div><div id="cvSecCal" class="rsec"></div><div id="cvSecMC" class="rsec"></div><div id="cvSecStop" class="rsec"></div><div id="cvSecKelly" class="rsec"></div></div>
  </div>

  <!-- R77 item 4: the replay card is the LABS group's centerpiece (replay sim + auto-tune optimizer).
       Its body is a scaffold of .rsec sections so the other groups can borrow slices: spot-momentum +
       per-signal feature buckets show under EDGE, the exit comparison under EXITS, the combo/ML-parlay
       backtest under BACKTEST. The 'replay-backtest' grid widget still adopts #replayBody whole. -->
  <div id="replaycard" class="card" style="display:none;">
    <div class="cardhead"><strong>🧪 Replay lab</strong><span class="spacer"></span><span class="muted">research-only replay of logged signals at modeled flat stakes</span><button onclick="closeReplay()" style="margin-left:10px;">Close</button></div>
    <div class="muted" style="font-size:12.5px;margin:4px 0 8px;">Every RESOLVED signal is replayed as a hypothetical flat-stake bet using logged signal-time prices and modeled fees/fills. The displayed <b>MODELED NET</b> compares signals and buckets; green means that replay won, not that an exchange order filled or made money. This research cannot promote, size, or authorize LIVE.</div>
    <div id="replayInfo" class="muted rchrome" style="margin:0 0 8px;padding:7px 10px;background:var(--card2);border:1px solid var(--line);border-radius:8px;font-size:12.5px;">Click the ⓘ on any input for its simulation assumption. Per-bet cap / Max-exp drive the <b>Concurrent model</b> · Capacity / Max-hold drive the <b>Sequential model</b> · Flat stake drives only the per-strategy modeled-net buckets. None is exchange profit evidence.</div>
    <div id="replayTunables" class="rchrome" style="margin:0 0 10px;display:flex;align-items:center;gap:6px;flex-wrap:wrap"><span class="muted" style="font-size:12.5px">Bank $</span><input id="replayBank" type="number" min="1" step="50" value="500" style="width:66px" onchange="loadReplay()"><span onclick="bi('bank')" style="cursor:pointer;color:var(--accent);font-weight:700" title="what this does">ⓘ</span><span class="muted" style="font-size:12.5px">Flat stake $</span><input id="replayStake" type="number" min="1" step="1" value="50" style="width:70px" onchange="loadReplay()"><span onclick="bi('stake')" style="cursor:pointer;color:var(--accent);font-weight:700" title="what this does">ⓘ</span><span class="muted" style="font-size:12.5px">Custom Kelly &times;</span><input id="replayKelly" type="number" min="0" step="0.05" placeholder="e.g. 0.75" style="width:74px" onchange="loadReplay()"><span onclick="bi('kelly')" style="cursor:pointer;color:var(--accent);font-weight:700" title="what this does">ⓘ</span><span class="muted" style="font-size:12.5px">Conc per-bet cap</span><input id="replayPbcap" type="number" min="0" step="0.005" value="0.025" style="width:68px" onchange="loadReplay()"><span onclick="bi('pbcap')" style="cursor:pointer;color:var(--accent);font-weight:700" title="what this does">ⓘ</span><span class="muted" style="font-size:12.5px">Conc max-exp</span><input id="replayMaxexp" type="number" min="0" step="0.05" value="0.94" style="width:68px" onchange="loadReplay()"><span onclick="bi('maxexp')" style="cursor:pointer;color:var(--accent);font-weight:700" title="what this does">ⓘ</span><span class="muted" style="font-size:12.5px">Seq capacity $</span><input id="replayCap" type="number" min="0" step="100" value="3000" style="width:74px" onchange="loadReplay()"><span onclick="bi('cap')" style="cursor:pointer;color:var(--accent);font-weight:700" title="what this does">ⓘ</span><span class="muted" style="font-size:12.5px">Seq max-hold h</span><input id="replayHold" type="number" min="0.05" step="0.25" value="0.12" style="width:68px" onchange="loadReplay()"><span onclick="bi('maxhold')" style="cursor:pointer;color:var(--accent);font-weight:700" title="what this does">ⓘ</span><span class="muted" style="font-size:12.5px">Slippage &cent;</span><input id="replaySlip" type="number" min="0" step="0.5" value="2" style="width:58px" onchange="loadReplay()"><span onclick="bi('slip')" style="cursor:pointer;color:var(--accent);font-weight:700" title="what this does">ⓘ</span><label class="muted" style="font-size:12.5px;cursor:pointer"><input id="replayFokm" type="checkbox" checked onchange="loadReplay()" style="vertical-align:middle" title="R67h: MAKER (FOKM) is the default everywhere now — untick for a taker A/B run"> FOKM</label><span onclick="bi('fokm')" style="cursor:pointer;color:var(--accent);font-weight:700" title="what this does">ⓘ</span><span class="muted" style="font-size:12.5px">fill rate</span><input id="replayFill" type="number" min="0.05" max="1" step="0.05" value="0.85" style="width:58px" onchange="loadReplay()"><span onclick="bi('fill')" style="cursor:pointer;color:var(--accent);font-weight:700" title="what this does">ⓘ</span><span class="muted" style="font-size:12.5px">adverse</span><input id="replayAdv" type="number" min="0" max="1" step="0.05" value="0.15" style="width:54px" onchange="loadReplay()"><span onclick="bi('adv')" style="cursor:pointer;color:var(--accent);font-weight:700" title="what this does">ⓘ</span><button class="mini" onclick="loadReplay()">Run</button><button class="mini" onclick="autoTune()" title="grid-search these tunables against the walk-forward replay (real fees + measured fill model) and fill the boxes with the best drawdown-penalized setting">🎯 Auto-tune</button></div>
    <div id="tuneResult" class="rchrome" style="display:none;margin:0 0 10px;padding:7px 10px;background:var(--card2);border:1px solid var(--line);border-radius:8px;font-size:12.5px;"></div>
    <!-- R77 item 4 scaffold: loadReplay fills the sections; groups toggle visibility (Labs: conc ·
         Edge: mom+feat · Exits: exitcmp · Backtest: combo). Widget adoption un-hides all. -->
    <div id="replayBody"><div id="rpStatus">Loading…</div><div id="rpSecConc" class="rsec"></div><div id="rpSecMom" class="rsec"></div><div id="rpSecExitCmp" class="rsec"></div><div id="rpSecFeat" class="rsec"></div></div>
    <div id="rpSecCombo" class="rsec">
    <div class="cardhead" style="margin-top:14px;"><strong>🧪 Combo Lab grading</strong><span class="spacer"></span><span class="muted">Paper-simulation combinations + bounded historical research cohorts · not exchange profit evidence</span></div>
    <div id="replayParlay" style="margin-top:4px;">Loading…</div>
    </div>
  </div>

  <!-- R72-A #5 HORIZON: hours-out backtest in 15-min buckets (Research sub-panel). Read-only
       signal_log analytics — net EV/ct + n per 0.25h resolve-horizon bucket up to 8h, one strip
       per book context, with the LIVE config horizon cutoffs marked. -->
  <div id="horizoncard" class="card" style="display:none;">
    <div class="cardhead"><strong>⏳ Horizon</strong><span class="spacer"></span><span class="muted">net EV/ct by hours-to-resolve at signal time · 15-min buckets · up to 8h</span></div>
    <div class="muted" style="font-size:12.5px;margin:4px 0 8px;">Research-only clock buckets from resolved signal rows. Green/red show modeled net using logged prices and maker-blend fees; signal replay assumes entry/fill and is not exchange profit evidence. Dashed markers show current cutoffs. <b>AUTO</b> and <b>ML</b> are replay cohorts; <b>LIVE</b> alone contains rows priced from recorded LIVE fills. Replay rows cannot promote, size, or authorize LIVE.</div>
    <div id="horizonBody">Loading…</div>
  </div>

  <!-- R77 item 4 RESEARCH REORG: six groups, one consistent .cardhead style, EVERYTHING kept.
       Old → new: Curves §1 net-by-signal (+Total tradeable+venue splits) & §4 Monte Carlo → PERFORMANCE ·
       Edge finder + Curves §2 EV profile & §3 calibration + Replay spot-momentum & per-signal
       entry/conviction/category/momentum buckets → EDGE · Exit ladder + Replay exit-comparison → EXITS ·
       Stats signal-backtest table (INV column, borrowed) + combo A/B + N-leg ML parlays + Horizon →
       BACKTEST · Coverage matrix + famine/naming health → COVERAGE · Replay sim + auto-tune optimizer +
       Curves §5 stop lab & §6 kelly lab + honest-fill harness → LABS. Containers keep their ids; groups
       only reparent cards + toggle .rsec visibility (presentation-only). -->
  <div id="researchcard" class="card" style="display:none;">
    <div class="cardhead"><strong>🔬 Research</strong><span class="spacer"></span>
      <button id="resTab_performance" class="mini" onclick="showResearchSub('performance')" title="Paper-simulation net + research labels + per-venue splits · not exchange profit evidence">📊 Performance</button>
      <button id="resTab_edge" class="mini" onclick="showResearchSub('edge')" title="every feature-bin diagnostic: edge-finder bins, EV-by-entry-price hill, ML calibration, spot-momentum, per-strategy entry/conviction/category/momentum buckets">🔬 Edge</button>
      <button id="resTab_exits" class="mini" onclick="showResearchSub('exits')" title="modeled exits on recorded market-price paths after simulated entry; fills are not exchange-confirmed">🚪 Exits</button>
      <button id="resTab_systems" class="mini" onclick="showResearchSub('systems')" title="canonical book-native Systems Leaderboard and regime views + separate Combo Lab grading">🏁 Systems</button>
      <button id="resTab_coverage" class="mini" onclick="showResearchSub('coverage')" title="models × venues — logged / resolved / paper trades + a status chip per cell · plus signal-famine + naming health">🧭 Coverage</button>
      <button id="resTab_labs" class="mini" onclick="showResearchSub('labs')" title="replay sim + 🎯 auto-tune optimizer + stop lab + kelly lab + honest-fill harness">🧪 Labs</button></div>
    <div class="muted" style="font-size:12px;margin:2px 0 8px"><b>System</b> is the umbrella: a <b>model</b> estimates edge and emits a signal; an <b>execution route</b> proves maker/taker fills; a <b>strategy</b> turns those inputs into an executable policy. The Systems Leaderboard labels which layer each row measures.</div>
    <div id="resBody"></div>
    <div id="resExits" style="display:none">
      <div class="cardhead"><strong>🚪 Exit ladder</strong><span class="spacer"></span><span class="muted">modeled exits on recorded market-price paths after simulated entry · research only</span></div>
      <div id="resExitsBody">Loading…</div>
      <div id="resExitCmpHost"></div>
    </div>
    <!-- R73 COVERAGE MATRIX: families × venues off the EXISTING /api/curves payload
         (by_source_venue verdicts + R67k sig_log_venue counts) — no new endpoint. -->
    <div id="resCoverage" style="display:none">
      <div class="cardhead"><strong>🧭 Model coverage matrix</strong><span class="spacer"></span><span class="muted">every registered model × venue — logged L · resolved R · paper trades T · status</span></div>
      <div id="resHealth" class="muted" style="font-size:12px;margin:2px 0 8px"></div>
      <div id="resCoverageBody">Loading…</div>
    </div>
    <!-- R135c: SYSTEMS is the only primary profit-rate evidence surface. Legacy signal-price
         diagnostics remain archival under Stats and cannot rank, prove, or authorize a trade. -->
    <div id="resBT" style="display:none">
      <div class="cardhead"><strong>🏁 Systems Leaderboard</strong><span class="spacer"></span><span class="muted">one-share route evidence and research coverage · authenticated fills labeled separately</span></div>
      <div class="muted" style="font-size:12px;margin:2px 0 8px"><b>Which systems have usable exchange-fill evidence?</b> Profit-rate columns appear only for authenticated, fill-conditioned exchange cohorts. Visible-book one-share history assumed a fill and is shown as VOID/research coverage only; it cannot promote, size, or authorize LIVE. Net/day includes quiet calendar time, and unresolved rows never count as wins.</div>
      <div id="resLeaderboardBT" style="margin:4px 0 14px">Loading…</div>
      <div class="cardhead"><strong>🗓️ Systems regimes</strong><span class="spacer"></span><span class="muted">7d · 30d · 90d · all-time · exact clocks · UTC day-block uncertainty</span></div>
      <div class="muted" style="font-size:12px;margin:2px 0 8px">Same system, venue, and execution route across recent and older regimes. The 30-day lower bound is the only promotion-review lane; shorter windows expose decay but cannot grant authority. Missing native book/fill/fee rows stay missing rather than borrowing a signal price.</div>
      <div id="resSystemsRegime" style="margin:4px 0 14px">Loading…</div>
      <div id="resBTHost"></div>
    </div>
    <!-- R77 item 4: LABS extra — the honest-fill validation harness had a server report (/api/harness,
         exported) but NO surface anywhere in the UI. -->
    <div id="resLabs" style="display:none">
      <div class="cardhead"><strong>⚖ Honest-fill harness</strong><span class="spacer"></span><span class="muted">pre-registered maker-fill validation + kill criteria — measured venue truth vs the model</span></div>
      <div id="resHarnessBody">Loading…</div>
    </div>
    <!-- PERFORMANCE group — five fixed paper-portfolio banks + the EV-share allocation
         table the 30-min allocator applies to strategy stake weights (/api/alloc; exported).
         R79: the PLACEMENT POLICY table (family → ON / INVERTED / RETIRED / RESEARCH from the
         direct+inverted maker-net 95% LBs) renders right below it, same payload + cadence. -->
    <div id="resAlloc" style="display:none">
      <div class="cardhead"><strong>⚖ Legacy Paper allocation model</strong><span class="spacer"></span><span class="muted">five simulated portfolios · research display only · never sizes or authorizes cash</span></div>
      <div id="resAllocBody">Loading…</div>
    </div>
  </div>

  <div id="logscard" class="card" style="display:none;">
    <div class="cardhead"><strong>📜 Logs</strong><span class="spacer"></span>
      <button id="logTab_live" class="mini" onclick="showLogSub('live')">Live orders</button>
      <button id="logTab_audit" class="mini" onclick="showLogSub('audit')">Audit / Funnels</button>
      <button id="logTab_rejp" class="mini" onclick="showLogSub('rejp')" title="auto paper-pilot rejections (bets + combos)">Rejects: Paper</button>
      <button id="logTab_rejl" class="mini" onclick="showLogSub('rejl')" title="live proposal/combo drops + REFUSED placements">Rejects: Live</button>
      <button id="logTab_rejm" class="mini" onclick="showLogSub('rejm')" title="ML book buy-loop skips (band / implausible / EV floor / no live price)">Rejects: ML</button>
      <button id="logTab_name" class="mini" onclick="showLogSub('name')" title="ntfy naming fallbacks — tickers whose titles served raw">Naming</button>
      <button id="logTab_match" class="mini" onclick="showLogSub('match')" title="cross-venue + strike/submarket matchers — per-family hit rates + missed or guard-rejected pairings (date/type/kind gates, unparseable strike bins)">Match log</button>
      <button id="logTab_orders" class="mini" onclick="showLogSub('orders')" title="exact V2 order JSON previews — never sent (moved here from the retired ORDERS tab, R67e)">Orders</button>
      <button class="mini" onclick="downloadAllLogs(this)" title="download ONE json with every log: audit (accepts+errors+funnels), full live order trail, paper/live/ML rejections, naming fallbacks">⬇ All logs</button>
      <button class="mini" onclick="copyLogSub()" title="copy the current sub-tab as JSON">📋 Copy</button></div>
    <div class="muted" style="font-size:12px;margin:2px 0 8px">Every log in one place: full order JSON (places/cancels/WS-cancels/refusals), the persisted audit trail (PROPOSAL/COMBO FUNNEL lines every 5 min, guard skips, autotune applies, wxedge, resets), and the auto-pilot rejection ring. Naming fallbacks live in the readiness dots (name_fallbacks).</div>
    <!-- R107 Part 5: latency panel — live numbers + 1h sparklines from /api/latency (renders with the Logs tab) -->
    <div id="latpanel" style="font-size:12px;margin:2px 0 10px"></div>
    <div id="logsBody" style="font-size:12px;">Loading…</div>
  </div>

  <div id="historycard" class="card" style="display:none;">
    <div class="cardhead"><strong>🗂 History</strong><span class="spacer"></span><span class="muted">stat history + every closed bet</span><button onclick="clearHistory()" style="margin-left:10px;color:var(--warn)" title="wipe the closed-bet log, keep open positions">↺ Clear</button><button onclick="closeHistory()" style="margin-left:6px;">Close</button></div>
    <div id="histSummary" class="muted" style="margin:8px 0;">Loading…</div>
    <div class="muted" style="font-size:12px;margin:2px 0 4px">Net P&amp;L over time (after fees)</div>
    <div id="histpnlchart" style="margin-bottom:12px;"></div>
    <div class="cardhead" style="margin-top:8px;"><strong>By strategy</strong><span class="spacer"></span><span class="muted">closed Paper simulations by source · not exchange profit evidence</span></div>
    <div id="histBySource" style="margin-top:6px;"></div>
    <div class="cardhead" style="margin-top:16px;"><strong>Closed bets</strong><span class="spacer"></span><span class="muted" title="newest first">what we've done</span></div>
    <div id="histTrades" style="margin-top:6px;"></div>
    <div class="muted" style="margin-top:12px;font-size:12.5px;">Every closed Paper-simulation round: source, modeled entry → exit, close reason, and simulated P&amp;L. These records are research diagnostics, not authenticated exchange orders or profit evidence.</div>
  </div>

  <div id="betscard" class="card" style="display:none;">
    <div class="cardhead"><strong>👁 Who bet this</strong><span class="spacer"></span><span class="muted" id="betsTitle"></span><button onclick="closeBets()" style="margin-left:10px;">Close</button></div>
    <div class="muted" style="font-size:12.5px;margin:4px 0 10px;">Wallets that traded this market in the last ~13 min, shown only if they're on Polymarket's profit leaderboard (★) or have a high all-time P&amp;L. P&amp;L is pulled per-wallet from Polymarket — works for anyone, not just leaderboard names. Aggregated per trader + side.</div>
    <div id="betsBody"></div>
  </div>

  <div id="orderscard" class="card" style="display:none;">
    <div class="cardhead"><strong>🧾 Order JSON console</strong><span class="spacer"></span><span class="muted">preview only · never sent</span><button onclick="closeOrders()" style="margin-left:10px;">Close</button></div>
    <div class="muted" style="font-size:12.5px;margin:4px 0 10px;">The exact API order payload the bot WOULD send to Kalshi / Polymarket for each auto buy &amp; sell — built with a <b>random placeholder key</b> and placeholder signatures. <b>No live execution, nothing is sent.</b> Newest first.</div>
    <div id="ordersBody"></div>
  </div>

  <!-- R70 (audit §a P1): the orphaned #gatecard (AI GO/NO-GO panel) is GONE — openGate had zero
       callers ('gate' absent from UIPANELS/UITABS/SSE panelFns since the R62 tab rework), so the
       panel, its loader and its markdown renderer were pure dead weight polled every 4s. -->
  <div id="settingscard" class="card" style="display:none;">
    <div class="cardhead"><strong>⚙ Settings</strong><span class="spacer"></span><span class="muted">auto / AI tunables · applies live + saved to config.json</span></div>
    <div class="muted" style="font-size:12.5px;margin:4px 0 8px;">Edit the parameters the AUTO bot + AI use to size and qualify bets. Changes take effect on the next tick (paper). Blank a box to leave it unchanged.</div>
    <!-- R63 4e: relocated from the retired ⋯More menu. The reset-on-start checkbox moved AGAIN —
         it is now the ↺RST chip in bar1, directly left of RESET (operator). -->
    <div style="margin:2px 0 10px;display:flex;gap:16px;align-items:center;flex-wrap:wrap;">
      <span class="muted" style="font-size:9.5px" title="build version: git commit @ build time">v __BUILD_VERSION__</span>
    </div>
    <div id="settingsBody" style="overflow-y:auto;padding-right:6px;">Loading…</div>
    <button onclick="saveSettings()" style="margin-top:14px;">Save settings</button>
    <div id="settingsMsg" class="muted" style="margin-top:8px;"></div>
  </div>

  <div id="signalscard" class="card" style="display:none;">
    <div class="cardhead"><strong>🎛 Models</strong><span class="spacer"></span><span class="muted">signal models · on/off + invert · paper expression</span><button onclick="closeSignals()" style="margin-left:10px;">Close</button></div>
    <div id="signalsBody">Loading…</div>
  </div>

  <!-- Manual paper placement is retired. Historical manual rows remain visible in portfolio
       history, but there is no dashboard control or HTTP placement endpoint. -->
</div>

<script>
function setBadge(id,text,cls){var e=document.getElementById(id);if(!e)return;e.textContent=text;e.className="badge"+(cls?(" "+cls):"");}
// R60 CLOCK (bar1 right): 12-hour local + the ET venue twin — Kalshi/Poly settle on US Eastern.
function tickClk(){var el=document.getElementById('clk');if(!el)return;var d=new Date();
  var loc=d.toLocaleTimeString('en-US',{hour:'numeric',minute:'2-digit'});
  var et='';try{et=d.toLocaleTimeString('en-US',{hour:'numeric',minute:'2-digit',timeZone:'America/New_York'});}catch(e){}
  el.textContent=loc+(et?(' ('+et+' ET)'):'');}
// R73 RESET SWITCH (operator): the R63 arm/confirm two-step is REMOVED (its CONFIRM handler was
// broken in practice — the reset felt unfireable). One click on the plain ON/OFF-styled switch
// fires the existing POST /api/paper/reset IMMEDIATELY — it's paper-safe by design (closes open
// paper/ML positions at live marks, zeroes session P&L + graphs, KEEPS closed-bet history). The
// button shows a lit "RESETTING…" state for ~2s so the click visibly took, then reverts.
function resetNow(){
  var b=document.getElementById('resetBtn');
  if(b){if(b.disabled)return;b.disabled=true;b.textContent='RESETTING…';b.classList.add('on','live');}
  function done(){setTimeout(function(){var x=document.getElementById('resetBtn');if(x){x.disabled=false;x.textContent='RESET';x.classList.remove('on','live');}},2000);}
  fetch('/api/paper/reset',{method:'POST'}).then(function(r){return r.json();}).then(function(j){
    // R92: the server now RETRIES the sidecar book lock, so this is rare — but if the ML/shadow
    // books were still locked after every retry, SAY SO instead of silently looking done (the old
    // silent skip is exactly why "reset doesn't reset" got reported).
    if(j&&j.ml_lock_busy){alert('Reset ran, but the ML/shadow books were still locked by the sidecar after retries — click RESET once more.');}
    loadPaper();loadNetworth();loadStatsView();done();}).catch(function(){done();});
}
// R60 C1 elapsed helper: <60s → "37s" · <1h → "M:SS" · else "H:MM:SS" — the Bought columns.
function agoFmt(x){
  if(!x)return '';var t=(typeof x==='number')?x*1000:Date.parse(x);if(!t||isNaN(t))return '';
  var s=Math.max(0,Math.floor((Date.now()-t)/1000));
  if(s<60)return s+'s';
  var m=Math.floor(s/60),ss=('0'+(s%60)).slice(-2);
  if(s<3600)return m+':'+ss;
  return Math.floor(s/3600)+':'+('0'+(m%60)).slice(-2)+':'+ss;
}
// R60 C1 ET twin for venue-anchored rows (fills / history / log): "8:01 PM (ET 5:01 PM)".
function etFmt(x){
  if(!x)return '';var d=(typeof x==='number')?new Date(x*1000):new Date(x);if(isNaN(d.getTime()))return '';
  try{return d.toLocaleTimeString('en-US',{hour:'numeric',minute:'2-digit',timeZone:'America/New_York'});}catch(e){return '';}
}
function logWhen(ts){ // R60 C1: order-log stamps → 12-hour + ET twin; raw when unparseable
  if(!ts)return '';var t=Date.parse(ts);if(!t||isNaN(t))return escapeHtml(String(ts));
  var d=new Date(t),h=d.getHours(),ap=h>=12?'PM':'AM';h=h%12;if(h===0)h=12;
  var loc=h+':'+('0'+d.getMinutes()).slice(-2)+':'+('0'+d.getSeconds()).slice(-2)+' '+ap;
  var et=etFmt(ts);
  return loc+(et?(' <span style="opacity:.65">(ET '+et+')</span>'):'');
}
// R60 C2: green/red row wash by unrealized P&L, alpha 0.04–0.18 scaled vs the table max (variant 5).
function pnlTint(v,mx){
  v=v||0;if(!mx||mx<=0||Math.abs(v)<0.005)return '';
  var a=(0.04+0.14*Math.min(1,Math.abs(v)/mx)).toFixed(3);
  return ' style="background:rgba('+(v>=0?'22,199,132':'234,57,67')+','+a+')"';
}
// R70 (audit §a P2): toggleMode deleted — zero callers, and it targeted the retired id "mode".
// Exec mode is switched via POST /api/mode (curl / automation); the header shows the effective mode.
function fmtVol(v){v=Number(v)||0;if(v>=1000000)return (v/1000000).toFixed(1)+"M";if(v>=1000)return (v/1000).toFixed(1)+"k";return String(Math.round(v));}
function fmtClose(s){if(!s)return "";var d=new Date(s);if(isNaN(d.getTime()))return s.slice(0,16);return d.toLocaleString([], {month:"short",day:"numeric",hour:"numeric",minute:"2-digit"});}
function escapeHtml(s){return String(s).replace(/[&<>"']/g,function(c){return {"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c];});}

// ================= R138 RESEARCH-FIRST DIGEST =================
function r138Field(o){if(!o)return undefined;for(var i=1;i<arguments.length;i++){var k=arguments[i];if(o[k]!==undefined&&o[k]!==null)return o[k];}return undefined;}
function r138Num(v){v=Number(v);return isFinite(v)?v:0;}
function r138State(v){
  v=String(v||'UNKNOWN');var u=v.toUpperCase(),c='block';
  if(u.indexOf('ERROR')>=0||u.indexOf('FAILED')>=0||u.indexOf('CRITICAL')>=0)c='bad';
  else if(u==='HEALTHY'||u==='HEALTHY_EMPTY'||u.indexOf('REPLICATED_UNTOUCHED')>=0)c='ok';
  var plain={
    'COLLECTING_PARTIAL':'PARTLY COLLECTING','HEALTHY_EMPTY':'HEALTHY · NO MATCHES',
    'NEVER_RAN':'NOT RUN YET','READY_FOR_PREREGISTERED_INPUTS':'WAITING FOR FROZEN INPUTS',
    'WAITING_FOR_FROZEN_INPUTS':'WAITING FOR FROZEN INPUTS',
    'BLOCKED_NO_REPLICATED_ROUTE_LOWER_BOUNDS':'WAITING FOR PROVEN ROUTE RESULTS',
    'REPLICATED_UNTOUCHED':'PASSED UNTOUCHED TEST','ZERO AUTHORITY':'RESEARCH ONLY',
    'READY_NO_RECENT_CANDIDATE':'READY · NO RECENT CANDIDATE',
    'RECENT_ECONOMICS_NO_RECENT_CANDIDATE':'RECENT BOOK ROWS · NO NEW CANDIDATE',
    'ARCHIVED_BY_6_LEG_LIMIT':'ARCHIVED · OVER 6 LEGS','ARCHIVED_POLICY':'ARCHIVED',
    'HISTORICAL_UNMAPPED':'HISTORICAL · NO CURRENT PRODUCER','NEEDS_KEY':'NEEDS KEY',
    'CURRENT_UNREGISTERED':'CURRENT · REGISTRY GAP',
    'CONFIG_DISABLED':'DISABLED','SPECIFIED_NOT_IMPLEMENTED':'NOT IMPLEMENTED'
  };
  var label=plain[u]||v.replace(/_/g,' ');
  return '<span class="r138state '+c+'" title="raw state: '+escapeHtml(v)+'">'+escapeHtml(label)+'</span>';
}
// Reuse the dashboard-wide coalesced, abortable GET path. Research tabs can share expensive
// receipts; repeated repaints must never stack verification/database work or browser sockets.
var _r138Queue=Promise.resolve();
function r138Get(url){
  // Detailed research ledgers are deliberately serialized. Opening one diagnostic section must
  // never fan five large SQLite reports out beside live feed work again.
  var job=_r138Queue.catch(function(){}).then(function(){return jget(url,60000);});
  _r138Queue=job.then(function(){},function(){});
  return job;
}
function r138Settled(p){return p.status==='fulfilled'?p.value:{error:String((p.reason&&p.reason.message)||p.reason||'unavailable')};}
function r138Err(d){return d&&d.error?('<span style="color:var(--bad)">'+escapeHtml(d.error)+'</span>'):'';}
function r138Pane(title,body,edge){return '<div class="r138pane" style="--edge:'+(edge||'var(--accent)')+'"><h3>'+escapeHtml(title)+'</h3>'+body+'</div>';}
function r138Authority(d){return (d&&d.funded===false&&d.paper_authority===false&&d.live_authority===false)?r138State('ZERO AUTHORITY'):r138State('AUTHORITY UNKNOWN');}
function r138Table(head,rows){return '<div style="max-width:100%;overflow:auto"><table class="mkt" style="font-size:11px"><thead><tr>'+head.map(function(x){return '<th>'+escapeHtml(x)+'</th>';}).join('')+'</tr></thead><tbody>'+rows.join('')+'</tbody></table></div>';}

function r141Card(title,body,edge){return '<div class="r141card" style="--edge:'+(edge||'var(--accent)')+'"><h3>'+escapeHtml(title)+'</h3>'+body+'</div>';}
// Keep legacy allocation-mode mode4_* API/storage identifiers stable while translating them at
// the operator boundary. This is display-only and must not rename the separate SL/TP selector's
// mode 4 (auto-tuned exit ratio).
function r147OperatorTerms(v){return String(v==null?'':v).replace(/mode[_ -]?4/gi,'Adaptive Allocation Model');}
// Catalog counts are static code capability.  They must not be presented as runtime feed health
// or money authority.  A venue, side, or order-type change is a different typed variant.  Cross-
// venue input topology is shown separately because PINT may inform a K/PUS decision but can never
// become its execution venue.
function r147CatalogCard(cat,signalContracts){
  cat=cat||{};signalContracts=signalContracts||{};var sc=signalContracts.counts||signalContracts;
  var bases=Number(cat.base_systems||0),orders=Number(cat.order_originating_base_systems||0),typed=Number(cat.declared_typed_variants||cat.known_exact_execution_variants||0);
  var capable=sc.producer_capable_typed_variants!=null?Number(sc.producer_capable_typed_variants):Number(cat.producer_capable_typed_variants||0);
  var blocked=sc.producer_blocked_typed_variants!=null?Number(sc.producer_blocked_typed_variants):Number(cat.producer_blocked_typed_variants||0);
  var cross=sc.cross_venue_input_variants!=null?Number(sc.cross_venue_input_variants):Number(cat.cross_venue_input_variants||0);
  var bundleSystems=Number(cat.bundle_product_systems||0),bundleVariants=Number(cat.declared_bundle_product_variants||0),bundleConnected=Number(cat.connected_staged_bundle_variants||0),bundleBlocked=Number(cat.externally_blocked_bundle_variants||0);
  var external=Number(cat.externally_blocked_decision_systems||0),externalIDs=cat.externally_blocked_decision_system_ids||[];
  var feed=(sc.fresh_fed!=null)?('<b>'+Number(sc.fresh_fed)+' fresh-fed now</b>'):'<span class="muted">fresh-fed is runtime-only; static code capability is not counted as fresh</span>';
  var blockedLine=' · <b>'+blocked+' producer-blocked</b>';
  var externalLine=external?('<br><span style="color:var(--warn)"><b>'+external+' multi-leg decisions externally blocked</b>'+(externalIDs.length?(': '+escapeHtml(externalIDs.join(', '))):'')+'</span>'):'';
  var bundleLine=bundleSystems?('<br><b>'+bundleSystems+' multi-leg product systems · '+bundleVariants+' exact bundle variants</b> · '+bundleConnected+' staged connected / '+bundleBlocked+' externally blocked'):'';
  return '<span class="r141big">'+bases+'</span> base names · <b>'+orders+' order systems</b><br><b>'+typed+' typed venue/side/order variants</b>'+(capable?(' · '+capable+' producer-capable'):'')+blockedLine+(cross?(' · '+cross+' cross-venue-input'):'')+bundleLine+'<br>'+feed+externalLine+'<br><span class="muted">Different execution venue, side, or order type = a different variant. Multi-leg payoff products use exact bundle variants, never fake Y/N singles. Cross-venue inputs are separate from the order venue; PINT is input-only, while money routes are Kalshi or PolyUS. Controls, cohorts, and ledgers are not executable systems by themselves.</span>';
}
function r141Digest(host,render,retry){
  if(!host)return;
  jget('/api/research/digest',8000).then(function(d){
    if(d&&d.state==='WARMING'){
      host.innerHTML='<div class="r141card" style="--edge:var(--warn)"><h3>Research summary</h3><span class="r141big">🟡 Warming</span><br><span class="muted">'+escapeHtml(d.plain_language||'Building in the background.')+'</span></div>';
      if(retry)setTimeout(retry,3000);return;
    }
    if(d&&d.state==='FAILING'){
      host.innerHTML='<div class="r141card" style="--edge:var(--bad)"><h3>Research summary</h3><span class="r141big">🔴 Summary unavailable</span><br><span class="muted">'+escapeHtml(d.plain_language||'The summary could not be built yet.')+'</span>'+(d.error?'<div class="r141warn">'+escapeHtml(d.error)+'</div>':'')+'</div>';
      if(retry)setTimeout(retry,Math.max(3000,Number(d.retry_seconds||15)*1000));return;
    }
    render(d||{});
    if(d&&d.refresh_state==='WARMING')host.insertAdjacentHTML('beforeend','<div class="r141warn">🟡 Refreshing the compact summary in the background. Current cards are the last complete snapshot.</div>');
    if(d&&d.refresh_state==='RETRYING')host.insertAdjacentHTML('beforeend','<div class="r141warn">🟡 Refresh delayed; retrying automatically in '+Number(d.retry_seconds||15).toFixed(0)+'s. Current cards are the last complete snapshot.</div>');
    if(d&&d._report_stale){var when=d._report_last_good_at?new Date(d._report_last_good_at).toLocaleTimeString():'earlier';host.insertAdjacentHTML('beforeend','<div class="r141warn">'+escapeHtml(d._report_warning||'Refresh delayed.')+' Showing last good from '+escapeHtml(when)+'. Core feeds continue independently.</div>');}
  }).catch(function(e){host.innerHTML='<div class="r141card" style="--edge:var(--bad)"><h3>Report unavailable</h3>'+escapeHtml(e.message||String(e))+'<br><span class="muted">Core feeds continue independently. Retrying automatically.</span></div>';if(retry)setTimeout(retry,5000);});
}
function r144SampleBucket(m){m=Number(m||0);return m<=10?'🔴':(m<=40?'🟠':(m<=120?'🟡':(m<=500?'🟢':(m<=1000?'🔵':'🟣'))));}
function r145NetD(v){v=Number(v)||0;if(Math.abs(v)<0.00005)v=0;var d=Math.abs(v)>0&&Math.abs(v)<0.01?4:2;return (v>=0?'+':'-')+'$'+Math.abs(v).toFixed(d)+'/d';}
function r141SystemRows(rows){
  if(!rows||!rows.length)return '<span class="muted">No settled executable rows yet.</span>';
  return rows.map(function(r){var v=Number(r.net_per_day||0),profit=r.profit_evidence===true,c=v>=0?'var(--good)':'var(--bad)',side=String(r.side||'pooled').toUpperCase(),route=String(r.route||r.origin||'route'),state=r147OperatorTerms(String(r.state||'COLLECTING').replaceAll('_',' ')),n=Number(r.settled_unique_markets||r.n||0),o=Number(r.open_unique_markets||r.open||0),bucket=r.sample_bucket||r144SampleBucket(n),range=(r.net_per_day_lower==null||r.net_per_day_upper==null)?'bound collecting':(r145NetD(r.net_per_day_lower)+'…'+r145NetD(r.net_per_day_upper)),tier=String(r.evidence_tier||'unclassified').replaceAll('_',' '),metric=profit?(r145NetD(v)+'<div class="muted">'+range+'</div>'):'<span class="muted">not profit evidence</span>';return '<div class="r141row"><span class="name"><b>'+escapeHtml(srcLbl(r.family||''))+'</b><div class="muted">'+escapeHtml(platLabel(r.venue||''))+' · '+escapeHtml(side)+' · '+escapeHtml(route)+' · '+escapeHtml(state)+'</div><div class="muted">'+escapeHtml(tier)+' · fill-conditioned '+(r.fill_conditioned===true?'yes':'no')+' · LIVE '+(r.live_authorizes===true?'yes':'no')+'</div></span><span style="color:'+(profit?c:'var(--muted)')+';font-weight:800">'+metric+'</span><span class="muted">n'+n+' '+bucket+(o>0?' · open '+o:'')+'</span></div>';}).join('');
}
function r141ResearchRows(rows,attention){
  if(!rows||!rows.length)return '<span class="muted">No '+(attention?'actionable alerts':'exact research rows')+' yet.</span>';
  return rows.map(function(r){var activity=Number(r.current_cycle_matches||0),detail=activity>0?('latest cycle matched '+activity+' routes · '+Number(r.current_cycle_new||0)+' new · '+Number(r.current_cycle_duplicates||0)+' already stored'):('exact '+Number(r.exact_rows||0)+' · candidates '+Number(r.candidates||0));return '<div class="r141row"><span class="name"><b>'+escapeHtml(srcLbl(r.system_id||''))+'</b><div class="muted">'+escapeHtml(attention?(r.reason||r.state||'needs attention'):detail)+'</div></span>'+(attention?('<span style="color:var(--warn)">'+Number(r.alerts||0)+' alert'+(Number(r.alerts||0)===1?'':'s')+'</span>'):'')+'</div>';}).join('');
}

function loadFundedSystemPerformance(){
  var el=document.getElementById('r151FundedSystems');if(!el)return;
  el.innerHTML='<div class="r141card" style="--edge:#22c55e"><h3>Funded Paper simulation by system</h3><span class="muted">Loading sized simulated portfolio outcomes…</span></div>';
  r138Get('/api/funded-system-performance').then(function(d){
    var all=(d&&d.rows)||[],rows=all.slice();
    function money(v){v=Number(v)||0;return (v>=0?'+':'-')+'$'+Math.abs(v).toFixed(2);}
    function elapsed(s){s=Number(s)||0;return s>=86400?(s/86400).toFixed(1)+'d':(s/3600).toFixed(1)+'h';}
    var body='';
    if(!rows.length)body='<span class="muted">No funded Paper simulation rows yet. Historical assumed-fill evidence remains separate below.</span>';
    rows.forEach(function(r){
      var profit=Number(r.total_realized_profit_dollars||0),rate=Number(r.net_per_calendar_day||0),n=Number(r.n||0);
      var color=profit>=0?'var(--good)':'var(--bad)';
      body+='<div class="r141row"><span class="name"><b>'+escapeHtml(srcLbl(r.system_id||''))+'</b><div class="muted">'+escapeHtml(platLabel(r.venue||''))+' · '+escapeHtml(String(r.side||'').toUpperCase())+' · '+escapeHtml(r.route_kind||'')+' · '+escapeHtml(r.portfolio||'')+' · FUNDED PAPER SIMULATION</div><div class="muted">'+escapeHtml(String(r.evidence_tier||'funded_paper_simulation').replaceAll('_',' '))+' · fill-conditioned no · exchange profit evidence no · LIVE no</div><div class="muted">first '+escapeHtml(fmtClose(r.first_decision||''))+' · last '+escapeHtml(fmtClose(r.last_activity||''))+' · elapsed '+elapsed(r.elapsed_seconds)+'</div></span><span style="color:'+color+';font-weight:800">'+money(profit)+'<div class="muted">'+money(rate)+'/d</div></span><span class="muted">n'+n+' settled'+(Number(r.open_receipts||0)>0?' · open '+Number(r.open_receipts||0):'')+'<div>'+Number(r.unique_positions||0)+' unique positions · '+Number(r.settled_contracts||0).toFixed(0)+' contracts</div></span></div>';
    });
    var recon=(d&&d.portfolio_reconciliation)||[];
    var reconHTML=recon.map(function(x){var gap=Number(x.legacy_unattributed_dollars||0),pct=Number(x.absolute_dollar_coverage_pct||0);return '<div class="muted">'+escapeHtml(platLabel(x.portfolio||''))+' epoch total '+money(x.portfolio_realized_dollars)+' · exact system receipts '+money(x.system_attributed_dollars)+' · unattributed '+money(gap)+' · '+pct.toFixed(1)+'% covered</div>';}).join('');
    el.innerHTML='<div class="r141card" style="--edge:#22c55e"><h3>Funded Paper simulation by system</h3>'+body+'<div style="margin-top:7px">'+reconHTML+'</div><div class="muted" style="margin-top:6px">Sized dollars from durable accepted Paper simulation placements and settlements in the same reset epoch as the portfolio briefing. Winners and losers are both shown. These rows do not prove an exchange order or fill and never authorize LIVE. Money without an immutable system receipt is shown as unattributed instead of being guessed.</div></div>';
  }).catch(function(e){el.innerHTML='<div class="r141warn">Funded system performance unavailable: '+escapeHtml(e.message||String(e))+'</div>';});
}

// R142: one denominator-labelled Combo Lab snapshot shared by Systems and the detailed Combo
// panel. It rolls linkage classes together by leg count; the dense class/CI table remains behind
// an explicit details disclosure. Open candidates never masquerade as settled n.
function r142ComboSnapshotHTML(v){
  v=v||{};var max=Math.min(6,Math.max(2,Number(v.current_max_legs||6))),rows=v.summary||[],by={},settled=0;
  rows.forEach(function(r){var legs=Number(r.leg_count||0),co=String(r.cohort||'all-eligible');if(legs<2||legs>max)return;var n=Number(r.n||0),ev=Number(r['ev_real_per_$1']||0),k=co+'|'+legs;if(!by[k])by[k]={n:0,sum:0};by[k].n+=n;by[k].sum+=ev*n;settled+=n;});
  var openBy=v.open_by_legs||{},open=0;for(var k in openBy){var l=Number(k),n=Number(openBy[k]||0);if(l>=2&&l<=max)open+=n;}if(!Object.keys(openBy).length)open=Number(v.open_within_leg_limit!=null?v.open_within_leg_limit:(v.open_candidate_rows||v.open||0));
  var legacy=Number(v.open_above_leg_limit||0),promOpen=Number((v.open_by_cohort||{})['promoted-system-combo']||0),promN=0,chips=[];
  for(var legs=2;legs<=max;legs++){var c=by['all-eligible|'+legs]||{n:0,sum:0},mean=c.n?c.sum/c.n:0,col=mean>=0?'var(--good)':'var(--bad)';chips.push('<div class="r142combochip"><b>'+legs+' legs · n='+c.n+'</b>'+(c.n?'<span style="color:'+col+';font-weight:750">'+(mean>=0?'+':'')+mean.toFixed(2)+'/$1</span>':'<span class="muted">waiting for settlement</span>')+'</div>');var p=by['promoted-system-combo|'+legs];if(p)promN+=p.n;}
  var last='1h: +'+Number(v.logged_last_hour||0)+' candidates · +'+Number(v.graded_last_hour||0)+' settled';
  var extra=legacy?('<span style="color:var(--warn)">'+legacy+' old >'+max+'-leg rows retiring</span>'):'';
  return '<div class="r142combo"><div class="r142combohead"><span class="title">🧪 Combo Lab</span><span class="count">n='+settled+' settled</span><span>⏳ '+open+' open</span><span class="muted">'+last+'</span>'+extra+'</div><div class="r142combolegs">'+chips.join('')+'</div><div class="muted" style="margin-top:5px">All-eligible synthetic settlement sample. Sealed-system cohort: n='+promN+' settled · '+promOpen+' open. n never includes unresolved candidates or the much larger logical manifest.</div></div>';
}
function loadSystemsComboSnapshot(){var el=document.getElementById('r142SystemsCombo');if(!el)return;r138Get('/api/combolab').then(function(v){el.innerHTML=r142ComboSnapshotHTML(v);}).catch(function(e){el.innerHTML='<div class="r141warn">Combo evidence unavailable: '+escapeHtml(e.message||String(e))+'</div>';});}
function r141Components(d){return (((d||{}).ready||{}).components)||{};}
function r141ComponentPresentation(v){
  v=v||{};var raw=String(v.state||'').trim(),detail=String(v.detail||'No detail reported.').trim(),rawLow=raw.toLowerCase(),detailLow=detail.toLowerCase();
  if(v.ok===true)return {kind:'healthy',label:raw&&raw!=='ok'?raw:'healthy',dot:'🟢',detail:detail};
  if(/warm|start|initial|refresh|connect|wait|none/.test(rawLow)||/warm|start|initial|refresh|wait|not configured|no .* configured/.test(detailLow))return {kind:'warming',label:raw||'warming',dot:'🟡',detail:detail};
  return {kind:'failing',label:raw||'unhealthy',dot:'🔴',detail:detail};
}
function loadResearchOverview(){
  if(!tabArmed('overview'))return;var host=document.getElementById('r138OverviewBody');
  r141Digest(host,function(d){var rd=d.ready||{},rs=d.research||{},cat=d.system_catalog||{},pr=d.promotion||{},details=Number(rs.details_on_demand||0),comp=r141Components(d),bad=[];Object.keys(comp).forEach(function(k){if(comp[k]&&comp[k].ok===false&&k!=='poly_clob_ws'&&k!=='poly_consensus_research')bad.push(k);});var best=((d.systems||{}).top||[])[0];
    host.innerHTML='<div class="r141grid">'+
      r141Card('Runtime feeds','<span class="r141big" style="color:'+(rd.ready?'var(--good)':'var(--bad)')+'">'+(rd.ready?'READY':'NOT READY')+'</span><br><span class="muted">'+(bad.length?(bad.length+' core issue'+(bad.length===1?'':'s')):'Core feeds healthy')+' · this is feed health, not LIVE authorization</span>','#60a5fa')+
      r141Card('Best measured now',best?('<b>'+escapeHtml(srcLbl(best.family||''))+'</b><br><span style="color:'+(Number(best.net_per_day)>=0?'var(--good)':'var(--bad)')+'">'+r145NetD(best.net_per_day)+'</span><br><span class="muted">'+((best.net_per_day_lower==null||best.net_per_day_upper==null)?'bound collecting':(r145NetD(best.net_per_day_lower)+'…'+r145NetD(best.net_per_day_upper)))+' · n='+Number(best.n||0)+'</span><br><span class="muted">'+escapeHtml(String(best.evidence_tier||'unclassified').replaceAll('_',' '))+' · fill-conditioned '+(best.fill_conditioned===true?'yes':'no')+' · LIVE '+(best.live_authorizes===true?'yes':'no')+'</span>'):'<span class="muted">No settled measured system yet.</span>','#22c55e')+
      r141Card('System catalog',r147CatalogCard(cat,d.variant_signal_contracts),'#f59e0b')+
      r141Card('LIVE transfer gate','<b>CASH CLOSED</b><br><span class="muted">No Paper, model, assumed-fill, sealed research, New ML, AUTO RFQ, or staged-bundle result currently authorizes new cash. A future route needs positive settled authenticated LIVE profit evidence plus explicit operator approval. Historical Paper accepted '+Number(pr.paper_accepted||0)+' · historical LIVE dispatched '+Number(pr.live_dispatched||0)+'</span>','#c084fc')+
      '</div>'+(d.warnings&&d.warnings.length?'<div class="r141warn">One background summary refresh is partial; last-good values remain visible.</div>':'');
  },loadResearchOverview);
}

function loadResearchSystemsPage(){
  if(!tabArmed('systems'))return;var host=document.getElementById('r138SystemsBody');
  r141Digest(host,function(d){var sys=d.systems||{},rs=d.research||{},cat=d.system_catalog||{},pr=d.promotion||{},details=Number(rs.details_on_demand||0);
    host.innerHTML='<div class="r141grid">'+
      r141Card('System catalog',r147CatalogCard(cat,d.variant_signal_contracts),'#f59e0b')+
      r141Card('Best measured systems',r141SystemRows(sys.top||[]),'#22c55e')+
      r141Card('Worst measured systems',r141SystemRows(sys.worst||[]),'#ef4444')+
      r141Card('Collector status (subset)',details?('<b>'+Number(rs.registered||0)+' validation collectors</b><br><span class="muted">Collector rows are inputs/health checks, not extra executable systems. Open details for counts and blockers.</span>'):(r141ResearchRows(rs.progress||[],false)+'<div class="muted" style="margin-top:5px">'+Number(rs.registered||0)+' validation collectors · '+Number(rs.exact_rows||0)+' cumulative exact route rows · '+Number(rs.current_cycle_matches||0)+' latest-cycle matches</div>'),'#f59e0b')+
      r141Card('Needs attention',r141ResearchRows(rs.attention||[],true),'#f97316')+
      r141Card('Promotion','<b>'+escapeHtml(r147OperatorTerms(String(pr.state_label||pr.state||'WAITING').replaceAll('_',' ')))+'</b><br><span class="muted">sealed '+Number(pr.sealed_eligible||0)+' · Paper accepted '+Number(pr.paper_accepted||0)+' · LIVE dispatched '+Number(pr.live_dispatched||0)+'</span>','#c084fc')+
      '</div><div id="r151FundedSystems"></div><div id="r142SystemsCombo"></div>'+(d.warnings&&d.warnings.length?'<div class="r141warn">Summary refresh is partial; details remain last-good and no missing value is treated as n=0.</div>':'');
    loadFundedSystemPerformance();
    loadSystemsComboSnapshot();
  },loadResearchSystemsPage);
}

function loadResearchSystemDetails(){
  if(!tabArmed('systems'))return;var host=document.getElementById('r138SystemsDetail');if(!host)return;
  host.innerHTML='<span class="muted">Loading immutable registry + runtime collectors…</span>';
  loadLeaderboardBacktest('r139SystemsLeaderboard');
  Promise.allSettled([r138Get('/api/research-foundation'),r138Get('/api/research/system-evidence'),r138Get('/api/research/inference')]).then(function(x){
    var f=r138Settled(x[0]),se=r138Settled(x[1]),inf=r138Settled(x[2]);
    if(f.error){host.innerHTML=r138Err(f);return;}
    var cov=se.runtime_coverage||{},runtimeBy={},evidenceBy=se.systems||{},inferenceBy={};
    (cov.systems||[]).forEach(function(v){runtimeBy[String(r138Field(v,'SystemID','system_id')||'')]=v;});
    (inf.systems||[]).forEach(function(v){inferenceBy[String(r138Field(v,'SystemID','system_id')||'')]=v;});
    var rows=(f.systems||f.experiments||[]).map(function(e){
      var id=String(r138Field(e,'SystemID','system_id')||r138Field(e,'ExperimentID','experiment_id')||''),name=r138Field(e,'SystemName','system_name'),route=r138Field(e,'Route','route'),rv=runtimeBy[id],ev=evidenceBy[id]||{},iv=inferenceBy[id]||{};
      var state=rv?String(r138Field(rv,'State','state')||'BLOCKED'):'BLOCKED';
      var reason=rv?String(r138Field(rv,'Reason','reason')||'runtime receipt omitted a reason'):'registry only; no runtime collector receipt exists';
      var prereq=rv?(r138Field(rv,'Prerequisites','prerequisites')||[]):['runtime collector + immutable liveness receipt'];
      var collectors=rv?(r138Field(rv,'CollectorIDs','collector_ids')||[]):[];
      var inferState=String(r138Field(iv,'State','state')||'SCHEDULER_STARTING'),terminal=r138Num(r138Field(iv,'TerminalRows','terminal_rows'));
      var inferReason=String(r138Field(iv,'Reason','reason')||'waiting for first immutable inference receipt');
      var monitoringKnown=!!r138Field(iv,'MonitoringBoundsKnown','monitoring_bounds_known'),monitoringLow=r138Num(r138Field(iv,'MonitoringLower','monitoring_lower'));
      var auth=(r138Field(e,'Funded','funded')||r138Field(e,'PaperAuthority','paper_authority')||r138Field(e,'LiveAuthority','live_authority')||
        (rv&&(r138Field(rv,'Funded','funded')||r138Field(rv,'PaperAuthority','paper_authority')||r138Field(rv,'LiveAuthority','live_authority'))))?'VIOLATION':'registry object 0 / 0 / 0';
      return '<tr><td><b>'+escapeHtml(name||id)+'</b><div class="muted">'+escapeHtml(id)+'</div></td><td>'+r138State(state)+'</td><td class="r">'+r138Num(ev.rows)+'</td><td>'+r138State(inferState)+'<div class="muted">'+terminal+' exact terminal rows'+(monitoringKnown?' | rolling-monitor low '+monitoringLow.toFixed(4):'')+'</div><div class="muted">'+escapeHtml(inferReason)+'</div></td><td>'+escapeHtml(reason)+(prereq.length?'<div class="muted">needs: '+escapeHtml(prereq.join(' · '))+'</div>':'')+'</td><td>'+escapeHtml(collectors.length?collectors.join(', '):'none')+'<div class="muted">route: '+escapeHtml(route||'')+'</div></td><td>'+escapeHtml(auth)+'</td></tr>';
    });
    var summary=se.error?r138Err(se):('<b>'+r138Num(cov.reported_systems)+' / '+r138Num(cov.expected_systems||19)+' validation-collector receipts</b> · this 19-contract study is a subset of the full 119-base System catalog; registry alone never counts as implemented · '+r138Authority(se));
    var inferenceSummary=inf.error?r138Err(inf):('<b>Strict validation monitoring '+escapeHtml(String(inf.run_id||'starting'))+': '+r138Num(inf.exact_terminal_rows)+' exact terminal rows</b> | '+r138Num(inf.systems_reported||19)+'/19 validation contracts shown | '+r138Authority(inf)+'<br><span class="muted">No point estimate becomes a candidate. Exact scalar void/refund outcomes are included; open, censored, inexact, identity-incomplete, and route-incomplete rows stay excluded and counted. The rolling final slice is not untouched proof.</span>');
    host.innerHTML='<div class="r138hero">'+summary+'<br>'+inferenceSummary+'</div>'+(rows.length?r138Table(['Validation contract','Collector state','Rows','Train / validation / rolling-final monitoring','Collector reason / exact prerequisite','Collectors / frozen route','Registry object: Funded / Paper / LIVE'],rows):'<div class="r138pane">No immutable system registry rows exist. Nothing is operational. This statement applies to the 19-contract validation subset; the full System catalog and executable-variant matrix remain shown above.</div>');
  });
}

function loadResearchExperimentsPage(){
  if(!tabArmed('experiments'))return;var host=document.getElementById('r138ExperimentsBody');
  r141Digest(host,function(d){var rs=d.research||{},pr=d.promotion||{};host.innerHTML='<div class="r141grid">'+
    r141Card('Registered system tests','<span class="r141big">'+Number(rs.registered||0)+'</span><br><span class="muted">immutable validation contracts; not the full tracked-system total</span>','#c084fc')+
    r141Card('Executable outcomes','<b>Details on demand</b><br><span class="muted">These are fully priced actions, not the much larger input/control row count. Open validation details for exact outcome and candidate counts.</span>','#22d3ee')+
    r141Card('Sealed research passes','<span class="r141big">'+Number(pr.sealed_passes||0)+'</span><br><span class="muted">Frozen later samples remain research evidence only. They do not authorize cash; neither do New ML or prospective assumed-fill allocation.</span>','#f59e0b')+
    r141Card('LIVE decision','<b>'+escapeHtml(r147OperatorTerms(String(pr.state_label||pr.state||'WAITING').replaceAll('_',' ')))+'</b><br><span class="muted">Fresh exact candidates may explore one share in Paper now. Monitoring alone never authorizes LIVE.</span>','#ea3943')+
    '</div>';},loadResearchExperimentsPage);
}

function loadResearchExperimentDetails(){
  if(!tabArmed('experiments'))return;var host=document.getElementById('r138ExperimentsDetail');if(!host)return;
  Promise.allSettled([r138Get('/api/research-foundation'),r138Get('/api/proper-score'),r138Get('/api/research/collectors'),r138Get('/api/research/inference')]).then(function(x){
    var f=r138Settled(x[0]),p=r138Settled(x[1]),c=r138Settled(x[2]),inf=r138Settled(x[3]),counts=f.counts||{},sum=p.summary||{};
    var states=Object.keys(f.states||{}).sort().map(function(k){return r138State(k)+' × '+r138Num(f.states[k]);}).join('<br>')||'No states yet.';
    var infStates={},monitorPositives=0;(inf.systems||[]).forEach(function(v){var st=String(r138Field(v,'State','state')||'UNKNOWN');infStates[st]=(infStates[st]||0)+1;if(r138Field(v,'MonitoringLowerBoundPositive','monitoring_lower_bound_positive'))monitorPositives++;});
    var infStateText=Object.keys(infStates).sort().map(function(k){return r138State(k)+' x '+infStates[k];}).join('<br>')||'First bounded inference receipt is starting.';
    var ex=inf.exclusions||{};
    host.innerHTML=
      r138Pane('Immutable test registry',r138Err(f)||('<b>'+r138Num(counts.system_ids||counts.experiment_ids)+' validation contracts</b> · '+r138Num(counts.system_versions||counts.experiment_versions)+' versions<br>'+r138Num(counts.event_ids)+' event IDs · '+r138Num(counts.payoff_ids)+' payoff IDs<br><span class="muted">This is the 19-contract validation subset, not the full System catalog.</span><br>'+r138Authority(f)),'#c084fc')+
      r138Pane('State funnel',r138Err(f)||states,'#f59e0b')+
      r138Pane('Strict validation-subset monitoring',r138Err(inf)||('<b>Run '+escapeHtml(String(inf.run_id||'starting'))+' | '+r138Num(inf.systems_reported||19)+'/19 validation contracts | '+r138Num(inf.exact_terminal_rows)+' exact terminal rows</b><br>'+monitorPositives+' rolling-monitor lower-bound positives; 0 preregistered untouched passes<br>'+infStateText+'<br><span class="muted">Excluded: open '+r138Num(ex.open)+' | nonexact void '+r138Num(ex.void)+' | censored '+r138Num(ex.censored)+' | nonexact settlement '+r138Num(ex.nonexact)+' | identity '+r138Num(ex.identity)+' | route truth '+r138Num(ex.route_truth)+'. Exact scalar void/refund rows are included. Canonical-event chronology, one-day purge/embargo, quiet UTC days, shrinkage, Holm, and BY are monitoring diagnostics. A true untouched pass needs a freeze manifest created before the test rows and one sealed evaluation; that registry does not exist yet, so this validation registry has no execution candidate or authority.</span><br>'+r138Authority(inf)),'#fb923c')+
      r138Pane('Proper-score validation',r138Err(p)||('<b>'+r138Num(sum.rows)+' rows</b> · '+r138Num(sum.actions)+' route actions · '+r138Num(sum.abstains)+' abstentions<br>'+r138Num(sum.settled_actions)+' settled actions<br>'+r138State(p.state||'COLLECTING')),'#ec4899')+
      r138Pane('Collector contract',r138Err(c)||('<b>'+r138Num(c.active)+' registered collectors</b><br>'+r138Num(c.alerts)+' alerts. A quiet healthy collector says why zero is expected; errors never become empty data.'),'#22d3ee')+
      r138Pane('Validation-registry boundary','Train → validation → later untouched event/day holdout. This 19-contract registry cannot grant money authority by itself; the full System catalog and the separate sealed, New-ML, and prospective-allocation execution lanes retain their own gates.','#ea3943');
  });
}

function loadResearchEvidencePage(){
  if(!tabArmed('evidence'))return;var host=document.getElementById('r138EvidenceBody');
  r141Digest(host,function(d){var rs=d.research||{},pr=d.promotion||{},details=Number(rs.details_on_demand||0);host.innerHTML='<div class="r141grid">'+
    r141Card('Collector health',details?'<b>Loads on request</b><br><span class="muted">Open details for exact funnels and alerts.</span>':('<span class="r141big" style="color:'+(Number(rs.alerts||0)?'var(--warn)':'var(--good)')+'">'+Number(rs.alerts||0)+'</span> alerts<br>'+Number(rs.collecting||0)+' collecting · '+Number(rs.partial||0)+' partial'),'#22d3ee')+
    r141Card('Exact route evidence','<span class="r141big">'+Number(rs.exact_rows||0)+'</span> rows<br>'+Number(rs.candidates||0)+' candidates · '+Number(rs.open||0)+' open','#c084fc')+
    r141Card('Promotion blocker','<b>'+escapeHtml(r147OperatorTerms(String(pr.state_label||pr.state||'WAITING').replaceAll('_',' ')))+'</b><br><span class="muted">sealed eligible '+Number(pr.sealed_eligible||0)+'</span>','#f59e0b')+
    r141Card('Action needed',r141ResearchRows(rs.attention||[],true),'#f97316')+
    '</div>';},loadResearchEvidencePage);
}

function loadResearchEvidenceDetails(){
  if(!tabArmed('evidence'))return;var host=document.getElementById('r138EvidenceDetail');if(!host)return;
  host.innerHTML='<div class="r138pane">Loading collectors, system coverage, routes, and portfolio causes…</div>';
  Promise.allSettled([r138Get('/api/research/collectors'),r138Get('/api/research/routes'),r138Get('/api/research/portfolio'),r138Get('/api/research/system-evidence'),r138Get('/api/research/rules')]).then(function(x){
    var c=r138Settled(x[0]),r=r138Settled(x[1]),p=r138Settled(x[2]),se=r138Settled(x[3]),rr=r138Settled(x[4]),cov=se.runtime_coverage||{},rcc=cov.counts||{};
    var collectors=(c.collectors||[]).map(function(v){return '<tr><td>'+escapeHtml(r138Field(v,'CollectorID','collector_id')||'')+'</td><td>'+r138State(r138Field(v,'Status','status')||(r138Field(v,'NeverRan','never_ran')?'NEVER_RAN':'UNKNOWN'))+'</td><td>'+r138Num(r138Field(v,'Eligible','eligible'))+' / '+r138Num(r138Field(v,'Attempted','attempted'))+' / '+r138Num(r138Field(v,'Inserted','inserted'))+'</td><td>'+escapeHtml(r138Field(v,'ZeroReason','zero_reason')||r138Field(v,'ErrorText','error_text')||'—')+'</td></tr>';});
    var routes=(r.recent||[]).slice(0,30).map(function(v){return '<tr><td>'+escapeHtml(r138Field(v,'SystemName','system_name')||'')+'</td><td>'+escapeHtml((r138Field(v,'Venue','venue')||'')+' '+(r138Field(v,'Side','side')||''))+'</td><td>'+escapeHtml(r138Field(v,'Route','route')||'')+'</td><td>'+r138State(r138Field(v,'Decision','decision'))+'</td><td class="r">'+r138Num(r138Field(v,'ExpectedNetLow','expected_net_low')).toFixed(4)+'</td></tr>';});
    var runtimeRows=(cov.systems||[]).map(function(v){var prereq=r138Field(v,'Prerequisites','prerequisites')||[];return '<tr><td>'+escapeHtml(r138Field(v,'SystemID','system_id')||'')+'</td><td>'+r138State(r138Field(v,'State','state')||'BLOCKED')+'</td><td>'+escapeHtml(r138Field(v,'Reason','reason')||'missing reason')+'</td><td>'+escapeHtml(prereq.length?prereq.join(' · '):'—')+'</td></tr>';});
    var routeCounts=r.counts||{},evi=p.evi_scheduler||{},cause=p.cause_graph||{},eviReceipt=evi.latest_receipt||{},causeReceipt=cause.latest_receipt||{};
    var selected=(((eviReceipt.result||{}).Selected)||((eviReceipt.result||{}).selected)||[]).length;
    var clusters=Array.isArray(causeReceipt.result)?causeReceipt.result.length:0;
    var ruleCounts=rr.counts||{},ruleTypes=rr.pair_types||{},structuralRuleTypes=rr.structural_pair_types||{},ruleStates=rr.states||{},rulePairs=rr.recent_pairs||[];
    host.innerHTML=
      r138Pane('Collector funnels',r138Err(c)||(collectors.length?r138Table(['Collector','State','Eligible / tried / new','Zero or error truth'],collectors):'<span class="muted">No collector receipts exist. This is missing instrumentation, not a healthy zero.</span>'),'#22d3ee')+
      r138Pane('Registered collector runtime coverage',r138Err(se)||('<b>'+r138Num(cov.reported_systems)+' / '+r138Num(cov.expected_systems||19)+' collectors reported</b> · '+r138Num(rcc.COLLECTING)+' collecting · '+r138Num(rcc.COLLECTING_PARTIAL)+' partial · '+r138Num(rcc.BLOCKED)+' blocked<br>'+(runtimeRows.length?r138Table(['Collector / child','State','Why','Exact prerequisites'],runtimeRows):'<span class="muted">No runtime receipts. Registry rows are not operational.</span>')+'<br><span class="muted">This validation registry is a subset; controls and collectors do not inflate the executable-system total.</span><br>'+r138Authority(se)),'#fb923c')+
      r138Pane('Unified route ledger',r138Err(r)||('<b>'+r138Num(routeCounts.complete_money_truth)+' / '+r138Num(routeCounts.routes)+' routes have explicit quote-age, latency, tick, depth, and fee truth</b><br>'+(routes.length?r138Table(['System','Venue side','Route','Decision','Net low'],routes):'<span class="muted">No unified routes yet; missing rows are not proxied from signal price.</span>')+'<br>'+r138Authority(r)),'#c084fc')+
      r138Pane('Cross-venue rule review',r138Err(rr)||('<b>'+r138Num(ruleCounts.structural_pairs_seen)+' structural pairs reviewed · '+r138Num(ruleCounts.compatible_certificates)+' current compatible certificates</b><br>K-PUS '+r138Num(structuralRuleTypes['K-PUS'])+' seen / '+r138Num(ruleTypes['K-PUS'])+' current · K-PINT '+r138Num(structuralRuleTypes['K-PINT'])+' / '+r138Num(ruleTypes['K-PINT'])+' · PUS-PINT '+r138Num(structuralRuleTypes['PUS-PINT'])+' / '+r138Num(ruleTypes['PUS-PINT'])+' · complete three-way triangles '+r138Num(ruleCounts.three_way_current_certificates)+'<br>'+r138Num(ruleCounts.artifact_instruments)+' instruments have raw official rule artifacts · '+r138Num(rulePairs.length)+' current pair blockers shown by API<br><span class="muted">Point estimates and title similarity never certify rules. Every pair needs current raw artifacts plus an independently reviewed same/inverse orientation. A three-venue economic claim needs all three current pair certificates and transitive flips.</span><br>'+r138Authority(rr)),'#f97316')+
      r138Pane('EVI research queue',r138Err(p)||r138State(evi.state||'UNAVAILABLE')+'<br><b>'+r138Num(evi.current_input_versions)+' frozen task versions &middot; '+selected+' selected</b><br><b>The collector is waiting for qualified evidence.</b> '+escapeHtml(evi.plain_language||evi.reason||'')+'<br><span class="muted">Every task must link to a sealed preregistered untouched inference result with the exact result hash. Current rolling monitoring cannot qualify. Economic assumptions and budgets are then frozen; the scheduler never guesses from n and never starts work. Endpoint: '+escapeHtml(evi.endpoint||'POST /api/research/evi/inputs')+'</span>','#84cc16')+
      r138Pane('Portfolio cause graph',r138Err(p)||r138State(cause.state||'UNAVAILABLE')+'<br><b>'+r138Num(cause.current_input_versions)+' frozen route lower bounds &middot; '+clusters+' cause clusters</b><br>'+escapeHtml(cause.reason||'')+'<br><span class="muted">A cause exposure needs both a sealed preregistered inference gate and an immutable route conversion receipt for Net/d, capacity, and capital time. Current monitoring and self-asserted point estimates are refused. This graph cannot allocate or order.</span>','#f59e0b');
  });
}

function loadResearchDataPage(){
  if(!tabArmed('data'))return;var host=document.getElementById('r138DataBody');
  r141Digest(host,function(d){var cc=r141Components(d),issues=[],healthy=0,warming=0,failing=0,poly=null;
    Object.keys(cc).forEach(function(k){var v=cc[k]||{},p=r141ComponentPresentation(v);if(k==='poly_clob_ws')poly={value:v,presentation:p};if(p.kind==='healthy')healthy++;else if(k!=='poly_clob_ws'){issues.push({name:k,presentation:p});if(p.kind==='warming')warming++;else failing++;}});
    var polyView=poly?poly.presentation:{kind:'warming',label:'not reported',dot:'🟡',detail:'No Poly-int research receipt yet.'};
    var badHtml=issues.length?issues.slice(0,4).map(function(x){var p=x.presentation;return '<div class="r141row"><span class="name"><b>'+escapeHtml(x.name.replaceAll('_',' '))+'</b><div class="muted">'+escapeHtml(p.detail)+'</div></span><span>'+p.dot+' '+escapeHtml(p.label.replaceAll('_',' '))+'</span></div>';}).join(''):'<span style="color:var(--good)">No core source alerts.</span>';
    host.innerHTML='<div class="r141grid">'+
      r141Card('Core sources','<span class="r141big" style="color:'+(failing?'var(--bad)':(warming?'var(--warn)':'var(--good)'))+'">'+failing+'</span> failing · '+warming+' warming<br><span class="muted">'+healthy+' healthy source checks hidden</span>',failing?'#ef4444':(warming?'#f59e0b':'#38bdf8'))+
      r141Card('Poly-int research feed','<b>'+polyView.dot+' '+escapeHtml(polyView.label.replaceAll('_',' '))+'</b><br><span class="muted">'+escapeHtml(polyView.detail)+'</span>',polyView.kind==='healthy'?'#22c55e':(polyView.kind==='warming'?'#f59e0b':'#ef4444'))+
      r141Card('Needs attention',badHtml,'#f97316')+
      r141Card('Data meaning','The UI is a short view. Full source clocks, replay receipts, notices, APIs, and stored rows remain available below.','#60a5fa')+
      '</div>';},loadResearchDataPage);
}

function loadResearchDataDetails(){
  if(!tabArmed('data'))return;var host=document.getElementById('r138DataDetail');if(!host)return;
  host.innerHTML='<div class="r138pane">Loading source clocks, replay receipts, and official notices...</div>';
  Promise.allSettled([r138Get('/api/research/source-clocks'),r138Get('/api/research/replay'),r138Get('/api/venue-notices')]).then(function(x){
    var s=r138Settled(x[0]),rp=r138Settled(x[1]),n=r138Settled(x[2]);
    var clocks=(s.sources||[]).map(function(v){return '<tr><td>'+escapeHtml(r138Field(v,'DisplayName','display_name')||r138Field(v,'SourceID','source_id')||'')+'</td><td>'+r138State(r138Field(v,'Status','status')||(r138Field(v,'NeverRan','never_ran')?'NEVER_RAN':'UNKNOWN'))+'</td><td class="r">'+Math.round(r138Num(r138Field(v,'ReceiptLagS','receipt_lag_s')))+'s</td><td>'+escapeHtml(r138Field(v,'SchemaVersion','schema_version')||'')+'</td></tr>';});
    var notices=(n.recent||[]).slice(0,10).map(function(v){var url=String(v.source_url||'');var title=escapeHtml(v.title||v.source_id||'notice');return '<tr><td>'+escapeHtml(v.venue||'')+'</td><td>'+r138State(v.severity||'info')+'</td><td>'+(url.indexOf('https://')===0?'<a class="go" target="_blank" rel="noreferrer" href="'+escapeHtml(url)+'">'+title+'</a>':title)+'</td><td>'+escapeHtml(v.published_ts||v.first_seen_ts||'')+'</td></tr>';});
    host.innerHTML=
      r138Pane('Source clocks',r138Err(s)||(clocks.length?r138Table(['Source','State','Receipt age','Schema'],clocks):'No source-clock receipts yet.'),'#38bdf8')+
      r138Pane('Selected normalized audit sample',r138Err(rp)||((rp.healthy?r138State('RECENT TAIL VERIFIED'):r138State(rp.error?'CHAIN ERROR':'NO SEGMENTS'))+'<br>'+r138Num(rp.verified_segments)+' / '+r138Num(rp.segments)+' segments verified · scope '+escapeHtml(rp.verification_scope||'not reported')+'<br>'+r138Num(rp.frames)+' selected frames · '+escapeHtml((rp.frame_kinds||[]).join(' · ')||'frame kinds not reported')+'<br>'+r138Num(rp.orphan_segments)+' orphan segments · '+r138Num(rp.temporary_files)+' temp files<br><span class="muted">This is a bounded normalized research sample, not a full raw-market timeline.</span>'),'#60a5fa')+
      r138Pane('Official venue notices',r138Err(n)||(notices.length?r138Table(['Venue','Severity','Notice','Published'],notices):'Watcher has not stored a notice yet.'),'#f59e0b');
  });
}

function loadResearchOperationsPage(){
  if(!tabArmed('operations'))return;var host=document.getElementById('r138OperationsBody');
  r141Digest(host,function(d){var st=d.status||{},rd=d.ready||{},ks=st.kill_switch||{},warnings=d.warnings||[];
    host.innerHTML='<div class="r141grid">'+
      r141Card('Runtime',rd.ready?'<span class="r141big" style="color:var(--good)">READY</span>':'<span class="r141big" style="color:var(--bad)">NOT READY</span>','rgb(96,165,250)')+
      r141Card('Money controls','ARM <b>'+(d.live_armed?'ON':'OFF')+'</b> &middot; LIVE AUTO <b>'+(d.live_auto?'ON':'OFF')+'</b><br>Kill switch <b>'+(ks.tripped?'TRIPPED':'clear')+'</b>',ks.tripped?'#ef4444':'#22c55e')+
      r141Card('Research authority','<span class="r141big">0</span><br><span class="muted">Research evidence cannot place an order.</span>','#ea3943')+
      r141Card('Current blocker',warnings.length?'<b>'+escapeHtml(warnings[0])+'</b><br><span class="muted">'+warnings.length+' report warning'+(warnings.length===1?'':'s')+'</span>':'<span style="color:var(--good)">No digest warning.</span>','#f59e0b')+
      '</div>';},loadResearchOperationsPage);
}

function loadResearchOperationsDetails(){
  if(!tabArmed('operations'))return;var host=document.getElementById('r138OperationsDetail');if(!host)return;
  host.innerHTML='<div class="r138pane">Loading governance, compliance, and recovery diagnostics...</div>';
  Promise.allSettled([r138Get('/api/status'),r138Get('/api/ready'),r138Get('/api/research/governance'),r138Get('/api/research/recovery')]).then(function(x){
    var st=r138Settled(x[0]),rd=r138Settled(x[1]),g=r138Settled(x[2]),dr=r138Settled(x[3]),run=g.runtime||{};
    var snaps=dr.snapshots||[],latest=snaps[0];
    host.innerHTML=
      r138Pane('Runtime readiness',r138Err(rd)||((rd.ready?r138State('READY'):r138State('NOT READY'))+'<br>Build '+escapeHtml(st.build_name||st.build||'unknown')+' · mode '+escapeHtml(st.mode||'unknown')+'<br>Kill switch '+(run.kill_switch_blocked?'BLOCKED':'clear')),'#60a5fa')+
      r138Pane('ARM / AUTO separation',r138Err(g)||((run.arm_auto_invariant_ok?r138State('INVARIANT OK'):r138State('INVARIANT VIOLATION'))+'<br>LIVE armed: '+String(!!run.live_armed)+' · LIVE AUTO: '+String(!!run.live_auto)+'<br>'+r138State('RESEARCH AUTHORITY: 0')),'#ea3943')+
      r138Pane('Compliance',r138Err(g)||((g.compliant_for_research?r138State('RESEARCH GUARDS OK'):r138State('ATTENTION'))+'<br>'+r138Num(g.research_authority_violations)+' authority violations · '+r138Num(g.forbidden_systems_registered)+' forbidden systems<br>Credential scope: '+r138State((g.credential_scope||{}).status||'UNKNOWN')),'#94a3b8')+
      r138Pane('Recovery',r138Err(dr)||(snaps.length?((latest.hash_matches&&latest.sqlite_quick_check_ok?r138State('LATEST VERIFIED'):r138State('VERIFY FAILED'))+'<br>'+escapeHtml(latest.database_file||'')+'<br>'+snaps.length+' bounded snapshots retained'):r138State('NO SNAPSHOT YET')),'#84cc16');
  });
}

// R77 audit: value -> inline-JS string arg inside an HTML attribute (onclick="f('X')"): backslash-
// escape \ and ' for the JS string literal FIRST, then HTML-escape for the attribute context —
// attribute entities decode BEFORE the JS parses, so a quote in the data can't break out of either.
function jsq(s){return escapeHtml(String(s==null?"":s).replace(/\\/g,"\\\\").replace(/'/g,"\\'"));}
// R67f: EVERY external link opens in the user's DEFAULT BROWSER (POST /api/openurl → server execs
// the OS URL handler), never in a WebView2 child window. Anchors keep their href for middle-click.
function extOpen(u){
  u=String(u||'');
  if(!/^https?:\/\//i.test(u))return false;
  fetch('/api/openurl',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url:u})})
    .then(function(r){return r.json();}).then(function(d){if(d&&d.error)uiToast('open link: '+d.error);})
    .catch(function(){try{window.open(u,'_blank');}catch(e){}}); // server unreachable → last-resort old behavior
  return false;
}
// Capture-phase interceptor: any <a href="http(s)…"> to a NON-local origin routes through extOpen —
// one hook covers every venue link in every panel (left-click; middle-click keeps the raw href).
document.addEventListener('click',function(e){
  if(e.button!==0||e.ctrlKey||e.metaKey||e.shiftKey||e.altKey)return;
  var t=e.target,a=(t&&t.closest)?t.closest('a[href]'):null;
  if(!a)return;
  var h=a.getAttribute('href')||'';
  if(!/^https?:\/\//i.test(h))return;
  if(h.indexOf(location.origin+'/')===0||h===location.origin)return; // internal pages keep in-app behavior
  e.preventDefault();e.stopPropagation();
  extOpen(h);
},true);
// R57 nav dropdowns (thinkorswim pattern): 4 grouped text tabs open simple absolutely-positioned
// menus; a menu closes on outside click / Esc / picking an item. Items keep their original onclicks.
function closeDD(){document.querySelectorAll('.ddmenu').forEach(function(m){m.style.display='none';});document.querySelectorAll('.ddgrp.open').forEach(function(g){g.classList.remove('open');});}
function toggleDD(id,ev){if(ev)ev.stopPropagation();var el=document.getElementById(id);if(!el)return;var was=(el.style.display==='block');closeDD();if(!was){el.style.display='block';if(el.parentNode&&el.parentNode.classList)el.parentNode.classList.add('open');
  // R67b: #bar2 got overflow-x:auto + overflow-y:hidden in R63 4e, which CLIPS the absolutely-
  // positioned menu (its containing block is the .ddgrp INSIDE the scroll box) — the ＋Widgets menu
  // "opened" invisibly. Escape the clip: position:fixed at the trigger's on-screen rect.
  var tg=(ev&&ev.currentTarget&&ev.currentTarget.getBoundingClientRect)?ev.currentTarget:(el.parentNode||null);
  if(tg&&tg.getBoundingClientRect){var tr=tg.getBoundingClientRect();
    el.style.position='fixed';el.style.zIndex='400';
    el.style.left=Math.max(2,Math.min(tr.left,(window.innerWidth||1200)-210))+'px';
    el.style.top=(tr.bottom+1)+'px';
    el.style.maxHeight=Math.max(120,(window.innerHeight||800)-tr.bottom-12)+'px';el.style.overflowY='auto';}
}}
document.addEventListener('click',function(e){var t=e.target;if(!(t&&t.closest&&t.closest('.ddgrp')))closeDD();},true);
document.addEventListener('keydown',function(e){if(e.key==='Escape')closeDD();});
// withScroll keeps your reading position stable when a panel re-renders: if you're at
// the top it stays at the top (new entries appear above), otherwise it preserves the
// anchor by shifting scrollTop by the height the panel grew/shrank — so rows don't jump.
function withScroll(el,arg){
  if(!el)return;
  var isFn=(typeof arg==='function');
  // String form: dirty-check — skip the innerHTML rebuild when the markup is byte-identical to
  // last time. This is the real memory fix: it stops every panel from churning the DOM 7×/sec
  // when nothing changed, which is what grew memory until Chrome OOM'd on a long-open tab.
  if(!isFn){ if(el.__ls===arg)return; el.__ls=arg; }
  var card=(el.closest)?(el.closest('.wb')||el.closest('.card')):null; // R60: widget bodies anchor too
  if(!card){ if(isFn)arg(); else el.innerHTML=arg; return; }
  var top=card.scrollTop,h=card.scrollHeight,atTop=top<8;
  if(isFn)arg(); else el.innerHTML=arg;
  var nh=card.scrollHeight;
  card.scrollTop=atTop?0:Math.max(0,top+(nh-h));
}
function pill(p){var pct=Math.round(p||0);var bg,col;if(pct>=66){bg="rgba(52,211,153,.16)";col="#4ade80";}else if(pct>=33){bg="rgba(251,191,36,.16)";col="#fbbf24";}else{bg="rgba(148,163,184,.14)";col="#cbd5e1";}return '<span class="pill" style="background:'+bg+';color:'+col+'">'+pct+'%</span>';}
// R63 item 4 UNIFORM MARKETS CELLS — the three venue lists share ONE 11-column union:
// MARKET | BID | ASK | CHANCE | SIGNAL | Δ | 24H VOL | RESOLVES | LIVE | ≣ | ↗.
// A venue that lacks a field renders a blank "—" cell. sigCellU is THE signal renderer for all
// three (same pill style everywhere); dCell is THE Δ (24h move) renderer.
function pxCell(v){var c=Math.round((v||0)*100);return (c>=1&&c<=100)?('<td class="r vol">'+c+'</td>'):'<td class="r muted">—</td>';}
function dCell(mv){mv=mv||0;
  if(mv>=1)return '<td class="c" style="color:var(--good);font-weight:700" title="24h price move — up '+mv.toFixed(mv<10?1:0)+'¢">▲'+mv.toFixed(mv<10?1:0)+'</td>';
  if(mv<=-1)return '<td class="c" style="color:var(--bad);font-weight:700" title="24h price move — down '+Math.abs(mv).toFixed(mv>-10?1:0)+'¢">▼'+Math.abs(mv).toFixed(mv>-10?1:0)+'</td>';
  return '<td class="c muted">—</td>';}
function sigCellU(m){
  if(m&&m.arb&&m.arb>0)return '<td class="c"><span class="pill" style="background:rgba(52,211,153,.22);color:#4ade80" title="cross-venue arb — locked gross edge">ARB +'+(m.arb*100).toFixed(0)+'¢</span></td>';
  if(m&&m.flow_side&&(m.flow_strength||0)>=0.5)return '<td class="c"><span class="pill" style="background:'+(m.flow_side==='YES'?'rgba(52,211,153,.16)':'rgba(234,57,67,.16)')+';color:'+(m.flow_side==='YES'?'var(--good)':'var(--bad)')+'" title="recent aggressive (taker) money: '+(m.flow_notional?('$'+fmtVol(m.flow_notional)):'')+'">'+m.flow_side+' '+Math.round((m.flow_strength||0)*100)+'%</span></td>';
  return '<td class="c muted">—</td>';}
function liveDotCell(on,tip){return on?('<td class="c"><span class="dot" title="'+escapeHtml(tip||'live')+'"></span></td>'):'<td class="c muted">—</td>';}
// R67g SUBMARKET GROUPING: rows sharing one EVENT (kalshi: event-ticker prefix · poly: event slug/url
// · polyus: event_id) render as ONE main row — the group's highest-volume market, always visible —
// plus its siblings (O/U, totals, spreads, other outcomes) as an indented darker sub-block with the
// SAME columns, EXPANDED by default. Collapse state survives repaints via window._grpClosed.
window._grpClosed={};
function grpBuild(list,keyFn,volFn){
  var by={},order=[];
  list.forEach(function(m,i){var k=String(keyFn(m,i)||('solo_'+i));if(!by[k]){by[k]=[];order.push(k);}by[k].push(i);});
  return order.map(function(k){var idxs=by[k],mi=idxs[0],best=-1;
    idxs.forEach(function(i){var v=Number(volFn(list[i]))||0;if(v>best){best=v;mi=i;}});
    return {key:k,mi:mi,subs:idxs.filter(function(i){return i!==mi;})};});
}
function grpId(pfx,key){return pfx+'_'+String(key).replace(/[^A-Za-z0-9_-]/g,'').slice(-48);}
function tgGrp(gid,btn){
  var closed=!(window._grpClosed[gid]);window._grpClosed[gid]=closed;
  document.querySelectorAll('tr.sub_'+gid).forEach(function(tr){tr.style.display=closed?'none':'';});
  if(btn)btn.textContent=(closed?'▸':'▾')+String(btn.textContent||'').slice(1);
}
function grpCaret(gid,n){
  var closed=!!window._grpClosed[gid];
  return '<span style="cursor:pointer;color:var(--accent);font-weight:700" title="'+n+' sibling market'+(n>1?'s':'')+' in this game/event (moneyline · spreads · totals · props · other outcomes — R72-B groups across series) — click to collapse/expand" onclick="event.preventDefault();event.stopPropagation();tgGrp(\''+gid+'\',this);">'+(closed?'▸':'▾')+n+'</span> ';
}
function grpSubRow(gid){ // class + darker tint + honored collapse state; the name cell adds its own indent
  var closed=!!window._grpClosed[gid];
  return ' class="sub_'+gid+'" style="background:rgba(0,0,0,.28)'+(closed?';display:none':'')+'"';
}
function bookCell(js,tip){return '<td class="c"><button class="mini" style="padding:0 3px" title="'+escapeHtml(tip||'order book')+'" onclick="'+js+'">≣</button></td>';}
function goCell(url){return '<td class="c">'+(url?('<a class="go" href="'+escapeHtml(url)+'" target="_blank" rel="noopener" title="open on venue">↗</a>'):'<span class="muted">—</span>')+'</td>';}
var openTicker=null,lastEnv="";
// Request coalescing: if a GET to the same URL is already in flight, hand back the SAME promise
// instead of firing a duplicate. The dashboard polls many endpoints ~1.4/s; without this, a slow
// (cache-miss) response let polls stack up and saturate the browser's ~6 sockets per host — which is
// what made clicking AUTO / opening Settings hang for 10-30s while they waited for a free socket.
var _inflight={},_lastGoodGet={};
function jget(u,timeoutMs){
  if(_inflight[u])return _inflight[u];
  // TIMEOUT (audit §4): jget had no AbortController, so one stuck handler froze its panel silently
  // forever. 20s is beyond every server-side timeout — a hang now recovers on the next poll.
  var ac=(typeof AbortController!=="undefined")?new AbortController():null;
  var ms=Math.max(2000,Number(timeoutMs)||20000),timedOut=false;
  var tid=ac?setTimeout(function(){timedOut=true;ac.abort();},ms):null;
  var opts={cache:'no-store'};if(ac)opts.signal=ac.signal;
  var p=fetch(u,opts).then(function(r){
    if(!r.ok)throw new Error('Report returned HTTP '+r.status);
    return r.json();
  }).then(function(d){
    _lastGoodGet[u]={value:d,at:Date.now()};return d;
  }).catch(function(err){
    var plain=timedOut?'Report refresh timed out; retry shortly.':('Report unavailable; '+String((err&&err.message)||'request failed')+'.');
    var last=_lastGoodGet[u];
    if(last&&last.value){
      var stale=Object.assign({},last.value);stale._report_stale=true;stale._report_warning=plain;stale._report_last_good_at=last.at;return stale;
    }
    throw new Error(plain);
  });
  _inflight[u]=p;
  p.then(function(){if(tid)clearTimeout(tid);delete _inflight[u];},function(){if(tid)clearTimeout(tid);delete _inflight[u];});
  return p;
}
// COMPACT MODE (operator directive: comprehensive functional minimalism): hides the explainer
// paragraphs and chrome; persists in-memory per window (no storage APIs). Default ON.
// ONE-PORTFOLIO chip + readiness dots (SSE-era header: the two numbers that matter at a glance).
function loadNetworth(){
  jget("/api/networth").then(function(d){
    var el=document.getElementById("networth");if(!el||!d)return;
    if(d.building){setTimeout(loadNetworth,2000);return;}
    var t=d.total_net||0;
    // R60: the "net" chip is the PAPER book — renamed (operator) + the live chip split out beside it.
    el.innerHTML='PAPER <b style="color:'+((t>=0)?'var(--good)':'var(--bad)')+'">$'+(t>=0?'+':'')+t.toFixed(2)+'</b>';
    el.title="THE Paper simulation portfolio, SINCE last Reset P&L · paper $"+(d.paper_net||0).toFixed(2)+" · system combos $"+(d.parlay_net||0).toFixed(2)+" · ML combos $"+(d.ml_combo_net||0).toFixed(2)+" · RFQ comparison $"+(d.live_net||0).toFixed(2)+" (simulated settled net, fee-inclusive; not exchange profit evidence)";
    var lc=document.getElementById("livechip");
    if(lc){
      if(d.live_pnl!=null){var age=(d.live_pnl_age_s||0),lk=(d.live_pnl_kalshi||0),lp=(d.live_pnl_polyus||0),kok=d.live_pnl_kalshi_complete===true,pok=d.live_pnl_polyus_complete===true;
        var ktxt=kok?('<span title="Kalshi fee-net realized plus current open marks" style="color:'+((lk>=0)?'var(--good)':'var(--bad)')+'">K '+(lk>=0?'+':'')+'$'+Math.abs(lk).toFixed(2)+'</span>'):'<span class="muted" title="Kalshi P&amp;L is incomplete because current account, position, mark, or settlement truth is unavailable">K n/a</span>';
        var ptxt=pok?('<span title="PolyUS fee-net realized plus current open marks" style="color:'+((lp>=0)?'var(--good)':'var(--bad)')+'">PUS '+(lp>=0?'+':'')+'$'+Math.abs(lp).toFixed(2)+'</span>'):'<span class="muted" title="PolyUS P&amp;L is incomplete because current account, position, mark, or settlement truth is unavailable">PUS n/a</span>';
        lc.innerHTML='LIVE '+ktxt+' · '+ptxt+(age>120?('<span class="muted"> '+Math.round(age/60)+'m old</span>'):'');
        lc.title='LIVE P&L is shown per venue only when current authenticated account, every open mark, and the realized settlement ledger are complete.';
      }else{lc.innerHTML='LIVE <span class="muted">—</span>';}
    }
    // R60 live-pnl widget: draw the server's ~15s live P&L ring exactly like the session sparkline.
    var lw=document.getElementById('livePnlW');
    if(lw&&d.live_pnl_hist&&d.live_pnl_hist.length>1){
      renderPnLChart(d.live_pnl_hist.map(function(p){return {ts:new Date((p.t||0)*1000).toISOString(),pnl:(p.v||0)};}),'livePnlW');
    }
  }).catch(function(){});
}
function loadReady(){
  jget("/api/ready").then(function(d){
    var el=document.getElementById("readydots");if(!el||!d)return;
    var order=["feeds","kalshi_ws","poly_tape","ml_sidecar","db"];
    var h="";var names={feeds:"signal feeds",kalshi_ws:"Kalshi WS",poly_tape:"Poly tape",ml_sidecar:"ML sidecar",db:"database"};
    order.forEach(function(k){var c=(d.components||{})[k];var ok=c&&c.ok;h+='<span class="rdot" style="background:'+(ok?'var(--good)':'var(--bad)')+'" title="'+(names[k]||k)+': '+(c?(c.detail||''):'?')+'"></span>';});
    Object.keys(d.components||{}).sort().forEach(function(k){if(order.indexOf(k)>=0)return;var c=d.components[k];h+='<span class="rdot" style="background:'+(c&&c.state==='none'?'var(--muted)':(c&&c.state==='warming'?'#f59e0b':(c&&c.ok?'var(--good)':'var(--bad)')))+'" title="'+escapeHtml(k+': '+((c&&c.detail)||''))+'"></span>';}); // R142: WARMING is amber but never execution authority.
    el.innerHTML=h+(d.ready?'':' <span style="color:var(--warn);font-size:11px">warming</span>');
    el.title="system readiness — "+(d.ready?"all systems loaded":"still warming up");
    // R86: the 🔑 KALSHI AUTH LOCKED chip — shown (and kept shown) while kalshi_auth reports
    // state "locked" (credentials present but unusable). Tooltip = the exact fix text from the
    // server; hidden again the moment a restart loads the signer. R87: the fallback text names
    // BOTH fixes — key file (kalshi_key_file + kalshi_key_id in Settings) OR the passphrase.
    var ac=document.getElementById("authchip");
    if(ac){var ca=(d.components||{}).kalshi_auth;
      if(ca&&ca.state==='locked'){ac.title='Kalshi auth: '+(ca.detail||'credentials locked - fix: kalshi_key_file + kalshi_key_id (Settings) OR re-set KALSHI_SUITE_PASSPHRASE, then restart');ac.style.display='';}
      else{ac.style.display='none';}}
  }).catch(function(){});
}
// R107 Part 5: latency chip + panel. Chip always paints (global 8s cycle polls it with status);
// the panel renders only when the Logs tab has been opened (lazy-tab doctrine).
function _latSvg(pts,w,h,color){ // tiny self-contained sparkline: pts = [{t,ms}]
  if(!pts||pts.length<2)return '<span class="muted">—</span>';
  var max=0,min=1e18;pts.forEach(function(p){if(p.ms>max)max=p.ms;if(p.ms<min)min=p.ms;});
  if(max<=min)max=min+1;
  var xs=pts.map(function(p,i){return (i/(pts.length-1)*(w-2)+1).toFixed(1)+','+((1-(p.ms-min)/(max-min))*(h-2)+1).toFixed(1);});
  return '<svg viewBox="0 0 '+w+' '+h+'" width="'+w+'" height="'+h+'" style="vertical-align:middle"><polyline fill="none" stroke="'+color+'" stroke-width="1.2" points="'+xs.join(' ')+'"/></svg>';
}
function loadLatency(){
  jget("/api/latency").then(function(d){
    var el=document.getElementById("latchip");if(!el||!d)return;
    var col=d.chip==='red'?'var(--bad)':(d.chip==='yellow'?'var(--warn)':'var(--good)');
    var cur=d.current||{};
    var k=(cur.kalshi_rest||{}).p50, p=(cur.polyus_rest||{}).p50;
    el.innerHTML='<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:'+col+';margin-right:4px;vertical-align:middle"></span>lat '+(k!=null?Math.round(k)+'ms':'…');
    el.title='connection/latency health: '+d.chip+(d.why?(' ('+d.why+')'):'')+
      ' · Kalshi p50 '+(k!=null?Math.round(k)+'ms':'?')+' · PolyUS p50 '+(p!=null?Math.round(p)+'ms':'?')+
      ' · DB p95 '+((cur.db_ms||{}).p95!=null?Math.round(cur.db_ms.p95)+'ms':'?')+
      ' · clock skew '+(d.clock_skew_ms!=null?Math.round(d.clock_skew_ms)+'ms':'?')+
      ' · '+(((d.upload||{}).verdict)||'');
    var pn=document.getElementById("latpanel");
    if(pn&&TABSEEN.logs){
      var rows=[["kalshi_rest","Kalshi REST","#6ba3f8"],["polyus_rest","PolyUS REST","#f8b76b"],["polyint_rest","Poly-int REST","#b96bf8"],["db_ms","DB canary","#6bf8a3"],["briefing_ms","Briefing render","#f86b9d"],["order_polyus_ms","Live order RTT","#f8e36b"]];
      var h='<table style="border-collapse:collapse"><tr class="muted"><td style="padding:1px 8px 1px 0">metric</td><td style="padding:1px 8px">last</td><td style="padding:1px 8px">p50</td><td style="padding:1px 8px">p95</td><td style="padding:1px 8px">1h</td></tr>';
      rows.forEach(function(r){var c=cur[r[0]];if(!c)return;
        h+='<tr><td style="padding:1px 8px 1px 0">'+r[1]+'</td><td style="padding:1px 8px">'+Math.round(c.last)+'ms</td><td style="padding:1px 8px">'+Math.round(c.p50)+'ms</td><td style="padding:1px 8px">'+Math.round(c.p95)+'ms</td><td style="padding:1px 8px">'+_latSvg((d.rings||{})[r[0]],140,18,r[2])+'</td></tr>';});
      h+='</table><div class="muted" style="margin-top:4px">'+escapeHtml((((d.upload||{}).verdict)||''))+' · clock skew '+Math.round(d.clock_skew_ms||0)+'ms · WS: Kalshi '+(((cur.kalshi_ws_tickers||{}).last)||0)+' tickers · PolyUS '+(((cur.polyus_ws_fresh||{}).last)||0)+' fresh · priv '+((((cur.pus_priv_ok||{}).last)||0)>=1?'ok':'down')+'</div>';
      if(pn._h!==h){pn._h=h;pn.innerHTML=h;}
    }
  }).catch(function(){});
}
function loadStatus(){
  return jget("/api/status").then(function(s){
    lastEnv=s.environment; // env badge REMOVED (operator) — R70: the dead setBadge("conn",…) no-ops went with it (no such element)
    var k=s.kill_switch||{};var b=document.getElementById("ksbtn");
    window._ksTripped=!!k.tripped; // cached so toggleKill NEVER waits on a status round-trip (audit §4)
    b.textContent=k.tripped?"Reset kill switch":"Trip kill switch";b.className=k.tripped?"":"danger";
    var bc=document.getElementById("bldchip"); // R72-A #2: color+animal build chip (left of RESET)
    if(bc&&s.build_name){
      bc.style.display='';bc.title='build '+(s.version||'?');
      var bh='<span style="display:inline-block;width:7px;height:7px;border-radius:50%;background:'+(s.build_color||'#888')+';border:1px solid rgba(255,255,255,.35);margin-right:4px;vertical-align:middle"></span>'+s.build_name;
      if(bc._h!==bh){bc._h=bh;bc.innerHTML=bh;} // repaint only on change
    }
  }).catch(function(){});
}
/* R75 (operator: "Could not load markets (signal is aborted)"): /api/markets is now SERVER-
   PAGINATED (?q=&offset=&limit=, default 300) over the full R73 universe, and the MARKETS tab
   NEVER autoloads. Data fetches ONLY on the Load button, the search box (server-side search),
   or load-next-page (button/scroll); the 4s poll merely re-fetches rows ALREADY loaded, and
   only while a markets surface is visible (refresh() gates it). */
window._mktQ='';window._mktList=[];window._mktTotal=0;window._mktUniverse=0;window._mktBusy=false;window._mktWarmTries=0;
function mktUrl(off,lim){return "/api/markets?offset="+off+"&limit="+lim+(window._mktQ?("&q="+encodeURIComponent(window._mktQ)):"");}
function mktFetch(off,lim,append){
  if(window._mktBusy)return;
  window._mktBusy=true;
  jget(mktUrl(off,lim)).then(function(d){
    window._mktBusy=false;
    if(d&&d.building){ // first full-universe pull still warming — bounded auto-retry
      var rs=document.getElementById("rows");
      if(rs&&!append)rs.innerHTML='<tr><td colspan="11" class="muted">Kalshi board is warming up (first full-universe pull in flight) — retrying automatically…</td></tr>';
      if(window._mktWarmTries++<10)setTimeout(function(){mktFetch(off,lim,append);},2500);
      return;
    }
    window._mktWarmTries=0;
    var list=(d&&d.markets)||[];
    window._mktList=append?window._mktList.concat(list):list;
    window._mktTotal=(d&&d.total)||list.length;
    window._mktUniverse=(d&&d.universe)||0;
    renderMarkets();
  }).catch(function(e){
    window._mktBusy=false;
    if(!window._mktList.length){var rs=document.getElementById("rows");if(rs)rs.innerHTML='<tr><td colspan="11" class="muted">Could not load markets ('+escapeHtml((e&&e.message)||'fetch failed')+') — press Load markets to retry.</td></tr>';}
  });
}
function mktLoadFirst(){window._mktList=[];window._mktTotal=0;mktFetch(0,300,false);}
function mktLoadMore(){if(window._mktList.length<window._mktTotal)mktFetch(window._mktList.length,300,true);}
function mktSearchGo(){ // search box → SERVER-side search over the whole in-memory universe
  var box=document.getElementById('lmsearch');
  window._mktQ=((box&&box.value)||'').trim();
  window._mktList=[];window._mktTotal=0;mktFetch(0,300,false);
}
var _mktSearchT=null;
function mktSearchDeb(){if(_mktSearchT)clearTimeout(_mktSearchT);_mktSearchT=setTimeout(mktSearchGo,450);}
function mktBindScroll(){ // scroll-next-page: fetch the next page as the loaded list bottoms out
  var rows=document.getElementById("rows");if(!rows)return;
  var sc=rows;
  while(sc&&sc.nodeType===1&&sc!==document.body){
    var cs;try{cs=getComputedStyle(sc);}catch(e){return;}
    if(sc.scrollHeight>sc.clientHeight+8&&(cs.overflowY==="auto"||cs.overflowY==="scroll"))break;
    sc=sc.parentNode;
  }
  if(!sc||sc.nodeType!==1||sc===document.body||sc._mktScrollWired)return;
  sc._mktScrollWired=true;
  sc.addEventListener("scroll",function(){
    if(window._mktBusy||!window._mktList.length||window._mktList.length>=window._mktTotal)return;
    if(sc.scrollTop+sc.clientHeight>=sc.scrollHeight-120)mktLoadMore();
  },{passive:true});
}
/* R102 GAME TREE — the structural cross-venue view (see mkSecG). Lazy: loads on Markets-tab open,
   refreshes at most every 30s while the tab is visible. Expansion state survives re-renders. */
var _gtData=null,_gtAt=0,_gtBusy=false;window._gtOpen=window._gtOpen||{};
function gtEmoji(lg){lg=String(lg||'').toLowerCase();
  if(lg==='mlb'||lg==='cws'||lg==='bsl')return '⚾';
  if(lg==='nba'||lg==='wnba'||lg==='cbb'||lg==='wcbb'||lg==='fiba')return '🏀';
  if(lg==='nfl')return '🏈';
  if(lg==='nhl')return '🏒';
  if(lg==='ufc'||lg==='boxing')return '🥊';
  if(lg==='atp'||lg==='wta'||lg==='itfm'||lg==='itfw')return '🎾';
  if(lg==='lol'||lg==='cs2'||lg==='valorant'||lg==='dota2'||lg==='cod')return '🎮';
  if(lg==='ipl'||lg==='t20'||lg==='wt20'||lg==='test-cricket'||lg==='countychamp'||lg==='mlc')return '🏏';
  return '⚽';}
function loadGameTree(){
  if(_gtBusy)return;_gtBusy=true;
  jget('/api/gametree').then(function(d){_gtBusy=false;_gtAt=Date.now();_gtData=(d&&d.games)?d:{games:[]};renderGameTree();})
  .catch(function(){_gtBusy=false;var el=document.getElementById('gametree');if(el&&!_gtData)el.innerHTML='<span class="muted">Could not load the game tree — retrying on the next pass.</span>';});
}
function gtMaybeRefresh(){if(Date.now()-_gtAt>30000)loadGameTree();}
function gtToggleIdx(i){var g=(_gtData&&_gtData.games)?_gtData.games[i]:null;if(!g)return;window._gtOpen[g.game_id]=!window._gtOpen[g.game_id];renderGameTree();}
function gtPx(b,a){var f=function(v){return (v>0&&v<1)?Math.round(v*100)+'¢':'—';};return f(b)+' / '+f(a);}
function gtWhen(s){if(!s)return '';try{var d=new Date(s);if(isNaN(d))return '';return d.toLocaleString([],{weekday:'short',hour:'2-digit',minute:'2-digit'});}catch(e){return '';}}
function renderGameTree(){
  var el=document.getElementById('gametree');if(!el)return;
  var d=_gtData||{games:[]};var gs=d.games||[];
  var cEl=document.getElementById('gtcount');
  if(cEl){var st=d.stats||{};var kp=(st.kal_seen>0)?Math.round(100*st.kal_anchored/st.kal_seen):0;var pp=(st.pus_seen>0)?Math.round(100*st.pus_anchored/st.pus_seen):0;
    cEl.textContent=gs.length+' games · anchored: Kalshi '+kp+'% · PolyUS '+pp+'%';}
  if(!gs.length){el.innerHTML='<span class="muted">No anchored games in the next 72h yet — the join builds from live venue data within a few minutes of boot.</span>';return;}
  var h='';
  var typeNames={winner:'Moneyline',advance:'To Advance',spread:'Spread',total:'Total',prop:'Props'};
  gs.forEach(function(g,i){
    var open=!!window._gtOpen[g.game_id];
    var nm=(g.away_name||g.away)+' @ '+(g.home_name||g.home);
    var badges=(g.venues==='K+P')?'<span style="color:#84cc16;font-weight:700">K</span>+<span style="color:#3b82f6;font-weight:700">P</span>':(g.venues==='K'?'<span style="color:#84cc16;font-weight:700">K</span>':'<span style="color:#3b82f6;font-weight:700">P</span>');
    h+='<div style="border-top:1px solid var(--line);padding:5px 2px;cursor:pointer" onclick="gtToggleIdx('+i+')">'
      +'<span style="display:inline-block;width:14px;color:var(--accent)">'+(open?'▾':'▸')+'</span>'
      +gtEmoji(g.league)+' <b>'+escapeHtml(nm)+'</b>'
      +(g.live?' <span title="game live" style="color:#22c55e">● LIVE</span>':'')
      +' <span class="muted" style="font-size:12px">'+escapeHtml(gtWhen(g.start))+' · '+escapeHtml(String(g.league||'').toUpperCase())+' · '+badges+' · '+(g.markets?g.markets.length:0)+' markets</span></div>';
    if(!open)return;
    var byType={};(g.markets||[]).forEach(function(m){var t=m.type||'prop';(byType[t]=byType[t]||[]).push(m);});
    ['winner','advance','spread','total','prop'].forEach(function(t){
      var rows=byType[t];if(!rows||!rows.length)return;
      h+='<div style="padding:2px 0 2px 22px;font-size:12px;color:var(--accent);font-weight:700">'+typeNames[t]+'</div>';
      h+='<table class="mkt" style="margin-left:22px;width:calc(100% - 22px)"><colgroup><col><col style="width:120px"><col style="width:30px"><col style="width:120px"><col style="width:30px"></colgroup>';
      h+='<thead><tr><th></th><th class="c" style="color:#84cc16">Kalshi bid/ask</th><th></th><th class="c" style="color:#3b82f6">Poly US bid/ask</th><th></th></tr></thead><tbody>';
      rows.forEach(function(m){
        var kCell=m.k_ticker?('<td class="c">'+gtPx(m.k_bid,m.k_ask)+'</td><td class="c">'+(m.k_url?('<a class="go" href="'+escapeHtml(m.k_url)+'" target="_blank" rel="noopener" onclick="event.stopPropagation()">↗</a>'):'')+'</td>'):'<td class="c muted">—</td><td></td>';
        var pLbl=m.p_flip?' <span class="muted" title="Poly US quotes this from the other team — its YES equals the Kalshi NO" style="font-size:11px">↔NO</span>':'';
        var pCell=m.p_slug?('<td class="c">'+gtPx(m.p_bid,m.p_ask)+pLbl+'</td><td class="c">'+(m.p_url?('<a class="go" href="'+escapeHtml(m.p_url)+'" target="_blank" rel="noopener" onclick="event.stopPropagation()">↗</a>'):'')+'</td>'):'<td class="c muted">—</td><td></td>';
        h+='<tr><td class="name ell" title="'+escapeHtml(m.label||'')+'">'+escapeHtml(m.label||'')+'</td>'+kCell+pCell+'</tr>';
      });
      h+='</tbody></table>';
    });
  });
  el.innerHTML=h;
}
// ── R106 UNIVERSAL MARKET TREE (genre → event → members; venue-metadata genres, fully lazy) ──
function loadMarketTree(){
  var el=document.getElementById('markettree');if(!el)return;
  jget('/api/markettree').then(function(d){
    var el2=document.getElementById('markettree');if(!el2)return;
    window._mtGenres=d.genres||[];window._mtOpen=window._mtOpen||{};window._mtEv=window._mtEv||{};window._mtEvOpen=window._mtEvOpen||{};window._mtMem=window._mtMem||{};
    var c=document.getElementById('mtcount');if(c)c.textContent=(d.total_markets||0)+' markets · '+(window._mtGenres.length)+' genres · unknown '+(d.unknown_pct!=null?d.unknown_pct:'—')+'%';
    renderMarketTree();
  }).catch(function(){var el2=document.getElementById('markettree');if(el2)el2.innerHTML='<span class="muted">Could not load the market tree.</span>';});
}
function mtGenreEmoji(g){return {Sports:'🏟️',Crypto:'🪙',Weather:'⛅',Politics:'🏛️',Economics:'📈',Entertainment:'🎬','Other/Unknown':'❓'}[g]||'🗂️';}
function mtToggleGenre(g){
  window._mtOpen[g]=!window._mtOpen[g];
  if(window._mtOpen[g]&&!window._mtEv[g]){
    jget('/api/markettree?genre='+encodeURIComponent(g)).then(function(d){window._mtEv[g]=d.events||[];renderMarketTree();}).catch(function(){});
  }
  renderMarketTree();
}
function mtToggleEvent(g,key){
  var k=g+'||'+key;
  window._mtEvOpen[k]=!window._mtEvOpen[k];
  if(window._mtEvOpen[k]&&!window._mtMem[k]){
    jget('/api/markettree?genre='+encodeURIComponent(g)+'&event='+encodeURIComponent(key)).then(function(d){window._mtMem[k]=d.markets||[];renderMarketTree();}).catch(function(){});
  }
  renderMarketTree();
}
function renderMarketTree(){
  var el=document.getElementById('markettree');if(!el)return;
  var gs=window._mtGenres||[];
  if(!gs.length){el.innerHTML='<span class="muted">No live markets yet — the tree builds from the venue caches within a minute of boot.</span>';return;}
  var h='';
  gs.forEach(function(g){
    var open=!!window._mtOpen[g.genre];
    h+='<div style="border-top:1px solid var(--line);padding:5px 2px;cursor:pointer" onclick="mtToggleGenre('+JSON.stringify(g.genre).replace(/"/g,'&quot;')+')">'
      +'<span style="display:inline-block;width:14px;color:var(--accent)">'+(open?'▾':'▸')+'</span>'
      +mtGenreEmoji(g.genre)+' <b>'+escapeHtml(g.genre)+'</b>'
      +' <span class="muted" style="font-size:12px">'+(g.events||0)+' events · '+(g.markets||0)+' markets'+(g.genre==='Sports'?' · anchored games render in 🧩 above':'')+'</span></div>';
    if(!open)return;
    var evs=window._mtEv[g.genre];
    if(!evs){h+='<div class="muted" style="padding:2px 0 4px 22px;font-size:12px">Loading events…</div>';return;}
    if(!evs.length){h+='<div class="muted" style="padding:2px 0 4px 22px;font-size:12px">No event groups right now.</div>';return;}
    evs.slice(0,120).forEach(function(e){
      var k=g.genre+'||'+e.key;var eopen=!!window._mtEvOpen[k];
      var vTag=(e.venue==='K')?'<span style="color:#84cc16;font-weight:700">K</span>':(e.venue==='P'?'<span style="color:#3b82f6;font-weight:700">P</span>':'<span style="color:#94a3b8;font-weight:700" title="Poly-int (research venue)">I</span>');
      h+='<div style="padding:3px 0 3px 22px;cursor:pointer;font-size:12.5px" onclick="mtToggleEvent('+JSON.stringify(g.genre).replace(/"/g,'&quot;')+','+JSON.stringify(e.key).replace(/"/g,'&quot;')+')">'
        +'<span style="display:inline-block;width:14px;color:var(--accent)">'+(eopen?'▾':'▸')+'</span>'
        +escapeHtml(e.title||e.key)+' <span class="muted">· '+vTag+' · '+(e.n||0)+(e.series?(' · '+escapeHtml(e.series)):'')+'</span></div>';
      if(!eopen)return;
      var mem=window._mtMem[k];
      if(!mem){h+='<div class="muted" style="padding:2px 0 2px 44px;font-size:12px">Loading…</div>';return;}
      h+='<table class="mkt" style="margin-left:44px;width:calc(100% - 44px)"><colgroup><col><col style="width:52px"><col style="width:92px"><col style="width:30px"></colgroup>'
        +'<thead><tr><th>Market</th><th class="c">Kind</th><th class="c">YES bid/ask</th><th></th></tr></thead><tbody>';
      mem.forEach(function(m){
        var px=((m.yes_bid>0&&m.yes_bid<1)?Math.round(m.yes_bid*100)+'¢':'—')+' / '+((m.yes_ask>0&&m.yes_ask<1)?Math.round(m.yes_ask*100)+'¢':'—');
        h+='<tr><td class="name ell" title="'+escapeHtml(m.title||m.id||'')+'">'+escapeHtml(m.title||m.id||'')+'</td><td class="c muted">'+escapeHtml(m.kind||'')+'</td><td class="c">'+px+'</td><td class="c">'+(m.url?('<a class="go" href="'+escapeHtml(m.url)+'" target="_blank" rel="noopener" onclick="event.stopPropagation()">↗</a>'):'')+'</td></tr>';
      });
      h+='</tbody></table>';
    });
  });
  el.innerHTML=h;
}
function loadMarkets(){ // POLL path: refreshes ONLY the rows already on screen; never autoloads
  if(!window._mktList.length){renderMarkets();return;}
  if(window._mktBusy)return;
  // R77 audit (paging truncation): the server caps limit at 1000 PER REQUEST but pages to any offset,
  // so refresh the loaded set in <=1000-row pages — the old single clamped fetch REPLACED _mktList
  // with the first 1000 rows, silently dropping everything the user had paged past.
  var want=window._mktList.length;
  window._mktBusy=true;
  var acc=[];
  (function page(off){
    jget(mktUrl(off,Math.min(Math.max(want-off,300),1000))).then(function(d){
      if(!d||d.building){window._mktBusy=false;return;}
      var got=(d&&d.markets)||[];
      acc=acc.concat(got);
      window._mktTotal=(d&&d.total)||0;
      window._mktUniverse=(d&&d.universe)||0;
      if(got.length&&acc.length<want&&acc.length<window._mktTotal){page(acc.length);return;}
      window._mktBusy=false;
      window._mktList=acc;
      renderMarkets();
    }).catch(function(){window._mktBusy=false;});
  })(0);
}
function renderMarkets(){
    var rows=document.getElementById("rows");if(!rows)return;
    var note=document.getElementById("note");if(note)note.innerHTML="";
    var list=window._mktList||[];
    var cEl=document.getElementById("count");
    if(cEl)cEl.textContent=list.length?(list.length+" of "+(window._mktTotal||list.length)+" loaded · universe "+(window._mktUniverse||"?")+" · most active first"):"";
    window.mktById={};
    var mh="";
    if(list.length===0){
      mh='<tr><td colspan="11" class="muted" style="padding:10px 6px"><button class="mini" onclick="mktLoadFirst()" style="padding:3px 12px;font-weight:700">Load markets</button><span style="margin-left:10px">'+(window._mktQ?('No matches for '+escapeHtml(window._mktQ)+' — clear the search or load the board.'):'Markets load on demand now (operator R75): press Load, or search the whole universe above.')+'</span></td></tr>';
      if(lastEnv==="demo"&&note){note.innerHTML='<div class="muted" style="margin-top:8px;color:var(--warn)">You are on the demo environment (sparse data). Set "environment":"prod" in config.json and restart.</div>';}
    }else{ // R63 item 4: 11-col uniform union · R67g: grouped by EVENT (ticker prefix before the last dash)
      var kRow=function(m,gid,isSub,caret){
        window.mktById[m.ticker]=m;
        var name=(m.title||m.ticker)+(m.sub?(" — "+m.sub):"");
        var url=m.url||posURL({platform:'kalshi',ticker:m.ticker,title:m.title});
        return '<tr'+(isSub?grpSubRow(gid):'')+'><td class="name ell"'+(isSub?' style="padding-left:18px"':'')+' title="'+escapeHtml(name)+'">'+(caret||'')+'<a class="go" href="'+escapeHtml(url)+'" target="_blank" rel="noopener">'+escapeHtml(name)+'</a></td>'+
          pxCell(m.yes_bid)+pxCell(m.yes_ask)+
          '<td class="c">'+pill(m.implied_pct)+'</td>'+
          sigCellU({arb:m.arb_profit})+
          dCell(m.move_cents||0)+
          '<td class="r vol">'+fmtVol(m.volume_24h)+'</td>'+
          '<td class="r when">'+escapeHtml(fmtClose(m.close_time))+'</td>'+
          liveDotCell(m.live,'LIVE — price actively updating (refreshed ≤90s), or the ticker-embedded event start has passed and it has not resolved yet (in-play by schedule)')+
          bookCell("showBook('"+m.ticker+"')",'order book + whale prints')+
          goCell(url)+'</tr>';
      };
      /* R72-B SUB-MARKETS: group by GAME, not event ticker. A spread/total/prop lives in its OWN
         series+event (KXMLBSPREAD-25JUL04..BOSSEA vs KXMLBGAME-25JUL04..BOSSEA), so the R67g
         event-ticker key never merged them under the moneyline. The game key strips the series
         prefix and keeps the date+teams event segment WHEN it ends in a team-pair letter run
         (4+ letters); date-only segments (crypto dailies etc.) keep the full event ticker so
         KXBTCD/KXETHD can never merge. Volume still picks the group's parent row (the moneyline
         is virtually always the volume leader). */
      var kalGameKey=function(m){
        var t=String(m.ticker||'');var i=t.lastIndexOf('-');var ev=i>0?t.slice(0,i):t;
        var d=ev.indexOf('-');if(d<=0)return ev;
        var seg=ev.slice(d+1);
        return /[A-Z]{4,}$/.test(seg)?seg:ev;
      };
      grpBuild(list,kalGameKey,function(m){return m.volume_24h;}).forEach(function(g){
        var gid=grpId('k',g.key);
        mh+=kRow(list[g.mi],gid,false,g.subs.length?grpCaret(gid,g.subs.length):'');
        g.subs.forEach(function(si){mh+=kRow(list[si],gid,true,'');});
      });
      if(list.length<window._mktTotal){ // R75: next-page fetch — button (and the scroll hook below)
        mh+='<tr><td colspan="11" class="c" style="padding:6px"><button class="mini" onclick="mktLoadMore()" style="padding:3px 12px">Load next 300 ('+list.length+' of '+window._mktTotal+' loaded)</button></td></tr>';
      }
    }
    withScroll(rows,mh);
    mktBindScroll();
}
function applyLiveFilter(){
  var box=document.getElementById('lmsearch');var q=((box&&box.value)||'').trim().toLowerCase();
  // R75: the KALSHI list ('rows') left this client-side filter — the box now drives a SERVER-side
  // search over the full universe for it (mktSearchDeb) — a textContent filter would wrongly hide
  // server matches (e.g. ticker-only hits whose row text shows the friendly title).
  ['pmrows','pmusrows'].forEach(function(id){ // R63 1b: poly-int + Poly US stay client-filtered
    var tb=document.getElementById(id);if(!tb)return;
    Array.prototype.forEach.call(tb.querySelectorAll('tr'),function(tr){
      tr.style.display=(!q||(tr.textContent||'').toLowerCase().indexOf(q)>=0)?'':'none';
    });
  });
}
function showBets(i){var m=(window.lastPM||[])[i];if(!m)return;
  openBetsPop(m.question||'Market'); // R67f: centered popup again (buycard pattern), not a tab
  fetch('/api/market-bets?cond='+encodeURIComponent(m.cond_id||'')).then(function(r){return r.json();}).then(renderBets).catch(function(){document.getElementById('betsBody').innerHTML='<div class="muted" style="padding:8px 0">Could not load bets.</div>';});
}
function openBetsPop(title){
  document.getElementById('betscard').style.display='block';
  document.getElementById('betsTitle').textContent=title||'Market';
  document.getElementById('betsBody').innerHTML='<div class="muted" style="padding:8px 0">Loading recorded bets…</div>';
}
function closeBets(){document.getElementById('betscard').style.display='none';}
// R67q: gold ★ on the poly-int FLOW consensus rows → the same market-bets popup (wallet columns:
// trader / ★rank / all-time P&L / bet / avg / $ / when) via the row's cond_id.
function showBetsCond(i){var c=(window.lastPC||[])[i];if(!c||!c.cond_id)return;
  openBetsPop(c.question||'Market');
  fetch('/api/market-bets?cond='+encodeURIComponent(c.cond_id)).then(function(r){return r.json();}).then(renderBets).catch(function(){document.getElementById('betsBody').innerHTML='<div class="muted" style="padding:8px 0">Could not load bets.</div>';});
}
// R67f: Poly US ≣ — the venue has no public order book, so the popup is its whale/bets log: recent
// large taker prints on THIS market from the cached whales feed (anonymous — regulated exchange).
function showPusBets(i){var m=(window.lastPMUS||[])[i];if(!m)return;
  openBetsPop((m.game||prettySlug(m.slug)||'Market')+(m.team?(' — '+String(m.team).toUpperCase()):''));
  var rows=(window.lastPUSW||[]).filter(function(t){return t.slug===m.slug;});
  var el=document.getElementById('betsBody');
  /* R70-B #1: top-of-book resting SIZES off the markets-WS per-level book — the maker fill-odds read
     (how much rests ahead at the touch). Shown whenever the WS book has a fresh frame for the slug. */
  var tob='';
  if((m.bid_sz||0)>0||(m.ask_sz||0)>0){
    tob='<div class="muted" style="padding:4px 0" title="resting contracts at the best bid / best offer (live WS book) — the queue a maker order joins">Top of book: '+
      '<span style="color:var(--good);font-weight:700">'+Math.round((m.bid||0)*100)+'¢ × '+fmtVol(m.bid_sz||0)+'</span> bid · '+
      '<span style="color:var(--bad);font-weight:700">'+Math.round((m.ask||0)*100)+'¢ × '+fmtVol(m.ask_sz||0)+'</span> ask</div>';
  }
  if(!rows.length){el.innerHTML=tob+'<div class="muted" style="padding:8px 0">No large Poly US taker prints recorded on this market in the last ~20 min. Poly US is a regulated (anonymous) exchange — sizes only, no wallets.</div>';return;}
  var h=tob+'<table class="mkt"><colgroup><col style="width:90px"><col style="width:60px"><col><col style="width:64px"><col style="width:84px"></colgroup>'+
    '<thead><tr><th>When</th><th>Side</th><th>Market</th><th class="r">Price</th><th class="r">$ size</th></tr></thead><tbody>';
  rows.forEach(function(t){var col=(t.side==='YES')?'var(--good)':'var(--bad)';
    h+='<tr><td class="when">'+agoLabel(ageUnix(t.at))+'</td><td style="color:'+col+';font-weight:700">'+escapeHtml(t.side||'')+'</td>'+
      '<td class="ell">'+escapeHtml(t.game||t.slug||'')+'</td><td class="r">'+Math.round((t.price||0)*100)+'¢</td><td class="r vol">$'+fmtVol(t.notional||0)+'</td></tr>';});
  el.innerHTML=h+'</tbody></table>';
}
function renderBets(d){
  var list=(d&&d.bets)||[];var el=document.getElementById('betsBody');
  if(list.length===0){el.innerHTML='<div class="muted" style="padding:8px 0">No ★-ranked or high-P&L wallets recorded trading this market recently. We keep a rolling ~13-min on-chain trade buffer and only show wallets with a leaderboard rank or ≥$25k all-time P&L.</div>';return;}
  var h='<table class="mkt"><colgroup><col><col style="width:120px"><col style="width:56px"><col style="width:84px"><col style="width:66px"></colgroup>'+
    '<thead><tr><th>Trader</th><th>Bet</th><th class="r">Avg</th><th class="r">$ size</th><th class="r">When</th></tr></thead><tbody>';
  list.forEach(function(b){
    var who=b.profile?('<a class="go" href="'+escapeHtml(b.profile)+'" target="_blank" rel="noopener">'+escapeHtml(b.trader||'trader')+'</a>'):escapeHtml(b.trader||'trader');
    var rk=b.pro?(' <span style="color:'+goldFor(b.rank)+';font-weight:700;text-shadow:0 0 6px rgba(255,205,0,.4)" title="Polymarket profit-leaderboard rank — brighter gold = higher rank">★ '+escapeHtml(b.ranks||'leaderboard')+'</span>'):'';
    var pf=b.profit?(' <span style="font-weight:700;color:'+((b.profit||0)>=0?'var(--good)':'var(--bad)')+'" title="all-time profit/loss">'+(b.profit<0?'−$':'+$')+fmtVol(Math.abs(b.profit))+'</span>'):'';
    var col=(b.side==='BUY')?'var(--good)':((b.side==='SELL')?'var(--bad)':'var(--text)');
    h+='<tr><td class="name">'+who+rk+pf+'</td>'+
      '<td style="color:'+col+';font-weight:700">'+escapeHtml(((b.side||'')+' '+(b.outcome||'')).trim())+'</td>'+
      '<td class="r">'+Math.round((b.avg_price||0)*100)+'¢</td>'+
      '<td class="r vol">$'+fmtVol(b.notional||0)+'</td>'+
      '<td class="r when">'+agoLabel(ageUnix(b.time))+'</td></tr>';});
  h+='</tbody></table>';el.innerHTML=h;
}
function ageIso(s){var d=new Date(s);return isNaN(d.getTime())?1e9:(Date.now()-d.getTime())/1000;}
function ageUnix(ts){return Math.max(0,Date.now()/1000-(ts||0));}
function agoLabel(a){if(a>=1e8)return "";if(a<60)return Math.round(a)+"s ago";if(a<3600)return Math.round(a/60)+"m ago";if(a<86400)return Math.round(a/3600)+"h ago";return Math.round(a/86400)+"d ago";}
function recColor(a){var t=Math.max(0,Math.min(1,1-a/7200));var d=[70,78,96],b=[74,222,128];return "rgb("+Math.round(d[0]+(b[0]-d[0])*t)+","+Math.round(d[1]+(b[1]-d[1])*t)+","+Math.round(d[2]+(b[2]-d[2])*t)+")";}
window.kWhaleSort='recent';window.pWhaleSort='recent';window.pusWhaleSort='recent';
function markSort(){
  function mk(aId,rId,mode){var a=document.getElementById(aId),r=document.getElementById(rId);if(!a||!r)return;
    a.style.fontWeight=mode==='amt'?'700':'400';a.style.color=mode==='amt'?'var(--text)':'';
    r.style.fontWeight=mode!=='amt'?'700':'400';r.style.color=mode!=='amt'?'var(--text)':'';}
  mk('kSortAmt','kSortRec',window.kWhaleSort);
  mk('pSortAmt','pSortRec',window.pWhaleSort);
  mk('pusSortAmt','pusSortRec',window.pusWhaleSort);
}
function setKSort(m){window.kWhaleSort=m;markSort();loadWhales();}
function setPSort(m){window.pWhaleSort=m;markSort();loadPoly();}
function setPusSort(m){window.pusWhaleSort=m;markSort();loadPolyUS();}
// R57 UNIFIED FEED SCHEMA — every venue tape renders the SAME 6-column table:
// TIME 9% right (HH:MM:SS, recency-tinted) | SIDE 7% center tag | MARKET 51% ellipsis (+full text
// in title) | PRICE 11% right ¢ | SIZE 14% right $ | Δ 8% right ("—" when the venue has no move).
function ufHead(sortFn){ // R63 1d: Time/Size headers are CLICK-TO-SORT when a setter is passed (wired, not decorative)
  var t='Time',z='Size';
  if(sortFn){t='<a href="#" onclick="'+sortFn+'(\'recent\');return false;" title="sort newest first" style="color:inherit;text-decoration:none">Time</a>';
             z='<a href="#" onclick="'+sortFn+'(\'amt\');return false;" title="sort by $ size" style="color:inherit;text-decoration:none">Size</a>';}
  return '<table class="mkt uf"><colgroup><col class="c-time"><col class="c-side"><col class="c-mkt"><col class="c-px"><col class="c-sz"><col class="c-d"></colgroup>'
  +'<thead><tr><th class="r">'+t+'</th><th class="c">Side</th><th>Market</th><th class="r">Price</th><th class="r">'+z+'</th><th class="r">Δ</th></tr></thead><tbody>';}
function ufClock(x){ // R60: 12-hour h:MM AM/PM from an RFC3339 string or unix seconds (military killed)
  if(!x)return '—';var d=(typeof x==='number')?new Date(x*1000):new Date(x);if(isNaN(d.getTime()))return '—';
  var h=d.getHours(),ap=h>=12?'PM':'AM';h=h%12;if(h===0)h=12;
  return h+':'+('0'+d.getMinutes()).slice(-2)+' '+ap;}
function ufTime(x,age){return '<td class="r" style="color:'+recColor(age==null?7200:age)+'" title="'+escapeHtml(agoLabel(age==null?0:age))+'">'+ufClock(x)+'</td>';}
function ufSide(s){s=String(s||'').toUpperCase();var tag=(s==='YES')?'YES':((s==='NO')?'NO':s.slice(0,3));
  var cls=(s==='YES'||s==='UP'||s==='BUY'||s==='OVER')?'b':((s==='NO'||s==='DOWN'||s==='SELL'||s==='UNDER')?'s':'n');
  return '<td class="c"><span class="sidetag '+cls+'">'+escapeHtml(tag||'—')+'</span></td>';}
function loadWhales(){
  if(!tabArmed('whales','flow'))return Promise.resolve(); // R98 lazy tabs: no fetch until Whales/Flow first opened
  return jget("/api/whales").then(function(d){
    var minK=parseFloat((document.getElementById("minK")||{}).value)||0;
    var whales=((d&&d.whales)||[]).filter(function(w){return (w.notional||0)>=minK;});var smart=(d&&d.smart)||[];
    if(window.kWhaleSort==='recent'){whales.sort(function(a,b){return ageIso(a.time)-ageIso(b.time);});}else{whales.sort(function(a,b){return (b.notional||0)-(a.notional||0);});}
    markSort();
    var we=document.getElementById("whales");
    window._whalesEverLoaded=true; // R19: transient fetch errors keep this last-good content
    var kc=document.getElementById("kwCount");if(kc)kc.textContent=whales.length?(whales.length+" prints"):"";
    if(whales.length===0){we.innerHTML='<span class="muted">'+(minK>0?('No Kalshi trades over $'+fmtVol(minK)):'No big-money trades right now')+'.</span>';}
    else{ // R57 unified feed schema
      var h=ufHead('setKSort');
      window.lastKW=whales;
      whales.forEach(function(wh,i){var age=ageIso(wh.time);
        var mk=wh.url?('<a class="go" href="'+escapeHtml(wh.url)+'" target="_blank" rel="noopener">'+escapeHtml(wh.market)+'</a>'):escapeHtml(wh.market);
        h+='<tr>'+ufTime(wh.time,age)+ufSide(wh.side)+
          '<td class="mkt" title="'+escapeHtml(String(wh.market||'')+(wh.block?' · block trade':''))+'"><div class="mrow"><span class="mtxt">'+mk+(wh.block?' <span class="muted">·blk</span>':'')+'</span></div></td>'+
          '<td class="r">'+Math.round((wh.price||0)*100)+'¢</td>'+
          '<td class="r vol">$'+fmtVol(wh.notional)+'</td>'+
          '<td class="r muted">—</td></tr>';});
      withScroll(we,h+'</tbody></table>');
    }
    var ke=document.getElementById("ksmart");
    if(smart.length===0){ke.innerHTML='<span class="muted">No clearly one-sided aggressive flow right now.</span>';}
    else{
      var hs='<table class="mkt"><colgroup><col><col style="width:104px"><col style="width:48px"><col style="width:62px"><col style="width:58px"></colgroup>'+
        '<thead><tr><th>Market</th><th>Aggressive money</th><th class="r">Yes</th><th class="r">Move</th><th class="r">Vol</th></tr></thead><tbody>';
      window.lastKS=smart;
      smart.forEach(function(c,i){var col=c.side==="YES"?"var(--good)":"var(--bad)";
        var mk=c.url?('<a class="go" href="'+escapeHtml(c.url)+'" target="_blank" rel="noopener">'+escapeHtml(c.market)+'</a>'):escapeHtml(c.market);
        hs+='<tr><td>'+mk+'</td>'+
          '<td style="color:'+col+';font-weight:700">'+c.side+' '+Math.round((c.strength||0)*100)+'%</td>'+
          '<td class="r">'+Math.round(c.price||0)+'¢</td>'+
          '<td class="r">'+pmusMove(c.move)+'</td>'+
          '<td class="r vol">$'+fmtVol(c.notional||0)+'</td></tr>';});
      withScroll(ke,hs+'</tbody></table>');
    }
    renderBoth();
  }).catch(function(){var e=document.getElementById("whales");if(e&&!window._whalesEverLoaded)e.textContent="Could not load Kalshi whales (retrying)…";}); // R19: keep last-good content on transient fetch errors
}
function goldFor(rank){rank=rank||999;return rank<=5?'#FFD700':(rank<=20?'#F4C430':(rank<=100?'#D4A017':'#B8860B'));}
function loadPoly(){
  if(!tabArmed('whales','flow'))return Promise.resolve(); // R98 lazy tabs: no fetch until Whales/Flow first opened
  return jget("/api/poly").then(function(d){
    var minP=parseFloat((document.getElementById("minP")||{}).value)||0;
    var fa=(d&&typeof d.whale_fetch_age_sec==='number')?d.whale_fetch_age_sec:-1;
    var da=(d&&typeof d.whale_data_age_sec==='number')?d.whale_data_age_sec:-1;
    var fe=document.getElementById("pwfeed");
    if(fe){
      if(da<0){fe.innerHTML='<span style="color:var(--warn)">feed: waiting for first fetch…</span>';}
      else if(da<=25){fe.innerHTML='<span style="color:var(--good)">● live (WebSocket) · newest trade '+da.toFixed(0)+'s old</span>';}
      else{fe.innerHTML='<span style="color:var(--warn)">● feed lagging — newest trade '+da.toFixed(0)+'s old (WebSocket reconnecting; REST fallback)</span>';}
    }
    var pw=((d&&d.whales)||[]).filter(function(t){return (t.notional||0)>=minP;});var poly=(d&&d.poly)||[];
    if(window.pWhaleSort==='recent'){pw.sort(function(a,b){return (b.time||0)-(a.time||0);});}else{pw.sort(function(a,b){return (b.notional||0)-(a.notional||0);});}
    markSort();
    var pe=document.getElementById("pwhales");
    if(pw.length===0){pe.innerHTML='<span class="muted">'+(minP>0?('No Polymarket trades over $'+fmtVol(minP)):'No big Polymarket trades right now')+'.</span>';}
    else{ // R63 1d: RICH columns restored — rank ★ / trader / all-time PnL are real columns again
      // (pre-R58 they were inline; R58 folded them into the title tooltip). Conviction is NOT in the
      // whale-print payload (it only exists on consensus rows) so there is no conviction column.
      var h='<table class="mkt"><colgroup><col style="width:46px"><col style="width:36px"><col><col style="width:86px"><col style="width:34px"><col style="width:58px"><col style="width:36px"><col style="width:56px"></colgroup>'
        +'<thead><tr><th class="r"><a href="#" onclick="setPSort(\'recent\');return false;" title="sort newest first" style="color:inherit;text-decoration:none">Time</a></th><th class="c">Side</th><th>Market</th><th>Trader</th><th class="c" title="Polymarket profit-leaderboard rank">★</th><th class="r" title="all-time profit/loss">PnL</th><th class="r">@¢</th><th class="r"><a href="#" onclick="setPSort(\'amt\');return false;" title="sort by $ size" style="color:inherit;text-decoration:none">Size</a></th></tr></thead><tbody>';
      window.lastPW=pw;
      pw.forEach(function(t,i){var age=ageUnix(t.time);
        var mk=t.url?('<a class="go" href="'+escapeHtml(t.url)+'" target="_blank" rel="noopener">'+escapeHtml(t.market)+'</a>'):escapeHtml(t.market);
        var who=t.profile?('<a class="go" href="'+escapeHtml(t.profile)+'" target="_blank" rel="noopener">'+escapeHtml(t.trader)+'</a>'):escapeHtml(t.trader);
        var star=t.pro?('<span style="color:'+goldFor(t.best)+';font-weight:700;text-shadow:0 0 5px rgba(255,205,0,.35)" title="leaderboard rank '+escapeHtml(t.ranks||String(t.best||''))+'">★</span>'):'<span class="muted">—</span>';
        var pnl=t.profit?('<span style="font-weight:700;color:'+((t.profit||0)>=0?'var(--good)':'var(--bad)')+'">'+(t.profit<0?'−$':'+$')+fmtVol(Math.abs(t.profit))+'</span>'):'<span class="muted">—</span>';
        h+='<tr>'+ufTime(t.time,age)+ufSide(t.outcome)+
          '<td class="ell" title="'+escapeHtml(String(t.market||''))+'">'+mk+'</td>'+
          '<td class="ell" title="'+escapeHtml(String(t.trader||'trader'))+'">'+who+'</td>'+
          '<td class="c">'+star+'</td>'+
          '<td class="r">'+pnl+'</td>'+
          '<td class="r">'+Math.round((t.price||0)*100)+'</td>'+
          '<td class="r vol">$'+fmtVol(t.notional)+'</td></tr>';});
      withScroll(pe,h+'</tbody></table>');
    }
    var se=document.getElementById("poly");
    if(poly.length===0){se.innerHTML='<span class="muted">No leaderboard traders are clustered on one side right now.</span>';}
    else{
      var hs='<table class="mkt"><colgroup><col><col style="width:122px"><col style="width:104px"><col style="width:60px"></colgroup>'+
        '<thead><tr><th>Polymarket market</th><th>Agree</th><th>Rep</th><th class="r">Their $</th></tr></thead><tbody>';
      window.lastPC=poly;
      poly.forEach(function(c,i){
        var rep=c.rep||0;
        var rc=rep>=70?'var(--good)':(rep>=45?'var(--accent)':(rep>=25?'var(--warn)':'var(--muted)'));
        var star=(c.top_rank>0)?('<span style="color:'+goldFor(c.top_rank)+';font-weight:700;text-shadow:0 0 5px rgba(255,205,0,.35);cursor:pointer" title="best leaderboard rank #'+c.top_rank+' — click for the whale wallets betting this market (R67q)" onclick="showBetsCond('+i+')">★</span> '):''; /* R67q */
        var repcell=star+'<span style="color:'+rc+';font-weight:700" title="top rank #'+(c.top_rank||'?')+' · $'+fmtVol(c.profit||0)+' combined all-time P&amp;L">'+escapeHtml(c.rep_label||'')+' '+rep+'</span>';
        var conc=(c.concentration>0)?(' <span class="muted" style="font-weight:400" title="this side is '+Math.round(c.concentration*100)+'% of those traders combined portfolio — higher = stronger conviction">· '+Math.round(c.concentration*100)+'% conv</span>'):'';
        var hold=(c.holder_conc>0)?(' <span style="font-weight:400;color:'+((c.holder_side===c.side)?'var(--good)':'var(--warn)')+'" title="market-wide top holders are '+Math.round(c.holder_conc*100)+'% on '+escapeHtml(c.holder_side||'')+(c.holder_side===c.side?' — agrees with this side':' — DIVERGES from this side')+'">· holders '+Math.round(c.holder_conc*100)+'% '+escapeHtml(c.holder_side||'')+'</span>'):'';
        var halt=c.halted?' <span style="font-weight:700;color:var(--bad)" title="venue reports this market NOT accepting orders (suspended / resolution pending) — the shown price is a frozen book, not tradeable; bridges skip it">⛔ HALTED</span>':''; /* R70-B F3 */
        hs+='<tr><td><a class="go" href="'+escapeHtml(c.url)+'" target="_blank" rel="noopener">'+escapeHtml(c.question)+'</a>'+halt+'</td>'+
          '<td style="color:var(--good);font-weight:700">'+c.performers+'→ '+escapeHtml(c.side)+conc+hold+'</td>'+
          '<td>'+repcell+'</td>'+
          '<td class="r">$'+fmtVol(c.notional)+'</td></tr>';});
      withScroll(se,hs+'</tbody></table>');
    }
    renderBoth();
  }).catch(function(){document.getElementById("poly").textContent="Could not load Polymarket data.";});
}
function loadArb(){
  if(!tabArmed('xvenue'))return Promise.resolve(); // R98 lazy tabs: no fetch until Cross-venue first opened
  if(document.hidden||uiIdle())return Promise.resolve(); // paused while backgrounded or idle 30m (memory)
  if(Date.now()<uiHold)return Promise.resolve(); // hold re-render while the user is clicking
  // R125: per-venue-pair matcher coverage + lock-scanner summary (fire-and-forget; strip renders in renderBoth)
  jget("/api/xvpairs").then(function(d){window.lastXvPairs=d;}).catch(function(){});
  jget("/api/xvlock").then(function(d){window.lastXvLock=d;}).catch(function(){});
  return jget("/api/arb").then(function(d){
    var rows=(d&&d.rows)||[];
    // R57 aggregator: keep the RAW rows (incl. the PolyUS enrichment: has_polyus / polyus_pct /
    // polyus_url / polyus_slug). EDGE = widest same-side gap between the venues that price it (pp).
    rows.forEach(function(c){
      var ps=[Math.round(c.kalshi_pct||0),Math.round(c.poly_pct||0)];
      if(c.has_polyus)ps.push(Math.round(c.polyus_pct||0));
      c._edge=Math.max.apply(null,ps)-Math.min.apply(null,ps);
    });
    rows.sort(function(a,b){return (b._edge-a._edge)||((b.vol||0)-(a.vol||0));}); // EDGE desc, then VOL desc
    window.lastArbRows=rows;
    renderBoth();
  }).catch(function(){document.getElementById("both").textContent="Could not load cross-platform data.";});
}
var CROSS_STOP={will:1,win:1,wins:1,won:1,over:1,under:1,goals:1,goal:1,scored:1,score:1,more:1,than:1,winner:1,match:1,game:1,price:1,above:1,below:1,target:1,next:1,mins:1,draw:1,tie:1,yes:1,both:1,date:1,total:1,points:1,point:1,spread:1,returns:1,return:1,normal:1,traffic:1,first:1,half:1,corner:1,corners:1,cards:1,clean:1,sheet:1,team:1,teams:1,
  baseball:1,basketball:1,hockey:1,football:1,tennis:1,soccer:1,cricket:1,rugby:1,esports:1,crypto:1,combo:1,world:1,golf:1,motorsport:1,index:1,music:1}; // R62: friendlyName sport labels must never become the canonical match token
function teamToks(s){return String(s||'').toLowerCase().replace(/[^a-z0-9 ]/g,' ').split(/\s+/).filter(function(w){return w.length>=4&&!CROSS_STOP[w];});}
function canonTok(s){var t=teamToks(s);if(t.length===0)return '';t.sort(function(a,b){return b.length-a.length;});return t[0];}
function afterDash(s){var i=String(s||'').lastIndexOf('—');return i>=0?String(s).slice(i+1).trim():'';}
function polyBacked(market,outcome){var o=String(outcome||'').toLowerCase().trim();if(o==='yes')return market;if(o==='no'||o==='over'||o==='under'||o==='')return '';return outcome;}
// R57 CROSS-VENUE AGGREGATOR (OddsJam pattern): one row per matched event, a YES-¢ column per venue
// (KAL / PUS / INT), cheapest-YES cell tinted+bold (the only per-cell color), EDGE colored only at
// ≥2pp, ▸ expands a detail row with the existing lock/opposite-side text + venue links. The
// smart-money agreement sub-list stays BELOW the table (compact).
function xvKey(c){return (c.kalshi_ticker||c.kalshi_title||'')+'|'+(c.poly_id||c.question||'');}
// R63 2e: the lock/detail row is EXPANDED BY DEFAULT — a click collapses it (undefined = open).
function xvOpen(c){var v=(window._bothOpen||{})[xvKey(c)];return v===undefined?true:!!v;}
function toggleBothRow(i){var c=(window.lastArbRows||[])[i];if(!c)return;window._bothOpen=window._bothOpen||{};window._bothOpen[xvKey(c)]=!xvOpen(c);renderBoth();}
function renderBoth(){
  var be=document.getElementById("both");if(!be)return;
  var html="";
  window._bothOpen=window._bothOpen||{};
  // R125 strip: matcher coverage per venue pair + lock-scanner summary
  var xp=window.lastXvPairs,xl=window.lastXvLock;
  if(xp){
    var kp=xp.k_pus||{},ki=xp.k_pint||{},pp=xp.pus_pint||{};
    html+='<div class="muted" style="margin:4px 0 8px">Matcher: K↔PUS '+(kp.matched||0)+' matched ('+(kp.flip_matched||0)+' via NO-side) · K↔INT '+(ki.matched||0)+' · PUS↔INT '+(pp.matched_transitive||0)+' (transitive)';
    if(xl){html+=' &nbsp;|&nbsp; Locks ≥0.5¢: '+(xl.open||0)+' tracked / '+(xl.closed||0)+' graded / '+(xl.mismatch_n||0)+' rules-mismatch · ~'+(xl.opps_per_hour||0)+'/h · med margin '+(xl.margin_med_c||0)+'¢ · legB survives '+Math.round(((xl.legB_survival_rate||0)*100))+'%';}
    html+='</div>';
  }
  var rows=window.lastArbRows||[];
  if(rows.length){
    html+='<table class="mkt xv"><colgroup><col style="width:32%"><col style="width:11%"><col style="width:11%"><col style="width:11%"><col style="width:10%"><col style="width:12%"><col style="width:6%"></colgroup>'
      +'<thead><tr><th>Event</th><th class="r" title="Kalshi YES ¢ for this side">KAL</th><th class="r" title="Poly US YES ¢ for this side">PUS</th><th class="r" title="Poly-int YES ¢ for this side">INT</th><th class="r" title="widest same-side price gap between venues, percentage points — colored at ≥2pp">Edge</th><th class="r">Vol</th><th class="c"></th></tr></thead><tbody>';
    rows.forEach(function(c,i){
      var kp=Math.round(c.kalshi_pct||0),ip=Math.round(c.poly_pct||0),up=(c.has_polyus?Math.round(c.polyus_pct||0):null);
      var avail=[kp,ip];if(up!=null)avail.push(up);
      var best=Math.min.apply(null,avail);
      function cell(v){if(v==null)return '<td class="r muted">—</td>';var b=(v===best&&(c._edge||0)>=1);
        return '<td class="r'+(b?' best':'')+'"'+(b?' title="cheapest YES — best entry for this side"':'')+'>'+v+'¢</td>';}
      var open=xvOpen(c); // R63 2e: default-open
      html+='<tr><td class="ev" title="'+escapeHtml((c.question||'')+' → '+(c.side||''))+'">'+escapeHtml(c.question||'')+' <span class="muted">→ '+escapeHtml(c.side||'')+'</span></td>'
        +cell(kp)+cell(up)+cell(ip)
        +'<td class="r'+((c._edge||0)>=2?'':' muted')+'"'+((c._edge||0)>=2?' style="color:var(--good);font-weight:700"':'')+'>'+(c._edge||0)+'pp</td>'
        +'<td class="r vol">$'+fmtVol(c.vol||0)+'</td>'
        +'<td class="c"><button class="mini" onclick="toggleBothRow('+i+')" title="detail: lock legs, opposite-side price, venue links">'+(open?'▾':'▸')+'</button></td></tr>';
      if(open){
        var lk=(c.lock_edge>0.4)?('<span style="color:var(--good);font-weight:700">🔒 lock +'+(c.lock_edge||0).toFixed(1)+'¢</span> <span class="muted">'+escapeHtml(c.lock_txt||'')+'</span>')
                                :'<span class="muted" title="mid-price gap only — spreads eat it, not a real lock">⚡ no tradeable lock</span>';
        var opp=(c.poly_opp_label)?(' · <span class="muted">opp:</span> '+escapeHtml(c.poly_opp_label)+' '+Math.round(c.poly_opp_pct||0)+'%'):'';
        var nr=(c.neg_risk)?(' · <span style="color:var(--warn,#d9a03f);font-weight:700" title="Poly-int leg is a multi-outcome NEG-RISK event: its NO side is not an independent binary — two-leg locks against it misprice">⛓ neg-risk</span>'):''; // R69 SCHEMA_AUDIT #3
        var links=' · <a class="go" href="'+escapeHtml(c.kalshi_url||'#')+'" target="_blank" rel="noopener" title="'+escapeHtml(c.kalshi_title||'')+'">K↗</a>'
          +' <a class="go" href="'+escapeHtml(c.poly_url||'#')+'" target="_blank" rel="noopener">INT↗</a>'
          +((c.has_polyus&&c.polyus_url)?(' <a class="go" href="'+escapeHtml(c.polyus_url)+'" target="_blank" rel="noopener" title="'+escapeHtml(c.polyus_slug||'')+'">PUS↗</a>'):'');
        html+='<tr class="xvd"><td colspan="7">'+lk+nr+opp+links+'</td></tr>';
      }
    });
    html+='</tbody></table>';
  }
  // Smart-money agreement (whales + consensus backing the same team on both platforms) — below, compact.
  // R69: the PUS $ column renders the SERVER-side join now — /api/whales rows carry polyus_slug +
  // polyus_notional, matched by polyUSMatchForSide (telemetry family "smartmoney", Logs → Match log).
  // This replaces the R63 2e client-side canonTok join over lastPUSW/lastPMUS, which was unmeasured
  // and missed nickname-vs-fullname forms (canonTok takes the LONGEST token, so 'Washington
  // Nationals' → 'washington' never met 'Nationals' → 'nationals'). uMap keys off the SAME kalshi
  // entity token as addK and dedups per matched slug — each row carries that market's TOTAL recent
  // YES notional, so summing repeats would multiply-count.
  var kMap={},pMap={},uMap={};
  function addK(entity,amt){var c=canonTok(entity);if(!c||!(amt>0))return;var e=kMap[c]||(kMap[c]={label:(entity||'').trim()||c,amt:0});e.amt+=amt;}
  function addP(entity,amt){var c=canonTok(entity);if(!c||!(amt>0))return;var e=pMap[c]||(pMap[c]={label:(entity||'').trim()||c,amt:0});e.amt+=amt;}
  function addU(entity,slug,amt){var c=canonTok(entity);if(!c||!slug||!(amt>0))return;var e=uMap[c]||(uMap[c]={amt:0,slugs:{}});if(e.slugs[slug])return;e.slugs[slug]=1;e.amt+=amt;}
  (window.lastKW||[]).forEach(function(w){if(w.side==='YES'){addK(afterDash(w.market)||w.market,w.notional);addU(afterDash(w.market)||w.market,w.polyus_slug,w.polyus_notional);}});
  (window.lastKS||[]).forEach(function(c){if(c.side==='YES'){addK(afterDash(c.market)||c.market,c.notional);addU(afterDash(c.market)||c.market,c.polyus_slug,c.polyus_notional);}});
  (window.lastPW||[]).forEach(function(t){var b=polyBacked(t.market,t.outcome);if(b)addP(b,t.notional);});
  (window.lastPC||[]).forEach(function(c){var b=polyBacked(c.question,c.side);if(b)addP(b,c.notional);});
  var crows=[];
  Object.keys(kMap).forEach(function(c){if(pMap[c])crows.push({team:kMap[c].label||pMap[c].label,k:kMap[c].amt,p:pMap[c].amt,u:(uMap[c]?uMap[c].amt:0)});});
  crows.sort(function(a,b){return (b.k+b.p+b.u)-(a.k+a.p+a.u);});
  if(crows.length>0){
    html+='<div class="muted" style="margin:8px 0 2px;font-weight:700;color:var(--text)">💰 Smart money agrees across venues</div>';
    html+='<table class="mkt"><colgroup><col><col style="width:88px"><col style="width:88px"><col style="width:88px"></colgroup><thead><tr><th>Backing to win</th><th class="r">🟩 Kalshi $</th><th class="r">🟦 Poly $</th><th class="r">🇺🇸 PUS $</th></tr></thead><tbody>';
    crows.forEach(function(r){html+='<tr><td style="font-weight:700">'+escapeHtml(r.team)+'</td><td class="r" style="color:var(--good);font-weight:700">$'+fmtVol(r.k)+'</td><td class="r" style="color:var(--good);font-weight:700">$'+fmtVol(r.p)+'</td>'+(r.u>0?('<td class="r" style="color:var(--good);font-weight:700">$'+fmtVol(r.u)+'</td>'):'<td class="r muted">—</td>')+'</tr>';});
    html+='</tbody></table>';
  }
  if(html===""){be.innerHTML='<span class="muted">No cross-platform agreement or price matches right now.</span>';return;}
  withScroll(be,html);
}
function renderBook(id,levels,cls){
  var tb=document.getElementById(id);tb.innerHTML="";
  if(!levels||levels.length===0){tb.innerHTML='<tr><td colspan="2" class="muted">none</td></tr>';return;}
  levels.forEach(function(l){
    var tr=document.createElement("tr");tr.className=cls;
    tr.innerHTML='<td>'+Math.round((l.price||0)*100)+'%</td><td class="num">'+fmtVol(l.size)+'</td>';
    tb.appendChild(tr);
  });
}
function loadBook(){
  if(!openTicker)return;
  fetch("/api/orderbook?ticker="+encodeURIComponent(openTicker)).then(function(r){return r.json();}).then(function(ob){
    renderBook("bids",(ob.yes_bids||[]).slice(0,12),"bid");
    renderBook("asks",(ob.yes_asks||[]).slice(0,12),"ask");
    var bf=document.getElementById("bookFee");if(bf)bf.textContent=ob.fee_note||''; /* R70-B #2: per-series fee schedule line */
    var bt=document.getElementById("bookTitle");if(bt&&openTicker)bt.textContent="Order book — "+openTicker+(ob.source==="ws"?" · LIVE ws":""); /* R74: ws = real-time orderbook_delta book (REST fallback shows no tag) */
  }).catch(function(){document.getElementById("bids").innerHTML='<tr><td colspan="2" class="muted">error</td></tr>';});
}
function showBook(ticker){
  openTicker=ticker;document.getElementById("book").style.display="block";
  document.getElementById("bookTitle").textContent="Order book — "+ticker;
  document.getElementById("bids").innerHTML='<tr><td colspan="2" class="muted">Loading…</td></tr>';
  document.getElementById("asks").innerHTML="";
  loadBook();renderBookBets(ticker);document.getElementById("book").scrollIntoView({behavior:"smooth",block:"nearest"});
}
// R67f: the Kalshi ≣ popup = order book (Kalshi is the only venue with /api/orderbook) PLUS this
// market's whale/bets log — recent large aggressive prints on the ticker from the cached whale feed.
function renderBookBets(ticker){
  var el=document.getElementById('bookBets');if(!el)return;
  var rows=(window.lastKW||[]).filter(function(w){return w.ticker===ticker;});
  var h='<div class="muted" style="font-weight:700;color:var(--text);margin:4px 0 2px">🐋 Recent whale prints on this market</div>';
  if(!rows.length){el.innerHTML=h+'<div class="muted">None in the recent whale window (Kalshi tape is anonymous — sizes only).</div>';return;}
  h+='<table class="mkt"><colgroup><col style="width:90px"><col style="width:60px"><col><col style="width:64px"><col style="width:84px"></colgroup>'+
    '<thead><tr><th>When</th><th>Side</th><th>Market</th><th class="r">Price</th><th class="r">$ size</th></tr></thead><tbody>';
  rows.forEach(function(w){var col=(w.side==='YES')?'var(--good)':'var(--bad)';
    h+='<tr><td class="when">'+agoLabel(ageIso(w.time||w.ts||''))+'</td><td style="color:'+col+';font-weight:700">'+escapeHtml(w.side||'')+'</td>'+
      '<td class="ell">'+escapeHtml(w.market||w.ticker||'')+'</td><td class="r">'+Math.round((w.price||0)*100)+'¢</td><td class="r vol">$'+fmtVol(w.notional||w.amount||0)+'</td></tr>';});
  el.innerHTML=h+'</tbody></table>';
}
function hideBook(){openTicker=null;document.getElementById("book").style.display="none";}
// R70 (audit §a P2): the EV calculator (calcEV/openEV/closeEV) was dead TWICE — no callers AND its
// evcard/evPrice/… ids had no elements. Deleted; /api/ev remains for curl use.
function srcCat(s){s=String(s||'');if(s.indexOf('arb')>=0)return 'Arb';if(s.indexOf('gate')>=0||s.indexOf('ai')>=0)return 'AI';if(s.indexOf('auto')>=0||s.indexOf('signal')>=0)return 'Auto';return 'Manual';}
// srcLbl maps a raw signal source to the SAME friendly name shown in the Signals toggle menu.
// R25 (operator: "signals are different across Edge/Backtest/Stats/Signals — make them cohesive"):
// srcLbl is THE naming function. It consults the SIGNAMES registry first (plain-language names,
// defined near the Signals tab; same names as SIGNALMAP.md), normalizing auto-cons-* trade sources
// to their signal key — so every tab prints the identical name for the same signal.
function srcLbl(s){
  s=String(s==null?'—':s);
  var k=s.replace(/^auto-cons-/,'');
  if(typeof SIGNAMES!=='undefined'){ if(SIGNAMES[s])return SIGNAMES[s]; if(SIGNAMES[k])return SIGNAMES[k]; }
  var M={'kcrypto':'🟩 Kalshi 15M crypto — mid-priced favorite','pcrypto':'📡 Poly-int 15m crypto favorite (research)','xmatch':'🟩 Crypto divergence — Poly leads, Kalshi lags','pmatch':'🟩 Conviction bridge — Poly conviction → Kalshi twin','kthresh':'🟩 Threshold ladder — fade the mispriced strike','pflow':'📡 Poly-int flow (research)','confluence':'🟩 Confluence — 2–3 venues agree','cross':'🟩 Cross-venue consensus','xcrypto':'🟩 Crypto flow-chase (retired)','pbridge':'🟩 Concentration bridge — Poly whales → Kalshi twin','favlong':'🟩 Favorite-longshot tilt','kalshi':'🟩 Kalshi whale flow (retired: fee grinder)','poly':'📡 Poly-int leaderboard (research)','pusflow':'🇺🇸 Poly US flow','x':'🟩 Cross-venue consensus','auto-arb':'🟩 Two-leg arb','arb':'🟩 Two-leg arb','manual':'✋ Manual','auto-tp':'take-profit','auto-sl':'stop-loss','settled':'settled','gate-close':'external close (removed)','auto-arb-rollback':'arb rollback','auto-ml':'🤖 ML executor picks (retired — tracked in ML books)','auto-freeroll':'free-roll','auto-scaleout':'scale-out','gate':'external gate (removed)'};
  return M[s]||M[k]||s.replace(/gate/gi,'AI');
}
function platLabel(p){return {kalshi:"🟩 Kalshi",polyus:"🇺🇸 Poly US",polymarket:"🟦 Poly-int"}[p]||(p||"other");}
// bySrcPlatHTML renders the "which signal makes money" table GROUPED by platform (Kalshi / Poly US /
// Poly-int), each with its own total — so you can see exactly what each source gives on each venue.
function bySrcPlatHTML(bs){
  bs=bs||[];
  // R63 3c: EVERY SIGNAMES entry must appear — per-venue subsections show chip-matching signals
  // with no closed trades yet as n=0 rows (never hidden); non-venue entries (🤖/✋/…) get their
  // own trailing group. Presence test is on the normalized label (srcLbl), so auto-cons-* trade
  // sources collapse onto their signal names.
  var seen={};bs.forEach(function(s){seen[srcLbl(s.source)]=1;});
  var missing={kalshi:[],polyus:[],polymarket:[],other:[]};
  if(typeof SIGNAMES!=='undefined')Object.keys(SIGNAMES).forEach(function(k){
    var lbl=SIGNAMES[k];if(seen[lbl])return;seen[lbl]=1;
    var v=[];
    if(lbl.indexOf('🟩')>=0)v.push('kalshi');
    if(lbl.indexOf('🇺🇸')>=0)v.push('polyus');
    if(lbl.indexOf('📡')>=0)v.push('polymarket');
    if(!v.length)v.push('other');
    v.forEach(function(p){(missing[p]||missing.other).push({k:k,lbl:lbl});});
  });
  var by={};bs.forEach(function(s){(by[s.platform]||(by[s.platform]=[])).push(s);});
  var order=['kalshi','polyus','polymarket'];
  Object.keys(by).forEach(function(p){if(order.indexOf(p)<0)order.push(p);});
  var html='';
  // R73: zero rows carry the HONEST zero-state instead of bare dashes — "logged N · resolved M ·
  // no paper trades (log-only/research/retired — placement off)" from the per-venue signal_log
  // counts (window._sigLogCounts, filled by /api/stats + /api/curves), else the SIGZS reason.
  function zrow(pl,m){
    var cs=(window._sigLogCounts||[]).filter(function(c){return c&&c.signal_type===m.k&&(pl==='other'||c.platform===pl);});
    var lg=0,rs=0;cs.forEach(function(c){lg+=(c.logged||0);rs+=(c.resolved||0);});
    var note=cs.length?('logged '+lg+' · resolved '+rs+' · no paper trades (log-only/research/retired — placement off)')
                      :('never logged'+(pl!=='other'?' on this venue':'')+(SIGZS[m.k]?(' — '+SIGZS[m.k]):''));
    return '<tr style="opacity:.55"><td class="ell" title="'+escapeHtml(m.k+' — no closed paper trades\n'+note)+'">'+escapeHtml(m.lbl)+'</td><td class="r">0</td><td class="r muted" colspan="5" style="font-size:11px" title="'+escapeHtml(note)+'">'+escapeHtml(note)+'</td></tr>';}
  order.forEach(function(pl){
    var rows=by[pl]||[];var miss=missing[pl]||[];
    if(!rows.length&&!miss.length)return;
    var tot=0,totf=0;rows.forEach(function(s){tot+=(s.realized||0);totf+=(s.fees||0);});
    var pnet=tot-totf;
    html+='<div style="font-weight:700;color:var(--text);margin:10px 0 2px">'+platLabel(pl)+' <span style="color:'+((pnet>=0)?'var(--good)':'var(--bad)')+'">modeled net $'+money(pnet)+'</span> <span class="muted" style="font-weight:400;font-size:11px">(simulated gross $'+money(tot)+' − modeled fees $'+money(totf)+' · not exchange P&amp;L)</span></div>'+
      '<table class="mkt"><colgroup><col><col style="width:48px"><col style="width:46px"><col style="width:70px"><col style="width:60px"><col style="width:70px"><col style="width:64px"></colgroup>'+
      '<thead><tr><th>Signal that opened it</th><th class="r">Closed</th><th class="r" title="win rate inside this Paper simulation only">Win %</th><th class="r">Sim gross</th><th class="r">Modeled fees</th><th class="r">Modeled net</th><th class="r" title="simulated net per closed Paper round; research only, not an exchange edge">Modeled/bet</th></tr></thead><tbody>';
    rows.forEach(function(s){var n=(s.realized||0)-(s.fees||0);var npb=(s.closed||0)>0?n/s.closed:0;html+='<tr><td>'+escapeHtml(srcLbl(s.source))+'</td><td class="r">'+s.closed+'</td><td class="r">'+Math.round((s.win_rate||0)*100)+'%</td><td class="r" style="color:'+((s.realized||0)>=0?'var(--good)':'var(--bad)')+'">$'+money(s.realized)+'</td><td class="r" style="color:var(--bad)">-$'+money(s.fees||0)+'</td><td class="r" style="color:'+(n>=0?'var(--good)':'var(--bad)')+'">$'+money(n)+'</td><td class="r" style="font-weight:700;color:'+(npb>=0?'var(--good)':'var(--bad)')+'">$'+money(npb)+'</td></tr>';});
    miss.forEach(function(m){html+=zrow(pl,m);});
    html+='</tbody></table>';
  });
  if(missing.other.length){
    html+='<div style="font-weight:700;color:var(--text);margin:10px 0 2px">Other <span class="muted" style="font-weight:400;font-size:11px">no closed trades yet</span></div>'+
      '<table class="mkt"><colgroup><col><col style="width:48px"><col style="width:46px"><col style="width:70px"><col style="width:60px"><col style="width:70px"><col style="width:64px"></colgroup><tbody>';
    missing.other.forEach(function(m){html+=zrow('other',m);});
    html+='</tbody></table>';
  }
  return html||'<span class="muted">No closed trades yet.</span>';
}
// money() avoids the ugly "-0.00": clamp sub-cent magnitudes to a clean 0.
function money(v){v=(v||0);if(Math.abs(v)<0.005)v=0;return v.toFixed(2);}
// posURL builds the public market link for a position. Kalshi uses the two-segment
// /markets/{series}/{event} form (event = ticker minus its last segment); everything
// else is treated as Polymarket.
function posURL(p){var t=String((p&&p.ticker)||'');if(!t)return '#';if(p&&p.url)return p.url;var plat=(p&&p.platform)||'';var isKal=(plat==='kalshi')||(!plat&&t.indexOf('0x')!==0&&/[A-Z]/.test(t)&&t.indexOf('-')>0);
  if(isKal){var parts=t.split('-');var ev=parts.length>2?parts.slice(0,2).join('-'):t;return 'https://kalshi.com/markets/'+encodeURIComponent((parts[0]||t).toLowerCase())+'/'+encodeURIComponent(ev.toLowerCase())+'?op_market_ticker='+encodeURIComponent(t);} // R77 audit: path segments URI-encoded — ticker data can't smuggle quote/path chars into the href attribute
  // ^ Kalshi event = series + date/match (first TWO segments); slice(0,-1) was wrong for multi-outcome
  // tickers (goalscorer KXWCGOAL-DATE-PLAYER-N → it kept 3 segments → unresolvable → Kalshi bounced to a
  // default event). op_market_ticker pins the exact sub-market (the right player), not just the event.
  // FETCHFIX: NEVER fabricate a polymarket.com/event/<x> link. Poly US slugs (aec-/tec-…) live on the
  // polymarket.us host, and a Poly-int ticker is a conditionId (0x…), which is NOT an event slug — both
  // 404 on polymarket.com/event. Prefer the server-set p.url (real event slug); only construct for the
  // correct host, else don't link.
  if(plat==='polyus')return 'https://polymarket.us/markets/'+encodeURIComponent(t);
  if(t.indexOf('0x')===0){var q=(p&&p.title)?encodeURIComponent(String(p.title)):'';return q?('https://polymarket.com/markets?_q='+q):'https://polymarket.com';} // conditionId w/o a resolved slug → search by title (the server resolves the real /event link on later loads). NEVER /markets bare (301s to /predictions).
  return 'https://polymarket.com/event/'+encodeURIComponent(t);}
function closeAllPlat(platform){fetch("/api/paper/close-all",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({platform:platform})}).then(function(r){return r.json();}).then(function(){loadPaper();}).catch(function(){});}
// confColor maps a 0–1 confidence to a red→amber→green hue (0.35=red, 0.50=amber, 0.65+=green)
// so the portfolio Conf column is color-scaled at a glance.
function confColor(v){v=Math.max(0,Math.min(1,v||0));var h=Math.max(0,Math.min(1,(v-0.35)/0.30))*120;return 'hsl('+h.toFixed(0)+',72%,48%)';}
// MLEVCRYPTO: detect the coin from a Kalshi 15m ticker (KX{COIN}15M) or a Poly title ("Bitcoin Up or Down").
function cryptoCoin(s){s=String(s||'').toUpperCase();
  if(/KXBTC15M|BITCOIN|\bBTC\b/.test(s))return 'BTC';
  if(/KXETH15M|ETHEREUM|\bETH\b/.test(s))return 'ETH';
  if(/KXSOL15M|SOLANA|\bSOL\b/.test(s))return 'SOL';
  if(/KXXRP15M|\bXRP\b|RIPPLE/.test(s))return 'XRP';
  if(/KXDOGE15M|DOGECOIN|\bDOGE\b/.test(s))return 'DOGE';
  return '';}
// posMlEV: exact ticker|side first; for crypto fall back to a coin|direction key so a held 15m crypto
// position (Kalshi or Poly, any window) still shows the ML's current read even after the window rolls.
function posMlEV(p){if(!p)return null;var k=p.ticker+'|'+String(p.side||'').toUpperCase();
  if(window.mlEV&&window.mlEV[k]!=null)return window.mlEV[k];
  var coin=cryptoCoin(p.ticker)||cryptoCoin(p.title);if(!coin)return null;
  var s=String(p.side||'').toUpperCase(),dir=(s==='YES'||s==='UP')?'UP':((s==='NO'||s==='DOWN')?'DOWN':'');
  if(dir&&window.mlEVcrypto&&window.mlEVcrypto[coin+'|'+dir]!=null)return window.mlEVcrypto[coin+'|'+dir];
  return null;}
function tFmt(x){ // R60: bought/now time — unix SECONDS or RFC3339 → "7/2 6:04 PM" (12-hour, C1)
  if(!x)return "—";var d=(typeof x==="number")?new Date(x*1000):new Date(x);if(isNaN(d.getTime()))return "—";
  var h=d.getHours(),ap=h>=12?'PM':'AM';h=h%12;if(h===0)h=12;
  return (d.getMonth()+1)+"/"+d.getDate()+" "+h+":"+("0"+d.getMinutes()).slice(-2)+" "+ap;
}
// R60 C1: the Bought column leads with the agoFmt ELAPSED (37s / 4:07 / 2:14:09); the absolute
// 12-hour timestamp rides in the tooltip.
function boughtCell(x){var a=agoFmt(x);return '<td class="r" style="white-space:nowrap" title="bought '+tFmt(x)+'">'+(a||'—')+'</td>';}
function nowCell(){return '<td class="r muted" style="white-space:nowrap">'+tFmt(Date.now()/1000)+'</td>';}
function posTableHTML(idxs){
  function col(v){return (v||0)>=0?"var(--good)":"var(--bad)";}
  var mxT=0;idxs.forEach(function(i){var p=(window.lastPos||[])[i];if(p){var a=Math.abs(p.unrealized||0);if(a>mxT)mxT=a;}}); // R60 C2: table max for the P&L tint
  var h='<table class="mkt"><colgroup><col><col style="width:64px"><col style="width:48px"><col style="width:96px"><col style="width:118px"><col style="width:68px"><col style="width:78px"><col style="width:82px"><col style="width:58px"><col style="width:64px"><col style="width:72px"><col style="width:188px"></colgroup>'+
    '<thead><tr><th>Market</th><th>Side</th><th class="r" title="dollars you put in (contracts × entry price)">Held $</th><th class="r" title="your entry price (average across buys) → current market price">Entry→Cur</th><th class="r" title="when this bet was BOUGHT (first buy of the current lot) + how long ago">Bought</th><th class="r" title="current time — compare with Bought for hold time">Now</th><th class="r">Value</th><th class="r">P&amp;L</th><th class="r" title="signal confidence (0–1): the opening signal\'s current session win-rate × entry mid-price quality. Color-scaled red→green.">Conf</th><th class="r" title="the ML model current EV/contract for this market, if it has a live opinion right now; dash = no current prediction">ML EV</th><th class="r" title="take-profit / stop-loss levels set for this position, in ¢ (green TP / red SL). — = none set. Edit with the 🎯 button.">TP/SL</th><th></th></tr></thead><tbody>';
  idxs.forEach(function(i){
    var p=(window.lastPos||[])[i];if(!p)return;
    var tags='';
    // TP/SL levels render in their own column (tpslCell, below); keep only the hit badges inline.
    if(p.tp_hit)tags=' <span style="color:var(--good);font-weight:700">🎯 TP hit</span>';
    if(p.sl_hit)tags=' <span style="color:var(--bad);font-weight:700">🛑 SL hit</span>';
    var tpc=p.tp?Math.round(p.tp*100):0,slc=p.sl?Math.round(p.sl*100):0;
    var tpslCell=(tpc||slc)?((tpc?('<span style="color:var(--good);font-weight:700">'+tpc+'¢</span>'):'<span class="muted">–</span>')+'<span class="muted"> / </span>'+(slc?('<span style="color:var(--bad);font-weight:700">'+slc+'¢</span>'):'<span class="muted">–</span>')):'<span class="muted" title="no take-profit / stop-loss set — rides to settlement">ride</span>';
    var val=p.cur_price?(p.contracts*p.cur_price):(p.cost_basis||0);
    var me=posMlEV(p);
    h+='<tr'+pnlTint(p.unrealized,mxT)+'><td class="ell" title="'+escapeHtml((p.disp||p.title||p.ticker)+' · '+p.platform)+'"><a class="go" href="'+escapeHtml(posURL(p))+'" target="_blank" rel="noopener">'+escapeHtml(p.disp||p.title||p.ticker)+'</a><span class="muted"> · '+escapeHtml(p.platform)+'</span>'+tags+'</td>'+ // R63 4g: one-line ellipsis, full title in the tooltip (feeds MARKET-cell pattern)
      '<td style="font-weight:700;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'+escapeHtml(p.side)+'</td>'+
      '<td class="r">$'+(p.contracts*(p.avg_price||0)).toFixed(0)+'</td>'+
      '<td class="r">'+Math.round((p.avg_price||0)*100)+'→'+(p.cur_price?(Math.round(p.cur_price*100)+'¢'):'—')+'</td>'+
      boughtCell(p.opened)+nowCell()+
      '<td class="r">$'+val.toFixed(0)+'</td>'+
      '<td class="r" style="color:'+col(p.unrealized)+'">'+(p.cur_price?('$'+money(p.unrealized)):'—')+'</td>'+
      '<td class="r"><span style="font-weight:700;color:'+confColor(p.confidence)+'">'+((p.confidence||0)>0?(p.confidence).toFixed(2):'—')+'</span></td>'+
      '<td class="r">'+(me!=null?('<span style="font-weight:700;color:'+col(me)+'">'+(me>=0?'+':'')+(me*100).toFixed(1)+'¢</span>'):'<span class="muted">—</span>')+'</td>'+
      '<td class="r">'+tpslCell+'</td>'+
      '<td class="r"><button class="mini" title="edit take-profit / stop-loss" onclick="editStops('+i+')">🎯</button> <button class="mini" title="sell PART of this position (scale out): enter a number of contracts, or a percent like 25%" onclick="sellPos(\''+jsq(p.platform)+'\',\''+jsq(p.ticker)+'\',\''+jsq(p.side)+'\')">✂ Sell</button> <button class="mini" title="FREE-ROLL: sell exactly enough to recover your full cost — the rest then rides at zero net cost and can\'t lose" onclick="freeRollPos(\''+jsq(p.platform)+'\',\''+jsq(p.ticker)+'\',\''+jsq(p.side)+'\')">🔒 Free</button> <button class="mini" onclick="closePos(\''+jsq(p.platform)+'\',\''+jsq(p.ticker)+'\',\''+jsq(p.side)+'\')">Close</button></td></tr>';});
  return h+'</tbody></table>';
}
function loadPaper(){
  // R60: never repaint while the user is typing in a widget input (editable chips, gate popover).
  var aeP=document.activeElement;
  if(aeP&&(aeP.tagName==="INPUT"||aeP.tagName==="SELECT")&&aeP.closest&&aeP.closest('#wgrid'))return;
  loadProperPaper(); // three independent $400 Proper Betting ledgers; throttled inside the loader
  // R135 (auditor r63): PAPER repaints arrive from both the poll backstop and SSE. Route every
  // GET through jget so overlapping repaints share one request (and inherit its 20s abort) instead
  // of stacking duplicate /paper, /ml, /settings, and session-series requests in browser sockets.
  jget("/api/pnl-series?session=1").then(function(d){renderPnLChart((d&&d.series)||[],"portSessChart");}).catch(function(){}); // R29: session graph (net since suite start)
  Promise.all([jget("/api/paper"),
               jget("/api/ml").catch(function(){return null;}),
               jget("/api/settings").catch(function(){return {};})]).then(function(arr){
    var d=arr[0],ml=arr[1],cfg=arr[2]||{};
    window._mlData=ml; // R60: the ml-picks widget renders from this cache
    window.mlEV={};window.mlEVcrypto={};
    var mlarr=(ml&&ml.predictions&&(ml.predictions.all_scored||ml.predictions.predictions))||[];
    mlarr.forEach(function(x){if(!x||!x.ticker)return;var evv=(x.ev!=null?x.ev:x.ev_per_contract);window.mlEV[x.ticker+'|'+String(x.side||'').toUpperCase()]=evv;
      var cm=String(x.ticker).match(/KX(BTC|ETH|SOL|XRP|DOGE)15M/i);if(cm){var sd=String(x.side||'').toUpperCase();window.mlEVcrypto[cm[1].toUpperCase()+'|'+((sd==='YES'||sd==='UP')?'UP':'DOWN')]=evv;}});
    var s=(d&&d.summary)||{};var pos=(d&&d.positions)||[];window.lastPos=pos;
    var paperBooks=Array.isArray(d&&d.portfolios)?d.portfolios:[];
    var fundedPos=Array.isArray(d&&d.funded_positions)?d.funded_positions:[];
    var fundedAvailable=!!(d&&d.funded_positions_available);
    var net=(s.realized||0)+(s.unrealized||0)-(s.fees||0); // net AFTER fees
    function col(v){return (v||0)>=0?"var(--good)":"var(--bad)";}
    var bkK=cfg.book_kalshi_usd||600,bkU=cfg.book_polyus_usd||600,bkC=cfg.book_combos_usd||600,bkM=cfg.book_ml_usd||600,bkMC=cfg.book_ml_combos_usd||600,bkI=cfg.bankroll_polyint||125;
    // R146: five independent portfolio limits. paper_total_start/alloc_* remain API/history fields,
    // but never appear here as though they still split the operator's paper money.
    var bankChips='compounding portfolios <span class="muted">(🟩 Kalshi '+echip('book_kalshi_usd',bkK,'$'+Math.round(bkK),'Kalshi Paper starting/reset grant; current sizing compounds only Kalshi fee-net P&L')+' · 🇺🇸 PolyUS '+echip('book_polyus_usd',bkU,'$'+Math.round(bkU),'PolyUS Paper starting/reset grant; current sizing compounds only PolyUS fee-net P&L')+' · 🧩 Combos '+echip('book_combos_usd',bkC,'$'+Math.round(bkC),'System Combo Paper starting/reset grant; current sizing compounds only System Combo fee-net P&L')+' · 🤖 ML '+echip('book_ml_usd',bkM,'$'+Math.round(bkM),'One ML Paper starting/reset grant, split into independently compounding Kalshi and PolyUS sleeves')+' · 🤖🎲 ML Combo '+echip('book_ml_combos_usd',bkMC,'$'+Math.round(bkMC),'New-ML Combo Paper starting/reset grant; this portfolio compounds independently from ML singles and System Combo Paper')+' · 📡 Poly-int '+echip('bankroll_polyint',bkI,'$'+Math.round(bkI),'Poly-int research-only notional; click to edit')+')</span>';
    var pSum=document.getElementById("paperSummary");
    if(pSum){var psH= // R60 C8: bankroll chips are click-to-edit (Enter → /api/settings)
      bankChips+' · '+
      '<b>'+(s.open_positions||0)+'</b> open · cost basis <b>$'+fmtVol(s.cost_basis||0)+'</b> · '+
      'realized <b style="color:'+col(s.realized)+'">$'+money(s.realized)+'</b> · '+
      'unrealized <b style="color:'+col(s.unrealized)+'">$'+money(s.unrealized)+'</b> · '+
      'fees <b style="color:var(--bad)">-$'+money(s.fees)+'</b> · '+
      'net (after fees) <b style="color:'+col(net)+'">$'+money(net)+'</b>';
      if(paperBooks.length){
        psH+='<div style="margin-top:5px;display:flex;gap:7px;flex-wrap:wrap">';
        paperBooks.forEach(function(b){
          if(!b||!b.available||typeof b.equity_usd!=='number'||typeof b.available_usd!=='number'){
            psH+='<span class="muted"><b>'+escapeHtml((b&&b.label)||'Paper book')+'</b>: unavailable</span>';return;
          }
          var realized=Number(b.realized_fee_net_usd||0);
          psH+='<span><b>'+escapeHtml(b.label||b.id||'Paper book')+'</b> $'+money(b.equity_usd)+
            ' equity · $'+money(b.available_usd)+' free · <span style="color:'+col(realized)+'">'+
            (realized>=0?'+$':'-$')+money(Math.abs(realized))+' settled</span> · '+
            Number(b.funded_open_positions||0)+' open</span>';
        });
        psH+='</div>';
      }
      if(pSum._h!==psH){pSum._h=psH;pSum.innerHTML=psH;}} // R70 (audit §a P2): repaint only on change
    var pe=document.getElementById("paperPositions");
    var html='';
    if(pos.length===0){html+='<span class="muted">No open positions in the shared closeable Paper ledger.</span>';}
    else{
      var groups={kalshi:[],polyus:[],polymarket:[]};
      pos.forEach(function(p,i){(groups[p.platform]||(groups[p.platform]=[])).push(i);});
      [['kalshi','🟩 Kalshi'],['polyus','🇺🇸 Poly US'],['polymarket','🟦 Poly-int']].forEach(function(g){
        var idxs=groups[g[0]]||[];var rl=0,evs=0;idxs.forEach(function(i){var p=(window.lastPos||[])[i];if(!p)return;rl+=(p.unrealized||0);var pe=posMlEV(p);if(pe!=null)evs+=(p.contracts||0)*pe;});
        html+='<div style="display:flex;align-items:center;gap:8px;margin:12px 0 2px"><span style="font-weight:700;color:var(--text)">'+g[1]+' · '+idxs.length+'</span>'+(idxs.length?('<span class="muted" style="color:'+col(rl)+'">$'+money(rl)+' unreal.</span> <span class="muted" style="color:'+col(evs)+'" title="total ML EV of this venue\'s open book — sum of (contracts × ML EV/contract). Positive = the open positions are +EV by the model.">ML EV '+(evs>=0?'+':'')+'$'+money(evs)+'</span> <button class="mini" onclick="closeAllPlat(\''+g[0]+'\')">Close all</button>'):'')+'</div>';
        if(idxs.length){html+=posTableHTML(idxs);}else{html+='<div class="muted" style="margin:2px 0 6px;font-size:12.5px">None.</div>';}
      });
    }
    if(!fundedAvailable){
      html+='<div style="margin-top:12px;color:var(--warn)"><b>Funded system positions unavailable.</b> The owning ledger could not be read, so this is not shown as $0.</div>';
    }else if(fundedPos.length){
      html+='<div style="margin:14px 0 4px"><b>Funded system positions · '+fundedPos.length+
        '</b> <span class="muted">read-only here; these are committed portfolio lots, not shadow/model fills</span></div>'+
        '<table class="mkt"><thead><tr><th>Market</th><th>Book</th><th>Side</th><th class="r">Held $</th>'+
        '<th class="r">Entry→Cur</th><th class="r">Bought</th><th class="r">P&amp;L</th></tr></thead><tbody>';
      fundedPos.forEach(function(p){
        var cur=(typeof p.current_price==='number')?p.current_price:null;
        var unreal=(typeof p.unrealized_usd==='number')?p.unrealized_usd:null;
        html+='<tr><td class="ell" title="'+escapeHtml((p.title||p.ticker||'')+' · '+(p.execution_shadow_attempt_id||'funded legacy lot'))+
          '"><a class="go" href="'+escapeHtml(posURL(p))+'" target="_blank" rel="noopener">'+
          escapeHtml(p.title||p.ticker||'')+'</a></td><td>'+escapeHtml(p.sub_book||p.portfolio||'')+
          '</td><td><b>'+escapeHtml(p.side||'')+'</b></td><td class="r">$'+money(p.cost_basis_usd||0)+
          '</td><td class="r">'+Math.round(Number(p.entry_price||0)*100)+'→'+
          (cur===null?'—':Math.round(cur*100)+'¢')+'</td>'+boughtCell(p.opened)+
          '<td class="r" style="color:'+col(unreal||0)+'">'+(unreal===null?'—':'$'+money(unreal))+'</td></tr>';
      });
      html+='</tbody></table>';
    }else if(pos.length===0){
      html+='<div class="muted" style="margin-top:8px">No current funded system positions.</div>';
    }
    withScroll(pe,html);
    // R60: the ML sidecar book renders in its OWN widget now (ml-book) — same rows, plus an
    // explicit p_win column (C3) and the P&L row tint (C2).
    var mlEl=document.getElementById('mlBookW');
    var pf=ml&&ml.paper;
    if(mlEl){
      var mh='';
      if(!pf){mh='<span class="muted" style="padding:2px 4px;display:inline-block;">ML sidecar has not produced output yet (build-suite.bat launches it).</span>';}
      else{
        var st=pf.stats||{};
        var mopen=(pf.open||[]).slice().sort(function(a,b){return (b.unrealized||0)-(a.unrealized||0);});
        mh+='<div class="wsum">net <b style="color:'+col(st.net)+'">$'+money(st.net)+'</b> · closed '+(st.closed||0)+' · open '+mopen.length+' · <span title="the ML book\'s picks are chosen + settled by the sidecar">sidecar-managed</span> <button class="mini" onclick="openML()" title="model internals: calibration, scoring, shadow book">model →</button></div>';
        if(!mopen.length){mh+='<div class="muted" style="padding:2px 4px;">No open ML picks.</div>';}
        else{
          var mmx=0;mopen.forEach(function(p){var a=Math.abs(p.unrealized||0);if(a>mmx)mmx=a;});
          mh+='<table class="mkt"><colgroup><col><col style="width:34px"><col style="width:58px"><col style="width:44px"><col style="width:56px"><col style="width:54px"><col style="width:42px"><col style="width:44px"></colgroup><thead><tr><th>Pick</th><th>Side</th><th class="r" title="your entry → live mark, for YOUR side">@→Now¢</th><th class="r">Held $</th><th class="r" title="elapsed since bought (absolute in the tooltip)">Bought</th><th class="r">P&amp;L</th><th class="r" title="the ML model win probability for this pick">p_win</th><th class="r" title="ML EV/contract at entry">EV/ct</th></tr></thead><tbody>';
          mopen.forEach(function(p){var held=(p.contracts||0)*(p.price||0);
            var cur=(p.cur_price!=null)?p.cur_price:((p.contracts||0)>0?(p.price||0)+(p.unrealized||0)/(p.contracts||1):null); // server live-marks cur_price; fallback derives it from unrealized
            var pxc=Math.round((p.price||0)*100)+'→'+(cur!=null?Math.round(cur*100)+'¢':'—');
            mh+='<tr'+pnlTint(p.unrealized,mmx)+'><td><a class="go" href="'+posURL(p)+'" target="_blank" rel="noopener">'+escapeHtml((p.title||p.ticker||"").slice(0,120))+'</a></td><td>'+escapeHtml(p.side||"")+'</td><td class="r"'+(cur!=null?' style="color:'+col(cur-(p.price||0))+'"':'')+'>'+pxc+'</td><td class="r">$'+held.toFixed(0)+'</td>'+boughtCell(p.opened)+'<td class="r" style="color:'+col(p.unrealized)+'">$'+money(p.unrealized)+'</td><td class="r">'+(p.p_win||0).toFixed(2)+'</td><td class="r" style="color:'+col(p.ev)+'">'+(((p.ev||0)>=0?'+':'')+((p.ev||0)*100).toFixed(1))+'¢</td></tr>';});
          mh+='</tbody></table>';
        }
      }
      withScroll(mlEl,mh);
    }
    renderMlPicks(); // R60: the ml-picks widget shares this /api/ml payload
    renderShadowBook(); // R63 6e: the shadow-book widget rides the same cache
    renderRawflowBook(); // R105: the rawflow-book widget rides the same cache
    renderWeatherBook(); // R106: the weather-book widget rides the same cache
    renderFreshinvBook(); // R117: the freshinv-book widget rides the same cache
    loadRisk();
  }).catch(function(){document.getElementById("paperPositions").textContent="Could not load portfolio.";});
}
// The Brier/log/spherical Paper portfolios have always lived in the Proper ledger, but the
// dashboard never consumed that money view. Keep this lightweight read independent of the hot
// /api/paper request; it never runs the full diagnostic Proper research report.
// Missing/error data stays visibly unavailable; it is never converted into a fake $0 result.
function loadProperPaper(){
  var el=document.getElementById('properPaperW');if(!el)return;
  var now=Date.now();if(now<(window._properPaperNext||0))return;
  window._properPaperNext=now+300000;
  jget('/api/proper-score-paper',20000).then(function(d){
    var host=document.getElementById('properPaperW');if(!host)return;
    var books=d&&d.paper_portfolios;
    if(!books){host.innerHTML='<b>Proper Betting Paper</b> <span style="color:var(--warn)">unavailable — not counted as $0</span>';window._properPaperNext=Date.now()+30000;return;}
    function lane(key,label){
      var p=books[key];
      if(!p)return '<b>'+label+'</b> <span style="color:var(--warn)">unavailable</span>';
      var nav=(typeof p.entry_mark_equity_usd==='number')?p.entry_mark_equity_usd:null;
      var net=(typeof p.net_profit_usd==='number')?p.net_profit_usd:null;
      if(nav===null||net===null)return '<b>'+label+'</b> <span style="color:var(--warn)">unavailable</span>';
      var c=net>=0?'var(--good)':'var(--bad)';
      return '<b>'+label+'</b> $'+money(nav)+' <span style="color:'+c+'">('+(net>=0?'+$':'-$')+money(Math.abs(net))+')</span> · '+Number(p.fills||0)+' fills ('+Number(p.open_positions||0)+' open / '+Number(p.settled_positions||0)+' settled)';
    }
    host.innerHTML='<b>🧮 Proper Betting Paper · $400 each</b><br>'+lane('brier','Brier')+'<br>'+lane('log','Log')+'<br>'+lane('spherical','Spherical');
  }).catch(function(){var host=document.getElementById('properPaperW');if(host)host.innerHTML='<b>Proper Betting Paper</b> <span style="color:var(--warn)">unavailable — not counted as $0</span>';window._properPaperNext=Date.now()+30000;});
}
var PLATLBL={kalshi:"🟩 Kalshi",polyus:"🇺🇸 Poly US",polymarket:"🟦 Poly-int"};
function platLbl(p){return PLATLBL[p]||p;}
function legsHTML(legs){var h='';(legs||[]).forEach(function(l){ // R45b (operator): every leg CLICKABLE + ntfy naming (emoji via legSport server-side title)
  var oc=l.outcome||'';var t=escapeHtml(l.title||l.ticker);
  var url=posURL({platform:l.platform||'kalshi',ticker:l.ticker,title:l.title});
  var pick=oc?('<a class="go" href="'+url+'" target="_blank" rel="noopener" style="color:var(--accent);font-weight:700">'+escapeHtml(oc)+'</a> <span class="muted">('+t+')</span>'):('<a class="go" href="'+url+'" target="_blank" rel="noopener"><b>'+t+'</b></a>');
  h+='<div style="font-size:12px;margin:1px 0"><b>'+(l.side||'')+'</b> → '+pick+' <span class="muted">@'+Math.round((l.entry||0)*100)+'¢</span></div>';});return h;}
// R48/R49 📜 LOGS TAB: sub-tabs; human times ("July 3, 2026 at 11:18 AM (0 mins ago)"); Copy = JSON.
function fmtWhen(x){ // accepts RFC3339 string or unix seconds
  var t=(typeof x==="number")?x*1000:Date.parse(x);if(!t||isNaN(t))return escapeHtml(String(x||''));
  var d=new Date(t);var mins=Math.max(0,Math.round((Date.now()-t)/60000));
  var ago=mins<60?(mins+' mins ago'):(mins<2880?(Math.round(mins/60)+' hrs ago'):(Math.round(mins/1440)+' days ago'));
  var mo=['January','February','March','April','May','June','July','August','September','October','November','December'][d.getMonth()];
  var h=d.getHours(),ap=h>=12?'PM':'AM';h=h%12;if(h===0)h=12;
  return mo+' '+d.getDate()+', '+d.getFullYear()+' at '+h+':'+('0'+d.getMinutes()).slice(-2)+' '+ap+' <span class="muted">('+ago+')</span>';
}
// R62 item 5: any ticker printed in a log row is a clickable market link (posURL infers the venue).
function tickLink(tk,plat,title){
  if(!tk)return '';
  return '<a class="go" href="'+escapeHtml(posURL({platform:plat||'',ticker:tk,title:title||''}))+'" target="_blank" rel="noopener">'+escapeHtml(tk)+'</a>';
}
function showLogSub(sub){
  window._logSub=sub;['live','audit','rejp','rejl','rejm','name','match','orders'].forEach(function(k){var b=document.getElementById('logTab_'+k);if(b){b.style.fontWeight=(k===sub)?'700':'400';b.style.color=(k===sub)?'var(--text)':'';}});
  var el=document.getElementById("logsBody");
  // R67e: the Orders sub-tab ADOPTS the single #ordersBody node (same pattern as widgets) —
  // return it to its home panel BEFORE any rebuild wipes it out of existence.
  var _ob=document.getElementById('ordersBody'),_oh=document.getElementById('orderscard');
  if(_ob&&_oh&&el.contains(_ob))_oh.appendChild(_ob);
  if(sub==='orders'){
    el.innerHTML='<div class="muted" style="font-size:12px;margin:2px 0 8px">The exact API order payload the bot WOULD send to Kalshi / Polymarket for each auto buy &amp; sell — placeholder key + signatures. <b>Preview only, nothing is sent.</b> Newest first.</div>';
    var ob2=document.getElementById('ordersBody');
    if(ob2)el.appendChild(ob2);else el.innerHTML+='<span class="muted">the orders pane is adopted by a widget right now — remove that widget to view it here</span>';
    loadOrders();return;
  }
  el.textContent="Loading…";
  function rows(list,fmt){window._logData=list||[];if(!list||!list.length){el.innerHTML='<span class="muted">Nothing logged yet.</span>';return;}
    var h='<div style="max-height:60vh;overflow-y:auto;overflow-x:hidden;width:100%">';list.forEach(function(r){h+='<div style="border-top:1px solid var(--line);padding:3px 0;word-break:break-word;overflow-wrap:anywhere">'+fmt(r)+'</div>';});el.innerHTML=h+'</div>';} // R63 4d: wrap, never clip
  if(sub==='live'){fetch("/api/live").then(function(r){return r.json();}).then(function(d){rows((d&&d.log)||[],function(r){return (r.ts?fmtWhen(r.ts)+' ':'')+'<b>'+escapeHtml(r.event||'')+'</b> '+tickLink(r.ticker)+' <span class="muted">'+escapeHtml(JSON.stringify(r)).slice(0,600)+'</span>';});}).catch(function(){el.textContent='failed';});}
  else if(sub==='audit'){fetch("/api/auditlog?limit=400").then(function(r){return r.json();}).then(function(d){rows((d&&d.rows)||[],function(r){return fmtWhen(r.ts)+' <b>'+escapeHtml(r.category||'')+'</b> '+escapeHtml(r.message||'')+(r.detail?' <span class="muted">'+escapeHtml(String(r.detail)).slice(0,400)+'</span>':'');});}).catch(function(){el.textContent='failed';});}
  else if(sub==='rejp'){fetch("/api/rejections").then(function(r){return r.json();}).then(function(d){rows((d&&d.rows)||[],function(r){return fmtWhen(r.ts)+' <b>'+escapeHtml(r.source||'')+'</b> '+escapeHtml(r.platform||'')+' '+tickLink(r.ticker,r.platform)+' '+escapeHtml(r.side||'')+' @'+Math.round((r.price||0)*100)+'¢ — '+escapeHtml(r.reason||'');});}).catch(function(){el.textContent='failed';});}
  else if(sub==='rejl'){fetch("/api/live").then(function(r){return r.json();}).then(function(d){
    var out=[];(d&&d.proposal_drop_samples||[]).forEach(function(s){out.push({kind:'proposal',venue:s.venue,ticker:s.ticker,reason:s.reason});});
    var pd=(d&&d.proposal_drops)||{};Object.keys(pd).forEach(function(k){out.push({kind:'count',reason:k,n:pd[k]});});
    ((d&&d.log)||[]).forEach(function(r){if(/REFUSED/.test(r.event||''))out.push(r);});
    rows(out,function(r){return (r.ts?fmtWhen(r.ts)+' ':'')+(r.n!=null?('<b>×'+r.n+'</b> '):'')+'<b>'+escapeHtml(r.kind||r.event||'')+'</b> '+tickLink(r.ticker,r.venue)+' — '+escapeHtml(r.reason||'');});
  }).catch(function(){el.textContent='failed';});}
  else if(sub==='rejm'){fetch("/api/mlrejections").then(function(r){return r.json();}).then(function(d){rows((d&&d.rows)||[],function(r){return fmtWhen(r.ts)+' '+tickLink(r.ticker)+' '+escapeHtml(r.side||'')+' @'+Math.round((r.price||0)*100)+'¢ p_win '+((r.p_win||0).toFixed(2))+' — <b>'+escapeHtml(r.reason||'')+'</b>';});}).catch(function(){el.textContent='failed';});}
  else if(sub==='match'){fetch("/api/matchlog").then(function(r){return r.json();}).then(function(d){var fams=(d&&d.families)||{};var hdr=Object.keys(fams).sort().map(function(k){var f=fams[k]||{};return k+' '+(f.hits||0)+'/'+(f.attempts||0)+' ('+Math.round((f.rate||0)*100)+'%)';}).join(' · ');var l=((d&&d.misses)||[]).slice();l.unshift({ts:new Date().toISOString(),family:'(rates)',title:hdr||'no matcher attempts since boot'});rows(l,function(r){return fmtWhen(r.ts)+' <b>'+escapeHtml(r.family||'')+'</b> '+(r.reason?('<span style="color:var(--warn)">'+escapeHtml(r.reason)+'</span> '):'')+escapeHtml(r.title||'')+(r.side?(' — <b>'+escapeHtml(r.side)+'</b>'):'')+(r.slug?(' <span class="muted">'+escapeHtml(r.slug)+'</span>'):'');});}).catch(function(){el.textContent='failed';});}
  else{fetch("/api/naminglog").then(function(r){return r.json();}).then(function(d){var l=(d&&d.rows)||[];l.unshift({ts:new Date().toISOString(),ticker:'(total)',title:(d&&d.total_since_boot||0)+' raw-title fallbacks since boot'});rows(l,function(r){return fmtWhen(r.ts)+' <b>'+(r.ticker==='(total)'?escapeHtml(r.ticker):tickLink(r.ticker,'',r.title))+'</b> — '+escapeHtml(r.title||'');});}).catch(function(){el.textContent='failed';});}
}
function copyLogSub(){navigator.clipboard.writeText(JSON.stringify(window._logData||[],null,1)).then(function(){});}
// R55a PIPELINE STRIP: renders the engine's real funnel (SCAN › GATE › PROPOSE › PLACE › SETTLE)
// from the CACHED /api/live snapshot — zero extra server load, repaints only on change.
function loadPipeStrip(){
  if(document.hidden)return; // R77 audit: visibility-gated — refired once on return-to-visible
  jget("/api/live").then(function(d){ // R70 (audit §a P3): coalesce with loadLive's 1s poll
    var el=document.getElementById("pipestrip");if(!el||!d)return;
    var drops=d.proposal_drops||{},dn=0,ks=Object.keys(drops);ks.forEach(function(k){dn+=drops[k]||0;});
    var props=(d.proposals||[]).length,scanned=dn+props;
    var top=ks.sort(function(a,b){return (drops[b]||0)-(drops[a]||0);}).slice(0,3).map(function(k){return "×"+drops[k]+" "+k;}).join("\n");
    function seg(lbl,val,cls,tip){return '<span class="pseg '+cls+'"'+(tip?' title="'+escapeHtml(tip)+'"':'')+'>'+lbl+(val!=null?' <b>'+escapeHtml(String(val))+'</b>':'')+'</span>';}
    var a='<span class="parrow">›</span>';
    var h=seg('SCAN',scanned,scanned>0?'on':'','ML candidates screened this pass (survivors + drops)')
      +a+seg('GATE',dn,dn>0?'on':'','dropped by the gates this pass — top reasons:\n'+top)
      +a+seg('PROPOSE',props,props>0?'on':'','live-priced, net-EV-positive proposals on the board right now')
      +a+seg('PLACE',(d.auto?'AUTO':(d.armed?'ARMED':'SAFE')),(d.auto?'hot':(d.armed?'on':'')),d.auto?'automatic cash is closed in R165; no current route has authenticated profit authority':(d.armed?'armed alone does not grant a system cash authority':'not armed — nothing can place'))
      +a+seg('SETTLE',null,'on','grading + settlement runs continuously (positions graded even after retirement)');
    if(el._h!==h){el._h=h;el.innerHTML=h;}
  }).catch(function(){});
}
setInterval(loadPipeStrip,7000);setTimeout(loadPipeStrip,800);

// R50: one-click download of EVERY log (audit accepts+errors, live order trail, paper/live/ML rejections, naming fallbacks)
function downloadAllLogs(btn){if(btn){btn.textContent="⬇ building…";btn.disabled=true;}
  fetch("/api/logs/all").then(function(r){return r.json();}).then(function(d){
    var ts=new Date(),p=function(n){return ('0'+n).slice(-2);};
    var name='suite_logs_'+ts.getFullYear()+p(ts.getMonth()+1)+p(ts.getDate())+'_'+p(ts.getHours())+p(ts.getMinutes())+'.json';
    var a=document.createElement('a');a.href=URL.createObjectURL(new Blob([JSON.stringify(d,null,1)],{type:'application/json'}));a.download=name;
    document.body.appendChild(a);a.click();setTimeout(function(){URL.revokeObjectURL(a.href);a.remove();},2000);
    if(btn){btn.textContent="⬇ All logs";btn.disabled=false;}
  }).catch(function(){if(btn){btn.textContent="failed — retry";btn.disabled=false;}});}
function closeParlay(){setTab(WTAB);}
function loadParlay(){
  fetch("/api/parlay").then(function(r){return r.json();}).then(function(d){
    function col(v){return (v||0)>=0?"var(--good)":"var(--bad)";}
    var s=(d&&d.summary)||{};
    var bankroll=(typeof s.bankroll==='number')?('$'+money(s.bankroll)):'unavailable';
    var available=(typeof s.available==='number')?('$'+money(s.available)):'unavailable';
    document.getElementById("parlaySummary").innerHTML=
      'bankroll <b>'+bankroll+'</b> <span class="muted">(own combo book)</span> · available <b>'+available+'</b> · '+
      '<b>'+(s.open||0)+'</b> open · cost basis <b>$'+money(s.cost_basis)+'</b> · '+
      'value <b>$'+money(s.value)+'</b> · unrealized <b style="color:'+col(s.unrealized)+'">$'+money(s.unrealized)+'</b> · '+
      'realized <b style="color:'+col(s.realized)+'">$'+money(s.realized)+'</b>';
    // open
    var open=(d&&d.open)||[];var oe=document.getElementById("parlayOpen");
    if(!open.length){oe.innerHTML='<span class="muted">No open combos. Place one from the suggestions below.</span>';}
    else{var h='<table class="mkt"><thead><tr><th>Venue</th><th>Legs</th><th class="r">Stake</th><th class="r">Price</th><th class="r">Payout if win</th><th class="r">Mark</th><th class="r">Unreal.</th></tr></thead><tbody>';
      open.forEach(function(p){h+='<tr><td>'+platLbl((p.legs&&p.legs[0]&&p.legs[0].platform)||"")+'</td><td>'+legsHTML(p.legs)+'</td><td class="r">$'+money(p.stake)+'</td><td class="r">'+Math.round((p.price||0)*100)+'¢</td><td class="r">$'+money(p.contracts)+'</td><td class="r">'+Math.round((p.mark||0)*100)+'¢</td><td class="r" style="color:'+col(p.unrealized)+'">$'+money(p.unrealized)+'</td></tr>';});
      oe.innerHTML=h+'</tbody></table>';}
    // suggestions (sortable; default = ROI, which is EV per $1 = the best long-run pick)
    window.parlaySugs=(d&&d.suggestions)||[];
    window.parlayDropReasons=(d&&d.drop_reasons)||{}; // R20: per-venue WHY for the (0) zero-state
    if(!window.parlaySort)window.parlaySort='roi';
    renderParlaySuggest();
    // settled
    var settled=(d&&d.settled)||[];var te=document.getElementById("parlaySettled");
    if(!settled.length){te.innerHTML='<span class="muted">None yet.</span>';}
    else{var h3='<table class="mkt"><thead><tr><th>Venue</th><th>Legs</th><th class="r">Stake</th><th class="r">Payout</th><th class="r">Realized</th></tr></thead><tbody>';
      settled.slice(0,50).forEach(function(p){h3+='<tr><td>'+platLbl((p.legs&&p.legs[0]&&p.legs[0].platform)||"")+'</td><td>'+legsHTML(p.legs)+'</td><td class="r">$'+money(p.stake)+'</td><td class="r">'+Math.round((p.payout||0)*100)+'¢</td><td class="r" style="color:'+col(p.realized)+'">$'+money(p.realized)+'</td></tr>';});
      te.innerHTML=h3+'</tbody></table>';}
    loadMLComboPaper();
  }).catch(function(){document.getElementById("parlaySuggest").textContent="Could not load combos.";});
}
function loadMLComboPaper(){
  var sum=document.getElementById('mlComboPaperSummary'),rows=document.getElementById('mlComboPaperRows');
  if(!sum||!rows)return;
  fetch('/api/ml-combo-paper').then(function(r){return r.json();}).then(function(d){
    var s=d.status||{},open=d.open||[],closed=d.settled||[];
    // A bankrupt portfolio has real $0 equity.  Only a missing field falls back to its grant;
    // JavaScript's old OR fallback incorrectly turned that honest zero into a healthy-looking $600.
    var eq=(s.equity===null||s.equity===undefined)?((s.starting_grant===null||s.starting_grant===undefined)?600:s.starting_grant):s.equity;
    var avail=(s.available===null||s.available===undefined)?0:s.available;
    sum.innerHTML='<b>$'+money(eq)+'</b> equity · <b>$'+money(avail)+'</b> available · P&amp;L <b style="color:'+((s.realized_net||0)>=0?'var(--good)':'var(--bad)')+'">$'+money(s.realized_net||0)+'</b> · '+(s.open||0)+' open · '+(s.closed||0)+' closed · state '+escapeHtml(s.state||'warming');
    var all=open.concat(closed).slice(0,30);
    if(!all.length){rows.innerHTML='<span class="muted">No ML combo positions yet. The model needs at least two fresh, fee-positive, same-venue legs inside the funded horizon.</span>';return;}
    var h='<table class="mkt"><thead><tr><th>State</th><th>Venue</th><th>Legs</th><th class="r">Stake</th><th class="r">Net</th></tr></thead><tbody>';
    all.forEach(function(p){var isOpen=p.status==='open',net=isOpen?(p.unrealized||0):(p.realized||0);h+='<tr><td>'+(isOpen?'open':'closed')+'</td><td>'+platLbl((p.legs&&p.legs[0]&&p.legs[0].platform)||'')+'</td><td>'+legsHTML(p.legs||[])+'</td><td class="r">$'+money(p.stake||0)+'</td><td class="r" style="color:'+(net>=0?'var(--good)':'var(--bad)')+'">$'+money(net)+'</td></tr>';});
    rows.innerHTML=h+'</tbody></table>';
  }).catch(function(){sum.textContent='New-ML Combo Paper unavailable.';});
}
function resetMLComboPaper(){
  fetch('/api/ml-combo-paper/reset',{method:'POST'}).then(function(r){return r.json();}).then(function(d){
    document.getElementById('parlayMsg').textContent=d.ok?('New-ML Combo reset to $'+money(d.grant||600)+'; closed '+(d.closed||0)+' open positions.'):('Reset failed: '+(d.error||'unknown'));
    loadMLComboPaper();
  }).catch(function(){document.getElementById('parlayMsg').textContent='New-ML Combo reset failed.';});
}
// renderParlaySuggest draws the suggestions, sorted by the chosen key. ROI = EV per $1 staked = the
// mathematically best pick for long-run profit (win_prob × payout − 1); payout/win are there to eyeball.
function renderParlaySuggest(){
  var se=document.getElementById("parlaySuggest"); if(!se) return;
  var all=(window.parlaySugs||[]).slice();
  if(!all.length){ se.innerHTML='<span class="muted">No +EV combos right now. Needs the ML sidecar running (it writes the per-market picks combos are built from).</span>'; return; }
  var counts={}; all.forEach(function(g){counts[g.platform]=(counts[g.platform]||0)+1;});
  var venue=window.parlayVenue||'';
  var sugs=all.filter(function(g){return !venue||g.platform===venue;});
  var key=window.parlaySort||'evnet';
  function netev(g){return (g.ev_net!=null)?g.ev_net:((g.roi||0)*(g.stake||0));}
  function netroi(g){return (g.roi_net!=null)?g.roi_net:(g.roi||0);}
  var kf={ evnet:netev, roi:netroi, payout:function(g){return g.payout_mult||0;}, win:function(g){return g.p_win||0;} }[key] || netev;
  // R23: stable tiebreak (legs signature) so rows stop reshuffling when scores wiggle between sweeps
  function gKey(g){return ((g.legs||[]).map(function(l){return (l.ticker||"")+(l.side||"");}).sort().join("|"));}
  sugs.sort(function(a,b){ var d=kf(b)-kf(a); if(Math.abs(d)>1e-6)return d; return gKey(a)<gKey(b)?-1:1; });
  var sel='<div style="margin:2px 0 7px;font-size:12.5px;display:flex;align-items:center;gap:6px;flex-wrap:wrap">'
    +'<span class="muted">Venue</span>'
    +'<select onchange="window.parlayVenue=this.value;renderParlaySuggest()" style="font-size:12.5px;padding:2px 4px">'
    +'<option value=""'+(venue===""?" selected":"")+'>All venues ('+all.length+')</option>'
    +'<option value="kalshi"'+(venue==="kalshi"?" selected":"")+'>🟩 Kalshi ('+(counts.kalshi||0)+')</option>'
    +'<option value="polyus"'+(venue==="polyus"?" selected":"")+'>🇺🇸 Poly US ('+(counts.polyus||0)+')</option>'
    +'</select>'
    +'<span class="muted">Sort by</span>'
    +'<select onchange="window.parlaySort=this.value;renderParlaySuggest()" style="font-size:12.5px;padding:2px 4px">'
    +'<option value="evnet"'+(key==="evnet"?" selected":"")+'>Net EV (fee-adjusted $ — best)</option>'
    +'<option value="roi"'+(key==="roi"?" selected":"")+'>Net ROI per $1</option>'
    +'<option value="payout"'+(key==="payout"?" selected":"")+'>Payout multiple (biggest win)</option>'
    +'<option value="win"'+(key==="win"?" selected":"")+'>Win probability (safest)</option>'
    +'</select>'
    +'<span class="muted">· ★ = best by this sort · net EV = after fees</span></div>';
  if(!sugs.length){
    var hint=(all.length>0)?('<b>'+all.length+' combo'+(all.length>1?'s':'')+' exist on other venues</b> — the venue filter above is hiding them. Set it to "All venues".')
                           :'None composable this moment. Crypto legs roll every 15 minutes — combos vanish at window expiry (:00/:15/:30/:45) and regenerate ~1 min into the next window.';
    // R20 (operator: "Kalshi 0 again, cmon man"): the zero-state now shows exactly WHERE the legs
    // went — the last sweep's drop breakdown for the selected venue. No more mystery zeros.
    var dr=(window.parlayDropReasons||{})[venue||'kalshi'];
    if(venue&&dr){
      var parts=[];Object.keys(dr).sort(function(a,b){return dr[b]-dr[a];}).forEach(function(k){if(dr[k]>0)parts.push('<b>'+dr[k]+'</b> '+escapeHtml(k));});
      if(parts.length){hint+='<div style="margin-top:6px">Where '+(venue==='kalshi'?'🟩 Kalshi':'🇺🇸 Poly US')+' legs went last sweep: '+parts.join(' · ')+'.</div>'
        +'<div style="margin-top:4px;font-size:11.5px">Legs need: price 50–95¢ · p_win ≥ 55% · +EV net of fees · resolving inside the window · a combo-eligible event · live price still 35–97¢. Sports legs appear as game time nears.</div>';}
    }
    var zeroS=sel+'<div class="muted" style="margin-top:6px">'+hint+'</div>';
    if(window._sugsHTML!==zeroS){window._sugsHTML=zeroS;se.innerHTML=zeroS;} return; }
  var h='<table class="mkt"><thead><tr><th>Venue</th><th title="bets matched into the combo">Legs</th>'
    +'<th class="r" title="chance ALL legs hit">Win%</th><th class="r" title="payout multiple if it hits">Payout</th>'
    +'<th class="r" title="fee-adjusted expected profit on the recommended stake — the real EV">Net EV</th>'
    +'<th class="r" title="fee-adjusted expected return per $1 staked">Net ROI</th>'
    +'<th class="r" title="recommended stake (quarter-Kelly on the combo edge, capped)">Rec $</th><th></th></tr></thead><tbody>';
  sugs.forEach(function(g,i){
    var idx=(window.parlaySugs||[]).indexOf(g);
    var leglist=(g.legs||[]);
    var star=(i===0)?' <span title="best by this sort" style="color:var(--accent)">★</span>':'';
    var ev=netev(g), rn=netroi(g);
    h+='<tr'+(i===0?' style="background:var(--card2)"':'')+'>'
      +'<td>'+platLbl(g.platform)+'</td>'
      +'<td><span title="'+escapeHtml(leglist.map(function(l){return l.side+" "+(l.title||l.ticker);}).join(" + "))+'"><b>'+leglist.length+'-leg</b>'+star+' '+legsHTML(leglist)+'</span></td>'
      +'<td class="r">'+Math.round((g.p_win||0)*100)+'%</td>'
      +'<td class="r">'+(g.payout_mult||0).toFixed(2)+'×</td>'
      +'<td class="r" style="color:'+(ev>=0?"var(--good)":"var(--bad)")+'"><b>'+(ev>=0?"+$":"-$")+money(Math.abs(ev))+'</b></td>'
      +'<td class="r" style="color:'+(rn>=0?"var(--good)":"var(--bad)")+'">'+(rn>=0?"+":"")+Math.round(rn*100)+'%</td>'
      +'<td class="r"><b>$'+money(g.stake||0)+'</b></td>'
      +'<td class="r"><span class="muted" title="Manual Paper orders were removed; Paper AUTO alone may place a qualified combo.">AUTO only</span></td></tr>';
  });
  var fullS=sel+h+'</tbody></table>';
  if(window._sugsHTML!==fullS){window._sugsHTML=fullS;se.innerHTML=fullS;} // R23: repaint only on real change
}
function resetParlayPnL(){
  if(!confirm("Wipe the combo portfolio's realized P&L? This deletes settled combos (open ones stay)."))return;
  fetch("/api/parlay/reset",{method:"POST"}).then(function(r){return r.json();}).then(function(d){
    document.getElementById("parlayMsg").textContent=d.ok?("Reset — cleared "+(d.cleared||0)+" settled combos."):("Error: "+(d.error||"failed"));
    loadParlay();
  }).catch(function(){document.getElementById("parlayMsg").textContent="reset failed";});
}
function loadParlayBacktest(elId){
  var el=document.getElementById(elId||"parlayBacktest");if(!el)return;el.textContent="Running…";
  fetch("/api/parlay/backtest").then(function(r){return r.json();}).then(function(d){
    if(d&&d.building){el.textContent="Building snapshot… auto-refreshes.";setTimeout(function(){loadParlayBacktest(elId);},2500);return;}
    var pl=(d&&d.platforms)||{};var h='';
    // R7: REAL trials lead — the book's actually-placed parlays (no sampling of any kind).
    var real=(d&&d.real)||{};
    var anyReal=false;[['kalshi','🟩 Kalshi'],['polyus','🇺🇸 Poly US']].forEach(function(g){if((real[g[0]]||{}).rows)anyReal=true;});
    if(anyReal){
      h+='<div style="font-weight:700;margin:2px 0 2px">📓 Real placed combos <span class="muted" style="font-weight:400">· your settled book, artifact rows excluded — actual legs, actual joint outcomes, actual fees</span></div>';
      [['kalshi','🟩 Kalshi'],['polyus','🇺🇸 Poly US']].forEach(function(g){
        var v=real[g[0]];if(!v||!v.rows||!v.rows.length)return;
        h+='<div style="font-weight:700;margin:6px 0 2px;font-size:12.5px">'+g[1]+'</div>';
        h+='<table class="mkt" style="margin-bottom:6px"><thead><tr><th>Legs</th><th class="r">n</th><th class="r">Won</th><th class="r" title="net profit per combo (net ÷ n) — the EV metric; win% alone is misleading on longshots">EV/bet</th><th class="r">Net $</th><th class="r" title="ROI can go below −100% because FEES are paid on top of the stake — lose the stake AND the fees">ROI/$</th></tr></thead><tbody>';
        v.rows.forEach(function(rw){var rc=(rw.net||0)>=0?'var(--good)':'var(--bad)';var evb=(rw.n||0)>0?(rw.net||0)/rw.n:0;
          h+='<tr><td>'+rw.legs+'-leg</td><td class="r">'+rw.n+'</td><td class="r">'+rw.wins+'</td><td class="r" style="color:'+(evb>=0?'var(--good)':'var(--bad)')+'"><b>'+(evb>=0?'+':'')+'$'+evb.toFixed(2)+'</b></td><td class="r" style="color:'+rc+'">$'+(rw.net||0).toFixed(2)+'</td><td class="r" style="color:'+rc+'">'+Math.round((rw.roi||0)*100)+'%</td></tr>';});
        h+='</tbody></table>';
      });
      h+='<div style="font-weight:700;margin:10px 0 2px">🎲 Cohort simulation <span class="muted" style="font-weight:400">· real joint outcomes from co-open hours, sampled combinations</span></div>';
    }
    [['kalshi','🟩 Kalshi'],['polyus','🇺🇸 Poly US']].forEach(function(g){
      var v=pl[g[0]]||{};var rows=v.rows||[];var rowsB=v.rows_noolap||[]; // R67o: mode B side-by-side
      h+='<div style="font-weight:700;margin:8px 0 2px">'+g[1]+' <span class="muted" style="font-weight:400">· '+(v.pool||0)+' settled markets sampled · A = current sampler (legs may share an EVENT) · B = non-overlapping greedy (highest-EV legs, one per event)</span></div>';
      if(!rows.length){h+='<div class="muted" style="font-size:12px">Not enough resolved markets yet.</div>';return;}
      // R109 EV-FIRST (operator's law: "hit/wr/pwin is important but EV after all is the end all verdict") — ROI/$1 leads, win% demoted to context.
      h+='<table class="mkt"><thead><tr><th>Legs</th><th class="r" title="mode A — current: random cohort legs, dedupe by TICKER only. ROI/$1 = the EV verdict; win% is context">A ROI/$1</th><th class="r">A win%</th><th class="r" title="mode B (R67o) — greedy favorites-first, SKIPPING any leg whose event is already in the combo">B ROI/$1</th><th class="r">B win%</th><th class="r">Trials A/B</th></tr></thead><tbody>';
      rows.forEach(function(x,i){var b2=rowsB[i]||{};var roi=x.roi||0,roib=b2.roi||0;
        h+='<tr><td>'+x.legs+'-leg</td><td class="r" style="color:'+(roi>=0?'var(--good)':'var(--bad)')+'"><b>'+(roi>=0?'+':'')+Math.round(roi*100)+'%</b></td><td class="r muted">'+Math.round((x.win_rate||0)*100)+'%</td>'
          +'<td class="r" style="color:'+(roib>=0?'var(--good)':'var(--bad)')+'"><b>'+(roib>=0?'+':'')+Math.round(roib*100)+'%</b></td><td class="r muted">'+Math.round((b2.win_rate||0)*100)+'%</td>'
          +'<td class="r">'+(x.trials||0)+'/'+(b2.trials||0)+'</td></tr>';});
      h+='</tbody></table>';
    });
    // R72-A #4: N-LEG ML MODE — top-N tickets ranked by the model's ev_net at signal time
    // (settled ML-book candidates), OVERLAP vs NON-OVERLAP side-by-side, maker-blend fees.
    var mlv=(d&&d.ml)||{};
    if((mlv.rows_overlap||[]).length){
      h+='<div style="font-weight:700;margin:10px 0 2px">🤖 ML top-N combos <span class="muted" style="font-weight:400">· settled ML-book candidates ranked by ev_net at signal time · '+(mlv.pool||0)+' legs over '+(mlv.windows||0)+' co-open hours · hours without scored candidates are skipped</span></div>';
      // R109 EV-FIRST — ROI/$1 leads, win% demoted (EV is the verdict).
      h+='<table class="mkt"><thead><tr><th>Legs</th><th class="r" title="OVERLAP mode — the straight top-N by ev_net; legs may share an EVENT. ROI/$1 = the EV verdict; win% is context">OVLP ROI/$1</th><th class="r">OVLP win%</th><th class="r" title="NON-OVERLAP mode — walks the same ranking but skips any leg whose event is already on the ticket">N-OVLP ROI/$1</th><th class="r">N-OVLP win%</th><th class="r">n OV/NOV</th></tr></thead><tbody>';
      (mlv.rows_overlap||[]).forEach(function(x,i){var b2=(mlv.rows_noolap||[])[i]||{};var roi=x.roi||0,roib=b2.roi||0;
        h+='<tr><td>'+x.legs+'-leg</td><td class="r" style="color:'+(roi>=0?'var(--good)':'var(--bad)')+'"><b>'+(roi>=0?'+':'')+Math.round(roi*100)+'%</b></td><td class="r muted">'+Math.round((x.win_rate||0)*100)+'%</td>'
          +'<td class="r" style="color:'+(roib>=0?'var(--good)':'var(--bad)')+'"><b>'+(roib>=0?'+':'')+Math.round(roib*100)+'%</b></td><td class="r muted">'+Math.round((b2.win_rate||0)*100)+'%</td>'
          +'<td class="r">'+(x.trials||0)+'/'+(b2.trials||0)+'</td></tr>';});
      h+='</tbody></table>';
    }
	// R109 PARLAY LAB — the log-only research collector's live scoreboard (bucket × legs, EV/$1 first).
	fetch('/api/combolab').then(function(r){return r.json();}).then(function(v){
	  var en=v.enumeration||{},manifest=en.representation==='manifest';
	  var bridge=(v.live_route_proved_bridge||{}).status||{};
	  var hh=r142ComboSnapshotHTML(v)+'<details class="r141diag"><summary>Show class-level uncertainty, manifest coverage, and route proof</summary>';
      // R123 Part 2: the honest combinatorics of the exhaustive enumerator (floor is the only cut).
	  if(en.pool_n){
		if(manifest){
		  hh+='<div class="muted" style="font-size:12px;margin:4px 0">Manifest: '+en.pool_n+' executable legs represent '+escapeHtml(en.raw_space_dec||String(Math.round(en.raw_space||0)))+' logical 2–'+en.max_legs+'-leg subsets. '+(en.sample_inserted||0)+' new settlement rows this cycle · open sample cap '+(en.sample_open_cap||0)+' · '+Math.round(en.ms||0)+'ms.</div>';
		}else{
		  hh+='<div class="muted" style="font-size:12px;margin:4px 0">Legacy enumerator: '+en.pool_n+' picks · '+(en.enumerated||0)+' evaluated · '+(en.fee_screened||0)+' fee-screened · '+(en.logged||0)+' stored · '+Math.round(en.ms||0)+'ms.</div>';
		}
	  }
      // R123 Part 3: combo-class scoreboard — realized EV/$1 by linkage class with CIs + Δ7d.
      var cls=(v.classes||[]).filter(function(c){return (c.n||0)>=5;}).slice(0,14);
      if(cls.length){
        hh+='<div style="font-weight:700;margin:8px 0 2px">Class detail <span class="muted" style="font-weight:400">· post-R123 CI · ✓ = CI-positive n≥20 · research only</span></div>';
        hh+='<table class="mkt"><thead><tr><th>Class</th><th class="r">EV real/$1</th><th class="r">95% CI</th><th class="r">n</th><th class="r">win%</th><th class="r">Δ7d</th></tr></thead><tbody>';
        cls.forEach(function(x){var e=x['ev_real_per_$1']||0;var ci=x.ev_ci;var d7=x.delta_7d;
          hh+='<tr><td>'+(x.proven_pos?'✓ ':'')+x['class']+'</td><td class="r" style="color:'+(e>=0?'var(--good)':'var(--bad)')+'"><b>'+(e>=0?'+':'')+e.toFixed(3)+'</b></td>'
            +'<td class="r muted">'+(ci?('['+ci[0].toFixed(2)+','+ci[1].toFixed(2)+']'):'—')+'</td><td class="r">'+x.n+'</td>'
            +'<td class="r muted">'+Math.round((x.win_rate||0)*100)+'%</td>'
            +'<td class="r" style="color:'+((d7||0)>=0?'var(--good)':'var(--bad)')+'">'+(d7!=null?((d7>=0?'+':'')+d7.toFixed(3)):'—')+'</td></tr>';});
        hh+='</tbody></table>';
      }
	  hh+='<div class="muted" style="font-size:11px;margin-top:6px">Exact combo route: '+escapeHtml(bridge.state||'warming')+' · Paper RFQs '+Number(bridge.identical_paper_accepted||0)+'. Synthetic settlement rows are not placed profit.</div></details>';
      var dv=document.createElement('div');dv.innerHTML=hh;el.appendChild(dv);
    }).catch(function(){});
    el.innerHTML=h;
  }).catch(function(){el.textContent="Combo Lab grading failed.";});
}
function loadRisk(){
  fetch("/api/risk").then(function(r){return r.json();}).then(function(d){
    window._risk=d;
    function n(v){return Math.round(v||0).toLocaleString();}
    var exp=d.open_exposure_usd||0,pnl=d.today_realized_usd||0,stop=d.max_daily_loss_usd||0;
    var pc=pnl>=0?'var(--good)':'var(--bad)';
    var rb=document.getElementById("riskBox");
    if(rb){var rbH= // R60 C8: the daily stop is a click-to-edit chip (saves max_daily_loss_usd)
      '🛡️ Risk · exposure <b>$'+n(exp)+'</b>'+
      ' · today P&amp;L <b style="color:'+pc+'">$'+pnl.toFixed(2)+'</b>'+
      ' · <span class="muted">daily stop −'+echip('max_daily_loss_usd',stop,(stop>0?('$'+n(stop)):'off'),'max daily loss $ (0 = off) — click to edit')+'</span>'+
      ' <span class="muted">· more limits in ⚙ Settings</span>';
      if(rb._h!==rbH){rb._h=rbH;rb.innerHTML=rbH;}} // R70 (audit §a P2): repaint only on change
  }).catch(function(){});
}
var stopsCtx={platform:"",ticker:"",side:""};
function editStops(i){
  var p=(window.lastPos||[])[i];if(!p)return;
  stopsCtx={platform:p.platform,ticker:p.ticker,side:p.side};
  document.getElementById("stopsSub").textContent=(p.title||p.ticker)+" · "+p.side+" · avg "+Math.round((p.avg_price||0)*100)+"¢"+(p.cur_price?(" · now "+Math.round(p.cur_price*100)+"¢"):"");
  document.getElementById("stopsTP").value=p.tp?Math.round(p.tp*100):"";
  document.getElementById("stopsSL").value=p.sl?Math.round(p.sl*100):"";
  document.getElementById("stopsMsg").textContent="";
  document.getElementById("stopscard").style.display="block";
  document.getElementById("stopscard").scrollIntoView({behavior:"smooth",block:"nearest"});
}
function closeStops(){document.getElementById("stopscard").style.display="none";}
function saveStops(){
  var tp=(parseFloat(document.getElementById("stopsTP").value)||0)/100;
  var sl=(parseFloat(document.getElementById("stopsSL").value)||0)/100;
  fetch("/api/paper/stops",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({platform:stopsCtx.platform,ticker:stopsCtx.ticker,side:stopsCtx.side,tp:tp,sl:sl})}).then(function(r){return r.json();}).then(function(d){
    var m=document.getElementById("stopsMsg");
    if(d.error){m.innerHTML='<span style="color:var(--bad)">'+escapeHtml(d.error)+'</span>';return;}
    closeStops();loadPaper();
  }).catch(function(){document.getElementById("stopsMsg").textContent="error";});
}
// ================= R62 TAB SYSTEM (item 10) =================
// ALL destinations are top-level tabs on bar2 (exactly like PAPER|LIVE). PAPER/LIVE show the widget
// grid (their saved layouts); every other tab shows its card as THE main content region — a static
// panel filling shell row 3, never a fixed overlay, so nothing can sit under/over the bars (item 1).
// openX()/closeX() stay as tab-switch aliases because other code paths still call them.
var UIPANELS={overview:'overviewcard',systems:'systemscard',experiments:'experimentscard',evidence:'evidencecard',data:'datacard',operations:'operationscard',
  whales:'feedscard',flow:'flowcard',xvenue:'xvenuecard',research:'researchcard',ml:'mlcard',markets:'marketscard',stats:'statscard',settings:'settingscard',logs:'logscard',
  combos:'parlaycard',signals:'signalscard',orders:'orderscard',history:'historycard',edge:'edgecard',curves:'curvescard',replay:'replaycard'}; // R67e: bets left this map — it is a popup dialog again
var UITABS=['overview','systems','experiments','evidence','data','markets','paper','live','operations','settings'];
var UITAB='overview';
// R98 LAZY TABS (operator: "only ML/Live/Paper/Settings/Logs load at startup — every other tab
// loads on first click"). TABSEEN marks a tab as opened-at-least-once; the CORE tabs are pre-armed
// so their data flows from boot exactly as before. Every non-core loader (the whales/poly/polyus/
// polymarkets/arb feed pollers, the research/stats SSE repaints) no-ops until its tab is armed —
// after first open, polling resumes the existing design (feeds keep their tables warm). This kills
// the 19-tab boot request storm that starved the DB reads behind R97's briefing/famine timeouts.
// Server-side collection (feeds, signals, books) is untouched — this is browser fetch gating only.
var TABSEEN={overview:1,paper:1,live:1,ml:1,settings:1,logs:1};
function tabArmed(){for(var i=0;i<arguments.length;i++){if(TABSEEN[arguments[i]])return true;}return false;}
// research sub-tabs (edge/curves/replay) count as the research family for SSE repaint gating
function resArmed(){return tabArmed('research','edge','curves','replay');}
// R98: lightweight first-open loading state — painted into the tab's still-empty body targets the
// instant it is armed; the loader's first render replaces it (~1-2s). Empty-check keeps it from
// clobbering anything a widget/loader already drew.
var LZPH={whales:['whales','ksmart'],flow:['pwhales','poly'],xvenue:['both'],markets:['pmrows','pmusrows'],
  research:[],stats:['statsBacktest'],combos:['parlaySummary'],signals:[],history:['histSummary'],
  overview:['r138OverviewBody'],systems:['r138SystemsBody'],experiments:['r138ExperimentsBody'],
  evidence:['r138EvidenceBody'],data:['r138DataBody'],operations:['r138OperationsBody'],
  edge:['edgeBody'],curves:['curvesBody'],replay:['replayBody']};
function lzFirstOpen(t){
  (LZPH[t]||[]).forEach(function(id){
    var el=document.getElementById(id);
    if(el&&!el.textContent.trim()){
      var m='<span class="muted">Loading — first open of this tab…</span>';
      el.innerHTML=(el.tagName==='TBODY')?('<tr><td colspan="12" class="muted" style="padding:8px">Loading — first open of this tab…</td></tr>'):m;
    }
  });
}
function uiMarkTabs(){
  UITABS.forEach(function(k){
    var el=document.getElementById('tab_'+k);if(!el)return;
    el.className='mtab'+(k===UITAB?' on':'');
    el.setAttribute('aria-selected',k===UITAB?'true':'false'); // R70 (audit §a P1): screen readers track the active tab
  });
}
function uiTab(t){
  if(t==='paper'||t==='live'){setTab(t);return;} // the grid tabs route through setTab (layouts)
  if(!UIPANELS[t])return;
  if(t!=='research'&&typeof resReturnLoans==='function')resReturnLoans(); // R77 item 4: give the borrowed Stats table home (e.g. before the Stats tab shows)
  if(!TABSEEN[t]){TABSEEN[t]=1;lzFirstOpen(t);} // R98 lazy tabs: arm BEFORE the loader below so gated loaders pass
  UITAB=t;
  var g=document.getElementById('wgrid');if(g)g.style.display='none';
  var wrap=document.querySelector('.wrap');
  Object.keys(UIPANELS).forEach(function(k){
    var el=document.getElementById(UIPANELS[k]);if(!el)return;
    if(k===t){
      // R63 4e: the research sub-tabs REPARENT curves/edge/replay into #resBody — pull the panel
      // back to the shell + clear the overlay leftovers before showing it as a top-level tab.
      if(wrap&&el.parentNode!==wrap)wrap.appendChild(el);
      el.style.position='';el.style.transform='';el.style.width='';el.style.maxWidth='';el.style.maxHeight='';el.style.boxShadow='';el.style.inset='';
      el.style.display=(k==='whales'||k==='flow')?'flex':'block';
    }else{el.style.display='none';}
  });
  uiMarkTabs();
  try{
    ({overview:loadResearchOverview,
      systems:loadResearchSystemsPage,
      experiments:loadResearchExperimentsPage,
      evidence:loadResearchEvidencePage,
      data:loadResearchDataPage,
      operations:loadResearchOperationsPage,
      whales:function(){loadWhales();loadPoly();loadPolyUS();},
      flow:function(){loadWhales();loadPoly();loadPolyUS();},
      xvenue:function(){loadArb();renderBoth();},
      research:function(){showResearchSub(window._resSub||'performance');}, // R77 item 4: PERFORMANCE is the front page
      ml:loadML,
      markets:function(){loadGameTree();loadMarketTree();loadMarkets();loadPolyMarkets();loadPolyUS();}, // R102: the game tree loads on tab open (lazy per R98)
      stats:loadStatsView,
      settings:loadSettingsView,
      logs:function(){showLogSub(window._logSub||'live');},
      combos:loadParlay,
      signals:renderSignals,
      orders:loadOrders,
      history:loadHistoryView,
      edge:loadEdge,
      curves:loadCurves,
      replay:function(){loadReplay();loadParlayBacktest('replayParlay');}})[t]();
  }catch(e){}
  if(typeof redrawPnlCharts==='function')redrawPnlCharts(); // item 12: charts refit on tab switch
}
// R70 (audit §a P2): ~15 open/close tab aliases with ZERO callers deleted (openStats/closeStats,
// openMarkets/closeMarkets, openFeeds..closeXVenue, openEdge, openParlay, openLogs, openPaper/
// closePaper, openCurves, openReplay, openResearch/closeResearch, openHistory, openOrders,
// openSignals, openPolyUS, focusBlotter, resetSession, closeML, closeLive, showLiveTab, popLive).
// The close* aliases still wired to panel Close buttons are KEPT below.
function closeEdge(){setTab(WTAB);}
function loadEdge(){
  fetch("/api/edge").then(function(r){return r.json();}).then(function(d){
    if(d&&d.building){ // R4: cold snapshot is building in the background — show progress + re-poll
      document.getElementById("edgeBody").innerHTML='<span class="muted">Building the edge snapshot (first open after start — scanning the signal history)… auto-refreshes.</span>';
      setTimeout(loadEdge,2500);return;
    }
    var sigs=(d&&d.signals)||[];
    if(!sigs.length){document.getElementById("edgeBody").innerHTML='<span class="muted">No resolved signals logged yet — let it run a while, then check back.</span>';return;}
    var h='<div class="muted" style="font-size:11.5px;margin:2px 0 10px"><b>SIGNAL-LOG REPLAY · RESEARCH ONLY.</b> Entry and fill are assumed from logged prices. These buckets are not exchange profit evidence and cannot promote, size, or authorize LIVE.</div>';
    var rb=(d&&d.reliable_bins)||[], wb=(d&&d.worst_bins)||[];
    if(rb.length){
      h+='<div style="margin:2px 0 6px"><b>Best modeled replay bins</b> <span class="muted">· ranked by a 95% lower bound on signal-log modeled EV/contract (n&gt;='+(d.reliable_min_n||25)+') · discovery diagnostic only</span></div>';
      h+='<table style="width:100%;border-collapse:collapse;margin-bottom:12px"><thead><tr style="color:var(--muted);font-size:11.5px;text-align:left"><th style="padding:3px 6px">Strategy</th><th>Feature</th><th>Bin</th><th class="r">n</th><th class="r">WR</th><th class="r" title="win-rate floor inside the replay cohort">Consist.</th><th class="r">Modeled EV/ct</th><th class="r" title="95% lower bound for this assumed-fill replay; not exchange proof">Replay floor</th></tr></thead><tbody>';
      rb.forEach(function(b){h+='<tr style="border-top:1px solid var(--line)"><td style="padding:3px 6px"><b>'+escapeHtml(srcLbl(b.signal))+'</b></td><td class="muted">'+escapeHtml(b.feature)+'</td><td>'+escapeHtml(b.label)+'</td><td class="r">'+b.n+'</td><td class="r">'+Math.round((b.wr||0)*100)+'%</td><td class="r">'+Math.round((b.consistency||0)*100)+'%</td><td class="r" style="color:'+((b.ev||0)>=0?'var(--good)':'var(--bad)')+'">'+((b.ev||0)>=0?'+':'')+((b.ev||0)*100).toFixed(1)+'¢</td><td class="r" style="color:'+((b.ev_low||0)>=0?'var(--good)':'var(--bad)')+'"><b>'+((b.ev_low||0)>=0?'+':'')+((b.ev_low||0)*100).toFixed(1)+'¢</b></td></tr>';});
      h+='</tbody></table>';
    }
    if(wb.length){
      h+='<div style="margin:2px 0 6px"><b>Worst modeled replay bins</b> <span class="muted">· most-negative replay floor · useful for research, not an automatic cash gate</span></div>';
      h+='<table style="width:100%;border-collapse:collapse;margin-bottom:14px"><thead><tr style="color:var(--muted);font-size:11.5px;text-align:left"><th style="padding:3px 6px">Strategy</th><th>Feature</th><th>Bin</th><th class="r">n</th><th class="r">WR</th><th class="r">Modeled EV/ct</th><th class="r">Replay floor</th></tr></thead><tbody>';
      wb.forEach(function(b){h+='<tr style="border-top:1px solid var(--line)"><td style="padding:3px 6px"><b>'+escapeHtml(srcLbl(b.signal))+'</b></td><td class="muted">'+escapeHtml(b.feature)+'</td><td>'+escapeHtml(b.label)+'</td><td class="r">'+b.n+'</td><td class="r">'+Math.round((b.wr||0)*100)+'%</td><td class="r" style="color:'+((b.ev||0)>=0?'var(--good)':'var(--bad)')+'">'+((b.ev||0)>=0?'+':'')+((b.ev||0)*100).toFixed(1)+'¢</td><td class="r" style="color:var(--bad)"><b>'+((b.ev_low||0)*100).toFixed(1)+'¢</b></td></tr>';});
      h+='</tbody></table>';
    }
    sigs.forEach(function(s){
      h+='<div style="margin:16px 0 4px;border-top:1px solid var(--line);padding-top:10px"><b>'+escapeHtml(srcLbl(s.signal_type))+'</b> <span class="muted">· '+escapeHtml(s.signal_type)+' · <b style="color:'+((s.ev||0)>=0?'var(--good)':'var(--bad)')+'">'+((s.ev||0)>=0?'+':'')+((s.ev||0)*100).toFixed(1)+'¢</b> modeled signal-log EV/ct · n='+s.n+' resolved</span></div>';
      if(s.clv_n){h+='<div style="margin:-2px 0 7px;font-size:12px" class="muted">logged-price closing-line diagnostic <b style="color:'+((s.clv||0)>=0?'var(--good)':'var(--bad)')+'">'+((s.clv||0)>=0?'+':'')+((s.clv||0)*100).toFixed(1)+'¢</b>/ct over '+s.clv_n+' rows <span class="muted">· positive is a research clue, not proof of an exchange fill or profit</span></div>';}
      (s.features||[]).forEach(function(f){
        // R23 (operator: "tf up with all these tickers?"): the per-series feature grows a chip for
        // EVERY series ever signaled — hundreds of n1 chips are noise, not evidence. Hide bins with
        // n<5 on the dynamic-label features (series/category/market type); fixed-bin features keep
        // everything. Hidden count shown so nothing silently vanishes.
        var bks=(f.buckets||[]);var hidden=0;
        // R73 n-MISMATCH TOOLTIP (operator: "why does wxedge show n=X here but fewer in the bins?"):
        // Edge bins need FEATURE-COMPLETE rows — rows logged before a feature existed (or where the
        // source can't supply it) carry no value and are unbinned, so Σ(bin n) < the signal's n.
        var binned=0;(f.buckets||[]).forEach(function(b){binned+=(b.n||0);});
        var missFC=Math.max(0,(s.n||0)-binned);
        if(!f.labels||!f.labels.length){var kept=[];bks.forEach(function(b){if((b.n||0)>=5)kept.push(b);else hidden++;});bks=kept;}
        h+='<div style="display:flex;gap:6px;flex-wrap:wrap;align-items:center;margin:3px 0 7px"><span class="muted" style="min-width:108px;font-size:12px"'+(missFC>0?' title="'+escapeHtml('Edge bins need feature-complete rows; '+missFC+' older rows predate this feature (logged before it existed / source lacks it) and are unbinned — that is the n-mismatch vs the signal header')+'"':'')+'>'+escapeHtml(f.name)+(missFC>0?' <span style="cursor:help">('+missFC+' unbinned)</span>':'')+(hidden?' <span title="bins with fewer than 5 resolved signals are hidden (noise)">(+'+hidden+' tiny)</span>':'')+'</span>';
        bks.forEach(function(b){
          // WR chip removed (operator directive): the bin leads with NET EV/contract + its 95% lower
          // bound; ✓ marks bins that also survived the untouched newest-30% holdout (audit §7).
          var evl=(b.ev_low!=null)?((b.ev_low>=0?'+':'')+(b.ev_low*100).toFixed(1)+'¢ LB'):'';
          var hold=b.hold_ok?' <b style="color:var(--good)" title="edge survived the newest-30% holdout">✓</b>':'';
          h+='<span title="'+b.wins+' of '+b.n+' won (tracked, not shown) · net EV/ct '+((b.ev||0)*100).toFixed(2)+'¢ · holdout '+((b.hold_ev||0)*100).toFixed(1)+'¢ (n'+(b.hold_n||0)+')" style="border:1px solid var(--line);border-radius:8px;padding:2px 7px;font-size:12px">'+escapeHtml(b.label)+' <b style="color:'+((b.ev||0)>=0?'var(--good)':'var(--bad)')+'">'+((b.ev||0)>=0?'+':'')+((b.ev||0)*100).toFixed(1)+'¢</b> <span class="muted">'+evl+' · n'+b.n+'</span>'+hold+'</span>';
        });
        h+='</div>';
      });
    });
    document.getElementById("edgeBody").innerHTML=h;
  }).catch(function(){document.getElementById("edgeBody").textContent="Could not load edge report.";});
}
function openML(){uiTab('ml');}
// R25 (operator): LIVE defaults to MAKER on both venues — zero/negative fees, and the strict 2¢
// exit sweep jumps out the moment price leaves the scored level, so rests can't go stale. Paper
// and every backtest still price TAKER (fee_maker_share=0) so testing stays conservative.
// Toggle per venue any time; Kalshi maker fills are historically adversely selected (14.6% fill,
// −4.3¢) — the sweep is the counterweight.
if(window._liveTakerK==null)window._liveTakerK=false;
if(window._liveTakerP==null)window._liveTakerP=false;
function toggleTakerK(){window._liveTakerK=!window._liveTakerK;if(window._liveData)renderLive(window._liveData);}
function toggleTakerP(){window._liveTakerP=!window._liveTakerP;if(window._liveData)renderLive(window._liveData);}
// R70 (audit §a P2): the dead saveLiveLimits (+ its never-rendered #liveUnitIn/#liveCapIn inputs)
// is GONE. R105: the $/bet knob is GONE TOO (operator, repeated ask) — live per-bet size comes from
// the SAME engine as paper (per-venue wallets); only the EXPOSURE CAP chip remains editable via
// editTunable → POST /api/settings live_exposure_cap_usd (round-trips through GET /api/settings).
// R57: the blotter (livecard) is a PERMANENT region — openLive (re)starts its polls + paints now.
function openLive(){loadLive();loadCombos();
  if(window._liveTimer)clearInterval(window._liveTimer);window._liveTimer=setInterval(loadLive,1000); // R13: 1s — live means live
  if(window._comboTimer)clearInterval(window._comboTimer);window._comboTimer=setInterval(loadCombos,10000);} // combos refresh every 10s (R17: instant)
// LIVE writes remain behind the explicit ARM handshake. Adaptive Allocation Model bankroll-percentage rails + the
// kill switch are the guardrails; there is no fixed $2/order or $10-total choke.
function armLive(){ fetch("/api/live/arm",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({confirm:true})}).then(function(r){return r.json();}).then(function(){loadLive();}); }
function toggleLiveAuto(on){ // R41: AUTO LIVE — requires an armed session; confirm before turning ON
  if(on&&!confirm("AUTO LIVE is cash-closed in this build: no Paper, model, or assumed-fill result can authorize an order. This only records the runtime switch; ARM and a future operator-approved authenticated LIVE cohort would still be required. Turn ON?"))return;
  fetch("/api/live/auto",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({on:on?1:0})}).then(function(r){return r.json();}).then(function(){loadLive();}).catch(function(){});
}
function paintLiveAutoQuick(d){
  var b=document.getElementById('modeLiveAuto');if(!b)return;
  d=d||{};var paused=!!(d.auto&&d.live_auto_go&&d.live_auto_go.state==='PAUSED');b.className='modebtn'+(d.auto?' on live':'');
  if(paused){b.style.borderColor='var(--warn)';b.style.color='var(--warn)';}else{b.style.borderColor='';b.style.color='';}
  b.textContent=paused?'LIVE AUTO PAUSED':(d.auto?'LIVE AUTO ON':'LIVE AUTO');
  b.title=paused?'REAL-MONEY Auto remains ON but new orders are temporarily paused. Click to turn it OFF; it resumes automatically when runtime checks recover.'
    :(d.auto?'REAL-MONEY Auto is ON. Click to stop. ARM and Auto both reset OFF on restart.'
    :(d.armed?'ARMED and ready: click to enable the same confidence-gated Kalshi/PolyUS decisions as the paper venue books, plus conservative Kalshi combos.'
              :'Arm live orders in the Live panel first. This control cannot arm or place by itself.'));
}
function toggleLiveAutoQuick(){
  var d=window._liveData||{};
  if(d.auto){toggleLiveAuto(0);return;}
  if(!d.armed){try{alert('Arm live orders in the Live panel first. LIVE AUTO cannot arm the session.');}catch(_e){}return;}
  toggleLiveAuto(1);
}
function placeLive(p){ // routes by venue with PER-VENUE maker/taker (R23); exact dollars preserve sub-cent ticks
  var pd=(p.price_dollars>0?p.price_dollars:(p.price_cents||0)/100);
  if((p.platform||"kalshi")==="polyus"){ return fetch("/api/live/polyus/place",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({slug:p.ticker,outcome:(p.side||"").toUpperCase(),price_dollars:pd,count:p.count,post_only:!window._liveTakerP})}).then(function(r){return r.json();}); }
  return fetch("/api/live/place",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({ticker:p.ticker,side:p.side,price_dollars:pd,count:p.count,taker:!!window._liveTakerK})}).then(function(r){return r.json();}); }
function acceptLive(i){ var p=(window._liveProps||[])[i]; if(!p)return; if(window._liveData){window._liveData.proposals=(window._liveData.proposals||[]).filter(function(x){return x.ticker!==p.ticker;});renderLive(window._liveData);} placeLive(p).then(function(){loadLive();}).catch(function(){loadLive();}); }
function confirmAllLive(){ var ps=(window._liveProps||[]).slice(); if(!ps.length)return; if(window._liveData){window._liveData.proposals=[];renderLive(window._liveData);} (function nx(i){ if(i>=ps.length){loadLive();return;} placeLive(ps[i]).then(function(){nx(i+1);}).catch(function(){nx(i+1);}); })(0); }
function cancelLive(id){ if(window._liveData){window._liveData.resting_orders=(window._liveData.resting_orders||[]).filter(function(o){return (o.order_id||"")!==id;});renderLive(window._liveData);} fetch("/api/live/cancel",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({order_id:id})}).then(function(r){return r.json();}).then(function(){loadLive();}).catch(function(){loadLive();}); }
function cancelAllLive(){ var os=(window._liveResting||[]).slice(); if(!os.length)return; if(window._liveData){window._liveData.resting_orders=[];renderLive(window._liveData);} (function nx(i){ if(i>=os.length){loadLive();return;} fetch("/api/live/cancel",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({order_id:os[i].order_id})}).then(function(){nx(i+1);}).catch(function(){nx(i+1);}); })(0); }
// R77 audit: BOOK_WS health chip (bar1) — /api/live json.book_ws = {subs,fresh,gaps,err}. RED when
// fresh==0 (that exact state was a 62-minute invisible starvation incident, twice in 24h); amber on
// a non-empty err or a big seq-gap count; green otherwise. Tooltip carries gaps + err. Old servers
// without the field render no chip at all.
function bookWSChip(bw){
  var el=document.getElementById("bookws");if(!el)return;
  if(!bw||(bw.subs==null&&bw.fresh==null)){el.style.display="none";return;}
  var subs=bw.subs||0,fresh=bw.fresh||0,gaps=bw.gaps||0,err=bw.err||"";
  var col=(fresh<=0)?"var(--bad)":((err||gaps>25)?"var(--warn)":"var(--good)");
  var hh='books <b style="color:'+col+'">'+fresh+'/'+subs+'</b>';
  el.style.display="";
  if(el._h!==hh){el._h=hh;el.innerHTML=hh;}
  el.title="orderbook delta WS — "+fresh+" of "+subs+" subscribed books provable/fresh · seq gaps "+gaps+(err?(" · last err: "+err):"")+(fresh<=0?" — NO PROVABLE BOOKS (feed starved)":"");
}
// R77 item 7: MARKETS coverage chip (bar1, books-chip pattern) — "mkts LIVE/TRACKED" where live =
// the venues' current live-cache sizes and tracked = market_catalog rows, both CACHED server-side
// by the 5-min catalog sweep (/api/live "mkts" field — zero scans on this poll path). Tooltip
// carries the per-venue breakdown. Old servers without the field render no chip at all.
function mktsChip(mc){
  var el=document.getElementById("mktschip");if(!el)return;
  if(!mc||(mc.live==null&&mc.tracked==null)){el.style.display="none";return;}
  function k(n){n=n||0;return n>=10000?(Math.round(n/1000)+'k'):(n>=1000?((n/1000).toFixed(1)+'k'):String(n));}
  var hh='mkts <b>'+k(mc.live)+'</b>/<span class="muted">'+k(mc.tracked)+'</span>';
  el.style.display="";
  if(el._h!==hh){el._h=hh;el.innerHTML=hh;}
  var vs=mc.venues||{};var lines=['markets coverage — LIVE (in the venue caches right now) / TRACKED (market_catalog, 90d retention)'];
  [['kalshi','Kalshi'],['polyus','Poly US'],['polymarket','Poly-int']].forEach(function(v){var e=vs[v[0]]||{};lines.push(v[1]+': live '+(e.live||0)+' / tracked '+(e.tracked||0));});
  el.title=lines.join('\n');
}
function loadLive(){
  if(document.hidden)return; // R77 audit: no polling while backgrounded — visibilitychange fires one immediate load on return
  // R70 (audit §a P3): jget coalesces the triple-sourced /api/live (1s poll + 7s strip + SSE push)
  // onto one in-flight request instead of stacking duplicates.
  jget("/api/live").then(function(d){ window._lastDataTs=Date.now(); window._liveData=d; paintLiveAutoQuick(d); bookWSChip(d&&d.book_ws); mktsChip(d&&d.mkts); renderLive(d); }).catch(function(){var e=document.getElementById("liveHead");if(e)e.textContent="Could not load live data.";});
}
// R13: cancel one working PolyUS order from the dashboard.
function cancelPolyUSLive(id,slug){
  if(!id)return;
  fetch("/api/live/polyus/cancel",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({order_id:id,slug:slug})})
    .then(function(r){return r.json();}).then(function(){loadLive();}).catch(function(){loadLive();});
}
// R14: cancel EVERY open PolyUS order.
function cancelAllPolyUSLive(){
  fetch("/api/live/polyus/cancel-all",{method:"POST",headers:{"Content-Type":"application/json"},body:"{}"})
    .then(function(r){return r.json();}).then(function(){loadLive();}).catch(function(){loadLive();});
}
// R8: one guarded real-money PolyUS order (arm + caps + dup-fuse enforced server-side).
// (manual PolyUS form removed post-validation; /api/live/polyus/place remains for automation)
// prettySlug: readable fallback for a PolyUS slug when no live title source covers it (old markets
// age out of the gateway snapshot). "aec-mlb-chc-mil-2026-06-26" -> "MLB CHC–MIL 6/26";
// "tsc-fwc-jor-alg-2026-06-22-2pt5" -> "FWC JOR–ALG 6/22 · 2.5".
function prettySlug(s){
  var m=String(s||"").match(/^[a-z]+-([a-z0-9]+)-([a-z0-9]+)-([a-z0-9]+)-(\d{4})-(\d{2})-(\d{2})(?:-(.+))?$/i);
  if(!m)return s||"";
  var suf=m[7]?(' · '+m[7].replace(/pt/,'.').replace(/^pos-/,'+')):'';
  return m[1].toUpperCase()+' '+m[2].toUpperCase()+'–'+m[3].toUpperCase()+' '+parseInt(m[5],10)+'/'+parseInt(m[6],10)+suf;
}
function loadCombos(){
  if(document.hidden)return; // R77 audit: visibility-gated like loadLive — refired once on return-to-visible
  var el=document.getElementById("liveCombos");
  if(el&&!el.innerHTML)el.innerHTML='<span class="muted">Finding combos…</span>'; // R23: only on FIRST load — refreshes keep last content until new data lands (no more blink-to-blank)
  fetch("/api/live/combos").then(function(r){return r.json();}).then(function(d){
    function mny(v){v=v||0;return '$'+v.toFixed(2);}
    // R6: rank by ML EV/contract (p_win − price); R23: stable tiebreak on the legs signature so
    // rows stop swapping places when EVs wiggle by a tenth of a cent between sweeps.
    function legsKey(c){return ((c.legs||[]).map(function(l){return l.ticker+l.side;}).sort().join("|"));}
    window._liveCombos=((d&&d.proposals)||[]).slice().sort(function(a,b){
      var d1=((b.p_win||0)-(b.price||0))-((a.p_win||0)-(a.price||0));
      if(Math.abs(d1)>0.004)return d1;
      return legsKey(a)<legsKey(b)?-1:1;});
    renderMlPicks(); // R60: the combo picks half of the ml-picks widget rides this cache
    // R18 probe chip: venue-wide RFQ pulse (everyone's rfq_created broadcasts) + maker-online sample —
    // a no-quote result is now self-diagnosing: venue quiet vs our size.
    var pu=(d&&d.pulse)||{};var pchip='';
    if(pu.rfqs_1h!=null){ // R63 5: "typical ask $10 (ours $1)" fragment removed
      pchip='<div class="muted" style="font-size:11.5px;margin:2px 0 6px">Venue pulse: '+(pu.mve_rfqs_1h||0)+' combo RFQs/hr'
        +(pu.quoter_events!=null?(' · makers active on <b>'+pu.quoter_events+'</b> events ('+(pu.quoter_sampled_min_ago||0)+'m ago)'):'')
        +'</div>';
    }
    pchip+=dropsChip(d&&d.combo_drops,null,'combos'); // R63 5: collapsed chip; the RFQ-combos-are-Kalshi-only reason suppressed
    if(!window._liveCombos.length){var z=pchip+'<span class="muted" title="Crypto legs roll every 15 minutes — combos vanish at window expiry (:00/:15/:30/:45) and regenerate ~1 min into the next window. Auto-refreshes every 10s (or hit ↻).">No live combos this moment.</span>';if(el&&window._combosHTML!==z){window._combosHTML=z;el.innerHTML=z;}return;}
    var h='<table class="mkt"><thead><tr><th>Legs</th><th class="r">#</th><th class="r">Price</th><th class="r">Payout×</th><th class="r">p_win</th><th class="r" title="ML EV per contract = p_win − price — GROSS, before venue fees">ML EV/ct</th><th class="r" title="fee-adjusted expected return per $1 staked. Lower than EV/ct÷price because Kalshi\'s 7%·p·(1−p) fee (rounded UP per contract) eats ~1.5–2¢ of the raw edge — e.g. +3.9¢ gross on 35¢ ≈ +11% gross but ~5% net">Net ROI</th><th></th></tr></thead><tbody>';
    window._liveCombos.forEach(function(c,i){
      var names=(c.legs||[]).map(function(l){return '<a class="go" href="'+escapeHtml(posURL({platform:'kalshi',ticker:l.ticker}))+'" target="_blank" rel="noopener">'+escapeHtml((l.outcome||l.title||l.ticker||"").slice(0,26))+'</a>';}).join(" + ");
      var warn=c.quoters?"":' <span title="'+(c.quoter_n===0?'0 active market makers on these events RIGHT NOW (venue-side, not us) — an RFQ will very likely get no quotes; makers come online near game time':'no active market makers on these legs — an RFQ likely won'+"'"+'t get quoted')+'" style="color:var(--bad)">⚠ no makers</span>';
      var evc=(c.p_win||0)-(c.price||0);
      if((c.p_win||0)>2*(c.price||1)){warn+=' <span title="MODEL-OPTIMISTIC: the ML thinks this is worth >2× its market price — its absolute EV is least trustworthy exactly here. The accept-time fair-value ceiling still refuses overpriced quotes, so the risk is a no-fill, not an overpay." style="color:var(--warn)">◎</span>';}
      var pxc=(c.price||0)*100;
      h+='<tr><td>'+names+warn+'</td><td class="r">'+c.n_legs+'</td><td class="r">'+(pxc<10?pxc.toFixed(1):pxc.toFixed(0))+'¢</td><td class="r">'+((c.payout_mult||0)).toFixed(1)+'×</td><td class="r">'+((c.p_win||0)).toFixed(2)+'</td><td class="r" style="color:'+(evc>=0?'var(--good)':'var(--bad)')+'"><b>'+(evc>=0?'+':'')+(evc*100).toFixed(1)+'¢</b></td><td class="r">'+Math.round((c.roi_net||0)*100)+'%</td><td class="r"><button class="mini" onclick="placeCombo('+i+')"'+(d.armed?'':' disabled')+'>Accept $1</button></td></tr>';
    });
    h+='</tbody></table>';
    var full=pchip+h;
    if(el&&window._combosHTML!==full){window._combosHTML=full;el.innerHTML=full;} // R23: repaint ONLY on real change — kills the every-10s flicker
  }).catch(function(){if(el&&!el.innerHTML)el.innerHTML='<span class="muted">Could not load combos.</span>';});
}
function placeCombo(i){
  var c=(window._liveCombos||[])[i]; if(!c)return;
  var el=document.getElementById("liveCombos"); var prev=el.innerHTML; el.innerHTML='<span class="muted">Opening RFQ, waiting for a maker quote (up to ~12s)…</span>';
  fetch("/api/live/combo/place",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({collection_ticker:c.collection_ticker,legs:c.legs})})
    .then(function(r){return r.json();}).then(function(res){
      if(res&&res.ok){el.innerHTML='<span style="color:var(--good)">Accepted @ '+escapeHtml(res.quote_price||"?")+' YES — awaiting maker confirm/fill. See the order log + positions.</span>';}
      else{el.innerHTML='<span style="color:var(--bad)">'+escapeHtml((res&&res.error)||"failed")+'</span>';}
      loadLive(); setTimeout(loadCombos,1500);
    }).catch(function(){el.innerHTML=prev;});
}
// R70 (audit §a P1): _renderLiveLegacy (~97 lines, self-documented "never called", targeting ids
// that no longer exist) is DELETED, along with its toggleTaker/_liveTaker remnants and the
// popLive window helper (zero callers since the R60 widget system).
// R19/R60: WINDOW SYSTEM — ?win=<name> loads this same dashboard with ONE widget filling the
// window (all render/action paths shared). Old pane names map onto widget ids.
function initWinMode(){
  var p=new URLSearchParams(location.search).get('win'); if(!p)return;
  var map={props:'proposed',combos:'combos',pos:'live-positions',orders:'resting',hist:'history',log:'log'};
  var id=map[p]||(WREG[p]?p:'proposed');
  document.title='Live · '+p;
  document.body.classList.add('win'); // CSS zeroes the bar rows; the grid fills the window
  window._winMode=true;               // throwaway layout — wSave refuses to persist it
  WTAB='live';UITAB='live';
  WLAY.paper={};WLAY.live={};
  WLAY.live[id]=[1,1,12,7];
  wEnsure(id);renderWidgets();
  openLive();
}
function copyLiveLog(){
  var lg=((window._liveData&&window._liveData.log)||[]).slice().reverse();
  var txt=lg.map(function(e){return (e.ts||'')+' '+(e.event||'')+' '+(e.ticker||'')+(e.side?(' '+e.side):'')+(e.price_cents?(' @'+e.price_cents+'c'):'')+(e.status?(' ->'+e.status):'')+(e.error?(' ERR: '+e.error):'')+(e.reason?(' ('+e.reason+')'):'');}).join("\n");
  navigator.clipboard.writeText(txt).then(function(){var b=document.getElementById("copyLogBtn");if(b){b.textContent="Copied ✓";setTimeout(function(){b.textContent="Copy";},1500);}});
}
// R63 5 MEGA DROPPED LINE → CHIP: one compact "Dropped ×<total> ⓘ" chip. HOVER = the FULL
// per-reason breakdown (chip tooltip, one reason per line); CLICK toggles a compact 3-line
// top-reasons list (state remembered in-session via window._dropsOpen). The pure-noise reason
// "not a Kalshi suggestion (RFQ combos are Kalshi-only)" is suppressed entirely.
function dropsChip(map,samples,key){
  var ks=Object.keys(map||{}).filter(function(k){return k!=='not a Kalshi suggestion (RFQ combos are Kalshi-only)';});
  var tot=0;ks.forEach(function(k){tot+=map[k]||0;});
  if(!tot)return '';
  ks.sort(function(a,b){return (map[b]||0)-(map[a]||0);});
  window._dropsOpen=window._dropsOpen||{};
  var open=!!window._dropsOpen[key];
  var full='candidates rejected this pass — click for the top reasons\n'+ks.map(function(k){return '×'+map[k]+' '+k;}).join('\n');
  var samp=(samples||[]).slice(0,6).map(function(s){return (s.venue||'')+' '+(s.ticker||'')+' — '+(s.reason||'');}).join('\n');
  if(samp)full+='\n\nsamples:\n'+samp;
  var h='<div class="muted" style="font-size:11.5px;margin:3px 0"><span class="echip" onclick="toggleDrops(\''+key+'\')" title="'+escapeHtml(full)+'">Dropped ×'+tot+' ⓘ</span>';
  if(open){
    ks.slice(0,3).forEach(function(k){h+='<div style="padding-left:12px">×'+map[k]+' '+escapeHtml(k)+'</div>';});
    if(ks.length>3)h+='<div style="padding-left:12px;opacity:.7">… +'+(ks.length-3)+' more reasons — hover the chip for all</div>';
  }
  return h+'</div>';
}
function toggleDrops(k){
  window._dropsOpen=window._dropsOpen||{};window._dropsOpen[k]=!window._dropsOpen[k];
  window._livePropsHTML=null;window._combosHTML=null; // bust the change-detection caches so the toggle repaints
  if(window._liveData)renderLive(window._liveData);
  loadCombos();
}
// R77 BANDED AUTO-INVERT marker: proposal / ML-scored rows MAY carry inverted_band=true (the banded
// auto-invert evaluation says this family+price-band measures CI-negative and would flip). Renders
// NOTHING when the field is absent — fully backward compatible with servers that never send it.
function invBandBadge(r){return (r&&r.inverted_band)?' <span title="banded auto-invert: this family+band measures CI-negative; inverted EV positive" style="color:var(--warn);font-weight:700;cursor:help">↔B</span>':'';}
function renderLiveAutoGo(g){
  var el=document.getElementById('liveGoW');if(!el)return;g=g||{};
  var ok=!!g.safe_to_arm,running=g.state==='RUNNING',paused=g.state==='PAUSED',blockers=g.blockers||[],warnings=g.warnings||[];
  var color=paused?'var(--warn)':(ok?'var(--good)':'var(--bad)'),icon=running?'🟢':(paused?'⏸':(ok?'✅':'⛔'));
  var bg=paused?'rgba(245,158,11,.09)':(ok?'rgba(47,230,168,.08)':'rgba(234,57,67,.09)');
  var h='<div style="border:1px solid '+color+';border-left:5px solid '+color+';background:'+bg+';padding:8px 10px;border-radius:6px;min-height:58px">'+
    '<div style="font-size:18px;font-weight:800;color:'+color+'">'+icon+' '+escapeHtml(g.headline||'LIVE AUTO CHECKING…')+'</div>';
  if(blockers.length){h+='<div style="margin-top:5px">'+blockers.slice(0,3).map(function(x){return '• '+escapeHtml(x);}).join('<br>')+(blockers.length>3?'<br><span class="muted">+'+(blockers.length-3)+' more blocker(s); hover for all</span>':'')+'</div>';}
  else{h+='<div style="margin-top:4px">'+escapeHtml(g.opportunity_explanation||'Safe to arm; waiting for a qualifying signal.')+'</div>';}
  if(warnings.length){h+='<div style="margin-top:4px;color:var(--warn)">⚠ '+escapeHtml(warnings[0])+'</div>';}
  var authority=(g.authority||[]).join(' · '),all=blockers.concat(warnings).join('\n');
  h+='<div class="muted" style="margin-top:4px">ARM '+(g.armed?'ON':'OFF')+' · AUTO '+(g.auto?'ON':'OFF')+(authority?' · allowed: '+escapeHtml(authority):'')+' · updated '+escapeHtml(tFmt(g.generated_at||''))+'</div></div>';
  el.title=all||'All pre-arm runtime checks currently pass. A trade still needs a fresh qualifying signal and final book/risk checks.';
  if(el._h!==h){el._h=h;el.innerHTML=h;}
}
function renderLive(d){
  (function(){
    // Repaint guard: never repaint while the user is focused on an input inside the widget grid.
    var lc=document.getElementById("wgrid");
    var ae=document.activeElement;
    if(lc&&ae&&(ae.tagName==="INPUT"||ae.tagName==="SELECT")&&lc.contains(ae))return;
    function mny(v){v=v||0;return (v<0?'-$':'$')+Math.abs(v).toFixed(2);}
    function pnlSpan(v,has){if(!has)return '<span class="muted">—</span>';return '<span style="color:'+((v||0)>=0?'var(--good)':'var(--bad)')+';font-weight:700">'+((v||0)>=0?'+':'')+mny(v)+'</span>';}
    renderLiveAutoGo(d&&d.live_auto_go);
    window._liveProps=(d&&d.proposals)||[];
    var pus0=d.polyus||{};
    // ---- STATUS STRIP (R59: WRAPPING chip row inside #bstrip, next to the pipeline funnel —
    // balances/exposure/room/P&L first, controls last so the controls wrap to row 2 on narrow
    // widths; each chip keeps nowrap so it wraps BETWEEN chips, never mid-chip) ----
    var h='<div style="display:flex;gap:2px 10px;flex-wrap:wrap;align-items:center;font-size:10.5px;white-space:nowrap;min-width:0">';
    h+='<span>🟩 Kalshi <b>'+mny(d.balance)+'</b></span>';
    if(pus0.enabled&&pus0.balance!=null){h+='<span title="PolyUS balance · the second number is buying power">🇺🇸 PolyUS <b>'+mny(pus0.balance)+'</b> <span class="muted" title="buying power">'+mny(pus0.buying_power)+'</span></span>';} // R60 C4: "bp" label dropped — chip reads PolyUS $x
    if(pus0.enabled){h+=(pus0.push?'<span style="color:var(--good);font-size:11.5px" title="private WebSocket connected — orders/fills/positions arrive as pushes">● push</span>':'<span style="color:var(--warn);font-size:11.5px" title="private WebSocket down — REST polling">● poll</span>');}
    h+='<span title="COMBINED across both venues — Kalshi $'+((d.exposure_kalshi||0)).toFixed(2)+' + PolyUS $'+((d.exposure_polyus||0)).toFixed(2)+'">Exposure <b>'+mny(d.exposure)+'</b> / <b>'+mny(d.cap)+'</b> <span class="muted">(🟩 '+mny(d.exposure_kalshi)+' · 🇺🇸 '+mny(d.exposure_polyus)+')</span></span>';
    h+='<span>Room <b>'+mny(d.remaining)+'</b></span>';
    // R103 PER-VENUE LIVE WALLETS — money cannot move between venues: each venue shows its own
    // bankroll (source-tagged), deployed and available; caps enforce per venue AND combined.
    if(d.venues){var vk=d.venues.kalshi||{},vp=d.venues.polyus||{};
      h+='<span title="Kalshi live wallet ('+(vk.bankroll_src||'?')+') — bankroll / deployed / available · venue cap $'+((vk.cap||0)).toFixed(2)+', headroom $'+((vk.cap_remaining||0)).toFixed(2)+', gross turnover today $'+((vk.day_turnover||0)).toFixed(2)+' (resets at UTC midnight)'+(vk.arm_bankroll!=null?(' · armed at $'+(vk.arm_bankroll||0).toFixed(2)):'')+'">🟩 bank <b>'+mny(vk.bankroll)+'</b> <span class="muted">dep '+mny(vk.deployed)+' · avail '+mny(vk.available)+'</span></span>';
      h+='<span title="PolyUS live wallet ('+(vp.bankroll_src||'?')+'; no REST balance API on this venue — the private-WS balance or config polyus_live_bankroll pins it) · venue cap $'+((vp.cap||0)).toFixed(2)+', headroom $'+((vp.cap_remaining||0)).toFixed(2)+', gross turnover today $'+((vp.day_turnover||0)).toFixed(2)+' (resets at UTC midnight)'+(vp.arm_bankroll!=null?(' · armed at $'+(vp.arm_bankroll||0).toFixed(2)):'')+'">🇺🇸 bank '+(vp.bankroll_src==='unknown'?'<b class="muted">unknown</b>':'<b>'+mny(vp.bankroll)+'</b>')+' <span class="muted">dep '+mny(vp.deployed)+' · avail '+(vp.bankroll_src==='unknown'?'—':mny(vp.available))+'</span></span>';
    }
    if(d.pnl_total!=null){h+='<span title="Fee-net realized P&L plus current open-position marks (🟩 '+mny(d.pnl_kalshi)+' · 🇺🇸 '+mny(d.pnl_polyus)+'). A venue is incomplete when its authenticated realized receipt chain is unavailable.">P&L total '+pnlSpan(d.pnl_total,true)+'</span>';}
    h+=d.armed?'<span style="color:var(--bad);font-weight:700">● ARMED</span>':'<button class="mini" onclick="armLive()">Arm live orders</button>';
    var ad=d.account_drawdown||{};
    if(ad.configured){
      var dd=(ad.session_drawdown_pct||0)*100,adColor=ad.breached?'var(--bad)':((ad.session_pnl_usd||0)>=0?'var(--good)':'var(--warn)');
      h+='<span style="color:'+adColor+';font-weight:700" title="Full real-account change since LIVE activation '+escapeHtml(tFmt(ad.activation_at||''))+': current NAV '+mny(ad.current_nav_usd)+' minus starting NAV '+mny(ad.baseline_usd)+'. Original peak '+mny(ad.peak_usd)+'. Current 7.5% stop reset '+escapeHtml(tFmt(ad.risk_epoch_at||''))+' at '+mny(ad.risk_baseline_usd)+'; post-reset peak '+mny(ad.risk_peak_usd)+'. Cumulative turnover '+mny(ad.turnover_usd)+'; cumulative fees '+mny(ad.fee_usd)+'. The risk reset survives UTC midnight and ARM. Accepted turnover is reporting-only and has no daily allowance; loss, exposure, order-size, and event-conflict rails remain.'+(ad.reason?(' PAUSED: '+escapeHtml(ad.reason)):'')+'">Since LIVE start '+pnlSpan(ad.session_pnl_usd,true)+' ('+(dd>=0?'+':'')+dd.toFixed(1)+'%)</span>';
    }
    if(d.armed){ // R44: the AUTO LIVE button belongs in THIS header too (the window-grid render the operator actually uses)
      if(d.auto){h+='<button class="mini" onclick="toggleLiveAuto(0)" style="color:var(--bad);font-weight:700" title="AUTO LIVE ON: engine-sized maker singles under the caps + $1 combos ($'+((d.auto_combo_spent||0)).toFixed(0)+'/$5 used). Click to STOP.">🤖 AUTO ON</button>';}
      else{h+='<button class="mini" onclick="toggleLiveAuto(1)" title="AUTO LIVE: the suite places the proposals below by itself — engine-sized maker (same sizing as paper), cap-bounded, $1 combos with a $5 session budget. Session-scoped.">🤖 Auto</button>';}
    }
    // R23: PER-VENUE mode + Live-editable sizing. Kalshi maker fills are adversely selected (14.6%
    // fill, −4.3¢ when filled) → taker default; PolyUS maker EARNS a rebate since July 1 → your call.
    h+='<button class="mini" onclick="toggleTakerK()" title="Kalshi execution. Maker rests post-only — historically 14.6% fill with −4.3¢ adverse selection; taker crosses now.">🟩 '+(window._liveTakerK?'TAKER':'MAKER')+'</button>';
    h+='<button class="mini" onclick="toggleTakerP()" title="PolyUS execution. Maker rests post-only and EARNS the −1.25% rebate (adverse ≈ 0 historically); taker fills now at 6% θ fee.">🇺🇸 '+(window._liveTakerP?'TAKER':'MAKER')+'</button>';
    h+='<span style="font-size:10.5px" title="The Adaptive Allocation Model sizes an already proof-qualified LIVE trade: route lower-bound fractional Kelly off each venue wallet, with bankroll, uncertainty, drawdown, event-cluster and daily-loss rails. It is separate from the C4 speed rank.">Allocation · proof-Kelly · bankroll-scaled cap <b>'+mny(d.cap||0)+'</b></span>';
    // R59: errors join the wrapping chip row (spans, internal text wrap allowed) so they can't
    // vanish off a sideways-scrolling strip; they also land in the order log pane.
    if(d.balance_error){h+='<span style="color:var(--bad);white-space:normal;min-width:0">balance error: '+escapeHtml(d.balance_error)+'</span>';}
    if(pus0.error){h+='<span style="color:var(--bad);white-space:normal;min-width:0">PolyUS error: '+escapeHtml(pus0.error)+'</span>';}
    h+='</div>';
    // R70 (audit §a P1): renderLive runs at 1s — EVERY pane below now carries the same string
    // dirty-guard liveProps always had (repaint only when the markup actually changed), instead
    // of rebuilding 4 tables + the header via raw innerHTML every second.
    var hd=document.getElementById("liveHead");if(hd&&hd._h!==h){hd._h=h;hd.innerHTML=h;}
    // ---- TRADE PANE: proposals (combos live in the same pane, rendered by loadCombos) ----
    var age=(d.ml_age_s!=null&&d.ml_age_s>=0)?d.ml_age_s:null;
    var ageChip='';
    if(age!=null){
      var atxt=(age<90)?(Math.round(age)+'s'):(Math.round(age/60)+'m');
      var acol=(age<120)?'var(--good)':((age<600)?'var(--warn)':'var(--bad)');
      ageChip=' <span title="age of the ML predictions these proposals are built from — red means the sidecar is stale" style="color:'+acol+';font-size:11.5px">● picks '+atxt+' old'+(age>=600?' — sidecar stale?':'')+'</span>';
    }
    var p='<div style="margin:2px 0 4px"><b>Proposed orders</b> <span class="muted" title="ML best picks · excludes anything held/resting/placed this session">🟩 '+(window._liveTakerK?'taker':'maker')+' · 🇺🇸 '+(window._liveTakerP?'taker':'maker')+'</span>'+ageChip; // R63 5: explainer → tooltip
    if(window._liveProps.length){p+=' <button class="mini" onclick="confirmAllLive()"'+(d.armed?'':' disabled')+'>Confirm All</button>';}
    p+='</div>';
    if(!window._liveProps.length){p+='<span class="muted">No route currently has authenticated exchange-profit authority for an automatic order.</span>';}
    p+=dropsChip(d.proposal_drops,d.proposal_drop_samples,'props'); // R63 5: collapsed "Dropped ×N ⓘ" chip
    // R63: the table renders whenever proposals EXIST (the old else-on-the-drops-if hid live
    // proposals any pass that also had drops — drops and proposals coexist now).
    if(window._liveProps.length){p+='<table class="mkt"><thead><tr><th>Market</th><th>Side</th><th class="r">@¢</th><th class="r">Ct</th><th class="r">Cost</th><th class="r" title="estimated fee in the CURRENT mode (taker default)">Fee~</th><th class="r">p_win</th><th class="r" title="est. NET EV per contract at the LIVE price: p_win − price − fee">est EV/ct</th><th></th></tr></thead><tbody>';
      window._liveProps.forEach(function(pp,i){var evn=(pp.ev_net!=null?pp.ev_net:((pp.p_win||0)-(pp.price_cents||0)/100));var plat=pp.platform||'kalshi';var chip=(plat==='polyus')?'🇺🇸 ':'🟩 ';var nm=(plat==='polyus')?(pp.title||prettySlug(pp.ticker)):(pp.title||pp.ticker||'');
        var tk=(plat==='polyus')?window._liveTakerP:window._liveTakerK; // R23: per-venue mode
        var fee=(tk?(pp.fee_taker!=null?pp.fee_taker:pp.est_maker_fee):(pp.fee_maker!=null?pp.fee_maker:pp.est_maker_fee)); // mode-aware; PolyUS maker fee is a REBATE (negative)
        p+='<tr><td>'+chip+'<a class="go" href="'+escapeHtml(posURL({platform:plat,ticker:pp.ticker,title:pp.title}))+'" target="_blank" rel="noopener">'+escapeHtml(String(nm).slice(0,120))+'</a></td><td>'+escapeHtml((pp.side||"").toUpperCase())+invBandBadge(pp)+'</td><td class="r">'+pp.price_cents+'</td><td class="r">'+pp.count+'</td><td class="r">'+mny(pp.cost)+'</td><td class="r muted"'+((fee||0)<0?' title="maker REBATE — resting post-only orders EARN this" style="color:var(--good)"':'')+'>'+mny(fee)+'</td><td class="r">'+((pp.p_win||0)).toFixed(2)+'</td><td class="r" style="color:'+(evn>=0?'var(--good)':'var(--bad)')+'"><b>'+(evn>=0?'+':'')+(evn*100).toFixed(1)+'¢</b></td><td class="r"><button class="mini" onclick="acceptLive('+i+')"'+(d.armed?'':' disabled')+'>Accept</button></td></tr>';});
      p+='</tbody></table>';}
    var pe=document.getElementById("liveProps");if(pe&&window._livePropsHTML!==p){window._livePropsHTML=p;pe.innerHTML=p;} // R23: 1s poll repaints only on change
    // ---- POSITIONS PANE: Kalshi + PolyUS, same columns, PnL each + totals (operator #4/#5) ----
    var g='';
    (function(){
      var all=d.positions||[];var open=all.filter(function(x){return (x.position||0)!==0;});var closed=all.filter(function(x){return (x.position||0)===0;});
      g+='<div style="margin:2px 0 4px"><b>Live positions</b> <span class="muted">🟩 Kalshi ('+open.length+' open)</span>'+(d.pnl_kalshi_complete?(' · total '+pnlSpan(d.pnl_kalshi,true)+' <span class="muted">(settled + all fees '+mny(d.pnl_ledger_kalshi)+' · open price move '+mny(d.pnl_unrealized_kalshi)+')</span>'):' · total <span class="bad">unavailable</span>')+'</div>';
      if(!open.length){g+='<div class="muted" style="font-size:12.5px">None.</div>';}
      else{var kmx=0;open.forEach(function(x){var a=Math.abs(x.pnl_usd||0);if(a>kmx)kmx=a;}); // R60 C2: tint scale
        g+='<table class="mkt"><thead><tr><th>Market</th><th>Side</th><th class="r" title="payout if this side wins — contracts × $1 (matches the Kalshi app\'s To win)">To win $</th><th class="r">Cost</th><th class="r" title="your avg entry → the live mark, both for YOUR side (R44)">Entry→Mark¢</th><th class="r" title="elapsed since the suite first saw this position (absolute in the tooltip)">Bought</th><th class="r" title="unrealized at the live mark">P&L</th><th class="r">Realized</th></tr></thead><tbody>';
        open.forEach(function(x){var sd=x.side||((x.position||0)<0?'NO':'YES');var tw=(x.qty!=null?x.qty:Math.abs(x.position||0));
          g+='<tr'+pnlTint(x.has_pnl?x.pnl_usd:0,kmx)+'><td><a class="go" href="'+escapeHtml(posURL({platform:'kalshi',ticker:x.ticker}))+'" target="_blank" rel="noopener" title="'+escapeHtml(x.ticker)+'">'+escapeHtml(((x.title||x.ticker)+"").slice(0,120))+'</a></td><td style="color:'+(sd==='YES'?'var(--good)':'var(--bad)')+';font-weight:700">'+sd+'</td><td class="r">$'+(tw||0).toFixed(2)+'</td><td class="r">'+mny(Math.abs(x.exposure_usd))+'</td><td class="r">'+(x.entry_c?x.entry_c:'—')+'→'+(x.mark_c?(x.mark_c+'¢'):'—')+'</td>'+boughtCell(x.opened_ts)+'<td class="r">'+pnlSpan(x.pnl_usd,x.has_pnl)+'</td><td class="r" style="color:'+((x.realized_usd||0)>=0?'var(--good)':'var(--bad)')+'">'+mny(x.realized_usd)+'</td></tr>';});
        g+='</tbody></table>';}
      if(closed.length){g+='<div class="muted" style="font-size:12px;margin:4px 0" title="settled/flattened markets — kept for the realized P&L trail, not live risk">closed: ';
        closed.forEach(function(x,i){g+=(i?' · ':'')+'<span title="'+escapeHtml(x.ticker)+'">'+escapeHtml(x.title||x.ticker)+'</span> <span style="color:'+((x.realized_usd||0)>=0?'var(--good)':'var(--bad)')+'">'+mny(x.realized_usd)+'</span>';});g+='</div>';} // R63 2d: friendlyName'd title (server kmkts join), raw ticker in the tooltip
    })();
    if(pus0.enabled){
      var pp2=(pus0.positions||[]).filter(function(x){return (x.qty||0)!==0&&!x.expired;});
      g+='<div style="margin:12px 0 4px"><b>Live positions</b> <span class="muted">🇺🇸 PolyUS ('+pp2.length+' open)</span>'+(d.pnl_polyus_complete?(' · total '+pnlSpan(d.pnl_polyus,true)+' <span class="muted">(settled + all fees '+mny(d.pnl_ledger_polyus)+' · open price move '+mny(d.pnl_unrealized_polyus)+')</span>'):' · total <span class="bad">unavailable</span>')+'</div>';
      if(!pp2.length){g+='<div class="muted" style="font-size:12.5px">None.</div>';}
      else{var pmx=0;pp2.forEach(function(x){var a=Math.abs(x.pnl_usd||0);if(a>pmx)pmx=a;}); // R60 C2: tint scale
        g+='<table class="mkt"><thead><tr><th>Market</th><th>Side</th><th class="r">Ct</th><th class="r">Cost</th><th class="r" title="elapsed since the suite first saw this position (absolute in the tooltip)">Bought</th><th class="r" title="venue cash value − cost">P&L</th><th class="r">Realized</th></tr></thead><tbody>';
        pp2.forEach(function(x){var sd=x.side||'YES';
          g+='<tr'+pnlTint(x.has_pnl?x.pnl_usd:0,pmx)+'><td title="'+escapeHtml(x.slug||"")+'"><a class="go" href="'+escapeHtml(posURL({platform:'polyus',ticker:x.slug,title:x.title}))+'" target="_blank" rel="noopener">'+escapeHtml((x.title||prettySlug(x.slug)||"").slice(0,120))+'</a></td><td style="color:'+(sd==='YES'?'var(--good)':'var(--bad)')+';font-weight:700">'+sd+'</td><td class="r">'+(x.qty||0)+'</td><td class="r">'+mny(x.exposure_usd)+'</td>'+boughtCell(x.opened_ts)+'<td class="r">'+pnlSpan(x.pnl_usd,x.has_pnl)+'</td><td class="r" style="color:'+((x.realized_usd||0)>=0?'var(--good)':'var(--bad)')+'">'+mny(x.realized_usd)+'</td></tr>';});
        g+='</tbody></table>';}
    }
    var ge=document.getElementById("livePosGrid");if(ge&&ge._h!==g){ge._h=g;ge.innerHTML=g;}
    // ---- RESTING PANE: both venues' maker rests in ONE section (operator #6) ----
    var o='';
    window._liveResting=(d.resting_orders||[]);
    var po=pus0.orders||[];
    o+='<div style="margin:2px 0 4px"><b>Resting orders</b> <span class="muted" title="maker rests waiting to fill">🟩 '+window._liveResting.length+' · 🇺🇸 '+po.length+'</span>'; // R63 5: explainer → tooltip
    if(window._liveResting.length)o+=' <button class="mini" onclick="cancelAllLive()">Cancel all Kalshi</button>';
    if(po.length)o+=' <button class="mini" onclick="cancelAllPolyUSLive()">Cancel all PolyUS</button>';
    o+='</div>';
    if(!window._liveResting.length&&!po.length){o+='<div class="muted" style="font-size:12.5px" title="taker mode fills or dies immediately; only maker (post-only) orders rest here">None.</div>';} // R63 5: explainer → tooltip
    else{o+='<table class="mkt"><thead><tr><th>Market</th><th>Side</th><th class="r">@¢</th><th class="r">Qty</th><th class="r">Left</th><th class="r">TIF</th><th></th></tr></thead><tbody>';
      window._liveResting.forEach(function(ro){var pc=(ro.yes_price_dollars?Math.round(ro.yes_price_dollars*100):null);o+='<tr><td>🟩 <a class="go" href="'+escapeHtml(posURL({platform:'kalshi',ticker:ro.ticker}))+'" target="_blank" rel="noopener">'+escapeHtml(ro.ticker||"")+'</a>'+(ro.queue_known?' <span class="muted" style="font-size:10.5px" title="other resting contracts at your price level (live WS book) x that price — the $ that must fill before your maker order; price-time priority makes this an upper bound">queue ~$'+Number(ro.queue_ahead_usd||0).toFixed(2)+' ahead</span>':'')+'</td><td>'+escapeHtml(ro.side||"")+'</td><td class="r">'+(pc!=null?pc:'—')+'</td><td class="r">'+(ro.initial_count_fp||'—')+'</td><td class="r">'+(ro.remaining_count_fp!=null?ro.remaining_count_fp:'—')+'</td><td class="r muted">GTC</td><td class="r"><button class="mini" onclick="cancelLive(\''+jsq(ro.order_id||"")+'\')">Cancel</button></td></tr>';});
      po.forEach(function(oo){o+='<tr><td title="'+escapeHtml(oo.slug||"")+'">🇺🇸 <a class="go" href="'+escapeHtml(posURL({platform:'polyus',ticker:oo.slug,title:oo.title}))+'" target="_blank" rel="noopener">'+escapeHtml((oo.title||prettySlug(oo.slug)||"").slice(0,120))+'</a></td><td>'+escapeHtml((oo.action||"")+' '+(oo.outcome||""))+'</td><td class="r">'+Math.round((oo.price||0)*100)+'</td><td class="r">'+(oo.qty||0)+'</td><td class="r">'+(oo.leaves||0)+'</td><td class="r muted">'+escapeHtml(oo.tif||"")+'</td><td class="r"><button class="mini" onclick="cancelPolyUSLive(\''+jsq(oo.id||"")+'\',\''+jsq(oo.slug||"")+'\')">Cancel</button></td></tr>';});
      o+='</tbody></table>';}
    var oe=document.getElementById("liveOrdersGrid");if(oe&&oe._h!==o){oe._h=o;oe.innerHTML=o;}
    // ---- HISTORY PANE: R63 1b UNIFORM columns for both venues (TYPE | MARKET | SIDE | @¢ | QTY |
    // P&L | EXEC — blank cell when a venue lacks a field) · R63 5 text diet: venue chip only.
    function histHead(){return '<table class="mkt"><colgroup><col style="width:44px"><col><col style="width:66px"><col style="width:36px"><col style="width:40px"><col style="width:64px"><col style="width:44px"></colgroup><thead><tr><th>Type</th><th>Market</th><th>Side</th><th class="r">@¢</th><th class="r">Qty</th><th class="r">P&amp;L</th><th class="r">Exec</th></tr></thead><tbody>';}
    var t='<div style="margin:2px 0 4px"><b>🟩 Kalshi</b></div>';
    if((d.kalshi_fills||[]).length){t+=histHead();
      d.kalshi_fills.forEach(function(f){t+='<tr><td class="muted">trade</td><td class="ell" title="'+escapeHtml(f.ticker||"")+'"><a class="go" href="'+escapeHtml(posURL({platform:'kalshi',ticker:f.ticker,title:f.title}))+'" target="_blank" rel="noopener">'+escapeHtml(((f.title||f.ticker)||""))+'</a></td><td>'+escapeHtml(((f.disp_act||f.action||"")+' '+(f.disp_side||f.side||"")).trim())+'</td><td class="r">'+(f.px_c!=null?f.px_c:(f.yes_price||0))+'</td><td class="r">'+(f.qty!=null?f.qty:(f.count||0))+'</td>'+(f.fee_usd?'<td class="r muted" title="venue fee for this fill (fee_cost)">-$'+f.fee_usd.toFixed(2)+'</td>':'<td class="r muted">—</td>')+'<td class="r muted">'+(f.is_taker?'taker':'maker')+'</td></tr>';});t+='</tbody></table>';} // R69 F1: normalized fields + real per-fill fee_cost in the P&L cell
    else{t+='<div class="muted" style="font-size:12.5px">None yet.</div>';}
    var ph=pus0.activities||[];
    if(ph.length){t+='<div style="margin:12px 0 4px"><b>🇺🇸 PolyUS</b></div>'+histHead();
      ph.forEach(function(a){ // R60 C7: FULL list (no slicing) — the widget body scrolls
        var px=a.price?Math.round(a.price*100):(a.settle_px?Math.round(a.settle_px*100):null);
        var pl=a.pnl?('<span style="color:'+((a.pnl||0)>=0?'var(--good)':'var(--bad)')+'">'+mny(a.pnl)+'</span>')
                    :(a.type==="TRADE"&&a.price&&a.qty?('<span class="muted" title="opening trade — no realized P&L yet; this is its cash value">$'+(a.price*a.qty).toFixed(2)+' in</span>'):"—");
        var ex=(a.type==="TRADE")?(a.taker?'taker':'maker'):'settle';
        t+='<tr><td class="muted">'+escapeHtml((a.type||"").replace("POSITION_","")).toLowerCase()+'</td><td class="ell" title="'+escapeHtml(a.slug||"")+'"><a class="go" href="'+escapeHtml(posURL({platform:'polyus',ticker:a.slug,title:a.title}))+'" target="_blank" rel="noopener">'+escapeHtml((a.title||prettySlug(a.slug)||""))+'</a></td><td class="muted">—</td><td class="r"'+(a.settle_px&&!a.price?' title="derived settlement value per contract"':'')+'>'+(px!=null?px:"—")+'</td><td class="r">'+(a.qty||"—")+'</td><td class="r">'+pl+'</td><td class="r muted">'+ex+'</td></tr>';});
      t+='</tbody></table>';}
    var te=document.getElementById("liveHistGrid");if(te&&te._h!==t){te._h=t;te.innerHTML=t;}
    // ---- LOG PANE: order log + Copy (operator #8) ----
    var lg=(d.log||[]).slice().reverse();
    var L='<div style="margin:2px 0 4px"><b>Order log</b> <button id="copyLogBtn" class="mini" onclick="copyLiveLog()" title="copies the whole log as plain text — full session trail, scrolls">Copy</button></div>'; // R63 5: label removed
    if(!lg.length){L+='<div class="muted" style="font-size:12.5px">Nothing yet this session.</div>';}
    else{L+='<div style="font-size:10.5px;font-family:inherit">'; // R60 C7: FULL list — the widget body scrolls
      lg.forEach(function(e){var col=e.error?'var(--bad)':(e.event==='PLACE'||e.event==='PUS-FILLED'?'var(--good)':'var(--muted)');L+='<div style="color:'+col+'">'+logWhen(e.ts)+' '+escapeHtml(e.event||'')+' '+tickLink(e.ticker)+' '+(e.status?('&rarr;'+escapeHtml(e.status)):'')+(e.error?(' ERR: '+escapeHtml(e.error)):'')+(e.reason?(' ('+escapeHtml(e.reason)+')'):'')+'</div>';});L+='</div>';}
    var le=document.getElementById("liveLogGrid");if(le&&le._h!==L){le._h=L;le.innerHTML=L;}
  })();
}
function loadML(){
  fetch("/api/ml").then(function(r){return r.json();}).then(function(d){
    var pr=d&&d.predictions;
    // R98 (operator: "@29→99 still showed +44¢ at the top"): actionability = fee-net EV at the
    // CURRENT live price (ev_net_live, stamped by the sidecar's live repricing pass). A pick that
    // already ran has ev_net_live ≤ 0 and drops OFF the list; rows without a live quote this cycle
    // fall back to their signal-time EV (px_src='sig-stale'). Client-side sort keeps the ranking
    // honest even against an older sidecar file.
    window._mlLoadFails=0;
    var ps=((pr&&pr.predictions)||[]).filter(function(x){return evLive(x)>0;}).sort(function(a,b){return evLive(b)-evLive(a);});
    renderMLBody(d,ps,{}); // R63 7c: now_c rides the /api/ml payload itself (server kmkts join) — no extra fetch
  }).catch(function(e){
    // R105: the catch now (1) NAMES the failure instead of a bare blank line and (2) retries on a
    // bounded 5s timer — a transient fetch/parse hiccup can no longer park the tab on an error until
    // a manual reload. Render-time errors can't reach here anymore (renderMLBody isolates each
    // section via mlSecErr) — anything caught here is fetch/JSON-level.
    var n=(window._mlLoadFails=(window._mlLoadFails||0)+1);
    var el=document.getElementById("mlBody");
    if(el)el.innerHTML='<span class="muted">Could not load ML data — '+escapeHtml(String((e&&e.message)||e))+' (attempt '+n+', auto-retry in 5s)</span>';
    if(!window._mlRetryT&&n<=120){window._mlRetryT=setTimeout(function(){window._mlRetryT=null;loadML();},5000);}
  });
}
// R105 ROOT FIX (operator screenshot 06:24 "Could not load ML data."): evLive was a loadML LOCAL
// while renderMLBody — a DIFFERENT top-level function — also called it. Lexical scoping made that a
// ReferenceError on the FIRST +EV row of the Top-scored table; loadML's tab-wide catch then blanked
// the whole tab. Boards with ZERO +EV picks never entered the forEach body, which is why quiet
// overnight boards masked it and the morning picks tripped it on every SSE repaint. ONE top-level
// definition now, shared by loadML + renderMLBody (pinned by r105_test.go).
function evLive(x){return (x&&x.ev_net_live!=null)?x.ev_net_live:((x&&x.ev_net!=null)?x.ev_net:((x&&x.ev_per_contract!=null)?x.ev_per_contract:0));}
// R105: a failed ML-tab section rolls back to its checkpoint and renders as ONE named error line —
// a tab must never go fully blank because one book file / payload block hiccuped.
function mlSecErr(name,e){try{console.error('ML tab — '+name+' section failed to render:',e);}catch(_){}
  return '<div style="color:var(--bad);font-size:12px;margin:8px 0">⚠ '+name+' section failed to render: '+escapeHtml(String((e&&e.message)||e))+' — the rest of the tab is unaffected.</div>';}
function mlNowPx(x,pxm){ // R63 7c: current SIDE price for a pick — the SERVER joins now_c (¢, side-aware,
  // kmkts tape cache) onto /api/ml kalshi rows (a client /api/livepx join was ruled out as pricier);
  // the kalshi markets snapshot is the fallback for rows the tape hasn't cached. No price → "@" only.
  if(x&&x.now_c!=null&&x.now_c>0)return x.now_c/100;
  if(pxm){var key=(x.platform||'kalshi')+'|'+x.ticker+'|'+String(x.side||'YES').toUpperCase();if(pxm[key]!=null)return pxm[key];}
  var m=(window.mktById||{})[x.ticker];
  if(m&&m.implied_pct){var y=m.implied_pct/100;return (String(x.side||'YES').toUpperCase()==='NO')?(1-y):y;}
  return null;
}
function renderMLBody(d,ps,pxm){
  (function(){
    function mny(v){v=v||0;return (v<0?'-$':'$')+Math.abs(v).toFixed(2);}
    function nc(v){return (v||0)>=0?'var(--good)':'var(--bad)';}
    function ev(v){return ((v||0)>=0?'+':'')+((v||0)*100).toFixed(1)+'¢';}
    var pr=d&&d.predictions, pf=d&&d.paper, h='';
    var eqSeries=null,shSeries=null; // R63 3e: equity curves render through renderPnLChart AFTER the innerHTML lands
    if(!pr&&!pf){document.getElementById("mlBody").innerHTML='<span class="muted">ML sidecar has not produced output yet. Start the suite (build-suite.bat launches it), give it a few minutes and enough resolved signals (>=150).</span>';return;}
    if(pr){
      var ms=String(pr.model_status||'UNKNOWN'),warm=ms.indexOf('WARMING')===0;
      var provisional=pr.metric_scope==='provisional_paper';
      var aucRaw=provisional?pr.provisional_auc:pr.oos_auc;
      var brRaw=provisional?pr.provisional_brier:pr.oos_brier;
      var logRaw=provisional?pr.provisional_log_loss:pr.oos_log_loss;
      var eceRaw=provisional?pr.provisional_ece:pr.oos_ece;
      var auc=(typeof aucRaw==='number'&&isFinite(aucRaw))?aucRaw:null;
      var br=(typeof brRaw==='number'&&isFinite(brRaw))?brRaw:null;
      var logloss=(typeof logRaw==='number'&&isFinite(logRaw))?logRaw:null;
      var ece=(typeof eceRaw==='number'&&isFinite(eceRaw))?eceRaw:null;
      var sr=pr.split_receipt||{},lv=pr.live_validation||{},schema=pr.feature_schema||pr.model_cohort||'unknown';
      var metric=(warm||auc==null||br==null)?'<b>AUC n/a · Brier n/a · log loss n/a</b> <span class="muted">— warming; no score is borrowed from the old price-only model</span>':
        '<b>AUC '+auc.toFixed(3)+' · Brier '+br.toFixed(4)+(logloss!=null?' · log loss '+logloss.toFixed(4):'')+(ece!=null?' · ECE '+ece.toFixed(4):'')+'</b>';
      var scope=provisional?'<b style="color:var(--warn)">Paper provisional</b>':'<b>rolling holdout</b>';
      h+='<div class="r138hero" style="margin-bottom:10px"><b>🧠 New book-native ML</b> · '+r138State(ms)+' · '+scope+' · '+metric+'<br>'+
        '<span class="muted">schema '+escapeHtml(schema)+' · model '+escapeHtml(pr.model_version||'warming')+
        (sr.test_rows!=null?(' · train '+Number(sr.train_rows||0)+' / validation '+Number(sr.validation_rows||0)+' / rolling-final test '+Number(sr.test_rows||0)):'')+
        ' · LIVE validation '+Number(lv.days_observed||sr.days||0)+'/'+Number(lv.days_required||5)+' UTC days. AUC ranks picks; Brier, log loss, and ECE measure probability accuracy (lower is better). '+
        (pr.paper_authority?'Paper sampling is on. ':'Paper sampling is warming. ')+'LIVE is locked.</span></div>';
    }else if(pf){
      h+='<div class="r138hero" style="margin-bottom:10px"><b>🧠 New book-native ML</b> · '+r138State('OUTPUT UNAVAILABLE')+' · <b>AUC n/a · Brier n/a · log loss n/a</b><br><span class="muted">The historical ML paper ledger loaded, but the current book-native model receipt did not. No stale metric is substituted; research and trading consumers fail closed until a fresh sidecar receipt appears.</span></div>';
    }
    var _ck1=h.length;try{ // R105: per-section isolation — see mlSecErr
    if(pf&&pf.stats){var st=pf.stats;
      h+='<div style="margin:2px 0 6px"><b>Book-v2 Paper portfolio</b> <span class="muted">· current epoch only; legacy rows excluded · $'+Math.round(pf.bank0||600)+' start</span></div>';
      var mev=0;(pf.open||[]).forEach(function(p){mev+=(p.contracts||0)*(p.ev||0);}); // total ML EV of the open book
      var nb=(st.closed||0)>0?(st.net||0)/st.closed:0; // WR chip removed (operator directive): net/bet leads
      h+='<div style="display:flex;gap:16px;flex-wrap:wrap;margin-bottom:8px;font-size:13px"><span>Equity <b>'+mny(st.equity!=null?st.equity:(600+(st.net||0)))+'</b></span><span>Net <b style="color:'+nc(st.net)+'">'+mny(st.net)+'</b></span><span title="net per closed bet">Net/bet <b style="color:'+nc(nb)+'">'+mny(nb)+'</b></span><span>ROI <b style="color:'+nc(st.roi)+'">'+((st.roi||0)*100).toFixed(1)+'%</b></span><span>Win <b>'+((st.closed||0)?((st.win_rate||0)*100).toFixed(1)+'%':'n/a')+'</b></span><span>Closed <b>'+(st.closed||0)+'</b></span><span>Open <b>'+(st.open_n||0)+'</b></span><span title="total ML EV of the open book (Σ contracts × ML EV/contract)">Open EV <b style="color:'+nc(mev)+'">'+mny(mev)+'</b></span><span>Live unreal <b style="color:'+nc(pf.live_unreal)+'">'+mny(pf.live_unreal||0)+'</b></span></div>';
      // R63 3e FIX (ML equity curve stopped rendering): eqSpark built a raw string into innerHTML with
      // its own sizing; the R62 chart-autofit path never covered it. Both book curves now render via
      // renderPnLChart into DEDICATED divs after the DOM lands (net vs the $-start baseline).
      if(pf.equity&&pf.equity.length>1){
        var b0=pf.bank0||250;
        eqSeries=pf.equity.map(function(p){return {ts:new Date((p.t||0)*1000).toISOString(),pnl:(p.eq||0)-b0};});
        h+='<div class="muted" style="font-size:11px;margin:0 0 2px">equity curve · net vs $'+Math.round(b0)+' start</div><div id="mlEqChart" style="height:110px;max-width:680px;margin-bottom:8px"></div>';
      }
      var op=(pf.open||[]).slice().sort(function(a,b){return (b.unrealized||0)-(a.unrealized||0);});
      if(op.length){h+='<table style="width:100%;border-collapse:collapse;margin-bottom:12px"><thead><tr style="color:var(--muted);font-size:11.5px;text-align:left"><th style="padding:3px 6px">Open pick</th><th>Side</th><th class="r">Held $</th><th class="r">Entry→Cur</th><th class="r" title="when this bet was BOUGHT + how long ago">Bought</th><th class="r" title="current time — compare with Bought for hold time">Now</th><th class="r">Value</th><th class="r">P&amp;L</th><th class="r" title="p_win — the ML model\'s calibrated win probability for this pick (R83: was mislabeled Conf; this column has always shown p_win)">p_win</th><th class="r" title="ML EV per contract at entry (p_win − price)">ML EV</th><th class="r" title="the ML book rides every pick to settlement (no stops set)">TP/SL</th></tr></thead><tbody>';
        op.forEach(function(p){var cur=(p.cur_price!=null)?p.cur_price:null;var held=(p.contracts||0)*(p.price||0);var val=(cur!=null)?(p.contracts||0)*cur:held;var pxT=(p.px_src==='live')?(' title="bought at the LIVE venue price'+(p.sig_price?(' — the signal logged '+Math.round(p.sig_price*100)+'¢'):'')+'"'):((p.px_src==='signal')?' title="no live feed at buy time — booked at the signal price"':'');h+='<tr style="border-top:1px solid var(--line)"><td style="padding:3px 6px"><a class="go" href="'+posURL(p)+'" target="_blank" rel="noopener">'+escapeHtml((p.title||p.ticker||"").slice(0,120))+'</a></td><td>'+escapeHtml(p.side||"")+'</td><td class="r">$'+held.toFixed(0)+'</td><td class="r"'+pxT+'>'+Math.round((p.price||0)*100)+'→'+(cur!=null?(Math.round(cur*100)+"¢"):"—")+(p.px_src==='live'?' <span style="color:var(--good)" title="entered at the live venue price">●</span>':'')+'</td>'+boughtCell(p.opened)+nowCell()+'<td class="r">$'+val.toFixed(0)+'</td><td class="r" style="color:'+nc(p.unrealized)+'">'+(cur!=null?mny(p.unrealized):"—")+'</td><td class="r"><span style="font-weight:700;color:'+confColor(p.p_win)+'">'+(p.p_win||0).toFixed(2)+'</span></td><td class="r" style="color:'+nc(p.ev)+'">'+ev(p.ev)+'</td><td class="r muted" title="the ML book holds to settlement (no stop set)">ride</td></tr>';});
        h+='</tbody></table>';}
    }
    }catch(_e1){h=h.slice(0,_ck1)+mlSecErr('ML paper book',_e1);}
    var _ck2=h.length;try{
    if(d&&d.shadow&&d.shadow.stats){var ss=d.shadow.stats;
      h+='<div style="margin:16px 0 6px;padding-top:10px;border-top:1px solid var(--line)"><b>🩶 Shadow book</b> <span class="muted">· every gross-+EV candidate from the FULL scored file (R67h — was top-N), MAKER fees charged (kalshi ≈¼-taker bound, Poly US −1.25% rebate) · $800 · flat $20/bet, no EV floors · the max-breadth control vs the gated ML book</span></div>';
      var snb=(ss.closed||0)>0?(ss.net||0)/ss.closed:0; // WR chip removed (operator directive)
      h+='<div style="display:flex;gap:16px;flex-wrap:wrap;margin-bottom:8px;font-size:13px"><span>Equity <b>'+mny(ss.equity)+'</b></span><span title="net SINCE the last RESET (R77) — lifetime '+mny(ss.net_lifetime!=null?ss.net_lifetime:(ss.net||0))+' stays in the book file/export">Net <b style="color:'+nc(ss.net)+'">'+mny(ss.net)+'</b></span><span title="net per closed bet">Net/bet <b style="color:'+nc(snb)+'">'+mny(snb)+'</b></span><span>ROI <b style="color:'+nc(ss.roi)+'">'+Math.round((ss.roi||0)*100)+'%</b></span><span title="closed SINCE the last RESET (R77) — lifetime '+(ss.closed_lifetime!=null?ss.closed_lifetime:(ss.closed||0))+'">Closed <b>'+(ss.closed||0)+'</b></span><span>Open <b>'+(ss.open_n||0)+'</b></span><span>Live unreal <b style="color:'+nc(d.shadow.live_unreal)+'">'+mny(d.shadow.live_unreal||0)+'</b></span><span title="mean PREDICTED EV/ct vs REALIZED across every bet — a gap = the model\'s EV is optimistic">EV pred/real <b>'+ev(ss.ev_pred_mean)+' / '+ev(ss.ev_real_mean)+'</b></span></div>';
      if(d.shadow.equity&&d.shadow.equity.length>1){ // R63 3e: same renderPnLChart path as the ML book curve
        var sb0=ss.bank0||250;
        shSeries=d.shadow.equity.map(function(p){return {ts:new Date((p.t||0)*1000).toISOString(),pnl:(p.eq||0)-sb0};});
        h+='<div class="muted" style="font-size:11px;margin:0 0 2px">shadow equity · net vs $'+Math.round(sb0)+' start</div><div id="shadowEqChart" style="height:110px;max-width:680px;margin-bottom:8px"></div>';
      }
      // R63 3e: shadow open picks table gains the ML book's FULL column set (uniform headers) —
      // Value / p_win / ML EV / TP-SL blank ("—" / "ride") where the shadow rows lack the field.
      // R83: the column header is p_win (it renders p.p_win and always has; "Conf" was a misnomer).
      var sop=((d.shadow.open)||[]).slice().sort(function(a,b){return (b.unrealized||0)-(a.unrealized||0);});
      if(sop.length){
        var scap=sop.slice(0,40);
        h+='<table style="width:100%;border-collapse:collapse;margin-bottom:10px"><thead><tr style="color:var(--muted);font-size:11.5px;text-align:left"><th style="padding:3px 6px">Shadow open pick'+(sop.length>scap.length?(' <span class="muted">(top '+scap.length+' of '+sop.length+' by P&amp;L)</span>'):'')+'</th><th>Side</th><th class="r">Held $</th><th class="r">Entry→Cur</th><th class="r" title="when this bet was BOUGHT + how long ago">Bought</th><th class="r" title="current time — compare with Bought for hold time">Now</th><th class="r">Value</th><th class="r">P&amp;L</th><th class="r" title="p_win — the ML model\'s calibrated win probability for this pick (R83: was mislabeled Conf; the cell has always rendered p_win)">p_win</th><th class="r" title="ML EV per contract at entry">ML EV</th><th class="r" title="the shadow book rides every pick to settlement">TP/SL</th></tr></thead><tbody>';
        scap.forEach(function(p){var cur=(p.cur_price!=null)?p.cur_price:null;var held=(p.contracts||0)*(p.price||0);var val=(cur!=null)?(p.contracts||0)*cur:held;
          h+='<tr style="border-top:1px solid var(--line)"><td style="padding:3px 6px"><a class="go" href="'+posURL(p)+'" target="_blank" rel="noopener">'+escapeHtml((p.title||p.ticker||"").slice(0,120))+'</a></td><td>'+escapeHtml(p.side||"")+'</td><td class="r">$'+held.toFixed(0)+'</td><td class="r">'+Math.round((p.price||0)*100)+'→'+(cur!=null?(Math.round(cur*100)+"¢"):"—")+'</td>'+boughtCell(p.opened)+nowCell()+'<td class="r">$'+val.toFixed(0)+'</td><td class="r" style="color:'+nc(p.unrealized)+'">'+(cur!=null?mny(p.unrealized):"—")+'</td><td class="r">'+(p.p_win!=null?('<span style="font-weight:700;color:'+confColor(p.p_win)+'">'+(p.p_win||0).toFixed(2)+'</span>'):'<span class="muted">—</span>')+'</td><td class="r"'+(p.ev!=null?(' style="color:'+nc(p.ev)+'"'):'')+'>'+(p.ev!=null?ev(p.ev):'<span class="muted">—</span>')+'</td><td class="r muted">ride</td></tr>';});
        h+='</tbody></table>';
      }
    }
    }catch(_e2){h=h.slice(0,_ck2)+mlSecErr('Shadow book',_e2);}
    // R72-B: KFLOW TWIN BOOKS — the R71 flow-only family split by game state at signal time
    // (pre-game vs in-play), $10 flat/signal, maker fees, Go-side + sidecar-independent.
    var kfSeriesP=null,kfSeriesL=null;
    var _ck3=h.length;try{
    if(d&&d.kflow_books&&d.kflow_books.enabled!==false){
      var kb=d.kflow_books;
      h+='<div style="margin:16px 0 6px;padding-top:10px;border-top:1px solid var(--line)"><b>🟩 KFLOW twin books</b> <span class="muted">· the R71 flow-only family split by game state at signal time (study: pre-game +8.7¢/ct vs pooled +4.35¢) · $10 flat per signal · maker fees · rides to resolution · R77: RESET re-epochs the shown stats like every other book (lifetime history kept internally; open lots ride)</span></div>';
      var kfCard=function(name,tip,b,cid){ if(!b)return '';
        var n=(b.closed||0);var wr=(n>0)?(Math.round((b.win_rate||0)*100)+'%'):'—';
        return '<div class="card" style="flex:1;min-width:250px;max-width:420px;padding:8px" title="'+tip+'"><div style="font-size:12px;margin-bottom:4px"><b>'+name+'</b> <span class="muted">· $'+Math.round(b.bank||500)+' book</span></div>'+
          '<div style="display:flex;gap:12px;flex-wrap:wrap;font-size:12.5px"><span title="net since the last RESET — lifetime '+mny(b.net_lifetime!=null?b.net_lifetime:(b.net||0))+' rides in the file/export">Net <b style="color:'+nc(b.net)+'">'+mny(b.net||0)+'</b></span><span title="settled lots since the last RESET — lifetime '+(b.closed_lifetime!=null?b.closed_lifetime:n)+'">n <b>'+n+'</b></span><span>Open <b>'+(b.open||0)+'</b></span><span>Win <b>'+wr+'</b></span></div>'+
          '<div id="'+cid+'" style="height:70px;margin-top:4px"></div></div>';
      };
      h+='<div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:10px">'+
        kfCard('kflow_pre','every kflow signal logged PRE-GAME (is_live=0) — the study measured the pre-game slice at +8.7¢/ct maker-net',kb.pre,'kfPreChart')+
        kfCard('kflow_live','every kflow signal logged IN-PLAY (is_live=1); unknown game state (−1) is skipped',kb.live,'kfLiveChart')+'</div>';
      var kmap=function(b){return (b&&b.equity&&b.equity.length>1)?b.equity.map(function(p){return {ts:new Date((p[0]||0)*1000).toISOString(),pnl:(p[1]||0)};}):null;};
      kfSeriesP=kmap(kb.pre);kfSeriesL=kmap(kb.live);
    }
    }catch(_e3){h=h.slice(0,_ck3)+mlSecErr('Kflow twin books',_e3);}
    // R105 (operator): RAWFLOW BOOK panel — the R103 raw kalshi-flow experiment book gets the same
    // ML-tab visibility as the kflow twins: equity, open lots, closed, win rate, its band + rules
    // one-liner, and an equity sparkline. Reads the /api/ml "rawflow" block (rawFlowMLPayload).
    var rfSeries=null;
    var _ck4=h.length;try{
    if(d&&d.rawflow&&d.rawflow.enabled!==false){
      var rb=d.rawflow;
      h+='<div style="margin:16px 0 6px;padding-top:10px;border-top:1px solid var(--line)"><b>🌊 RawFlow book</b> <span class="muted" title="'+escapeHtml(rb.rules||'')+'">· raw kalshi-flow signals · band '+escapeHtml(rb.band||'10–50¢')+' · realized-only Kelly on own equity · no ML gate/haircut/borders · one lot per market · rides to settlement (ratio stop frozen at entry)</span></div>';
      var rn=(rb.closed||0);var rwr=(rn>0)?(Math.round((rb.win_rate||0)*100)+'%'):'—';
      h+='<div style="display:flex;gap:12px;flex-wrap:wrap;font-size:12.5px;margin-bottom:6px"><span title="cash + open lots at cost">Equity <b>'+mny(rb.equity!=null?rb.equity:(rb.bank||0))+'</b></span><span title="net since the epoch start — lifetime '+mny(rb.net_lifetime!=null?rb.net_lifetime:(rb.net||0))+' rides in the book file">Net <b style="color:'+nc(rb.net)+'">'+mny(rb.net||0)+'</b></span><span title="settled lots this epoch — lifetime '+(rb.closed_lifetime!=null?rb.closed_lifetime:rn)+'">Closed <b>'+rn+'</b></span><span>Open <b>'+(rb.open||0)+'</b></span><span>Win <b>'+rwr+'</b></span><span class="muted">deployed '+mny(rb.open_cost||0)+' · bank '+mny(rb.bank||0)+'</span></div>';
      // R111: correlated-exposure clusters — several lots on one underlying (F5+full game,
      // spread lines, corners bands) are ALLOWED since the restatement correction; this line
      // keeps the concentration visible (entry brake at cluster_exposure_cap).
      if(rb.corr_clusters&&rb.corr_clusters.length){
        var ccl=rb.corr_clusters.map(function(c){return escapeHtml(String(c.cluster||'').replace(/^k(cl|ev):/,''))+' <b>'+mny(c.stake||0)+'</b> ('+(c.lots||0)+' lots)';}).join(' · ');
        h+='<div class="muted" style="font-size:11.5px;margin:0 0 6px" title="open stake summed across correlated variants of one underlying — capped at entry by cluster_exposure_cap ('+mny((rb.corr_clusters[0]&&rb.corr_clusters[0].cap)||50)+')">⛓ correlated clusters: '+ccl+'</div>';
      }
      if(rb.equity_series&&rb.equity_series.length>1){
        h+='<div class="muted" style="font-size:11px;margin:0 0 2px">equity curve · net since epoch start</div><div id="rfEqChart" style="height:90px;max-width:680px;margin-bottom:8px"></div>';
        rfSeries=rb.equity_series.map(function(p){return {ts:new Date((p[0]||0)*1000).toISOString(),pnl:(p[1]||0)};});
      }
    }
    }catch(_e4){h=h.slice(0,_ck4)+mlSecErr('RawFlow book',_e4);}
    // R106 (operator): WEATHER BOOK panel — the wxedge paper book gets the same ML-tab visibility
    // as RawFlow: equity, net, closed, open, win rate, its tunable band + rules one-liner, and an
    // equity sparkline. Reads the /api/ml "weather" block (weatherMLPayload).
    var wxSeries=null;
    var _ck4b=h.length;try{
    if(d&&d.weather&&d.weather.enabled!==false){
      var wb=d.weather;
      h+='<div style="margin:16px 0 6px;padding-top:10px;border-top:1px solid var(--line)"><b>⛅ Weather book</b> <span class="muted" title="'+escapeHtml(wb.rules||'')+'">· wxedge signals (NWS-anchored temp ladders) · band '+escapeHtml(wb.band||'20–40¢')+' (tunable) · realized-only Kelly on own equity · no ML · one lot per market · rides to settlement (ratio stop frozen at entry)</span></div>';
      var wn=(wb.closed||0);var wwr=(wn>0)?(Math.round((wb.win_rate||0)*100)+'%'):'—';
      h+='<div style="display:flex;gap:12px;flex-wrap:wrap;font-size:12.5px;margin-bottom:6px"><span title="cash + open lots at cost">Equity <b>'+mny(wb.equity!=null?wb.equity:(wb.bank||0))+'</b></span><span title="net since the epoch start — lifetime '+mny(wb.net_lifetime!=null?wb.net_lifetime:(wb.net||0))+' rides in the book file">Net <b style="color:'+nc(wb.net)+'">'+mny(wb.net||0)+'</b></span><span title="settled lots this epoch — lifetime '+(wb.closed_lifetime!=null?wb.closed_lifetime:wn)+'">Closed <b>'+wn+'</b></span><span>Open <b>'+(wb.open||0)+'</b></span><span>Win <b>'+wwr+'</b></span><span class="muted">deployed '+mny(wb.open_cost||0)+' · bank '+mny(wb.bank||0)+'</span></div>';
      if(wb.equity_series&&wb.equity_series.length>1){
        h+='<div class="muted" style="font-size:11px;margin:0 0 2px">equity curve · net since epoch start</div><div id="wxEqChart" style="height:90px;max-width:680px;margin-bottom:8px"></div>';
        wxSeries=wb.equity_series.map(function(p){return {ts:new Date((p[0]||0)*1000).toISOString(),pnl:(p[1]||0)};});
      }
    }
    }catch(_e4b){h=h.slice(0,_ck4b)+mlSecErr('Weather book',_e4b);}
    // R117 (promotion pipeline): FRESHINV BOOK panel — the freshlist-fade book (promotion #1)
    // gets the same ML-tab visibility. Reads the /api/ml "freshinv" block (freshInvMLPayload).
    var fiSeries=null;
    var _ck4c=h.length;try{
    if(d&&d.freshinv&&d.freshinv.enabled!==false){
      var fb2=d.freshinv;
      h+='<div style="margin:16px 0 6px;padding-top:10px;border-top:1px solid var(--line)"><b>🌱 Fresh-list book</b> <span class="muted" title="'+escapeHtml(fb2.rules||'')+'">· Kalshi fades · PolyUS follows · same-venue evidence and allocation · paper only</span></div>';
      var fn2=(fb2.closed||0);var fwr2=(fn2>0)?(Math.round((fb2.win_rate||0)*100)+'%'):'—';
      h+='<div style="display:flex;gap:12px;flex-wrap:wrap;font-size:12.5px;margin-bottom:6px"><span title="cash + open lots at cost">Equity <b>'+mny(fb2.equity!=null?fb2.equity:(fb2.bank||0))+'</b></span><span title="net since the epoch start — lifetime '+mny(fb2.net_lifetime!=null?fb2.net_lifetime:(fb2.net||0))+' rides in the book file">Net <b style="color:'+nc(fb2.net)+'">'+mny(fb2.net||0)+'</b></span><span title="settled lots this epoch — lifetime '+(fb2.closed_lifetime!=null?fb2.closed_lifetime:fn2)+'">Closed <b>'+fn2+'</b></span><span>Open <b>'+(fb2.open||0)+'</b></span><span>Win <b>'+fwr2+'</b></span><span class="muted">deployed '+mny(fb2.open_cost||0)+' · bank '+mny(fb2.bank||0)+(fb2.winding_down?' · winding down':'')+'</span></div>';
      if(fb2.equity_series&&fb2.equity_series.length>1){
        h+='<div class="muted" style="font-size:11px;margin:0 0 2px">equity curve · net since epoch start</div><div id="fiEqChart" style="height:90px;max-width:680px;margin-bottom:8px"></div>';
        fiSeries=fb2.equity_series.map(function(p){return {ts:new Date((p[0]||0)*1000).toISOString(),pnl:(p[1]||0)};});
      }
    }
    }catch(_e4c){h=h.slice(0,_ck4c)+mlSecErr('FreshInv book',_e4c);}
    // R63 3e: the Top-scored list renders into #mlTopScored (its own container → widget-adoptable).
    var h2='';
    var _ck5=h2.length;try{
    if(pr){var _prov=pr.metric_scope==='provisional_paper',_qa=_prov?pr.provisional_auc:pr.oos_auc,_qb=_prov?pr.provisional_brier:pr.oos_brier,_ql=_prov?pr.provisional_log_loss:pr.oos_log_loss,_qe=_prov?pr.provisional_ece:pr.oos_ece;
      h2+='<div style="margin:8px 0 4px"><b>Top scored open markets</b> <span class="muted">· '+ps.length+' actionable at the CURRENT book · '+(_prov?'Paper-provisional':'rolling-holdout')+' AUC '+(_qa!=null?_qa:"n/a")+' · Brier '+(_qb!=null?_qb:"n/a")+' · log loss '+(_ql!=null?_ql:"n/a")+' <span title="Expected Calibration Error: average probability gap; lower is better">· ECE '+(_qe!=null?_qe:"n/a")+'</span></span></div>';
      if(ps.length){h2+='<div style="max-height:46vh;overflow-y:auto;overflow-x:hidden"><table style="width:100%;border-collapse:collapse"><thead><tr style="color:var(--muted);font-size:11.5px;text-align:left"><th style="padding:3px 6px">Market</th><th>Type</th><th class="r" title="signal-time entry price → CURRENT venue price for the pick side (history column — the EV/ROI columns price at NOW)">@→Now</th><th class="r">p_win</th><th class="r" title="fee-NET EV per contract at the CURRENT live price (R98); ~ = no live quote this cycle, signal-time EV shown">EV/ct now</th><th class="r" title="EV now ÷ current price">ROI</th></tr></thead><tbody>';
        ps.forEach(function(x){ // R63 2b: FULL friendly title + explicit — YES/NO pick suffix · 7c: @→now¢
          var sd=String(x.side||'').toUpperCase();
          var pick=sd?(' <b style="color:'+((sd==='YES'||sd==='UP')?'var(--good)':'var(--bad)')+'">— '+escapeHtml(sd)+'</b>'):'';
          var now=(x.live_price!=null)?x.live_price:mlNowPx(x,pxm);
          var pxc='@'+Math.round((x.price||0)*100)+(now!=null?('→<b style="color:'+nc(now-(x.price||0))+'">'+Math.round(now*100)+'¢</b>'):'');
          var isLive=(x.ev_net_live!=null);
          var evShow=evLive(x), roiShow=isLive?(x.roi_live||0):(x.roi||0);
          h2+='<tr style="border-top:1px solid var(--line)"><td style="padding:3px 6px"><a class="go" href="'+escapeHtml(posURL(x))+'" target="_blank" rel="noopener">'+escapeHtml(x.title||x.ticker||"")+'</a>'+pick+invBandBadge(x)+'</td><td class="muted">'+escapeHtml(x.signal_type||"")+'</td><td class="r" style="white-space:nowrap">'+pxc+'</td><td class="r">'+(x.p_win||0).toFixed(2)+'</td><td class="r" style="color:'+nc(evShow)+'"'+(isLive?'':' title="no live quote this cycle — signal-time EV"')+'>'+(isLive?'':'~')+ev(evShow)+'</td><td class="r muted">'+Math.round(roiShow*100)+'%</td></tr>';});
        h2+='</tbody></table></div>';}else{h2+='<span class="muted">No +EV open markets right now.</span>';}
    }
    }catch(_e5){h2=h2.slice(0,_ck5)+mlSecErr('Top scored open markets',_e5);}
    // R72-A #3 MODEL-QUALITY TREND: per-retrain OOS AUC + Brier from ml_model_history.jsonl —
    // the drift chart + a latest-vs-7d-ago delta chip. Renders after the DOM lands (own fetch).
    h2+='<div style="margin:14px 0 4px;border-top:1px solid var(--line);padding-top:8px"><b>Model quality trend</b> <span class="muted">· one point per full retrain (walk-forward OOS) · AUC up = better ranking · Brier down = better calibration</span> <span id="mlHistDelta" style="font-size:12px"></span></div><div id="mlHistChart" style="height:110px;max-width:680px;margin-bottom:6px"><span class="muted">Loading…</span></div>';
    // R165: Paper-model calibration card. This grades forecasts, not exchange execution or profit.
    h2+='<div style="margin:14px 0 4px;border-top:1px solid var(--line);padding-top:8px"><b>🎯 Paper-model calibration (weekly)</b> <span class="muted">· settled simulation decisions only · useful for forecast calibration, not exchange-profit proof</span></div><div id="mlAccCard" style="max-width:680px;margin-bottom:6px"><span class="muted">Loading…</span></div>';
    document.getElementById("mlBody").innerHTML=h;
    var tsEl=document.getElementById('mlTopScored');if(tsEl)withScroll(tsEl,h2);
    // R105: chart draws individually guarded — a bad series must not kill the later charts (an
    // error here used to bubble into loadML's catch and blank the whole tab).
    try{if(eqSeries)renderPnLChart(eqSeries,'mlEqChart');}catch(_c1){}   // R63 3e: charts fit their real boxes post-insert
    try{if(shSeries)renderPnLChart(shSeries,'shadowEqChart');}catch(_c2){}
    try{if(kfSeriesP)renderPnLChart(kfSeriesP,'kfPreChart');}catch(_c3){} // R72-B: kflow twin book sparklines
    try{if(kfSeriesL)renderPnLChart(kfSeriesL,'kfLiveChart');}catch(_c4){}
    try{if(rfSeries)renderPnLChart(rfSeries,'rfEqChart');}catch(_c5){} // R105: rawflow sparkline
    try{if(wxSeries)renderPnLChart(wxSeries,'wxEqChart');}catch(_c5b){} // R106: weather sparkline
    try{if(fiSeries)renderPnLChart(fiSeries,'fiEqChart');}catch(_c5c){} // R117: freshinv sparkline
    try{renderMLHistory();}catch(_c6){} // R72-A #3: draws into #mlHistChart + #mlHistDelta
    try{renderMLAccuracy();}catch(_c7){} // R117 Part 4: real-bet accuracy card into #mlAccCard
  })();
}
// R72-A #3: the model-quality trend chart — AUC + Brier polylines (each on its own min/max scale,
// renderPnLChart-style hand-built SVG) + the latest-vs-7d-ago delta chip. Reads /api/mlhistory
// (the sidecar's ml_model_history.jsonl relayed by the Go side, loose-parsed).
function renderMLHistory(){
  var el=document.getElementById('mlHistChart');if(!el)return;
  jget('/api/mlhistory').then(function(d){
    var el2=document.getElementById('mlHistChart');if(!el2)return; // repaint may have replaced the DOM
    var dl=document.getElementById('mlHistDelta');
    var rows=((d&&d.rows)||[]).map(function(r){if(!r)return r;var x=Object.assign({},r);if(x.metric_scope==='provisional_paper'){x.oos_auc=x.provisional_auc;x.brier=x.provisional_brier;x.ece=x.provisional_ece;}return x;}).filter(function(r){return r&&r.ts&&(r.oos_auc!=null||r.brier!=null);})
      .sort(function(a,b){return (a.ts||0)-(b.ts||0);});
    if(rows.length<2){el2.innerHTML='<span class="muted" style="font-size:11.5px">Accrues one point per full retrain — needs ≥2 logged retrains.</span>';if(dl)dl.innerHTML='';return;}
    // delta chip BEFORE downsampling: latest vs the newest point ≥7d older (fallback: the oldest)
    if(dl){
      var last=rows[rows.length-1],base=rows[0];
      for(var bi=rows.length-2;bi>=0;bi--){if((last.ts-rows[bi].ts)>=604800){base=rows[bi];break;}}
      var parts=[];
      if(last.oos_auc!=null&&base.oos_auc!=null){var da=last.oos_auc-base.oos_auc;
        parts.push('AUC <b style="color:'+(da>=0?'var(--good)':'var(--bad)')+'">'+(da>=0?'+':'')+da.toFixed(3)+'</b>');}
      if(last.brier!=null&&base.brier!=null){var db=last.brier-base.brier;
        parts.push('Brier <b style="color:'+(db<=0?'var(--good)':'var(--bad)')+'">'+(db>=0?'+':'')+db.toFixed(4)+'</b>');}
      var span=Math.max(1,Math.round((last.ts-base.ts)/86400));
      dl.innerHTML=parts.length?('· vs '+span+'d ago: '+parts.join(' · ')+' <span class="muted">(n_resolved '+(last.n_resolved||0)+' · '+escapeHtml(last.cal_method||'')+' · '+escapeHtml(last.backend||'')+')</span>'):'';
    }
    if(rows.length>400){var st=Math.ceil(rows.length/400),ds=[];for(var i=0;i<rows.length;i+=st)ds.push(rows[i]);if(ds[ds.length-1]!==rows[rows.length-1])ds.push(rows[rows.length-1]);rows=ds;}
    var W=680,H=110,padL=40,padR=46,padT=14,padB=16;
    var mw=el2.clientWidth||0;if(mw>120)W=mw;
    var mh=el2.clientHeight||0;if(mh>48&&mh<W)H=mh;
    var xs=rows.map(function(r){return (r.ts||0)*1000;});
    var x0=Math.min.apply(null,xs),x1=Math.max.apply(null,xs);if(x1===x0)x1=x0+1;
    function X(t){return padL+(t-x0)/(x1-x0)*(W-padL-padR);}
    function scale(key){ // own min/max per metric (AUC ~0.5–0.7, Brier ~0.2 — one shared axis would flatline both)
      var vs=[];rows.forEach(function(r){if(r[key]!=null)vs.push(r[key]);});
      if(!vs.length)return null;
      var lo=Math.min.apply(null,vs),hi=Math.max.apply(null,vs);if(hi===lo){hi=lo+0.001;}
      var pd=(hi-lo)*0.12;lo-=pd;hi+=pd;
      return {lo:lo,hi:hi,Y:function(v){return H-padB-(v-lo)/(hi-lo)*(H-padT-padB);}};
    }
    function path(key,sc){var p='',started=false;rows.forEach(function(r){if(r[key]==null)return;p+=(started?'L':'M')+X((r.ts||0)*1000).toFixed(1)+' '+sc.Y(r[key]).toFixed(1)+' ';started=true;});return p;}
    var sa=scale('oos_auc'),sb=scale('brier');
    var s='<svg viewBox="0 0 '+W+' '+H+'" width="100%" height="100%" preserveAspectRatio="none" style="display:block;background:rgba(255,255,255,.025);border-radius:8px">';
    if(sa){s+='<path d="'+path('oos_auc',sa)+'" fill="none" stroke="#4ade80" stroke-width="1.8"/>';
      s+='<text x="4" y="'+(sa.Y(sa.hi)+9).toFixed(1)+'" fill="#4ade80" font-size="10">'+sa.hi.toFixed(3)+'</text>';
      s+='<text x="4" y="'+(sa.Y(sa.lo)-2).toFixed(1)+'" fill="#4ade80" font-size="10">'+sa.lo.toFixed(3)+'</text>';}
    if(sb){s+='<path d="'+path('brier',sb)+'" fill="none" stroke="#f59e0b" stroke-width="1.8" stroke-dasharray="5 3"/>';
      s+='<text x="'+(W-2)+'" y="'+(sb.Y(sb.hi)+9).toFixed(1)+'" fill="#f59e0b" font-size="10" text-anchor="end">'+sb.hi.toFixed(3)+'</text>';
      s+='<text x="'+(W-2)+'" y="'+(sb.Y(sb.lo)-2).toFixed(1)+'" fill="#f59e0b" font-size="10" text-anchor="end">'+sb.lo.toFixed(3)+'</text>';}
    var lr=rows[rows.length-1];
    s+='<text x="'+(padL+4)+'" y="11" font-size="10.5"><tspan fill="#4ade80">— AUC'+(lr.oos_auc!=null?(' '+lr.oos_auc.toFixed(3)):'')+'</tspan><tspan fill="#f59e0b" dx="10">-- Brier'+(lr.brier!=null?(' '+lr.brier.toFixed(3)):'')+'</tspan><tspan fill="#8a93a6" dx="10">'+rows.length+' v2 retrains'+(lr.metric_scope==='provisional_paper'?' · latest Paper-provisional':'')+'</tspan></text>';
    s+='</svg>';
    el2.innerHTML=s;
  }).catch(function(){var e2=document.getElementById('mlHistChart');if(e2)e2.innerHTML='<span class="muted">model history unavailable</span>';});
}
// Paper-model calibration card — /api/mlaccuracy (content-addressed current reset epoch).
// Headline numbers + calibration decile table (gap colored) + the plain-language sentence, with
// the sidecar's latest all-signals walk-forward numbers alongside for comparison.
function renderMLAccuracy(){
  var el=document.getElementById('mlAccCard');if(!el)return;
  jget('/api/mlaccuracy').then(function(d){
    var el2=document.getElementById('mlAccCard');if(!el2)return; // repaint may have replaced the DOM
    if(!d||d.ready===false||d.n_bets==null){el2.innerHTML='<span class="muted" style="font-size:11.5px">'+escapeHtml((d&&d.note)||'Not computed yet — first pass runs ~2 min after boot.')+'</span>';return;}
    var h='';
    if(d.plain_answer)h+='<div style="font-size:13px;margin:2px 0 6px"><b>'+escapeHtml(d.plain_answer)+'</b></div>';
    var bb=d.by_book||{};
    var f3=function(v,dp){return (v==null)?'—':Number(v).toFixed(dp==null?3:dp);};
    var als=d.allsignals||{};
    h+='<div style="display:flex;gap:14px;flex-wrap:wrap;font-size:12.5px;margin-bottom:6px">'
      +'<span title="rank-based AUC on settled Paper-model decisions — 0.5 = coin flip; this is calibration/ranking, not exchange profit">Paper AUC <b>'+f3(d.auc)+'</b><span class="muted"> vs all-signals '+f3(als.auc)+'</span></span>'
      +'<span title="Brier score on settled current-v2 Paper simulations; lower is better. No exchange fill is claimed.">Paper-model Brier <b>'+f3(d.book_v2_brier,4)+'</b><span class="muted"> n='+(d.book_v2_brier_n||0)+' · holdout '+f3(als.brier,4)+'</span></span>'
      +'<span title="Binary log loss on settled current-v2 Paper simulations; confident wrong forecasts are penalized heavily. No exchange fill is claimed.">Paper-model log loss <b>'+f3(d.book_v2_log_loss,4)+'</b><span class="muted"> n='+(d.book_v2_log_loss_n||0)+' · holdout '+f3(als.log_loss,4)+'</span></span>'
      +'<span title="Expected Calibration Error: n-weighted avg gap between stated probability and realized win rate (same formula as the sidecar) — lower = more honest confidence">ECE <b>'+f3(d.ece)+'</b><span class="muted"> vs all-signals '+f3(als.ece)+'</span></span>'
      +'<span title="plain mean |p − outcome| per bet — NOT comparable to Brier/ECE; retained as a secondary error measure">MAE <b>'+f3(d.mae)+'</b></span>'
      +'<span title="'+escapeHtml(d.icir_note||'')+'">ICIR <b>'+(d.icir!=null?f3(d.icir,2):'n/a')+'</b>'+(d.icir==null?'<span class="muted"> · '+escapeHtml(d.icir_note||'')+'</span>':'')+'</span>'
      +'</div>';
    var ds=d.disc||{},dh=ds.hi||{},dl2=ds.lo||{};
    h+='<div class="muted" style="font-size:12px;margin-bottom:6px">Confident picks (p&gt;60%): won <b style="color:var(--text)">'+Math.round((dh.win_rate||0)*100)+'%</b> of '+(dh.n||0)+' · long shots (p&lt;40%): won <b style="color:var(--text)">'+Math.round((dl2.win_rate||0)*100)+'%</b> of '+(dl2.n||0)+' — a real model shows a wide spread.</div>';
    var dec=d.deciles||[];
    if(dec.length){
      h+='<table style="border-collapse:collapse;font-size:12px"><thead><tr style="color:var(--muted);font-size:11px;text-align:left"><th style="padding:2px 8px 2px 0">Model said</th><th class="r" style="padding:2px 8px">n</th><th class="r" style="padding:2px 8px">predicted</th><th class="r" style="padding:2px 8px">actually won</th><th class="r" style="padding:2px 0 2px 8px" title="realized − predicted: negative = model overconfident in this band">gap</th></tr></thead><tbody>';
      dec.forEach(function(b){
        var g=(b.gap||0);
        h+='<tr style="border-top:1px solid var(--line)"><td style="padding:2px 8px 2px 0">'+Math.round((b.lo||0)*100)+'–'+Math.round((b.hi||0)*100)+'%</td><td class="r" style="padding:2px 8px">'+(b.n||0)+'</td><td class="r" style="padding:2px 8px">'+Math.round((b.pred||0)*100)+'%</td><td class="r" style="padding:2px 8px">'+Math.round((b.real||0)*100)+'%</td><td class="r" style="padding:2px 0 2px 8px;color:'+(Math.abs(g)<0.02?'var(--muted)':(g>0?'var(--good)':'var(--bad)'))+'">'+(g>=0?'+':'')+Math.round(g*100)+'pt</td></tr>';
      });
      h+='</tbody></table>';
    }
    // R118: genre × venue — placed bets vs the market-implied universe contrast
    var uni=d.universe||null,uc={},pc=d.placed_cells||[];
    if(uni&&uni.cells)uni.cells.forEach(function(c){uc[c.genre+'|'+c.venue]=c;});
    if(pc.length){
      var gp=function(c){var g=(c.gap||0);return '<span style="color:'+(Math.abs(g)<0.02?'var(--muted)':(g>0?'var(--good)':'var(--bad)'))+'">'+(g>=0?'+':'')+(g*100).toFixed(1)+'pt</span>';};
      h+='<div style="font-size:11.5px;color:var(--muted);margin:8px 0 2px"><b style="color:var(--text)">Where the gap lives (genre × venue)</b> — placed bets vs <span title="'+escapeHtml((uni&&uni.note)||'')+'">market prices (no model p, contrast only)</span></div>';
      h+='<table style="border-collapse:collapse;font-size:12px"><thead><tr style="color:var(--muted);font-size:11px;text-align:left"><th style="padding:2px 8px 2px 0">genre · venue</th><th class="r" style="padding:2px 8px">n rows</th><th class="r" style="padding:2px 8px">said→won</th><th class="r" style="padding:2px 8px">gap</th><th class="r" style="padding:2px 8px" title="descriptive separation inside this evaluation cohort; not exchange profit evidence">&gt;60% / &lt;40% win</th><th class="r" style="padding:2px 8px" title="pairwise AUC inside the cell">AUC</th><th class="r" style="padding:2px 0 2px 8px" title="same genre+venue across ALL resolved logged markets, prediction = market price">mkt gap (n)</th></tr></thead><tbody>';
      pc.forEach(function(c){
        var u=uc[c.genre+'|'+c.venue];
        h+='<tr style="border-top:1px solid var(--line)'+(c.thin?';opacity:.62':'')+'"><td style="padding:2px 8px 2px 0">'+escapeHtml(c.genre)+' · '+escapeHtml(c.venue)+(c.thin?' <span class="muted" style="font-size:10px">(thin n'+c.n+')</span>':'')+'</td>'
          +'<td class="r" style="padding:2px 8px">'+c.n+'</td>'
          +'<td class="r" style="padding:2px 8px">'+Math.round((c.pred||0)*100)+'%→'+Math.round((c.real||0)*100)+'%</td>'
          +'<td class="r" style="padding:2px 8px">'+gp(c)+'</td>'
          +'<td class="r" style="padding:2px 8px">'+(c.hi_n?Math.round((c.hi_wr||0)*100)+'% ('+c.hi_n+')':'—')+' / '+(c.lo_n?Math.round((c.lo_wr||0)*100)+'% ('+c.lo_n+')':'—')+'</td>'
          +'<td class="r" style="padding:2px 8px">'+(c.auc!=null?Number(c.auc).toFixed(3):'—')+'</td>'
          +'<td class="r" style="padding:2px 0 2px 8px">'+(u?gp(u)+' <span class="muted">('+u.n+')</span>':'—')+'</td></tr>';
      });
      h+='</tbody></table>';
    }
    if(uni)h+='<div class="muted" style="font-size:11.5px;margin-top:5px">Market-price baseline: across <b style="color:var(--text)">'+(uni.n||0)+'</b> resolved logged markets the price itself is calibrated to <b style="color:var(--text)">'+((uni.gap||0)>=0?'+':'')+((uni.gap||0)*100).toFixed(1)+'pt</b> (ECE '+f3(uni.ece,4)+', AUC '+f3(uni.auc)+') — no model probability exists there; it is a contrast, not model accuracy.</div>';
    if(d.truncation_note)h+='<div class="muted" style="font-size:10.5px;margin-top:4px">⚠ '+escapeHtml(d.truncation_note)+'</div>';
    h+='<div class="muted" style="font-size:11px;margin-top:4px">'+d.n_bets+' settled book-native-v2 evaluation rows · Paper simulation '+(bb.ml_paper||0)+' · maker-fill model '+(bb.maker_fills||0)+' · authenticated LIVE combos '+(bb.live_combos||0)+' · evidence classes remain separate · legacy/v1 excluded · refreshed weekly · as of '+escapeHtml(String(d.computed_at||''))+'</div>';
    el2.innerHTML=h;
  }).catch(function(){var e2=document.getElementById('mlAccCard');if(e2)e2.innerHTML='<span class="muted">book-native evaluation unavailable</span>';});
}
function closeCurves(){setTab(WTAB);}
function evSvg(ep){
  if(!ep||!ep.length)return '<span class="muted">No closed trades yet.</span>';
  var W=540,H=150,mx=0.001;ep.forEach(function(b){var a=Math.abs(b.ev||0);if(a>mx)mx=a;});
  var zy=H/2,s='<svg viewBox="0 0 '+W+' '+H+'" style="width:100%;max-width:'+W+'px;height:auto">';
  s+='<line x1="0" y1="'+zy+'" x2="'+W+'" y2="'+zy+'" stroke="var(--line)"/>';
  ep.forEach(function(b){var x=(b.lo||0)*W;var w=((b.hi||0)-(b.lo||0))*W*0.9;var hh=(Math.abs(b.ev||0)/mx)*(H/2-12);var y=((b.ev||0)>=0)?(zy-hh):zy;var col=((b.ev||0)>=0)?'var(--good)':'var(--bad)';s+='<rect x="'+x.toFixed(1)+'" y="'+y.toFixed(1)+'" width="'+w.toFixed(1)+'" height="'+hh.toFixed(1)+'" fill="'+col+'"><title>'+Math.round((b.lo||0)*100)+'-'+Math.round((b.hi||0)*100)+'c: modeled EV '+((b.ev||0)>=0?'+':'')+((b.ev||0)*100).toFixed(1)+'c/ct (n'+(b.n||0)+') · assumed-fill research</title></rect>';});
  s+='<text x="2" y="11" fill="var(--muted)" font-size="10">+'+(mx*100).toFixed(1)+'c</text><text x="2" y="'+(H-3)+'" fill="var(--muted)" font-size="10">cheap (0) -> expensive (1.00)</text></svg>';
  return s;
}
function calSvg(rel){
  if(!rel||!rel.length)return '<span class="muted">Needs the ML sidecar (build-suite.bat) + resolved signals.</span>';
  var S=210,m=26,P=S-m-6,s='<svg viewBox="0 0 '+S+' '+S+'" style="width:'+S+'px;height:'+S+'px">';
  s+='<line x1="'+m+'" y1="'+(S-m)+'" x2="'+(m+P)+'" y2="'+(S-m-P)+'" stroke="var(--line)" stroke-dasharray="3 3"/>';
  s+='<line x1="'+m+'" y1="6" x2="'+m+'" y2="'+(S-m)+'" stroke="var(--line)"/><line x1="'+m+'" y1="'+(S-m)+'" x2="'+(m+P)+'" y2="'+(S-m)+'" stroke="var(--line)"/>';
  var pts='';rel.forEach(function(b){var x=m+(b.pred||0)*P;var y=(S-m)-(b.actual||0)*P;pts+=x.toFixed(1)+','+y.toFixed(1)+' ';s+='<circle cx="'+x.toFixed(1)+'" cy="'+y.toFixed(1)+'" r="3" fill="var(--accent)"><title>pred '+(b.pred||0)+' -> actual '+(b.actual||0)+' (n'+(b.n||0)+')</title></circle>';});
  s+='<polyline points="'+pts+'" fill="none" stroke="var(--accent)" stroke-width="1.5"/>';
  s+='<text x="'+m+'" y="'+(S-4)+'" fill="var(--muted)" font-size="10">predicted prob -></text><text x="3" y="14" fill="var(--muted)" font-size="10">actual</text></svg>';
  return s;
}
function mcSvg(mc){
  if(!mc||!mc.hist)return '<span class="muted">No closed trades yet.</span>';
  var W=540,H=150,hist=mc.hist,mx=1;hist.forEach(function(c){if(c>mx)mx=c;});
  var lo=mc.lo||0,hi=mc.hi||1,rng=(hi-lo)||1,bw=W/hist.length;
  function vx(v){return ((v-lo)/rng)*W;}
  var s='<svg viewBox="0 0 '+W+' '+H+'" style="width:100%;max-width:'+W+'px;height:auto">';
  hist.forEach(function(c,i){var hh=(c/mx)*(H-28);var bl=lo+(i/hist.length)*rng;var col=(bl>=0)?'var(--good)':'var(--bad)';s+='<rect x="'+(i*bw).toFixed(1)+'" y="'+(H-20-hh).toFixed(1)+'" width="'+(bw*0.92).toFixed(1)+'" height="'+hh.toFixed(1)+'" fill="'+col+'" opacity="0.7"/>';});
  if(lo<0&&hi>0)s+='<line x1="'+vx(0).toFixed(1)+'" y1="0" x2="'+vx(0).toFixed(1)+'" y2="'+(H-18)+'" stroke="var(--fg)" stroke-dasharray="2 2"/>';
  ['p2_5','p50','p97_5'].forEach(function(k){if(mc[k]!=null)s+='<line x1="'+vx(mc[k]).toFixed(1)+'" y1="0" x2="'+vx(mc[k]).toFixed(1)+'" y2="'+(H-18)+'" stroke="'+(k==="p50"?'var(--accent)':'var(--muted)')+'"/>';});
  s+='<text x="2" y="'+(H-4)+'" fill="var(--muted)" font-size="10">$'+Math.round(lo)+'</text><text x="'+(W-58)+'" y="'+(H-4)+'" fill="var(--muted)" font-size="10">$'+Math.round(hi)+'</text>';
  s+='<text x="'+(W/2-92)+'" y="12" fill="var(--muted)" font-size="11">P(modeled profit) '+Math.round((mc.p_profit||0)*100)+'% · sim median $'+Math.round(mc.p50||0)+'</text></svg>';
  return s;
}
function vpill(v){
  var m={promote:['MODELED +','var(--good)'],retire:['MODELED −','var(--bad)'],track:['RESEARCH','var(--muted)']}[v||'track']||['RESEARCH','var(--muted)'];
  return '<span style="font-size:10px;font-weight:700;color:#0d1117;background:'+m[1]+';border-radius:3px;padding:1px 5px">'+m[0]+'</span>';
}
// SIGZS — R71/R73 shared honest zero-states: families whose "never logged" has a KNOWN
// config/condition cause. Used by the Curves grey rows, the Stats zero rows AND the R73
// Coverage matrix, so the explanation is identical everywhere.
var SIGZS={sharpline:'dormant — needs an Odds API key (Settings → odds_api_key); logs Pinnacle-vs-Kalshi deviations once set',
        fundtilt:'waiting for OKX perp funding ≥2bp off baseline on a 15m crypto coin',
        arb:'waiting for a cross-venue arb window (mirrors arb_log at the same floor)',
        xvlag:'waiting for a lead/lag move on a Kalshi↔PolyUS matched pair',
        xvgap:'waiting for a Kalshi↔PolyUS matched game priced ≥3¢ apart (R93 log-only)',
        meanrev:'waiting for a sharp spike to a price extreme on a matched pair',
        kflow:'new family (R71) — accrues from Kalshi flow-consensus rows within minutes of a restart'};
function srcBars(rows,splits){
  rows=rows||[]; // R63 3c: never hide a signal — SIGNAMES entries with no closed trades render as n=0 TRACK rows below
  // R73 NEVER-BLANK GUARD (same pattern as statsBySource/_lastBSRows): a mid-rebuild empty
  // by_source payload used to zero the bars AND the Total (tradeable) row for a refresh cycle —
  // cache the last non-empty rows+splits and serve those instead.
  if(rows&&rows.length){window._lastSrcRows=rows;window._lastSrcSplits=splits||[];}
  else if(window._lastSrcRows&&window._lastSrcRows.length){rows=window._lastSrcRows;splits=(splits&&splits.length)?splits:(window._lastSrcSplits||[]);}
  // R63 8c: splits = by_source_venue rows (same verdict math per source×venue) → per-venue sub-rows.
  var subs={};(splits||[]).forEach(function(v){if(!v||!v.source)return;(subs[v.source]=subs[v.source]||[]).push(v);});
  var VORD={kalshi:0,polyus:1,polymarket:2};
  var mx=0.01,tot=0,totT=0;rows.forEach(function(r){var a=Math.abs(r.net||0);if(a>mx)mx=a;tot+=(r.net||0);if(r.platform!=='polymarket')totT+=(r.net||0);});
  var h='<div class="muted" style="font-size:11px;margin-bottom:6px"><b>PAPER SIMULATION.</b> All dollars and labels in this block are modeled results, not booked exchange P&amp;L. They cannot promote, retire, size, or authorize LIVE.</div><div style="display:flex;flex-direction:column;gap:4px">';
  rows.forEach(function(r){
    var net=r.net||0;var pct=Math.min(100,Math.abs(net)/mx*100);var col=(net>=0)?'var(--good)':'var(--bad)';
    var ci='90% research interval of modeled Paper net/trade ['+(r.ci_lo!=null?r.ci_lo.toFixed(2):'?')+', '+(r.ci_hi!=null?r.ci_hi.toFixed(2):'?')+']; not exchange proof';
    var research=(r.platform==='polymarket')?' <span title="poly-int — READ-ONLY research venue: this P&L is not attainable; it exists to feed bridge signals">📡</span>':'';
    h+='<div style="display:flex;align-items:center;gap:8px;font-size:12px'+(r.platform==='polymarket'?';opacity:0.55':'')+'">'
      +'<div style="width:200px;text-align:right;color:var(--muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="'+(r.source||'')+'">'+escapeHtml(srcLbl(r.source||'?'))+research+'</div>'
      +'<div style="flex:1;background:#0d1117;border-radius:3px;height:14px"><div style="width:'+pct.toFixed(1)+'%;height:14px;background:'+col+';border-radius:3px"></div></div>'
      +'<div style="width:74px;text-align:center" title="'+ci+'">'+vpill(r.verdict)+'</div>'
      +'<div style="width:104px;text-align:right;color:'+col+'">'+(net>=0?'+':'')+'$'+net.toFixed(2)+' <span class="muted" style="font-size:10px">n'+(r.n||0)+'</span></div>'
      +'<div style="width:74px;text-align:right;color:'+col+'" title="modeled Paper net per closed simulation row; not an exchange edge">'+((r.n||0)>0?((net/r.n>=0?'+':'')+'$'+(net/r.n).toFixed(2)):'—')+'</div>'
      +'</div>';
    // R63 8c: venue sub-rows (kalshi → polyus → polyint) — verdict + net + n + net/bet per venue.
    (subs[r.source]||[]).slice().sort(function(a,b){return (VORD[a.platform]!=null?VORD[a.platform]:9)-(VORD[b.platform]!=null?VORD[b.platform]:9);}).forEach(function(v){
      var vnet=v.net||0;var vcol=(vnet>=0)?'var(--good)':'var(--bad)';
      h+='<div style="display:flex;align-items:center;gap:8px;font-size:11px;opacity:.72">'
        +'<div style="width:200px;text-align:right;color:var(--muted)" title="'+escapeHtml((r.source||'')+' on '+(v.platform||''))+'">↳ '+platLabel(v.platform)+'</div>'
        +'<div style="flex:1"></div>'
        +'<div style="width:74px;text-align:center">'+vpill(v.verdict)+'</div>'
        +'<div style="width:104px;text-align:right;color:'+vcol+'">'+(vnet>=0?'+':'')+'$'+vnet.toFixed(2)+' <span class="muted" style="font-size:10px">n'+(v.n||0)+'</span></div>'
        +'<div style="width:74px;text-align:right;color:'+vcol+'">'+((v.n||0)>0?((vnet/v.n>=0?'+':'')+'$'+(vnet/v.n).toFixed(2)):'—')+'</div></div>';
    });
  });
  // R63 3c: append every SIGNAMES entry with no closed trades yet — n=0, TRACK, never hidden.
  // R67k: each grey row explains its REAL zero-state per venue from the signal_log counts —
  // "logged N · resolved 0 — awaiting settlement" vs "never logged on this venue".
  (function(){
    var seenB={};rows.forEach(function(r){seenB[srcLbl(r.source||'?')]=1;});
    if(typeof SIGNAMES==='undefined')return;
    var counts=window._sigLogCounts||[];
    Object.keys(SIGNAMES).forEach(function(k){
      var lbl=SIGNAMES[k];if(seenB[lbl])return;seenB[lbl]=1;
      var cs=counts.filter(function(c){return c&&c.signal_type===k;});
      // R71: honest zero-states (SIGZS, shared with Stats + the Coverage matrix) for families whose
      // "never logged" has a KNOWN config/condition cause — sharpline in particular is dormant
      // without an odds_api_key, which "n=0" alone never said.
      var tip,lg=0,rs=0;
      if(cs.length){
        tip=cs.map(function(c){lg+=(c.logged||0);rs+=(c.resolved||0);
          return platLabel(c.platform)+': logged '+(c.logged||0)+' · resolved '+(c.resolved||0)+(((c.resolved||0)===0&&(c.logged||0)>0)?' — awaiting settlement':'');
        }).join('\n');
      }else{tip='never logged on any venue'+(SIGZS[k]?(' — '+SIGZS[k]):'');}
      // R73 zero-state wording: say WHY there are no paper trades — the family logs for research
      // (log-only / research venue / retired) with placement off, not because it's broken.
      var note=cs.length?((rs===0&&lg>0)?('logged '+lg+' · awaiting settlement'):('logged '+lg+' · resolved '+rs+' · no paper trades (log-only/research/retired — placement off)')):(k==='sharpline'?'needs API key':'never logged');
      h+='<div style="display:flex;align-items:center;gap:8px;font-size:12px;opacity:.5">'
        +'<div style="width:200px;text-align:right;color:var(--muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="'+escapeHtml(k+' — no closed paper trades yet\n'+tip)+'">'+escapeHtml(lbl)+'</div>'
        +'<div style="flex:1;background:#0d1117;border-radius:3px;height:14px"></div>'
        +'<div style="width:74px;text-align:center">'+vpill('track')+'</div>'
        +'<div style="width:104px;text-align:right;color:var(--muted)"><span class="muted" style="font-size:10px" title="'+escapeHtml(tip)+'">'+escapeHtml(note)+'</span></div>'
        +'<div style="width:74px;text-align:right;color:var(--muted)">—</div></div>';
    });
  })();
  h+='<div style="display:flex;align-items:center;gap:8px;font-size:12px;border-top:1px solid var(--line);margin-top:3px;padding-top:4px">'
    +'<div style="width:128px;text-align:right"><b>Paper total (bettable venues)</b></div><div style="flex:1"></div><div style="width:74px"></div>'
    +'<div style="width:104px;text-align:right;color:'+(totT>=0?'var(--good)':'var(--bad)')+'" title="Kalshi + PolyUS Paper simulations only · assumed/modelled fills are not exchange P&amp;L"><b>'+(totT>=0?'+':'')+'$'+totT.toFixed(2)+'</b></div><div style="width:74px"></div></div>';
  h+='<div style="display:flex;align-items:center;gap:8px;font-size:11.5px;opacity:0.6">'
    +'<div style="width:128px;text-align:right">incl. 📡 poly-int</div><div style="flex:1"></div><div style="width:74px"></div>'
    +'<div style="width:104px;text-align:right;color:'+(tot>=0?'var(--good)':'var(--bad)')+'">'+(tot>=0?'+':'')+'$'+tot.toFixed(2)+'</div><div style="width:74px"></div></div>';
  h+='</div>';return h;
}
function slRender(sl){
  if(!sl||!sl.n){return '<span class="muted">Needs resolved signals with a logged post-entry path (BREADCRUMB) — fills in over a paper run.</span>';}
  function rowm(name,net,note){
    var col=(net>=0)?'var(--good)':'var(--bad)';var star=(name===sl.best);
    return '<div style="display:flex;align-items:center;gap:8px;font-size:12px'+(star?';font-weight:700':'')+'">'
      +'<div style="width:64px;color:var(--muted)">'+name+(star?' ★':'')+'</div>'
      +'<div style="flex:1;color:var(--muted);font-size:11px">'+(note||'')+'</div>'
      +'<div style="width:100px;text-align:right;color:'+col+'">'+(net>=0?'+':'')+'$'+net.toFixed(2)+'</div></div>';
  }
  var h='<div style="display:flex;flex-direction:column;gap:3px">';
  h+=rowm('ride',sl.ride||0,'hold to resolution');
  h+=rowm('offset',sl.offset||0,'SL −'+(sl.sl_cents||0)+'¢ from entry (global TP removed R100)');
  h+=rowm('abs',sl.abs||0,'SL exit level '+(sl.sl_cents||0)+'¢ (global TP removed R100)');
  h+=rowm('ratio',sl.ratio_best||0,'best r='+(sl.ratio_r||0)+' (= what auto picks)');
  h+='</div>';
  // R133: added exit shapes stay research-only. Selection happens on the older half and the
  // untouched newer-half delta is shown explicitly; no grid winner becomes an execution mode.
  var xr=sl.research||{};
  if(xr.n){
    h+='<div style="font-weight:700;margin:10px 0 2px;font-size:12px">Added exit shapes <span class="muted" style="font-weight:400">· research only · never auto-enabled</span></div>';
    h+='<div class="muted" style="font-size:11px;margin-bottom:4px">'+escapeHtml(xr.verdict||'')+'</div>';
    h+='<table class="mkt" style="font-size:11.5px"><thead><tr><th>Mode</th><th class="r">EV/ct</th><th class="r">vs ride</th><th class="r">newer vs ride</th><th class="r" title="timed net divided by entry dollars × held days; negative means faster turnover compounds a loss">$/capital-day</th></tr></thead><tbody>';
    (xr.grid||[]).slice(0,7).forEach(function(c){var ev=c.ev||0,dr=c.delta_ride_ev||0,nr=c.newer_delta_ride_ev||0,cd=c.return_per_capital_day||0;h+='<tr><td>'+escapeHtml(c.label||c.mode||'')+(c.holdout_pass?' ✓':'')+'</td><td class="r">'+(ev>=0?'+':'')+(ev*100).toFixed(2)+'¢</td><td class="r">'+(dr>=0?'+':'')+(dr*100).toFixed(2)+'¢</td><td class="r">'+(nr>=0?'+':'')+(nr*100).toFixed(2)+'¢</td><td class="r">'+(cd>=0?'+':'')+cd.toFixed(3)+'</td></tr>';});
    h+='</tbody></table><div class="muted" style="font-size:10.5px">top 6 added cells by all-sample EV + ride baseline · '+(xr.timed_n||0)+'/'+(xr.n||0)+' paths have an honest ≤6h capital clock · ✓ = newer holdout cleared, not a live verdict</div>';
  }
  // TP-LEVEL sweep: net at each absolute take-profit point (no stop; ride if not hit).
  if(sl.tp_grid&&sl.tp_grid.length){
    h+='<div style="font-weight:700;margin:10px 0 2px;font-size:12px">Take-profit point sweep <span class="muted" style="font-weight:400">· best '+(sl.tp_best?Math.round(sl.tp_best*100)+'¢':'ride')+'</span></div>';
    h+='<table class="mkt" style="font-size:11.5px"><thead><tr><th>TP @</th><th class="r">Net</th></tr></thead><tbody>';
    sl.tp_grid.forEach(function(g){var b=(Math.abs((sl.tp_best||0)-(g.tp||0))<1e-6);h+='<tr'+(b?' style="font-weight:700"':'')+'><td>'+Math.round((g.tp||0)*100)+'¢'+(b?' ★':'')+'</td><td class="r" style="color:'+((g.net||0)>=0?'var(--good)':'var(--bad)')+'">'+((g.net||0)>=0?'+':'')+'$'+(g.net||0).toFixed(2)+'</td></tr>';});
    h+='</tbody></table>';
  }
  // SCALE-OUT ladder sweep vs pure hold.
  if(sl.scale_grid&&sl.scale_grid.length){
    h+='<div style="font-weight:700;margin:10px 0 2px;font-size:12px">Scale-out (variable share-sell) vs hold <span class="muted" style="font-weight:400">· best: '+(sl.scale_best||'ride')+'</span></div>';
    h+='<table class="mkt" style="font-size:11.5px"><thead><tr><th>Ladder</th><th class="r">Net</th></tr></thead><tbody>';
    h+='<tr><td>ride (no scale-out)</td><td class="r" style="color:'+((sl.ride||0)>=0?'var(--good)':'var(--bad)')+'">'+((sl.ride||0)>=0?'+':'')+'$'+(sl.ride||0).toFixed(2)+'</td></tr>';
    sl.scale_grid.slice().sort(function(a,b){return (b.net||0)-(a.net||0);}).slice(0,12).forEach(function(g){h+='<tr><td>sell '+Math.round((g.frac||0)*100)+'% each +'+Math.round((g.step||0)*100)+'¢</td><td class="r" style="color:'+((g.net||0)>=0?'var(--good)':'var(--bad)')+'">'+((g.net||0)>=0?'+':'')+'$'+(g.net||0).toFixed(2)+'</td></tr>';});
    h+='</tbody></table><div class="muted" style="font-size:10.5px">top 12 of '+sl.scale_grid.length+' configs (8 gain-steps × 9 fractions) · full grid in export</div>';
  }
  h+='<div class="muted" style="font-size:11px;margin-top:4px">n='+sl.n+' resolved paths · ★ = highest modeled net · recorded market-price paths after simulated entry, with modeled exit fills, fees, and slippage. Research only; never an exchange-profit verdict.</div>';
  return h;
}
function kRender(kl){
  if(!kl||!kl.methods||!kl.methods.length){return '<span class="muted">Needs closed trades to compound through.</span>';}
  var best=null;kl.methods.forEach(function(m){if(!best||m.final>best.final)best=m;});
  var h='<div style="display:flex;flex-direction:column;gap:3px">';
  kl.methods.forEach(function(m){
    var star=(best&&m.method===best.method);var col=((m.mult||0)>=1)?'var(--good)':'var(--bad)';
    h+='<div style="display:flex;align-items:center;gap:8px;font-size:12px'+(star?';font-weight:700':'')+'">'
      +'<div style="width:70px;color:var(--muted)">'+m.method+(star?' ★':'')+'</div>'
      +'<div style="flex:1;color:var(--muted);font-size:11px">max DD '+Math.round((m.maxdd||0)*100)+'%</div>'
      +'<div style="width:128px;text-align:right;color:'+col+'">$'+Math.round(m.final||0)+' ('+(m.mult||0).toFixed(2)+'x)</div></div>';
  });
  h+='</div><div class="muted" style="font-size:11px;margin-top:4px">A hypothetical $1,000 compounded through '+(kl.n||0)+' closed Paper simulations · ★=highest simulated final equity · in-sample research only; it cannot set LIVE sizing.</div>';
  return h;
}
var CURVES_INFO={
 src:"<b>Modeled Paper net by signal</b> — simulated settled P/L after modeled fees. <b>MODELED +</b>, <b>MODELED −</b>, and <b>RESEARCH</b> describe this Paper cohort only. They are not promotion or retirement instructions, are not exchange profit evidence, and cannot size or authorize LIVE. <b>n</b> is the number of closed Paper-simulation rows.",
 ev:"<b>Modeled EV profile</b> — assumed-fill Paper profit per contract grouped by logged entry price. Bars above/below zero describe this replay only. Use it to form a hypothesis; an authenticated fill-conditioned exchange cohort must test that hypothesis before any money decision.",
 cal:"<b>Calibration</b> — checks if the ML's stated probabilities are honest, on data it never trained on. When it says 70% does it actually win ~70%? Dots ON the dashed diagonal = trustworthy. Dots below = overconfident (says 70, wins 50); above = underconfident. This is how we know the model isn't just bragging.",
 mc:"<b>Paper Monte Carlo</b> — re-deals closed Paper-simulation rows 2,000 times. <b>P(modeled profit)</b> is only the share of those simulated re-deals that ended green; it does not estimate the chance an exchange-executed strategy is profitable and cannot promote or authorize LIVE.",
 stop:"<b>SL/TP simulation</b> — replays stop styles on recorded market-price paths after a simulated entry. Exit touches, fills, fees, and slippage remain modeled. <b>★</b> marks the best result inside this research grid only; it never changes cash execution or proves an exchange edge.",
 kelly:"<b>Bet-sizing simulation</b> — compounds a hypothetical $1,000 through closed Paper rows under flat and Kelly-style rules. <b>★</b> and max drawdown describe this in-sample simulation only. It cannot choose or justify LIVE sizing; only separate authenticated exchange-fill evidence can do that."
};
function ci(k){var el=document.getElementById("curvesInfo");if(!el)return;if(el.getAttribute("data-k")===k&&el.style.display!=="none"){el.style.display="none";el.setAttribute("data-k","");return;}el.innerHTML=CURVES_INFO[k]||"";el.style.display="block";el.setAttribute("data-k",k);}
function secHead(t,sub,k){return '<div style="margin:16px 0 4px"><b>'+t+'</b> <span onclick="ci(\''+k+'\')" style="cursor:pointer;color:var(--accent);font-weight:bold" title="What is this? (tap)">ⓘ</span> <span class="muted">'+sub+'</span></div>';}
function loadCurves(){
  var pe=document.getElementById("curvesPlat");var plat=pe?pe.value:"";
  // R77 item 4: curvesBody is a permanent scaffold — status text goes to #cvStatus so a rebuild
  // never wipes the section divs the Research groups toggle.
  var stEl=document.getElementById("cvStatus");if(stEl)stEl.innerHTML='<span class="muted">Computing…</span>';
  Promise.all([fetch("/api/curves"+(plat?("?platform="+encodeURIComponent(plat)):"")).then(function(r){return r.json();}),
               fetch("/api/ml").then(function(r){return r.json();}).catch(function(){return null;}),
               fetch("/api/stoplab").then(function(r){return r.json();}).catch(function(){return null;}),
               fetch("/api/kellylab").then(function(r){return r.json();}).catch(function(){return null;})]).then(function(a){
    var d=a[0]||{},ml=a[1]||{},sl=a[2]||{},kl=a[3]||{},pr=ml&&ml.predictions;
    var st2=document.getElementById("cvStatus");
    if(d.building||(sl&&sl.building)||(kl&&kl.building)){ // R4: cold snapshots building — re-poll
      if(st2)st2.innerHTML='<span class="muted">Building snapshots (first open after start)… auto-refreshes.</span>';
      setTimeout(loadCurves,2500);return;
    }
    if(st2)st2.innerHTML='';
    window._sigLogCounts=d.sig_log_venue||window._sigLogCounts||[]; // R67k: per-venue logged/resolved context for grey rows
    function put(id,html){var el=document.getElementById(id);if(el)el.innerHTML=html;}
    // R23 (operator: "add ALL signals to net by signal"): the bars are REALIZED paper P&L by trade
    // source; #allSigEV below them is every LOGGED signal type ranked by net EV/ct × resolved n —
    // filled by loadAllSigEV (R77 item 3: own loader with building/error retries + SSE hook).
    put("cvSecSrc",secHead("Modeled Paper net by system","research labels only · per-venue Paper rows · not exchange profit evidence","src")+srcBars(d.by_source,d.by_source_venue)
      +'<div style="margin:4px 0 12px"><div class="muted" style="font-size:12px;margin-bottom:3px">All logged models · flat-stake signal-time EV/ct × resolved count (n≥20) — discovery diagnostic, not booked P&L</div><div id="allSigEV">Loading…</div></div>');
    loadAllSigEV();
    put("cvSecEV",secHead("Modeled EV profile","assumed-fill Paper EV/contract by logged entry price · research only","ev")+evSvg(d.ev_profile));
    var rel=(pr&&pr.reliability)||[];
    put("cvSecCal",secHead("Calibration","ML predicted vs actual (out-of-sample) · on the dashed diagonal = honest","cal")+calSvg(rel));
    put("cvSecMC",secHead("Paper Monte Carlo",((d.monte_carlo&&d.monte_carlo.n_trades)||0)+" closed simulation rows · modeled variance only","mc")+mcSvg(d.monte_carlo));
    put("cvSecStop",secHead("SL/TP simulation (stop lab)","modeled exits on recorded market-price paths after simulated entry","stop")+slRender(sl));
    put("cvSecKelly",secHead("Bet-sizing simulation (Kelly lab)","hypothetical compounding · never a LIVE sizing instruction","kelly")+kRender(kl));
  }).catch(function(){var st3=document.getElementById("cvStatus");if(st3)st3.innerHTML='<span style="color:var(--bad)">Could not load curves.</span>';});
}
// R77 item 3 FIX ("Total (tradeable): Loading…" stuck, 2nd report): the All-logged-signal-types
// block that renders directly under the Total (tradeable) rows fetched /api/backtest exactly ONCE —
// a cold {building:true} answer (or a mid-rebuild empty signals list, or one failed fetch) left
// "Loading…" on screen FOREVER: no building re-poll, no error retry, and the SSE "backtest" hint
// only fed loadStatsView. Now: dedicated loader with a last-non-empty cache painted first (stale
// beats spinner), building/error re-polls while the block is on screen, and the SSE hook below.
function loadAllSigEV(){
  var el=document.getElementById("allSigEV");if(!el)return;
  if(window._lastAllSig)el.innerHTML=window._lastAllSig;
  clearTimeout(window._allSigT);
  function again(ms){var e2=document.getElementById("allSigEV");if(e2){clearTimeout(window._allSigT);window._allSigT=setTimeout(loadAllSigEV,ms);}}
  fetch("/api/backtest").then(function(r){return r.json();}).then(function(bt){
    var el2=document.getElementById("allSigEV");if(!el2)return;
    if(bt&&bt.building){if(!window._lastAllSig)el2.innerHTML='<span class="muted">Building the model backtest snapshot… auto-refreshes.</span>';again(2500);return;}
    var rows=((bt&&bt.signals)||[]).filter(function(s){return s.ev_net!=null&&(s.ev_n||0)>=20;})
      .map(function(s){return {t:s.signal_type,ev:s.ev_net,n:s.ev_n,tot:s.ev_net*s.ev_n};})
      .sort(function(a,b){return b.tot-a.tot;});
    if(!rows.length){if(!window._lastAllSig)el2.innerHTML='<span class="muted">No resolved model EV yet (needs n≥20 in a model) — fills in as signals settle.</span>';return;}
    var mx=1e-9;rows.forEach(function(r){mx=Math.max(mx,Math.abs(r.tot));});
    var hh='';rows.forEach(function(r){var w=Math.max(2,Math.abs(r.tot)/mx*220);var c=r.tot>=0?'var(--good)':'var(--bad)';
      hh+='<div style="display:flex;align-items:center;gap:8px;margin:2px 0;font-size:12px"><span style="width:230px;text-align:right;color:var(--muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="'+escapeHtml(r.t)+'">'+escapeHtml(srcLbl(r.t))+'</span><div style="width:'+w.toFixed(0)+'px;height:12px;background:'+c+';border-radius:3px"></div><span style="color:'+c+'">'+(r.ev>=0?'+':'')+(r.ev*100).toFixed(1)+'¢/ct · n'+r.n+'</span></div>';});
    window._lastAllSig=hh;el2.innerHTML=hh;
  }).catch(function(){var e3=document.getElementById("allSigEV");if(e3&&!window._lastAllSig)e3.innerHTML='<span class="muted">model backtest fetch failed — retrying…</span>';again(4000);});
}
// R38 SINGLE RESEARCH TAB · R77 item 4 REORG: six groups — PERFORMANCE / EDGE / EXITS / BACKTEST /
// COVERAGE / LABS. Groups REPARENT the existing cards (ids + renderers untouched) and toggle .rsec
// SECTION visibility inside the multi-section bodies (curvesBody / replaycard) — presentation only,
// zero data loss. The full old→new map sits in the researchcard HTML comment.
function resSecVis(cardId,ids){ // show ONLY the listed .rsec sections inside a card (null = show all)
  var el=document.getElementById(cardId);if(!el)return;
  var secs=el.querySelectorAll('.rsec');
  for(var i=0;i<secs.length;i++){secs[i].style.display=(!ids||ids.indexOf(secs[i].id)>=0)?'':'none';}
}
function resChromeVis(on){ // the replay card's cardhead/inputs/info/tune-result belong to LABS only
  var el=document.getElementById('replaycard');if(!el)return;
  var cs=el.querySelectorAll('.rchrome');
  for(var i=0;i<cs.length;i++){cs[i].style.display=on?((cs[i].id==='tuneResult'&&!cs[i].innerHTML)?'none':''):'none';}
}
// resAdopt borrows a single-instance panel (e.g. the Stats #statsBacktest table) into a research
// slot; resReturnLoans gives it back. Widget adoption (.wadopt) always wins — same precedence rule
// as wAdoptSync.
function resAdopt(id,slotId){
  var el=document.getElementById(id),slot=document.getElementById(slotId);
  if(!el||!slot)return;
  if(el.parentNode&&el.parentNode.classList&&el.parentNode.classList.contains('wadopt'))return;
  if(!el.__rhp){el.__rhp=el.parentNode;el.__rhn=el.nextSibling;}
  if(el.parentNode!==slot)slot.appendChild(el);
}
function resReturnLoans(){
  ['statsBacktest'].forEach(function(id){
    var el=document.getElementById(id);if(!el||!el.__rhp)return;
    if(el.parentNode&&el.parentNode.id==='resBTslot'){
      try{if(el.__rhn&&el.__rhn.parentNode===el.__rhp)el.__rhp.insertBefore(el,el.__rhn);else el.__rhp.appendChild(el);}catch(e){}
    }
  });
}
function showResearchSub(sub){
  var alias={curves:'performance',horizon:'systems',backtest:'systems',replay:'labs'}; // old saved names keep working
  sub=alias[sub]||sub;
  if(['performance','edge','exits','systems','coverage','labs'].indexOf(sub)<0)sub='performance';
  window._resSub=sub;
  var G={
    performance:{host:'resBody',panels:['curvescard'],curves:['cvStatus','cvSecSrc','cvSecMC'],statics:['resAlloc'],load:function(){loadCurves();loadAlloc();}},
    edge:{host:'resBody',panels:['edgecard','curvescard','replaycard'],curves:['cvStatus','cvSecEV','cvSecCal'],replay:['rpStatus','rpSecMom','rpSecFeat'],load:function(){loadEdge();loadCurves();loadReplay();}},
    exits:{host:'resExitCmpHost',panels:['replaycard'],replay:['rpStatus','rpSecExitCmp'],statics:['resExits'],load:function(){loadExitLadder();loadReplay();}},
    systems:{host:'resBTHost',panels:['replaycard'],replay:['rpSecCombo'],statics:['resBT'],load:function(){loadLeaderboardBacktest();loadSystemsRegimes();loadParlayBacktest('replayParlay');}},
    coverage:{host:'resBody',panels:[],statics:['resCoverage'],load:function(){loadCoverage();loadResHealth();}},
    labs:{host:'resBody',panels:['replaycard','curvescard'],curves:['cvStatus','cvSecStop','cvSecKelly'],replay:['rpStatus','rpSecConc'],statics:['resLabs'],load:function(){loadReplay();loadCurves();loadHarness();}}
  };
  var g=G[sub];
  resReturnLoans(); // legacy signal diagnostics always stay under Stats
  ['resExits','resCoverage','resBT','resLabs','resAlloc'].forEach(function(k){var el=document.getElementById(k);if(el)el.style.display=(g.statics&&g.statics.indexOf(k)>=0)?'block':'none';});
  var all=['replaycard','curvescard','edgecard','horizoncard'];
  all.forEach(function(k){var el=document.getElementById(k);if(el)el.style.display='none';});
  var host=document.getElementById(g.host)||document.getElementById('resBody');
  (g.panels||[]).forEach(function(k){
    var el=document.getElementById(k);if(!el)return;
    if(el.parentNode!==host)host.appendChild(el); // reparent INTO the active group's host
    el.style.display='block';
    el.style.position='static';el.style.transform='none';el.style.width='100%';el.style.maxWidth='none';el.style.maxHeight='none';el.style.boxShadow='none';
  });
  resSecVis('curvescard',g.curves||null);
  resSecVis('replaycard',g.replay||null);
  resChromeVis(sub==='labs');
  ['performance','edge','exits','systems','coverage','labs'].forEach(function(k){var b=document.getElementById('resTab_'+k);if(b){b.style.fontWeight=(k===sub)?'700':'400';b.style.color=(k===sub)?'var(--text)':'';}});
  try{g.load();}catch(e){}
}
// R77 item 4: BACKTEST group loader for the borrowed #statsBacktest table — same endpoint + renderer
// (+ never-blank cache) as Stats; renderBacktest targets the id wherever it currently lives.
// R135: exact prospective, book-native, one-share profit-rate ranking. This intentionally does
// not read a bankroll or replay sizing input; portfolio allocation is a separate decision layer.
function systemLayerName(v){
  v=String(v||'').toLowerCase();
  if(v==='system')return 'system';
  if(v==='execution_route'||v==='taker'||v==='maker')return 'execution route';
  if(v==='observed_book')return 'observed book';
  if(v==='paper_simulation')return 'Paper simulation';
  if(v==='research_quote')return 'research simulation';
  if(v==='model'||v==='signal'||v==='invert')return 'model';
  return 'strategy';
}
function systemLayerBadge(v){
  var n=systemLayerName(v),ico=n==='model'?'🧠':(n==='execution route'?'🛣️':(n==='system'?'🌐':'⚙️'));
  var tip=n==='model'?'MODEL: estimates edge and emits signals':(n==='execution route'?'EXECUTION ROUTE: exchange order/fill evidence':(n==='observed book'?'OBSERVED BOOK: ask, visible depth and fee were seen; fill is simulated':(n==='Paper simulation'?'PAPER SIMULATION: simulated portfolio placement and settlement; not an exchange fill':(n==='research simulation'?'RESEARCH SIMULATION: counterfactual quote evidence; not an exchange fill':(n==='system'?'SYSTEM: the full model, signal, strategy, and route stack':'STRATEGY: the executable decision and risk policy')))));
  return '<span style="font-size:9px;font-weight:700;color:var(--muted);white-space:nowrap" title="'+tip+'">'+ico+' '+n.toUpperCase()+'</span>';
}
function loadLeaderboardBacktest(targetID){
  var el=document.getElementById(targetID||'resLeaderboardBT');if(!el)return;
  el.innerHTML='<span class="muted">Calculating one-share rates…</span>';
  r138Get('/api/systems-leaderboard').then(function(d){
    var rows=(d&&d.rows)||[],counts=(d&&d.counts)||{},runtime=(d&&d.variant_signal_runtime)||{};
    var runtimeLine='<b>Exact runtime paths:</b> '+Number(runtime.current_signal||0)+' signal waiting · '+Number(runtime.terminal||0)+' terminal · '+Number(runtime.excluded||0)+' explicitly excluded · '+Number(runtime.not_applicable||0)+' no current opportunity'+(runtime.signal_contracts!=null?(' / '+Number(runtime.signal_contracts)+' total paths'):'')+'<br><b>Paper execution:</b> '+Number(runtime.realistic_paper_contracts||0)+' delayed two-touch paths · '+Number(runtime.legacy_or_other_paper_contracts||0)+' legacy, route-specific, or not yet migrated'+(runtime.refresh_current===false?' · <span style="color:var(--warn)">tracker refresh stale</span>':'')+(runtime.as_of?(' · as of '+fmtClose(runtime.as_of)):'');
    var catalogHTML='<div class="r138hero">'+r147CatalogCard(counts,(d&&d.variant_signal_contracts)||{})+'<br><span class="muted">'+runtimeLine+'<br>Producer-capable means a named signal handoff exists; it still does not claim a fresh signal, positive economics, Paper permission, or LIVE authority. The table below is evidence/coverage, so its row count is not the catalog total.</span></div>';
    if(!rows.length){el.innerHTML=catalogHTML+'<span class="muted">No settled book-native one-share trials yet. Rows begin here only after an executable side ask and exact fee were captured.</span>';return;}
    function cents(v,dig){v=Number(v)||0;return (v>=0?'+':'')+(v*100).toFixed(dig==null?2:dig)+'¢';}
    function color(v){return (Number(v)||0)>=0?'var(--good)':'var(--bad)';}
    function proof(r){
      if(r.economics_ready===false)return r138State(r.collection_state||r.proof||'COLLECTING');
      var p=r.proof||'COLLECTING',c=p.endsWith('+')?'var(--good)':(p.endsWith('-')?'var(--bad)':'var(--muted)');
      var tier=String(r.evidence_tier||'unclassified'),fill=r.fill_conditioned===true?'yes':'no',profit=r.profit_evidence===true?'yes':'no',live=r.live_authorizes===true?'yes':'no';
      return '<span style="font-size:10px;font-weight:700;color:'+c+'" title="Evidence: '+escapeHtml(tier)+'. Fill-conditioned: '+fill+'. Profit evidence: '+profit+'. LIVE authority: '+live+'. '+escapeHtml(r.void_reason||'')+'">'+escapeHtml(p)+'</span><div class="muted" style="font-size:9px">'+escapeHtml(tier.replaceAll('_',' '))+' · fill-conditioned '+fill+' · profit evidence '+profit+' · LIVE '+live+'</div>';
    }
    var h=catalogHTML+'<table class="mkt" style="font-size:11.5px"><thead><tr><th># · system @ venue</th>'+
      '<th class="r" title="current execution-backed fee-net dollars per elapsed calendar day">$/d</th>'+
      '<th class="r" title="current execution-backed fee-net cents per share">¢/share</th>'+
      '<th class="r" title="SUM profit divided by SUM occupied share-days; exact open-to-resolution seconds, normalized to days, with a one-second guard">¢/share-day</th>'+
      '<th class="r">opps/day</th><th class="r">depth</th><th class="r" title="n = distinct settled venue+ticker contracts; repeated fills, shares, snapshots, and re-entries count once">n · open</th><th>proof</th></tr></thead><tbody>';
    rows.forEach(function(r,i){
      var fam=srcLbl(r.family||''),ven=platLabel(r.platform||''),layer=systemLayerBadge(r.layer||'observed_book');
      var routeSide=String(r.side||'').toUpperCase();if(routeSide==='YES'||routeSide==='NO'||routeSide==='BUNDLE')fam+=' ['+routeSide+']';
      var origin=systemLayerName(r.origin_layer||'model');
	  var routeName=String(r.route||'not-yet-executable');
      var econ=r.economics_ready!==false;
	  var profitEvidence=r.profit_evidence===true;
	  var contractN=Number(r.settled_unique_markets||0),openN=Number(r.open_unique_markets||0),shownMean=Number(r.mean_pc||0),packageUnit=String(r.economic_unit||'')==='package_unit';
	  var rateReady=Boolean(r.confidence_ready),rateLo=Number(r.net_per_calendar_day_lo||0),rateHi=Number(r.net_per_calendar_day_hi||0);
	  var rateCI=rateReady?('range '+r145NetD(rateLo)+'…'+r145NetD(rateHi)):'conservative bound collecting';
      var lock=r.venue_locked?' <span title="venue locked; cannot execute" style="font-size:10px">🔒</span>':'';
      var research=r.research_only?' <span title="research or simulation evidence; not exchange-fill proof and cannot authorize LIVE" style="font-size:10px">🧪 research-only</span>':'';
      var reason=r.collection_reason||'';
      var nf=r.native_collection||{},nfState=nf.collection_state||'',nfReason=nf.reason||'';
      var nfLine=nfState?('<div style="margin-top:2px">'+r138State(nfState)+' <span class="muted" style="font-size:9px">'+escapeHtml(nfReason)+(nf.last_input_ts?(' · last candidate '+fmtClose(nf.last_input_ts)):'')+(nf.last_economic_ts?(' · last ledger activity '+fmtClose(nf.last_economic_ts)):'')+'</span></div>'):'';
      var ph=r.promotion_handoff||'',pc=r.promotion_class||'';
      var phLine=pc?('<div class="muted" style="font-size:9px"><b>handoff:</b> '+escapeHtml(pc.replaceAll('_',' '))+' · '+escapeHtml(ph)+'</div>'):'';
	  h+='<tr style="border-top:1px solid var(--line)"><td title="'+escapeHtml((r.system_id||r.family||'')+' @ '+(r.platform||'')+' · origin '+origin+' · route '+routeName+(reason?(' · '+reason):'')+(nfReason?(' · '+nfReason):''))+'"><span class="muted">'+(i+1)+'</span> · '+layer+' '+escapeHtml(fam)+' <span class="muted">@ '+ven+' · from '+escapeHtml(origin)+' · '+escapeHtml(routeName)+'</span>'+lock+research+(reason?('<div class="muted" style="font-size:9px">'+escapeHtml(reason)+'</div>'):'')+phLine+nfLine+'</td>'+
        (!profitEvidence?'<td class="r muted">VOID<div style="font-size:9px">assumed-fill history</div></td>':(econ&&rateReady?('<td class="r" style="font-weight:700;color:'+color(rateLo)+'" title="point dollars/day '+r145NetD(r.net_per_calendar_day)+'">'+r145NetD(rateLo)+'<div class="muted" style="font-size:9px;font-weight:400">'+rateCI+'</div></td>'):(econ?'<td class="r muted">collecting<div style="font-size:9px">'+rateCI+'</div></td>':'<td class="r muted">Not measured<div style="font-size:9px">0 settled execution-backed rows</div></td>')))+
        (profitEvidence&&econ?('<td class="r" title="fee-net cents per exchange-filled share" style="color:'+color(shownMean)+'">'+cents(shownMean,1)+'</td>'):'<td class="r muted">VOID</td>')+
        (profitEvidence&&econ?('<td class="r" style="color:'+color(r.net_per_occupied_share_day)+'">'+cents(r.net_per_occupied_share_day)+'</td>'):'<td class="r muted">VOID</td>')+
        (econ?('<td class="r">'+Number(r.opportunities_per_day||0).toFixed(2)+'</td>'):'<td class="r muted">-</td>')+
        (econ?('<td class="r" title="share of trials whose top-of-book size was known">'+Math.round(Number(r.depth_known_share||0)*100)+'%</td>'):'<td class="r muted">-</td>')+
        (econ?('<td class="r" title="'+contractN+(packageUnit?' distinct graded package opportunities; repeated quote snapshots count once':' distinct settled venue+ticker contracts · '+Number(r.n||0)+' raw route receipts retained in diagnostics')+' · '+Number(r.tracked_days||0).toFixed(1)+' tracked days">n'+contractN+' '+r144SampleBucket(contractN)+(openN>0?' <span class="muted">· open '+openN+'</span>':'')+'</td>'):
          ('<td class="r" title="'+escapeHtml(r.collection_source||'collector inputs')+'">'+(Number(r.collection_current_cycle||0)>0?('latest cycle '+Number(r.collection_current_cycle||0)+'<div class="muted" style="font-size:9px">current candidates '+Number(r.collection_current_candidates||0)+' · cumulative economics not loaded here'+(Number(r.collection_alerts||0)>0?(' · alerts '+Number(r.collection_alerts||0)):'')+'</div>'):('inputs '+Number(r.collection_n||0)+'<div class="muted" style="font-size:9px">cycles '+Number(r.collection_cycles||0)+' · candidates '+Number(r.collection_candidates||0)+' · O'+Number(r.collection_open||0)+(Number(r.collection_alerts||0)>0?(' · alerts '+Number(r.collection_alerts||0)):'')+'</div>'))+'</td>'))+
        '<td>'+proof(r)+'</td></tr>';
    });
    var pb=((d||{}).promotion_bridge||{}).status||{};
    h+='</tbody></table><div class="muted" style="font-size:10.5px;margin-top:5px"><b>Every registered system stays visible in this expanded diagnostic.</b> Legacy one-share cents/share and dollars/day are VOID as profit evidence because they assumed a fill from visible depth. Their raw values remain in the API only for historical audit; this table hides them from current profit columns. Evidence tier, fill conditioning, profit-evidence status and LIVE authority are shown on every row. n is coverage—not proof that contracts are independent.<br><b>Promotion bridge:</b> '+escapeHtml(pb.state||'unavailable')+' · sealed '+Number(pb.sealed_eligible||0)+' · Paper accepted '+Number(pb.paper_accepted||0)+' · LIVE dispatched '+Number(pb.live_dispatched||0)+'. Paper simulation is not LIVE proof. Input/control rows can never promote.</div>';
    var ss=(d&&d.side_selection)||{},ssc=ss.registered_counts||ss.counts||{};
    h+='<div class="muted" style="font-size:10.5px;margin-top:5px"><b>YES/NO selector:</b> '+Number(ssc.true_two_side_selector||0)+' true two-side · '+Number(ssc.one_semantically_valid_side||0)+' one-side · '+Number(ssc.multi_leg||0)+' multi-leg · '+Number(ssc.router_or_data_control||0)+' router/data. True two-side means separate current YES/NO asks, depth, fees and side proof; choose the stronger positive lower bound or abstain. Opposite exposure remains blocked.</div>';
    var cv=(d&&d.coverage)||{},missing=[];((cv.strategies)||[]).forEach(function(x){if(!x.timed_one_share_ledger)missing.push(x);});
    var coverageN=cv.coverage_rows||0;
    h+='<details style="margin-top:6px"><summary class="muted" style="cursor:pointer">Coverage: '+(cv.timed||0)+'/'+coverageN+' route/evidence rows have a timed native-unit ledger · '+(cv.missing||0)+' visible but not proxied</summary><div class="muted" style="font-size:10.5px;margin-top:4px">'+(missing.length?missing.map(function(x){return systemLayerBadge(x.layer||x.group)+' '+escapeHtml(srcLbl(x.family||''))+' <span title="'+escapeHtml(x.note||'')+'">('+escapeHtml(x.group||'')+')</span>';}).join(' · '):'Every visible route/evidence row is timed.')+'</div></details>';
    el.innerHTML=h;
  }).catch(function(e){el.innerHTML='<span style="color:var(--bad)">'+escapeHtml((e&&e.message)||'Systems Leaderboard report unavailable.')+'</span><br><span class="muted">Core feeds continue independently; reopen to retry.</span>';});
}
function setSystemsRegimeWindow(w){
  if(['7d','30d','90d','all'].indexOf(w)<0)w='30d';
  window._systemsRegimeWindow=w;loadSystemsRegimes();
}
function loadSystemsRegimes(){
  var el=document.getElementById('resSystemsRegime');if(!el)return;
  var win=window._systemsRegimeWindow||'30d';
  if(!window._systemsRegimeLast)el.innerHTML='<span class="muted">Building native route regimes…</span>';
  jget('/api/systems-regimes').then(function(d){
    window._systemsRegimeLast=d;
    var rows=((d&&d.rows)||[]).filter(function(r){return r.window===win;});
    function signed(v,dig,suf){if(v==null)return '—';v=Number(v)||0;return (v>=0?'+':'')+v.toFixed(dig==null?2:dig)+(suf||'');}
    function cents(v,dig){return signed((Number(v)||0)*100,dig==null?2:dig,'¢');}
    function col(v){return (Number(v)||0)>0?'var(--good)':((Number(v)||0)<0?'var(--bad)':'var(--muted)');}
    var tabs='<div style="display:flex;gap:4px;margin:2px 0 7px">';
    ['7d','30d','90d','all'].forEach(function(w){tabs+='<button class="mini" onclick="setSystemsRegimeWindow(\''+w+'\')" style="font-weight:'+(w===win?'700':'400')+';color:'+(w===win?'var(--text)':'var(--muted)')+'">'+(w==='all'?'ALL':w.toUpperCase())+'</button>';});
    tabs+='</div>';
    if(!rows.length){el.innerHTML=tabs+'<span class="muted">No native '+escapeHtml(win)+' route rows yet.</span>';return;}
    var h=tabs+'<table class="mkt" style="font-size:11px"><thead><tr><th>system @ venue · route</th><th>state</th><th class="r">$/d</th><th class="r">¢/share</th><th class="r">fill</th><th class="r">depth</th><th class="r">n · open</th><th class="r">UTC blocks</th></tr></thead><tbody>';
    rows.slice(0,120).forEach(function(r){
      var st=r.state||'COLLECTING',sc=st==='LOWER-BOUND+'?'var(--good)':(st==='LOWER-BOUND-'?'var(--bad)':'var(--muted)');
	  var profitEvidence=r.profit_evidence===true;
	  if(!profitEvidence&&String(r.evidence||'').indexOf('assumed_fill')>=0)st='VOID ASSUMED-FILL HISTORY';
      var range=(r.net_per_day_lower==null||r.net_per_day_upper==null)?'day-bound collecting':(r145NetD(r.net_per_day_lower)+'…'+r145NetD(r.net_per_day_upper));
      var fill=r.fill_rate==null?'—':(Number(r.fill_rate)*100).toFixed(0)+'%';
      var depth=r.median_depth==null?'—':Number(r.median_depth).toFixed(1);
      var tags=(r.research_only?' 🧪':'')+(r.venue_locked?' 🔒':'')+(r.qualifies_for_review?' ✅ review':'');
      h+='<tr><td title="'+escapeHtml((r.source||'')+' · '+(r.evidence||'')+' · origin '+(r.origin_layer||''))+'"><b>'+escapeHtml(srcLbl(r.system||''))+'</b> <span class="muted">@ '+escapeHtml(platLabel(r.venue||''))+' · '+escapeHtml(r.route||'')+'</span>'+tags+'<div class="muted" style="font-size:9px">'+escapeHtml(String(r.evidence||'unclassified').replaceAll('_',' '))+' · fill-conditioned '+(r.fill_conditioned===true?'yes':'no')+' · profit evidence '+(profitEvidence?'yes':'no')+' · LIVE '+(r.live_authorizes===true?'yes':'no')+'</div></td>'+
        '<td><b style="color:'+sc+'">'+escapeHtml(st)+'</b></td>'+
        (profitEvidence?('<td class="r" style="color:'+col(r.net_per_day)+'"><b>'+r145NetD(r.net_per_day)+'</b><div class="muted" style="font-size:9px">'+range+'</div></td>'):'<td class="r muted">VOID</td>')+
        (profitEvidence?('<td class="r" style="color:'+col(r.mean_pnl_pc)+'">'+cents(r.mean_pnl_pc,1)+'</td>'):'<td class="r muted">VOID</td>')+'<td class="r">'+fill+'</td><td class="r">'+depth+'</td>'+
        '<td class="r">'+(r.settled||0)+' <span class="muted">· O'+(r.open||0)+'</span></td><td class="r">'+(r.utc_block_days||0)+'</td></tr>';
    });
    h+='</tbody></table>';
    var reviews=(d&&d.review_candidates_30d)||[];
    h+='<div class="muted" style="font-size:10.5px;margin-top:5px">'+rows.length+' '+escapeHtml(win)+' route rows · '+reviews.length+' current 30d research-review candidates. 🧪 = unfunded research; 🔒 = venue locked. Visible-depth capacity is deliberately omitted here because it is only a mechanical ceiling, not expected fillable profit.</div>';
    if(rows.length>120)h+='<div class="muted" style="font-size:10px">Showing the first 120 ranked rows; the API retains all rows.</div>';
    el.innerHTML=h;
  }).catch(function(){el.innerHTML='<span style="color:var(--bad)">Systems regimes failed to load.</span>';});
}
function loadResBTSignals(){
  fetch("/api/backtest").then(function(r){return r.json();}).then(function(d){
    if(d&&d.building){var bt=document.getElementById('resBT');if(bt&&bt.style.display!=='none')setTimeout(loadResBTSignals,2500);return;}
    renderBacktest((d&&d.signals)||[]);
  }).catch(function(){});
}
// R77 item 4: COVERAGE health strip — the signal-famine + naming components from /api/ready (the
// readiness dots' payload), so Coverage answers "is anything starving / serving raw names?" in place.
function loadResHealth(){
  var el=document.getElementById("resHealth");if(!el)return;
  jget("/api/ready").then(function(d){
    var c=(d&&d.components)||{};var out=[];
    [['signal_famine','famine'],['name_fallbacks','naming']].forEach(function(p){
      var v=c[p[0]];if(!v)return;
      out.push('<span style="white-space:nowrap"><span style="background:'+(v.ok?'var(--good)':'var(--bad)')+';display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:4px"></span><b>'+p[1]+'</b> <span class="muted">'+escapeHtml(v.detail||'')+'</span></span>');
    });
    el.innerHTML=out.length?out.join(' &nbsp;·&nbsp; '):'<span class="muted">no famine/naming components in /api/ready (older server)</span>';
  }).catch(function(){el.textContent="health check failed";});
}
// R77 item 4 LABS: first UI surface for /api/harness — the pre-registered honest-fill validation
// (fee-net EV t-LB, T−60s CLV, chronological halves, measured maker microstructure, kill criteria).
// The report existed server-side (and in exports) but rendered NOWHERE.
function loadHarness(){
  var el=document.getElementById("resHarnessBody");if(!el)return;
  fetch("/api/harness").then(function(r){return r.json();}).then(function(d){
    if(d&&d.building){el.textContent="Building the harness snapshot… auto-refreshes.";var lb=document.getElementById('resLabs');if(lb&&lb.style.display!=='none')setTimeout(loadHarness,2500);return;}
    var el2=document.getElementById("resHarnessBody");if(!el2)return;
    var h='';
    if(d&&d.note){h+='<div class="muted" style="font-size:11.5px;margin:2px 0 6px">'+escapeHtml(d.note)+'</div>';}
    var mf=(d&&d.maker_fills)||{};var mrow=[];
    [['kalshi','🟩 Kalshi'],['polyus','🇺🇸 Poly US']].forEach(function(v){
      var m=mf[v[0]];if(!m)return;
      mrow.push(v[1]+' fill '+Math.round((m.fill_rate||0)*100)+'% · adverse '+(((m.adverse_5m||0)*100).toFixed(1))+'¢ · n'+(m.attempts||0)+(m.kill?' <b style="color:var(--bad)">KILL</b>':''));
    });
    if(mrow.length){h+='<div style="font-size:12px;margin:0 0 6px"><b>Measured maker microstructure</b> · '+mrow.join(' &nbsp;·&nbsp; ')+'</div>';}
    var srcs=(d&&d.sources)||[];
    if(!srcs.length){h+='<span class="muted">No strategies with n≥10 resolved yet.</span>';}
    else{
      h+='<table class="mkt" style="font-size:11.5px"><thead><tr><th>Strategy</th><th>Verdict</th><th class="r">n</th><th class="r" title="rows priced at a REAL captured fill">fills</th><th class="r">days</th><th class="r">EV/ct</th><th class="r" title="one-sided 95% t lower bound">EV LB</th><th class="r" title="T−60s closing-line value">CLV</th><th class="r" title="older-half vs newer-half EV — divergence = regime artifact">old/new</th></tr></thead><tbody>';
      srcs.forEach(function(x2){
        var vd=x2.verdict||'';var vc=(vd==='ALIVE')?'var(--good)':((vd==='KILL')?'var(--bad)':'var(--muted)');
        var kt=(x2.kills&&x2.kills.length)?(' title="'+escapeHtml(x2.kills.join(' · '))+'"'):'';
        h+='<tr><td title="'+escapeHtml(x2.source||'')+'">'+escapeHtml(srcLbl(x2.source||''))+'</td><td'+kt+'><b style="color:'+vc+'">'+escapeHtml(vd)+'</b></td><td class="r">'+(x2.n||0)+'</td><td class="r">'+(x2.fill_priced_n||0)+'</td><td class="r">'+(x2.days||0)+'</td>'
          +'<td class="r" style="color:'+(((x2.ev_net_mean_pc||0)>=0)?'var(--good)':'var(--bad)')+'">'+(((x2.ev_net_mean_pc||0)>=0?'+':'')+((x2.ev_net_mean_pc||0)*100).toFixed(1))+'¢</td>'
          +'<td class="r" style="color:'+(((x2.ev_net_lb_pc||0)>=0)?'var(--good)':'var(--bad)')+'">'+(((x2.ev_net_lb_pc||0)>=0?'+':'')+((x2.ev_net_lb_pc||0)*100).toFixed(1))+'¢</td>'
          +'<td class="r">'+((x2.clv_n||0)>0?((((x2.clv_t60||0)>=0?'+':'')+((x2.clv_t60||0)*100).toFixed(1))+'¢ <span class="muted">n'+x2.clv_n+'</span>'):'—')+'</td>'
          +'<td class="r muted">'+(((x2.older_half_ev||0)*100).toFixed(1))+'/'+(((x2.newer_half_ev||0)*100).toFixed(1))+'¢</td></tr>';
      });
      h+='</tbody></table>';
    }
    el2.innerHTML=h;
  }).catch(function(){el.textContent="harness failed to load";});
}
// Research → PERFORMANCE allocation panel (/api/alloc) — five fixed portfolio banks plus
// the EV-share table the 30-min allocator applies to strategy stakes inside those portfolios
// (weight = 95% LOWER bound of realized net EV/ct at n≥30; explorers keep a small stake so data
// accrues; hard 50% cap on venues with 2+ enabled families; scales stake ONLY — every gate stays).
function loadAlloc(){
  var el=document.getElementById("resAllocBody");if(!el)return;
  jget("/api/alloc").then(function(d){
    var el2=document.getElementById("resAllocBody");if(!el2)return;
    if(!d){el2.textContent="no allocation data";return;}
    var b=d.budgets||{};
    var h='<div style="font-size:12.5px;margin:0 0 6px"><b>Legacy Paper allocation model · research only</b> · 🟩 Kalshi <b>$'+(b.kalshi||0).toFixed(0)+'</b> · 🇺🇸 PolyUS <b>$'+(b.polyus||0).toFixed(0)+'</b> · 🧩 System Combos <b>$'+(b.combos||0).toFixed(0)+'</b> · 🤖 ML <b>$'+(b.ml||0).toFixed(0)+'</b> · 🤖🎲 ML Combos <b>$'+(b.ml_combos||0).toFixed(0)+'</b> <span class="muted">— simulated bookkeeping, not exchange equity or profit evidence; never sizes or authorizes cash</span></div>';
    var rows=d.rows||[];
    if(!rows.length){h+='<span class="muted">No legacy Paper-allocation table yet — the research snapshot normally builds within ~30s of boot.</span>';}
    else{
      h+='<table class="mkt" style="font-size:11.5px"><thead><tr><th>Family</th><th>Venue</th><th class="r" title="closed Paper-simulation rows behind this research bound">n</th><th class="r" title="one-sided lower bound of modeled Paper net per contract; not exchange proof">Modeled LB/ct</th><th class="r" title="legacy simulated portfolio share; never a cash allocation">sim share</th><th class="r" title="legacy Paper-simulation multiplier; never applied to cash">sim ×stake</th><th>research state</th></tr></thead><tbody>';
      rows.forEach(function(x){
        var st=x.neutral?'<span class="muted" title="legacy Paper model is neutral; this says nothing about exchange profitability">MODEL NEUTRAL</span>':(x.explore?'<span style="color:var(--warn)" title="research sampling weight inside the Paper simulation only">RESEARCH SAMPLE</span>':'<span style="color:var(--good)" title="positive modeled Paper bound; not proven exchange profit and never a LIVE weight">MODELED WEIGHT</span>');
        h+='<tr><td title="'+escapeHtml(x.source||'')+'">'+escapeHtml(srcLbl(x.source||''))+'</td><td>'+platLabel(x.venue||'')+'</td><td class="r">'+(x.n||0)+'</td><td class="r" style="color:'+(((x.ev_lb_pc||0)>=0)?'var(--good)':'var(--bad)')+'">'+(((x.ev_lb_pc||0)>=0?'+':'')+((x.ev_lb_pc||0)*100).toFixed(1))+'¢</td><td class="r">'+Math.round((x.share||0)*100)+'%</td><td class="r"><b>×'+(x.mult||0).toFixed(2)+'</b></td><td>'+st+'</td></tr>';
      });
      h+='</tbody></table>';
    }
    if(d.built_at){h+='<div class="muted" style="font-size:11px;margin-top:4px">research table built '+escapeHtml(d.built_at)+' · refreshes every 30 min · legacy Paper weights only · cannot promote, size, or authorize LIVE</div>';}
    // R79 PLACEMENT POLICY table (rendered NEXT TO the allocation table, same 30-min cadence):
    // Direct route policy only. The synthetic inverse remains a diagnostic hypothesis and never
    // mutates Paper/LIVE; an opposite system needs its own current trigger and route proof.
    var pr=d.policy||[];
    if(pr.length){
      h+='<div style="font-weight:700;margin:16px 0 2px">Legacy Paper policy display <span class="muted" style="font-weight:400;font-size:12px">— modeled direct/inverse signal-row bounds · research history only · does not place or authorize an exchange order</span></div>';
      h+='<table class="mkt" style="font-size:11.5px"><thead><tr><th>Family</th><th>Paper-model state</th><th class="r" title="deduped resolved research rows behind the modeled bounds">n</th><th class="r" title="modeled maker-net EV/ct, direct side; assumed fill">Modeled EV/ct</th><th class="r" title="research lower bound on the direct assumed-fill model">direct model LB</th><th class="r" title="research lower bound on a synthetic opposite side; never an executable inverse">inverse model LB</th></tr></thead><tbody>';
      var stm={on:'<span style="color:var(--good);font-weight:700" title="legacy Paper-simulation route enabled; no cash authority">LEGACY PAPER ON</span>',
               inverted:'<span style="color:var(--bad);font-weight:700" title="legacy state normalized to retired; synthetic inverse cannot execute">LEGACY INVERT BLOCKED</span>',
               retired:'<span style="color:var(--bad);font-weight:700" title="modeled Paper bound was negative; research logging continues">MODELED OFF</span>',
               research:'<span class="muted" title="poly-int research strategy — data-only, never places (standing rule, unchanged)">📡 RESEARCH</span>'};
      var lbc=function(v){return '<span style="color:'+((v||0)>0?'var(--good)':'var(--bad)')+'">'+(((v||0)>=0?'+':'')+((v||0)*100).toFixed(1))+'¢</span>';};
      pr.forEach(function(x){
        var hasN=(x.n||0)>0;
        h+='<tr><td title="'+escapeHtml(x.family||'')+'">'+escapeHtml(srcLbl(x.family||''))+'</td><td>'+(stm[x.status]||escapeHtml(x.status||''))+'</td><td class="r">'+(x.n||0)+'</td><td class="r">'+(hasN?lbc(x.dir_ev_pc):'—')+'</td><td class="r">'+(hasN?lbc(x.dir_lb_pc):'—')+'</td><td class="r">'+(hasN?lbc(x.inv_lb_pc):'—')+'</td></tr>';
      });
      h+='</tbody></table>';
      if(d.policy_built_at){h+='<div class="muted" style="font-size:11px;margin-top:4px">legacy Paper-policy snapshot built '+escapeHtml(d.policy_built_at)+' · refreshes every 30 min · display/research state only; current cash authority ignores it</div>';}
    }else{
      h+='<div class="muted" style="font-size:11.5px;margin-top:10px">Legacy Paper-policy snapshot has not built yet. This empty research state does not default any cash strategy on.</div>';
    }
    el2.innerHTML=h;
  }).catch(function(){el.textContent="allocation failed to load";});
}
// R73 COVERAGE MATRIX loader: one glance answers "why is family X greyed?" — for every SIGNAMES
// family × venue it shows logged/resolved signal_log rows, closed paper trades, and a status chip:
// the realized PROMOTE/RETIRE/TRACK verdict where trades exist, LOG-ONLY where rows accrue with
// placement off, NEEDS KEY / never-logged (with the SIGZS reason) otherwise. Built ENTIRELY from
// the existing /api/curves payload (by_source_venue + R67k sig_log_venue) — no new endpoint.
function loadCoverage(){
  var el=document.getElementById("resCoverageBody");if(!el)return;el.textContent="Computing…";
  fetch("/api/curves").then(function(r){return r.json();}).then(function(d){
    if(d&&d.building){el.textContent="Building snapshot… auto-refreshes.";setTimeout(loadCoverage,2500);return;}
    var counts=d.sig_log_venue||window._sigLogCounts||[];if(counts.length)window._sigLogCounts=counts;
    var vens=['kalshi','polyus','polymarket'];
    var tradesBy={}; // family|venue -> {n,verdict,net}
    (d.by_source_venue||[]).forEach(function(v){
      if(!v||!v.source)return;
      var k=String(v.source).replace(/^auto-cons-/,'');
      tradesBy[k+'|'+(v.platform||'')]={n:v.n||0,verdict:v.verdict||'track',net:v.net||0};
    });
    var cBy={}; // family|venue -> {lg,rs}
    counts.forEach(function(c){if(!c)return;cBy[(c.signal_type||'')+'|'+(c.platform||'')]={lg:c.logged||0,rs:c.resolved||0};});
    function chip(t,bg,tip){return '<span title="'+escapeHtml(tip||'')+'" style="font-size:10px;font-weight:700;color:#0d1117;background:'+bg+';border-radius:3px;padding:1px 5px;cursor:default;white-space:nowrap">'+t+'</span>';}
    var h='<div class="muted" style="font-size:11.5px;margin:2px 0 8px">Cells: <b>L</b> logged signal rows · <b>R</b> resolved · <b>T</b> closed Paper-simulation rows. Chips are modeled research labels, never exchange-profit or LIVE verdicts. <b>LOG-ONLY</b> = rows accrue without Paper placement; <b>NEEDS KEY</b> = dormant until configured; <b>never</b> = never logged there. 📡 poly-int is research-only.</div>';
    h+='<table class="mkt" style="font-size:12px"><colgroup><col><col style="width:186px"><col style="width:186px"><col style="width:186px"></colgroup>'+
       '<thead><tr><th>Strategy</th><th>'+platLabel('kalshi')+'</th><th>'+platLabel('polyus')+'</th><th>'+platLabel('polymarket')+'</th></tr></thead><tbody>';
    Object.keys(SIGNAMES).forEach(function(k){
      var lbl=SIGNAMES[k];
      var home={kalshi:lbl.indexOf('🟩')>=0,polyus:lbl.indexOf('🇺🇸')>=0,polymarket:lbl.indexOf('📡')>=0};
      var anyHome=home.kalshi||home.polyus||home.polymarket;
      h+='<tr><td class="ell" title="'+escapeHtml(k+' — '+lbl)+'">'+escapeHtml(srcLbl(k))+'</td>';
      vens.forEach(function(vp){
        var c=cBy[k+'|'+vp]||null,t=tradesBy[k+'|'+vp]||null;
        var cell='';
        if(t&&t.n>0){
          cell='L '+(c?c.lg:0)+' · R '+(c?c.rs:0)+' · T '+t.n+' '+vpill(t.verdict);
        }else if(c&&c.lg>0){
          var why=(vp==='polymarket')?'research venue — placement off by design':'log-only / research / retired — placement off';
          cell='L '+c.lg+' · R '+c.rs+' · T 0 '+chip('LOG-ONLY','#6ba3f8','logged '+c.lg+' · resolved '+c.rs+' · no paper trades ('+why+')');
        }else if(k==='sharpline'&&(home[vp]||!anyHome)){
          cell='— '+chip('NEEDS KEY','var(--warn)',SIGZS.sharpline);
        }else if(home[vp]||!anyHome){
          cell='<span class="muted" title="'+escapeHtml('never logged on this venue'+(SIGZS[k]?(' — '+SIGZS[k]):' — the detector has not fired here yet'))+'" style="cursor:help">never</span>';
        }else{
          cell='<span class="muted" title="not a home venue for this strategy (bridged rows would still show)" style="opacity:.45">·</span>';
        }
        h+='<td>'+cell+'</td>';
      });
      h+='</tr>';
    });
    h+='</tbody></table>';
    el.innerHTML=h;
  }).catch(function(){el.innerHTML='<span style="color:var(--bad)">coverage matrix failed to load</span>';});
}
// R72-A #5 HORIZON: /api/horizon → one compact bar strip per book context (auto/ml/live) —
// net EV/ct per 0.25h resolve-horizon bucket up to 8h, config cutoffs as dashed markers.
function loadHorizon(){
  var el=document.getElementById("horizonBody");if(!el)return;el.textContent="Computing…";
  fetch("/api/horizon").then(function(r){return r.json();}).then(function(d){
    if(d&&d.building){el.textContent="Building snapshot… auto-refreshes.";setTimeout(loadHorizon,2500);return;}
    var cs=(d&&d.contexts)||[];
    if(!cs.length){el.innerHTML='<span class="muted">No resolved strategies yet.</span>';return;}
    var maxH=d.max_h||8,h='';
    cs.forEach(function(c){
      var bks=c.buckets||[];
      h+='<div style="margin:10px 0 2px"><b>'+escapeHtml(c.label||c.name||'')+'</b> <span class="muted">· '+(c.n||0)+' resolved in ≤'+maxH+'h</span></div>';
      if(!(c.n>0)){h+='<div class="muted" style="font-size:12px">no rows in this context yet</div>';return;}
      var W=760,H=96,padB=14,mx=0.001;
      bks.forEach(function(b){if((b.n||0)>0&&Math.abs(b.ev||0)>mx)mx=Math.abs(b.ev);});
      var zy=(H-padB)/2;
      var s='<svg viewBox="0 0 '+W+' '+H+'" style="width:100%;max-width:'+W+'px;height:auto;background:rgba(255,255,255,.025);border-radius:8px">';
      s+='<line x1="0" y1="'+zy+'" x2="'+W+'" y2="'+zy+'" stroke="rgba(255,255,255,.18)"/>';
      var bw=W/Math.max(1,bks.length);
      bks.forEach(function(b,i){
        if(!((b.n||0)>0))return;
        var hh=Math.abs(b.ev||0)/mx*(zy-10),y=((b.ev||0)>=0)?(zy-hh):zy,col=((b.ev||0)>=0)?'var(--good)':'var(--bad)';
        s+='<rect x="'+(i*bw+1).toFixed(1)+'" y="'+y.toFixed(1)+'" width="'+Math.max(bw-2,1).toFixed(1)+'" height="'+Math.max(hh,0.8).toFixed(1)+'" fill="'+col+'" opacity="0.85"><title>'+(b.lo||0).toFixed(2)+'–'+(b.hi||0).toFixed(2)+'h: '+(((b.ev||0)>=0?'+':'')+((b.ev||0)*100).toFixed(1))+'¢/ct net · n'+(b.n||0)+'</title></rect>';
      });
      (d.cutoffs||[]).forEach(function(co){ // the CURRENT config horizons (e.g. 4h auto / 2h crypto)
        if(!(co.h>0)||co.h>maxH)return;var x=co.h/maxH*W;
        s+='<line x1="'+x.toFixed(1)+'" y1="0" x2="'+x.toFixed(1)+'" y2="'+(H-padB)+'" stroke="#f59e0b" stroke-dasharray="4 3"/><text x="'+(x+3).toFixed(1)+'" y="10" fill="#f59e0b" font-size="9.5">'+co.h+'h '+escapeHtml(co.label||'')+'</text>';
      });
      for(var hx=0;hx<=maxH;hx+=2){var xx=hx/maxH*W;s+='<text x="'+(hx===0?2:xx).toFixed(1)+'" y="'+(H-3)+'" fill="#8a93a6" font-size="9.5"'+(hx>=maxH?' text-anchor="end"':'')+'>'+hx+'h</text>';}
      s+='<text x="'+(W-4)+'" y="10" fill="#8a93a6" font-size="9.5" text-anchor="end">±'+(mx*100).toFixed(1)+'¢</text></svg>';
      h+=s;
    });
    el.innerHTML=h;
  }).catch(function(){el.textContent="Could not load horizon.";});
}
function loadExitLadder(){
  var el=document.getElementById("resExitsBody");el.textContent="Building… (10-min cache; first build ~1min)";
  fetch("/api/exitladder").then(function(r){return r.json();}).then(function(d){
    if(d&&d.building){setTimeout(loadExitLadder,3000);return;}
    var ts=(d&&d.types)||[];
    if(!ts.length){el.innerHTML='<span class="muted">No post-entry paths yet — they accrue as positions breadcrumb (BREADCRUMB) + candle backfill runs.</span>';return;}
    var h='';
    if(d.summary){h+='<div style="font-size:12px;margin:2px 0 4px;font-weight:700;color:var(--good)" title="R67n: the single best exit cell across all strategies vs its ride baseline (n≥30 paths preferred)">📌 '+escapeHtml(d.summary)+'</div>';}
    h+='<div class="muted" style="font-size:11.5px;margin:2px 0 8px">'+escapeHtml(d.grid||'')+' · an exit only earns its place by BEATING ride (hold-to-settle)</div>';
    /* R73 (operator: "why so few strategies here?"): the ladder needs ≥8 REAL post-entry price paths
       per family — the count grows as paths accrue on resolved rows. */
    h+='<div class="muted" style="font-size:11.5px;margin:2px 0 8px"><b>'+ts.length+' strateg'+(ts.length===1?'y':'ies')+'</b> have ≥8 price-paths so far — strategies appear as price-paths accrue (R72 widened the candidate pool 1.6k → ~140k resolved rows; backfill + live breadcrumbs fill them in over days)</div>';
    ts.forEach(function(t){
      var rideC=(t.ride_ev>=0?'+':'')+((t.ride_ev||0)*100).toFixed(1)+'¢';
      h+='<div style="margin:12px 0 4px;border-top:1px solid var(--line);padding-top:8px"><b>'+escapeHtml(srcLbl(t.type))+'</b> <span class="muted">· n='+t.n+' paths · ride (baseline) <b style="color:'+((t.ride_ev||0)>=0?'var(--good)':'var(--bad)')+'">'+rideC+'</b>/ct</span></div>';
      h+='<table style="width:100%;border-collapse:collapse"><thead><tr style="color:var(--muted);font-size:11px;text-align:left"><th style="padding:2px 6px">Exit</th><th class="r">EV/ct</th><th class="r">vs ride</th><th class="r" title="capital velocity: avg hold-hours RETURNED to the bankroll vs riding — multiply by your redeploy edge; a tiebreaker between near-equal cells">hrs freed</th><th class="r">n</th></tr></thead><tbody>';
      (t.best||[]).forEach(function(c){
        var d1=(c.ev||0)-(t.ride_ev||0);
        h+='<tr style="border-top:1px solid var(--line)"><td style="padding:2px 6px">'+escapeHtml(c.exit)+'</td><td class="r" style="color:'+((c.ev||0)>=0?'var(--good)':'var(--bad)')+'">'+((c.ev>=0?'+':'')+(c.ev*100).toFixed(1))+'¢</td><td class="r" style="color:'+(d1>=0?'var(--good)':'var(--bad)')+'">'+((d1>=0?'+':'')+(d1*100).toFixed(1))+'¢</td><td class="r muted">'+((c.hrs_freed||0).toFixed(1))+'h</td><td class="r muted">'+c.n+'</td></tr>';
      });
      if(t.worst&&t.worst.exit){h+='<tr style="border-top:1px solid var(--line)"><td style="padding:2px 6px" class="muted">worst: '+escapeHtml(t.worst.exit)+'</td><td class="r" style="color:var(--bad)">'+((t.worst.ev>=0?'+':'')+(t.worst.ev*100).toFixed(1))+'¢</td><td></td><td></td><td class="r muted">'+t.worst.n+'</td></tr>';}
      h+='</tbody></table>';
    });
    el.innerHTML=h;
  }).catch(function(){el.innerHTML='<span style="color:var(--bad)">exit ladder failed to load</span>';});
}
// R7: grid-search the replay tunables server-side, show the top-5 surface, fill the inputs with the winner.
function autoTune(){
  var el=document.getElementById("tuneResult");el.style.display="block";el.innerHTML='<span class="muted">Searching the tunable grid against the walk-forward replay (30–90s, runs in background)…</span>';
  fetch("/api/replay/optimize").then(function(r){return r.json();}).then(function(d){
    if(d&&d.building){setTimeout(autoTune,3000);return;}
    if(!d||!d.best){el.innerHTML='<span style="color:var(--bad)">'+escapeHtml((d&&d.error)||"optimizer returned nothing")+'</span>';return;}
    var b=d.best;
    function put(id,v){var e=document.getElementById(id);if(e!=null&&v!=null)e.value=v;}
    put("replayPbcap",b.pbcap);put("replayMaxexp",b.maxexp);put("replayHold",b.maxhold_h);
    var fk=document.getElementById("replayFokm");if(fk)fk.checked=!!b.fokm;
    put("replayKelly",b.kelly_frac>0?b.kelly_frac:"");
    function cfgline(t){return 'cap '+t.pbcap+' · exp '+t.maxexp+' · '+t.maxhold_h+'h · '+(t.fokm?'FOKM (maker)':'taker')+' · '+escapeHtml(t.kelly_label||'flat')+' → <b style="color:var(--good)">'+(t.growth||0).toFixed(2)+'×</b>, DD '+Math.round((t.max_dd||0)*100)+'% ('+(t.bets||0)+' bets, score '+(t.score||0).toFixed(2)+')';}
    var h='<b>🎯 Best of '+d.tested+'/'+d.grid+' configs</b> <span class="muted">('+(d.elapsed_s||0)+'s · score = ln(growth) − 2.5·maxDD, walk-forward · near-ties prefer longer hold'+((d.fill_priced_rows||0)>0?(' · '+d.fill_priced_rows+' rows at REAL fill prices'):'')+')</span><br>';
    h+='<b>Concurrent</b> <span class="muted" title="many overlapping positions, each a fraction of CURRENT equity, reinvested — this is how the live bot actually behaves, so this winner DRIVES the live tunables">(drives live)</span>: '+cfgline(b)+'<br>';
    if(d.best_sequential){h+='<b>Sequential</b> <span class="muted" title="ONE bet at a time, whole-bankroll turnover, capacity+max-hold capped — the fast-crypto compounding read; reported only, does not change live settings">(read-only)</span>: '+cfgline(d.best_sequential)+'<br>';}
    if(d.refined){h+='<b>🔬 Refined</b> <span class="muted" title="coordinate descent from the coarse winner over ALL continuous dims (cap, exposure, hold, conf floor, Kelly) in shrinking steps — the objective is a noisy step function, so descent finds the plateau center ('+(d.refine_steps||0)+' extra sims)">(continuous)</span>: '+cfgline(d.refined)+'<br>';}
    if(d.holdout&&d.holdout.length){h+='<div class="muted" style="font-size:11.5px;margin:2px 0">Holdout (newest 30%, '+(d.test_rows||0)+' rows — the TEST winner is what gets applied): ';
      d.holdout.forEach(function(x,i){h+=(i?' · ':'')+escapeHtml(x.label||'')+' → '+(x.test_ok?('test '+(x.test_score!=null?x.test_score.toFixed(2):'—')+' ('+(x.test_growth||0).toFixed(2)+'×, DD '+Math.round((x.test_dd||0)*100)+'%, '+(x.test_bets||0)+' bets)'):'no test bets');});
      h+='</div>';}
    if(d.applied&&d.applied.kelly_frac!=null){h+='<div style="margin-top:3px;color:var(--good)">✓ Applied to live tunables: kelly_frac='+d.applied.kelly_frac+' · stake_pct='+d.applied.stake_pct+' · maker_first='+d.applied.maker_first+' · max-hold='+d.applied.consensus_max_hours_out+'h · max_exposure=$'+d.applied.max_exposure_usd+' <span class="muted">('+escapeHtml(d.applied_basis||'')+' · persisted to config; Settings shows them)</span></div>';}
    if((d.top||[]).length>1){h+='<div class="muted" style="margin-top:4px;font-size:11.5px">concurrent runners-up: ';
      d.top.slice(1).forEach(function(t,i){h+=(i?' · ':'')+'['+cfgline(t)+']';});
      h+=' <span title="a flat top-5 = robust surface; one spike = fragile, treat with suspicion">ⓘ</span></div>';}
    h+='<div class="muted" style="margin-top:2px;font-size:11.5px">Inputs filled — hit <b>Run</b> to see the full tables at this setting. A grid winner is a sane default, not a guarantee.</div>';
    el.innerHTML=h;loadAuto();
  }).catch(function(){el.innerHTML='<span style="color:var(--bad)">optimizer failed</span>';});
}
function closeReplay(){setTab(WTAB);}
function loadReplay(){
  var stEl=document.getElementById("replayStake");var stake=(stEl&&parseFloat(stEl.value))||10;
  var kEl=document.getElementById("replayKelly");var kel=(kEl&&parseFloat(kEl.value))||0;
  var cpEl=document.getElementById("replayCap");var cp=(cpEl&&parseFloat(cpEl.value))||0;
  var hdEl=document.getElementById("replayHold");var hd=(hdEl&&parseFloat(hdEl.value))||0;
  var pbEl=document.getElementById("replayPbcap");var pb=(pbEl&&parseFloat(pbEl.value))||0;
  var meEl=document.getElementById("replayMaxexp");var me=(meEl&&parseFloat(meEl.value))||0;
  var slEl=document.getElementById("replaySlip");var sl=(slEl?parseFloat(slEl.value):NaN);if(isNaN(sl))sl=-1;
  var fkEl=document.getElementById("replayFokm");var fk=(fkEl&&fkEl.checked)?1:0;
  var frEl=document.getElementById("replayFill");var fr=(frEl&&parseFloat(frEl.value))||0;
  var avEl=document.getElementById("replayAdv");var av=(avEl?parseFloat(avEl.value):NaN);if(isNaN(av))av=-1;
  var bkEl=document.getElementById("replayBank");var bk=(bkEl&&parseFloat(bkEl.value))||0;
  // R77 item 4: replayBody is a permanent scaffold — status into #rpStatus, sections into their
  // .rsec divs (conc → Labs, momentum + feature buckets → Edge, exit comparison → Exits).
  var rst=document.getElementById("rpStatus");if(rst)rst.innerHTML='<span class="muted">Running…</span>';
  fetch("/api/replay?stake="+encodeURIComponent(stake)+(kel>0?("&kelly="+encodeURIComponent(kel)):"")+(cp>0?("&cap="+encodeURIComponent(cp)):"")+(hd>0?("&maxhold="+encodeURIComponent(hd)):"")+(pb>0?("&pbcap="+encodeURIComponent(pb)):"")+(me>0?("&maxexp="+encodeURIComponent(me)):"")+(sl>=0?("&slip="+encodeURIComponent(sl)):"")+(fk?"&exec=fokm":"&exec=taker")+(fr>0?("&fillrate="+encodeURIComponent(fr)):"")+(av>=0?("&adverse="+encodeURIComponent(av)):"")+(bk>0?("&bank="+encodeURIComponent(bk)):"")).then(function(r){return r.json();}).then(function(d){
    var rst2=document.getElementById("rpStatus");
    if(d&&d.building){if(rst2)rst2.innerHTML='<span class="muted">Building snapshot… auto-refreshes.</span>';setTimeout(loadReplay,2500);return;}
    if(rst2)rst2.innerHTML='';
    function mny(v){v=v||0;return (v<0?'-$':'$')+Math.abs(v).toFixed(2);}
    function nc(v){return (v||0)>=0?'var(--good)':'var(--bad)';}
    function put(id,html){var el=document.getElementById(id);if(el)el.innerHTML=html;}
    var h='';
    if(d.exec){h+='<div class="muted" style="margin:0 0 6px;font-size:11.5px">Execution: <b style="color:var(--fg)">'+escapeHtml(d.exec)+'</b>'+(d.exec.indexOf("FOKM")>=0?(" · fill rate "+Math.round((d.fokm_fillrate||0)*100)+"% · adverse "+Math.round((d.fokm_adverse||0)*100)+"% (losers fill more) · maker fee, no slippage"):(" · slippage "+(d.slip_cents||0).toFixed(1)+"¢ · taker fee"))+'</div>';}
    var ks=d.kelly_sweep||[];
    if(ks.length){
      h+='<div style="margin:2px 0 4px"><b>Concurrent portfolio (realistic)</b> <span class="muted">· many bets at once, each a Kelly fraction of CURRENT equity (REINVESTED) · per-bet cap '+(d.conc_pbcap||0.1)+' · max exposure '+(d.conc_maxexp||0.6)+' · walk-forward · fees · $'+Math.round(d.kelly_start||500)+' start → how the live bot behaves</span></div>';
      h+='<table style="width:100%;border-collapse:collapse;margin-bottom:8px"><thead><tr style="color:var(--muted);font-size:11.5px;text-align:left"><th style="padding:4px 6px">Sizing</th><th class="r">Final</th><th class="r">Growth</th><th class="r">Max DD</th><th class="r">Bets</th><th class="r">Days</th></tr></thead><tbody>';
      ks.forEach(function(s){
        h+='<tr style="border-top:1px solid var(--line)"><td style="padding:4px 6px">'+escapeHtml(s.label)+'</td>'+
          '<td class="r" style="color:'+nc((s.final||0)-(d.kelly_start||1000))+'">'+mny(s.final)+'</td>'+
          '<td class="r" style="font-weight:700;color:'+nc((s.growth||0)-1)+'">'+(s.growth||0).toFixed(2)+'x</td>'+
          '<td class="r" style="color:var(--bad)">'+Math.round((s.max_dd||0)*100)+'%</td>'+
          '<td class="r"><span class="muted">'+(s.bets||0)+'</span></td>'+
          '<td class="r"><span class="muted">'+(s.days||0).toFixed(1)+'</span></td></tr>';
      });
      h+='</tbody></table>';
    }
    // R12: sequential tunables sit RIGHT ABOVE the sequential table (mirrors of the top inputs).
    var sq=d.seq_compound||[];
    if(sq.length){
      h+='<div style="margin:10px 0 2px;padding:6px 9px;background:var(--card2);border:1px solid var(--line);border-radius:8px;display:flex;gap:8px;align-items:center;flex-wrap:wrap;font-size:12.5px">'
        +'<b>Sequential tunables</b>'
        +'<span class="muted">capacity $/bet</span><input type="number" min="0" step="100" value="'+(d.seq_cap||3000)+'" style="width:74px" onchange="document.getElementById(\'replayCap\').value=this.value;loadReplay()">'
        +'<span class="muted">max-hold h</span><input type="number" min="0.05" step="0.25" value="'+(d.seq_maxhold||0.12)+'" style="width:64px" onchange="document.getElementById(\'replayHold\').value=this.value;loadReplay()">'
        +'<span class="muted" title="these are the same two knobs as the top strip — edits here re-run the replay">synced with the top inputs</span></div>';
    }
    if(sq.length){
      h+='<div style="margin:12px 0 4px"><b>Sequential compounding</b> <span class="muted">· one bet at a time — wait for each to resolve, reinvest the WHOLE bankroll · $'+Math.round(d.kelly_start||1000)+' start · &le;'+(d.seq_maxhold||4)+'h holds (mostly 15-min) · walk-forward · fees · $'+Math.round(d.seq_cap||0)+'/bet cap → the honest "reinvest" growth</span></div>';
      h+='<table style="width:100%;border-collapse:collapse;margin-bottom:8px"><thead><tr style="color:var(--muted);font-size:11.5px;text-align:left"><th style="padding:4px 6px">Sizing</th><th class="r">Final</th><th class="r">Growth</th><th class="r">Max DD</th><th class="r">Bets</th><th class="r">Days</th></tr></thead><tbody>';
      sq.forEach(function(s){
        h+='<tr style="border-top:1px solid var(--line)"><td style="padding:4px 6px">'+escapeHtml(s.label)+'</td>'+
          '<td class="r" style="color:'+nc((s.final||0)-(d.kelly_start||1000))+'">'+mny(s.final)+'</td>'+
          '<td class="r" style="font-weight:700;color:'+nc((s.growth||0)-1)+'">'+(s.growth||0).toFixed(2)+'x</td>'+
          '<td class="r" style="color:var(--bad)">'+Math.round((s.max_dd||0)*100)+'%</td>'+
          '<td class="r"><span class="muted">'+(s.bets||0)+'</span></td>'+
          '<td class="r"><span class="muted">'+(s.days||0).toFixed(1)+'</span></td></tr>';
      });
      h+='</tbody></table>';
    }
    put("rpSecConc",h); // Labs: execution line + concurrent + sequential compounding tables
    var sm=d.spot_momentum||[];var hm='';
    if(sm.length){
      hm+='<div style="margin:2px 0 6px"><b>Spot-momentum → Kalshi 15m</b> <span class="muted">· does the underlying\'s recent move alone predict the outcome? (50% = no edge)</span></div><div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:6px">';
      sm.forEach(function(s){hm+='<span style="border:1px solid var(--line);border-radius:8px;padding:3px 9px;font-size:12.5px">'+escapeHtml(s.bucket)+' → Kalshi up <b style="color:'+confColor(s.up_rate)+'">'+Math.round((s.up_rate||0)*100)+'%</b> <span class="muted">n'+s.n+'</span></span>';});
      hm+='</div>';
    }
    put("rpSecMom",hm); // Edge: pure spot-momentum diagnostic
    var ex=d.exit_comparison||[];var hx='';
    if(ex.length){
      hx+='<div style="margin:10px 0 4px"><b>Exit comparison</b> <span class="muted">· real closed trades — does riding to settlement beat exiting early (stop/flip)?</span></div>';
      hx+='<table style="width:100%;border-collapse:collapse;margin-bottom:8px"><thead><tr style="color:var(--muted);font-size:11.5px;text-align:left"><th style="padding:4px 6px">Strategy</th><th class="r">Rode net</th><th class="r">Rode</th><th class="r">Early net</th><th class="r">Early</th></tr></thead><tbody>';
      ex.forEach(function(e){
        hx+='<tr style="border-top:1px solid var(--line)"><td style="padding:4px 6px">'+escapeHtml(srcLbl(e.signal_type))+'</td>'+
          '<td class="r" style="color:'+nc(e.rode_net)+'">'+(e.rode_n?mny(e.rode_net):'—')+'</td>'+
          '<td class="r"><span class="muted">'+(e.rode_n?(Math.round(e.rode_wr*100)+'% · n'+e.rode_n):'—')+'</span></td>'+
          '<td class="r" style="color:'+nc(e.early_net)+'">'+(e.early_n?mny(e.early_net):'—')+'</td>'+
          '<td class="r"><span class="muted">'+(e.early_n?(Math.round(e.early_wr*100)+'% · n'+e.early_n):'—')+'</span></td></tr>';
      });
      hx+='</tbody></table>';
    }
    put("rpSecExitCmp",hx); // Exits: rode-vs-early comparison
    var sigs=d.signals||[];var hf='';
    if(!sigs.length){hf+='<div class="muted" style="margin-top:8px">No resolved strategies logged yet — let it run a while, then backtest.</div>';}
    else{hf+='<div style="margin:8px 0 0"><b>Per-strategy buckets</b> <span class="muted">· flat-stake NET after fees per entry-price / conviction / category / momentum bucket — most-profitable first</span></div>';}
    sigs.forEach(function(s){
      hf+='<div style="margin:14px 0 4px;border-top:1px solid var(--line);padding-top:10px"><b>'+escapeHtml(srcLbl(s.signal_type))+'</b> <span class="muted">· EV/bet <b style="color:'+nc(s.n?(s.net||0)/s.n:0)+'">'+mny(s.n?(s.net||0)/s.n:0)+'</b> · n='+s.n+' · net </span><b style="color:'+nc(s.net)+'">'+mny(s.net)+'</b></div>';
      (s.features||[]).forEach(function(f){
        hf+='<div style="display:flex;gap:6px;flex-wrap:wrap;align-items:center;margin:3px 0 7px"><span class="muted" style="min-width:92px;font-size:12px">'+escapeHtml(f.name)+'</span>';
        (f.buckets||[]).forEach(function(b){
          hf+='<span title="'+Math.round((b.wr||0)*100)+'% win · n'+b.n+'" style="border:1px solid var(--line);border-radius:8px;padding:2px 7px;font-size:12px">'+escapeHtml(b.label)+' <b style="color:'+nc(b.net)+'">'+mny(b.net)+'</b> <span class="muted">n'+b.n+'</span></span>';
        });
        hf+='</div>';
      });
    });
    put("rpSecFeat",hf); // Edge: per-signal entry-price/conviction/category/momentum breakdowns
  }).catch(function(){var rst3=document.getElementById("rpStatus");if(rst3)rst3.innerHTML='<span style="color:var(--bad)">Could not run backtest.</span>';});
}
function closeHistory(){setTab(WTAB);}
// Export EVERYTHING (full closed-trade history + per-platform/per-source stats + net-P&L curve) as one
// JSON file. Hitting the endpoint also writes a copy to your PredictionMarket folder
// (kalshi-briefings/export-latest.json) for direct offline analysis.
// Export EVERYTHING as one JSON. The build runs in the BACKGROUND (parallel edge/backtest/curves/etc.
// over 500k+ rows) so the request no longer hangs for minutes — we poll /api/export/status and show a
// live progress bar, then download the cached bytes the moment they're ready.
function exportData(){
  // R67m: the export owns its OWN small bar in bar1 (#expbar) — it used to hijack #updated (the
  // feeds-live indicator) and fight refresh() for it every tick.
  var note=document.getElementById("expbar")||document.getElementById("updated");
  function show(t){if(!note)return;note.style.display='inline-flex';note.textContent=t;}
  function done(t){if(!note)return;note.textContent=t;clearTimeout(window._expHideT);window._expHideT=setTimeout(function(){note.style.display='none';},8000);}
  function bar(pct,stage){
    var p=Math.max(0,Math.min(100,pct||0)), filled=Math.round(p/10);
    show("Export "+p+"% ["+"█".repeat(filled)+"░".repeat(10-filled)+"] "+(stage||""));
  }
  function dl(){
    fetch("/api/export").then(function(r){
      if(r.status===202){poll();return null;}   // still building — poll for progress
      if(!r.ok)throw 0; return r.blob();
    }).then(function(b){
      if(!b)return;
      var u=URL.createObjectURL(b),a=document.createElement("a");
      a.href=u;a.download="predictionmarket-export.json";document.body.appendChild(a);a.click();
      setTimeout(function(){URL.revokeObjectURL(u);a.remove();},1500);
      done("Export ✓ saved (also export-latest.json)");
    }).catch(function(){done("Export failed — is the server running?");});
  }
  function poll(){
    fetch("/api/export/status").then(function(r){return r.json();}).then(function(s){
      if(s.err){done("Export failed: "+s.err);return;}
      if(s.ready){bar(100,"downloading…");dl();return;}
      bar(s.pct,s.stage);
      setTimeout(poll,1000);
    }).catch(function(){done("Export status check failed.");});
  }
  bar(1,"starting…");
  dl(); // kicks off the background job (202) or downloads instantly if a fresh build is cached
}
function clearHistory(){
  if(!confirm("Clear ALL closed-bet history? This permanently deletes the History records."))return; // audit §4: no more one-click data loss
  fetch("/api/history/clear",{method:"POST"}).then(function(r){return r.json();}).then(function(){loadHistoryView();loadPaper();}).catch(function(){}); // R59: the PAPER zone is always visible — repaint unconditionally
}
function loadHistoryView(){
  fetch("/api/pnl-series").then(function(r){return r.json();}).then(function(d){renderPnLChart((d&&d.series)||[],"histpnlchart");}).catch(function(){});
  fetch("/api/history").then(function(r){return r.json();}).then(function(d){
    function col(v){return (v||0)>=0?"var(--good)":"var(--bad)";}
    // WR removed from the headline (operator directive): net/bet is the profiting metric.
    var hnet=(d.realized||0)-(d.fees||0);var hpb=(d.closed||0)>0?hnet/d.closed:0;
    document.getElementById("histSummary").innerHTML=
      '<b>'+(d.closed||0)+'</b> closed bets · net <b style="color:'+col(hnet)+'">$'+money(hnet)+'</b> · net/bet <b style="color:'+col(hpb)+'">$'+money(hpb)+'</b> · '+
      'realized <b style="color:'+col(d.realized)+'">$'+money(d.realized)+'</b> · fees <b style="color:var(--bad)">-$'+(d.fees||0).toFixed(2)+'</b>';
    var bs=(d.by_source||[]);document.getElementById("histBySource").innerHTML=bySrcPlatHTML(bs);
    var trs=(d.trades||[]);var et=document.getElementById("histTrades");
    if(trs.length===0){et.innerHTML='<div class="muted" style="padding:8px 0">No closed bets yet — they appear here as positions close or settle.</div>';return;}
    function fpx(p){return Math.round((p||0)*100)+'¢';}
    function fwhen(ts){if(!ts)return '—';var dt=new Date(ts);if(isNaN(dt.getTime()))return '—';var et=etFmt(ts);return tFmt(ts)+(et?(' <span class="muted">(ET '+et+')</span>'):'');} // R60 C1: 12h + the ET venue twin
    var th='<table class="mkt"><thead><tr><th>Closed</th><th>Venue</th><th>Market</th><th>In</th><th>Out</th><th class="r">In $</th><th class="r">Entry&rarr;Exit</th><th class="r">P&amp;L</th></tr></thead><tbody>';
    trs.forEach(function(t){var pc=t.win?'var(--good)':'var(--bad)';
      th+='<tr><td class="muted" style="white-space:nowrap">'+fwhen(t.closed_at)+'</td>'+
        '<td class="muted" style="white-space:nowrap">'+platLabel(t.platform)+'</td>'+
        '<td><a class="go" href="'+escapeHtml(posURL({platform:t.platform,ticker:t.ticker,title:t.title}))+'" target="_blank" rel="noopener">'+escapeHtml(t.title||t.ticker||'')+'</a> <span class="muted">'+escapeHtml(t.side||'')+'</span></td>'+
        '<td>'+escapeHtml(srcLbl(t.source||'—'))+'</td>'+
        '<td>'+escapeHtml(srcLbl(t.exit_source||'—'))+'</td>'+
        '<td class="r">$'+((t.contracts||0)*(t.entry_price||0)).toFixed(2)+'</td>'+
        '<td class="r" style="white-space:nowrap">'+fpx(t.entry_price)+'&rarr;'+fpx(t.exit_price)+'</td>'+
        '<td class="r" style="color:'+pc+';font-weight:700">$'+(t.realized||0).toFixed(2)+'</td></tr>';});
    withScroll(et,th+'</tbody></table>'); // R70 (audit §a P2): string form = dirty-checked (the function form bypassed it and rebuilt every poll)
  }).catch(function(){document.getElementById("histSummary").textContent="Could not load history.";});
}
function closeOrders(){setTab(WTAB);}
function loadOrders(){
  jget("/api/order-previews").then(function(d){
    var list=(d&&d.orders)||[];var el=document.getElementById("ordersBody");if(!el)return;
    if(list.length===0){var z='<div class="muted" style="padding:8px 0">No orders yet — the exact API JSON for each auto buy/sell prints here (preview only, never sent).</div>';if(el._h!==z){el._h=z;el.innerHTML=z;}return;}
    var h="";
    list.forEach(function(o){
      var col=o.action==="BUY"?"var(--good)":"var(--bad)";
      h+='<div style="margin:6px 0;border:1px solid var(--line);border-radius:8px;overflow:hidden">'+
        '<div style="padding:5px 9px;background:rgba(255,255,255,.04);font-size:12px"><span style="color:'+col+';font-weight:700">'+escapeHtml(o.action)+'</span> <span class="muted">'+escapeHtml(o.ts)+'</span> · '+escapeHtml(o.summary||"")+'</div>'+
        '<pre style="margin:0;padding:8px 10px;font-size:11px;white-space:pre-wrap;word-break:break-word;color:#cbd5e1;background:rgba(0,0,0,.28)">'+escapeHtml(o.payload||"")+'</pre></div>';
    });
    if(el._h!==h){el._h=h;el.innerHTML=h;} // R70 (audit §a P2): repaint only on change (was an unguarded rebuild on every 4s tick)
  }).catch(function(){});
}
function renderBacktest(list){
  var el=document.getElementById("statsBacktest");if(!el)return;
  // R63 3a: the backtest panel used to BLANK intermittently — a mid-rebuild /api/backtest payload
  // with no rows overwrote a good table. Cache the last non-empty list and never replace non-empty
  // content with empty.
  if(list&&list.length){window._lastBTRows=list;}
  else if(window._lastBTRows&&window._lastBTRows.length){list=window._lastBTRows;}
  if(!list||list.length===0){el.innerHTML='<span class="muted">No strategy rows logged yet. The app logs Kalshi flow + whale strategies every ~60s; hit-rate fills in as their markets settle.</span>';return;}
  // R19 (operator): NET EV leads, hit rate demoted to a muted context column — a 58% hit rate on
  // 60¢ entries loses money; EV/ct is the profiting metric. Sorted best edge first.
  list=list.slice().sort(function(a,b){return (b.ev_net!=null?b.ev_net:-9)-(a.ev_net!=null?a.ev_net:-9);});
  var h='<table class="mkt"><colgroup><col><col style="width:64px"><col style="width:78px"><col style="width:92px"><col style="width:92px"><col style="width:70px"></colgroup>'+
    '<thead><tr><th>Signal type</th><th class="r">Logged</th><th class="r">Resolved</th><th class="r" title="average NET EV per contract across resolved, deduped signals — fee-inclusive, fill price when logged">Net EV/ct</th><th class="r" title="WHAT-IF INVERTED (R73): maker-net EV/ct had we taken the OPPOSITE side of every resolved row — inverted entry = 1−price (the complementary side at its complementary price), a row wins exactly when the original lost, fees priced MAKER at the inverted entry. Computed row-by-row from the same resolved set, not 1−hit_rate shorthand. A green number here on a red family = the signal is informative but backwards.">INV EV/ct</th><th class="r" title="context only — win% alone is misleading (buying overpriced favorites wins often and loses money)">Hit</th></tr></thead><tbody>';
  list.forEach(function(s){
    var hr=s.resolved>0?(Math.round((s.hit_rate||0)*100)+'%'):'—';
    var evc=(s.ev_net!=null)?('<b style="color:'+((s.ev_net||0)>=0?'var(--good)':'var(--bad)')+'">'+((s.ev_net||0)>=0?'+':'')+((s.ev_net||0)*100).toFixed(1)+'¢</b>'):'—';
    var evi=(s.ev_net_inv!=null)?('<span style="color:'+((s.ev_net_inv||0)>=0?'var(--good)':'var(--bad)')+'" title="what-if inverted — see column header">'+((s.ev_net_inv||0)>=0?'+':'')+((s.ev_net_inv||0)*100).toFixed(1)+'¢</span>'):'—';
    h+='<tr><td title="'+escapeHtml(s.signal_type)+'">'+escapeHtml(srcLbl(s.signal_type))+'</td><td class="r">'+s.total+'</td><td class="r">'+s.resolved+'</td><td class="r">'+evc+'</td><td class="r">'+evi+'</td><td class="r" style="color:var(--muted)">'+hr+'</td></tr>';});
  withScroll(el,h+'</tbody></table>'); // R70 (audit §a P2): string form = dirty-checked
}
function resetPnL(){
  if(!confirm("Reset the P&L graph + header baselines? (Positions and History are kept.)"))return; // audit §4
  fetch("/api/paper/reset",{method:"POST"}).then(function(r){return r.json();}).then(function(){loadStatsView();loadPaper();}).catch(function(){}); // R59: the PAPER zone is always visible — repaint unconditionally
}
function setPnlRange(r){window.pnlRange=r;renderPnLChart(window.lastPnlSeries||[]);}
function markPnlRange(){['all','week','day'].forEach(function(r){var b=document.getElementById('pnlR_'+r);if(b){var on=(window.pnlRange||'all')===r;b.style.fontWeight=on?'700':'400';b.style.color=on?'var(--text)':'var(--muted)';}});}
function renderPnLChart(series,elId){
  var el=document.getElementById(elId||"pnlchart");if(!el)return;
  if(!elId)window.lastPnlSeries=series||[]; // R29: only the Stats chart owns the range-toggle state (session/history charts must not hijack setPnlRange)
  window._pnlSeriesBy=window._pnlSeriesBy||{};window._pnlSeriesBy[elId||'pnlchart']=series||[]; // R62 item 12: cache per chart so redrawPnlCharts can refit without a refetch
  markPnlRange();
  var rng=window.pnlRange||'all';
  if(rng!=='all'&&series&&series.length){var cut;if(rng==='day'){var dd=new Date();dd.setHours(0,0,0,0);cut=dd.getTime();}else{cut=Date.now()-604800000;}series=series.filter(function(p){return Date.parse(p.ts)>=cut;});}
  if(!series||series.length<2){el.innerHTML='<div class="muted" style="padding:8px 0">No P&L '+(rng==='all'?'history yet — the line draws as positions close/settle':'activity in this window')+'.</div>';return;}
  // Downsample to ~500 points so the SVG never balloons (the live sampler can stream thousands
  // of points over a long session); you can't see more than ~chart-width points anyway. Always
  // keep the last point so the live tip is exact.
  if(series.length>500){var step=Math.ceil(series.length/500),ds=[];for(var di=0;di<series.length;di+=step)ds.push(series[di]);if(ds[ds.length-1]!==series[series.length-1])ds.push(series[series.length-1]);series=ds;}
  // R62 item 12 AUTOFIT: size the viewBox to the chart's CURRENT box. Widget-hosted charts
  // (inside a .wb body) also fill the body height and stretch live while resizing
  // (preserveAspectRatio none) until the next redraw refits them crisply.
  var W=860,H=200,padL=54,padR=14,padT=30,padB=22; // padT reserves a top strip for the value label (no overlap)
  var inWb=!!(el.closest&&el.closest('.wb'));
  var mw=el.clientWidth||0;if(mw>80)W=mw;
  // R67d: ANY fixed-height container sizes the viewBox — not just widget bodies. The ML tab's
  // 110px equity charts got an aspect-ratio svg (W×200 viewBox at width:100% ⇒ ~200px tall) that
  // PAINTED OVER the shadow-open-picks rows below. Fixed-height boxes now get height:100% +
  // preserveAspectRatio:none, and the container clips as a belt.
  var mh=el.clientHeight||0;var boxed=inWb||(mh>48&&mh<W);
  if(boxed&&mh>48)H=mh;
  el.style.overflow='hidden';
  var xs=series.map(function(p){return Date.parse(p.ts);}),ys=series.map(function(p){return p.pnl;});
  var x0=Math.min.apply(null,xs),x1=Math.max.apply(null,xs),y0=Math.min.apply(null,ys),y1=Math.max.apply(null,ys);
  if(y0>0)y0=0;if(y1<0)y1=0;if(y1===y0)y1=y0+1;if(x1===x0)x1=x0+1; // auto-fit + always show the zero line
  var pad=(y1-y0)*0.08;y0-=pad;y1+=pad;
  function X(t){return padL+(t-x0)/(x1-x0)*(W-padL-padR);}
  function Y(v){return H-padB-(v-y0)/(y1-y0)*(H-padT-padB);}
  var d="";series.forEach(function(p,i){d+=(i?'L':'M')+X(xs[i]).toFixed(1)+' '+Y(ys[i]).toFixed(1)+' ';});
  var last=ys[ys.length-1],lc=last>=0?'#4ade80':'#f87171',zy=Y(0).toFixed(1);
  function hm(ms){var d=new Date(ms);var h=d.getHours(),ap=h>=12?'PM':'AM';h=h%12;if(h===0)h=12;return (d.getMonth()+1)+'/'+d.getDate()+' '+h+':'+('0'+d.getMinutes()).slice(-2)+ap;}
  var s='<svg viewBox="0 0 '+W+' '+H+'" width="100%"'+(boxed?' height="100%" preserveAspectRatio="none"':'')+' style="display:block;background:rgba(255,255,255,.025);border-radius:8px">';
  s+='<line x1="'+padL+'" y1="'+zy+'" x2="'+(W-padR)+'" y2="'+zy+'" stroke="rgba(255,255,255,.18)" stroke-dasharray="4 4"/>';
  s+='<path d="'+d+'" fill="none" stroke="'+lc+'" stroke-width="2"/>';
  s+='<text x="6" y="'+(Y(y1)+9).toFixed(1)+'" fill="#8a93a6" font-size="11">$'+y1.toFixed(0)+'</text>';
  s+='<text x="6" y="'+(Y(y0)-3).toFixed(1)+'" fill="#8a93a6" font-size="11">$'+y0.toFixed(0)+'</text>';
  s+='<text x="6" y="'+(Number(zy)-3).toFixed(1)+'" fill="#8a93a6" font-size="11">0</text>';
  s+='<text x="'+padL+'" y="'+(H-5)+'" fill="#8a93a6" font-size="11">'+hm(x0)+'</text>';
  s+='<text x="'+(W-padR)+'" y="'+(H-5)+'" fill="#8a93a6" font-size="11" text-anchor="end">'+hm(x1)+'</text>';
  s+='<text x="'+(W-padR)+'" y="18" fill="'+lc+'" font-size="14" font-weight="700" text-anchor="end">net $'+last.toFixed(2)+'</text>';
  s+='</svg>';
  el.innerHTML=s;
}
// R62 item 12: re-render every cached P&L chart at its CURRENT box size — called on widget
// resize (drag handle), window resize and every tab switch. Hidden charts refit on next switch.
function redrawPnlCharts(){
  var by=window._pnlSeriesBy||{};
  Object.keys(by).forEach(function(id){
    var el=document.getElementById(id);
    if(!el||el.offsetWidth===0)return;
    renderPnLChart(by[id],id==='pnlchart'?undefined:id);
  });
}
function loadStatsView(){
  fetch("/api/backtest").then(function(r){return r.json();}).then(function(d){if(d&&d.building){setTimeout(function(){fetch("/api/backtest").then(function(r){return r.json();}).then(function(d2){renderBacktest((d2&&d2.signals)||[]);}).catch(function(){});},2500);return;}renderBacktest((d&&d.signals)||[]);}).catch(function(){});
  fetch("/api/pnl-series").then(function(r){return r.json();}).then(function(d){renderPnLChart((d&&d.series)||[]);}).catch(function(){});
  fetch("/api/stats").then(function(r){return r.json();}).then(function(d){
    function col(v){return (v||0)>=0?"var(--good)":"var(--bad)";}
    // WR REMOVED from the headline (operator directive): profiting = NET EV. Net/bet leads;
    // win rate stays tracked server-side but no longer headlines the UI.
    var netAll=(d.realized||0)-(d.fees||0);var perBet=(d.closed_trades||0)>0?netAll/d.closed_trades:0;
    // R24 (operator): poly-int is untradeable — the TRADEABLE line is the money that was actually
    // winnable (Kalshi + PolyUS only, all-time); the full line below keeps the research view.
    var tr=(d.tradeable||null);var trLine='';
    if(tr){var tb=(tr.closed_trades||0)>0?(tr.net||0)/tr.closed_trades:0;
      trLine='<div style="margin:2px 0 6px"><b>PAPER SIMULATION (bettable venues only): </b>'+tr.closed_trades+' closed · modeled net <b style="color:'+col(tr.net)+'">$'+money(tr.net)+'</b> · modeled/bet <b style="color:'+col(tb)+'">$'+money(tb)+'</b> <span class="muted" title="Kalshi + PolyUS Paper rows only; no exchange fill is implied">· not exchange profit evidence</span></div>';}
    document.getElementById("statsSummary").innerHTML=trLine+
      '<b>'+(d.closed_trades||0)+'</b> simulated closed <span class="muted">(incl. 📡 research)</span> · modeled/bet <b style="color:'+col(perBet)+'" title="simulated gross minus modeled fees per closed Paper round">$'+money(perBet)+'</b> · '+
      'sim gross <b style="color:'+col(d.realized)+'">$'+money(d.realized)+'</b> · '+
      'modeled fees <b style="color:var(--bad)">-$'+(d.fees||0).toFixed(2)+'</b> · simulated open <b>'+(d.open_positions||0)+'</b> · '+
      'sim unrealized <b style="color:'+col(d.unrealized)+'">$'+money(d.unrealized)+'</b> · '+
      'modeled net <b style="color:'+col((d.realized||0)+(d.unrealized||0)-(d.fees||0))+'">$'+money((d.realized||0)+(d.unrealized||0)-(d.fees||0))+'</b> <span class="muted">· cannot promote, size, or authorize LIVE</span>';
    if(d.sig_log_venue&&d.sig_log_venue.length){window._sigLogCounts=d.sig_log_venue;} // R73: /api/stats now carries the per-venue logged/resolved counts — the Stats zero rows explain themselves without a Curves visit
    var bs=(d.by_source||[]);var el=document.getElementById("statsBySource");
    // R63 3a-stats: same never-blank guard as the backtest panel — a mid-rebuild empty by_source
    // must not overwrite a good table; serve the cached last non-empty rows instead.
    if(bs&&bs.length){window._lastBSRows=bs;}
    else if(window._lastBSRows&&window._lastBSRows.length){bs=window._lastBSRows;}
    if(bs.length===0){el.innerHTML='<span class="muted">No closed trades yet. Open and close (or let settle) some paper positions to see performance by strategy.</span>';return;}
    withScroll(el,bySrcPlatHTML(bs)); // R70 (audit §a P2): string form = dirty-checked
  }).catch(function(){document.getElementById("statsSummary").textContent="Could not load stats.";});
}
// R70 (audit §a P1): the AI-gate panel cluster (openGate/closeGate/mdToHtml/loadGate + its 4s
// visibility poll) is DELETED with its orphaned #gatecard — unreachable since the R62 tab rework.
var SETTINGS_SPEC=[
  ['Paper simulation portfolios (separate hypothetical $600 starting grants)',[
    ['book_kalshi_usd','Kalshi Paper bank $','Simulated starting/reset grant shared by Kalshi Paper strategies. It is not exchange equity and never sizes cash.'],
    ['book_polyus_usd','PolyUS Paper bank $','Simulated starting/reset grant shared by PolyUS Paper strategies. It is not exchange equity and never sizes cash.'],
    ['book_combos_usd','Combo Paper bank $','Simulated starting/reset grant for Combo research. It is not an executable cash budget.'],
    ['book_ml_usd','ML Paper bank $','Hypothetical grant split into Kalshi and PolyUS Paper sleeves; modeled P&L remains research-only.'],
    ['book_ml_combos_usd','ML Combo Paper bank $','Independent hypothetical grant for 2–6 leg New-ML Combo Paper positions.'],
    ['explore_stake_frac','Paper exploration weight (0–1)','Legacy Paper-simulation sampling weight. It changes modeled stakes only and cannot promote, size, or authorize LIVE. Default 0.10.']]],
  ['History/API compatibility + Poly-int research notional',[
    ['paper_total_start','Legacy NAV seed $','Retained for historical NAV/reset reporting and API compatibility. It does not divide the four portfolio grants.'],
    ['alloc_kalshi','Legacy Kalshi fraction','Retained for older reports/API clients. It no longer sets the fixed Kalshi portfolio dollars.'],
    ['alloc_polyus','Legacy PolyUS fraction','Retained for older reports/API clients. It no longer sets the fixed PolyUS portfolio dollars.'],
    ['alloc_ml','Legacy ML fraction','Retained for older reports/API clients. It no longer sets the fixed ML portfolio dollars.'],
    ['bankroll_kalshi','Deprecated Kalshi fallback $','Legacy compatibility value; not the R133 Kalshi portfolio. Use Kalshi portfolio $ above.'],
    ['bankroll_polyint','Poly-int research notional $','Research-only hypothetical sizing. Poly-int never places a real or paper order and is outside the four funded portfolios.'],
    ['bankroll_polyus','Deprecated PolyUS fallback $','Legacy compatibility value; not the R133 PolyUS portfolio. Use PolyUS portfolio $ above.']]],
  ['Sizing & anti-churn',[
    ['stake_pct','Stake fraction (0–1)','Share of that strategy\'s parent portfolio per bet before evidence/risk adjustments. 0.03 = 3% ($18 on a $600 portfolio).'],
    ['stake_usd','Legacy Paper flat stake $','Historical Paper-simulation sizing input. R165 corrected system exploration records one share and does not let model, replay, or assumed-fill results resize it. This setting is not exchange profit evidence and cannot authorize LIVE.'],
    ['min_stake_usd','Min bet $','Floor per bet so tiny bets aren\'t eaten by fees (sub-$5 paid ~5.8% vs ~3.5% at $15+). Capped at 25% of the venue bankroll.'],
    ['fee_maker_share','Maker fill share (0–1)','R77: THE fee-mode key (the Fee model buttons above write it). Fraction of maker-intent orders modeled as filling at the MAKER fee vs crossing as taker: 0 = pure taker, 1 = pure maker (R67 default), between = hybrid blend. Drives paper fees, ML books, backtests and the sidecar mirror; the legacy maker_first flag is derived from it (share>0).'],
    ['max_open_positions','Max open positions','Most bets allowed open at once — stops the bot churning into hundreds of tiny bets.'],
    ['arb_min_gap_pct','Arb min gap %','How big a price gap between two exchanges before the arb bot acts (only if Two-leg arb is on).'],
    ['max_exposure_usd','Max auto exposure $','Total $ the automatic systems may hold open at once (shared budget).'],
    ['drawdown_floor_pct','Drawdown floor (0–1)','How far a venue\'s SIZING money may fall below its bankroll after paper losses. 0.10 = never size off less than 10% of the bankroll (so after big losses bets shrink to the $-min). Set to 1 = ALWAYS size off the FULL bankroll — bets never shrink on a losing run (also turns off the drawdown breaker). Default 0.10.']]],
  ['Flow & consensus gates',[
    ['consensus_flow_strength','Flow one-sidedness (0–1)','What share of the aggressive money must be on ONE side to count as a signal. 0.70 = 70%. Higher = pickier.'],
    ['consensus_kalshi_solo_strength','Kalshi solo bar (0–1)','The higher one-sidedness a Kalshi flow must clear to fire WITHOUT Polymarket also agreeing.'],
    ['consensus_min_traders','Poly min traders','How many leaderboard traders must be on the same side before the bot copies them.'],
    ['consensus_max_rank','Poly max rank','LEADERBOARD CUTOFF for the top-trader copy signals (poly consensus + poly whale flow): the agreeing group must include at least one trader ranked at or above this on the profit leaderboard, or the signal is skipped (0 = no rank gate). Lower = more elite. Default 50 — R82 probe: lb-api serves ONLY the top 50 per window (limit caps at 50, offset ignored), so 50 covers every rank the API can show and anything above 50 is unreachable slack. It does NOT govern the wallet-skill families (basket / whale-v2): their measured per-wallet SKILL scores (shrunk market-level skill, audit Q2) supersede raw rank there and feed the ML as trader_skill.'],
    ['consensus_min_conc','Poly min conviction (0–1)','How big the bet is relative to the trader bankroll — proof they have real money on it.'],
    ['consensus_bridge_min_conc','Bridge conviction floor (0–1)','CONSENSUS BRIDGE only: minimum Poly smart-money conviction before the bot bets the SAME market on Kalshi. Export 7: below ~0.05 (5%) it\'s a coinflip. Default 0.05.'],
    ['consensus_bridge_max_conc','Conviction cap (0–1)','Conviction CAP for BOTH the consensus bridge AND the Poly leaderboard copy: skip when conviction is ABOVE this — past ~0.10 (10%) the edge collapses (lone-whale YOLO). Default 0.10.'],
    ['bridge_min_gap_cents','Bridge min gap ¢','R70: pmatch GAP GATE — only bridge when the tradeable Kalshi twin is at least this many ¢ CHEAPER than Poly-int\'s live same-side price (no unpriced info = no bridge). 0 = off. Default 3.'],
    ['bridge_max_age_min','Bridge max age (min)','R70: pmatch FRESHNESS GATE — refuse to bridge when the Poly-int conviction snapshot is older than this many minutes (a stalled scrape must not drive bets). 0 = off. Default 10.'],
    ['consensus_min_trader_pnl','Poly min trader P&L $','Each counted trader must have at least this much all-time profit.']]],
  ['Timing, cross-match & slippage',[
    ['consensus_max_hours_out','Max hours out','AUTO only bets games resolving within this many HOURS — keeps it to LIVE / ending-soon games, not day+ futures. The ML book uses this same horizon.'],
    ['consensus_crypto_max_hours_out','Crypto max hours out','Same, tighter, for BTC/ETH price windows.'],
    ['consensus_cross_min_pct','Cross min %','In the both-exchanges-agree play, both must price the favorite at least this high.'],
    ['consensus_cross_max_gap_pct','Cross max gap %','The two exchanges prices must be within this many points to count as agreement.'],
    ['consensus_kalshi_agree_pct','Kalshi↔Poly agree %','How much Polymarket must back the same side for a Kalshi flow to count as confirmed.'],
    ['consensus_polyus_min_flow','Poly US min flow $','Gates the polyus-flow signal only: minimum EXECUTED taker notional ($) in the last 15 minutes on a Poly US market before its one-sided aggressor tape may fire — below this the tape is too thin to mean anything. How one-sided it must be is a separate gate (Flow one-sidedness), and the book must be healthy (two-sided, spread ≤10¢). Shipped default 500; the live file runs 150 (looser). Evidence status: family graded by the 30-min placement policy like every other — raise this first if polyus-flow bleeds on thin books.'],
    ['consensus_exit_leeway_cents','Auto-sell leeway ¢','How far a bet may slip below entry before the bot bails — stops it selling on a 1¢ wiggle.'],
    ['consensus_max_entry_slip_cents','Max entry slippage ¢','Right before placing a bet the bot re-checks the live price for that market. If it moved more than this many ¢ ABOVE the signal price, it skips the bet instead of chasing a stale/worse price. 0 = off.']]],
  ['New signals & gates (XVLAG · MEANREV · LIQGATE)',[
    ['xvlag_lead_cents','XVLAG lead ¢','Cross-venue lag: the LEADER venue must have moved at least this many ¢ toward a side since the last tick to count as leading. Default 3.'],
    ['xvlag_min_gap_cents','XVLAG min gap ¢','…and the LAGGARD must still be at least this many ¢ cheaper than the leader (room to converge) before xvlag bets it. Default 3.'],
    ['xvgap_max_episodes','XVGAP max episodes','R126: the gap book may RE-ENTER a market when its gap closes (≤1¢) and reopens — same direction only, one lot per episode, at most this many lots per market. Default 3.'],
    ['xvgap_episode_reopen_c','XVGAP reopen ¢','R126: a closed gap must reopen by at least this many ¢ to count as a NEW episode (re-entry allowed). 0 = same as the min-gap threshold.'],
    ['xvgap_combo_overlay','XVGAP combo overlay (1/0)','R126: when a Kalshi gap candidate has a LIVE comboable same-game partner, place a correlated 2-leg combo in the PAPER overlay ledger (tag combo_overlay). Paper only — never venue orders.'],
    ['roi_band_rank_arm','ROI band rank ARM (1/0)','R126: arm the ROI-per-dollar entry-price-band nudge in candidate ranking (weights from data/roi_bands.json). OFF = the weight is computed and logged on every candidate but never reorders anything.'],
    ['ev_day_rank_exp','C4 speed-rank exponent','C4 is ranking only—not Adaptive Allocation Model sizing. It prefers QUICKER-RESOLVING bets at equal proven EV: rank key = net EV × (6h ÷ hours-to-resolve)^this. 0 = off. 0.65 (med-high) makes a 1h market rank about 3.2× above a 6h market at equal EV. It cannot prove an edge, authorize a trade, or choose the stake.'],
    ['meanrev_extreme_cents','MEANREV extreme ¢','Mean-reversion: a price counts as "extreme" at ≤ this many ¢ (low) or ≥ 100−this (high). Default 8 → ≤8¢ or ≥92¢.'],
    ['meanrev_move_cents','MEANREV move ¢','…and it must have reached that extreme via a move of ≥ this many ¢ since the last tick (a sharp spike, not a slow drift). Default 5.'],
    ['liq_max_spread_cents','LIQGATE max spread ¢','Skip an AUTO bet on a Kalshi market whose top-of-book spread is WIDER than this many ¢ — a wide book means slippage eats the edge. 0 = off. Default 10.'],
    ['liq_min_depth','LIQGATE min depth','Optional: also require at least this much top-of-book depth (contracts) on Kalshi before betting. 0 = off (spread is the primary gate).']]],
  ['Variable share selling (auto scale-out)',[
    ['scaleout_enabled','Scale-out ladder (1/0)','Auto-sell a SLICE of a winning AUTO position each time it runs another step in your favor — banking profit in pieces instead of all-or-nothing at TP. Crypto 15m is excluded (rides to settlement). 1=on, 0=off.'],
    ['scaleout_gain_cents','Step size ¢','Sell another slice every time the position gains this many ¢ above your entry. Default 10 → slices at +10¢, +20¢, +30¢…'],
    ['scaleout_fraction','Slice fraction (0–1)','How much of the contracts STILL held to sell at each step. 0.25 = a quarter each step. Default 0.25.'],
    ['scaleout_max_steps','Max slices','Cap on how many slices to sell over the life of the position (so it never fully scales out and always keeps a runner). Default 3.'],
    ['scaleout_freeroll','Auto free-roll (1/0)','Once a position is up enough, auto-sell EXACTLY enough to recover your full cost — the rest then rides at zero net cost and can\'t lose money (a guaranteed free roll). Formula: sell ceil(cost ÷ current price). 1=on, 0=off.']]],
  ['Combos',[
    ['parlay_enabled','Combos on (1/0)','Build combo suggestions from the ML picks AND, when AUTO is on, auto-place the best combo per platform from the combo portfolio. 1=on, 0=off.'],
    ['parlay_bankroll_usd','Legacy combo bank $','Compatibility-only historical field. Funded combos use the fixed book_combos_usd portfolio; this value cannot widen or split that bankroll.'],
    ['parlay_stake_usd','Combo base stake $','Base $ per auto-placed combo. Each suggestion\'s recommended stake scales DOWN from this by the combo\'s edge + win-chance (quarter-Kelly). Default 20.'],
    ['parlay_max_legs','Combo max legs (2-6)','Hard project ceiling is 6 across suggestions, Paper AUTO placement, Combo Lab, and LIVE Kalshi RFQs. A lower value is allowed; 0 is normalized to 6.'],
    ['parlay_max_hours_out','LIVE combo horizon h','Final LIVE combo handoff: regular <=4h and crypto <=2h. This can tighten the regular limit but cannot widen it.'],
    ['paper_combo_max_hours_out','Paper Combo horizon h','Funded System Combo and ML Combo collection: regular <=24h. Every future LIVE handoff still rechecks the shorter LIVE limit.'],
    ['paper_combo_crypto_max_hours_out','Paper Combo crypto h','Funded System Combo and ML Combo crypto collection: <=6h. Unknown clocks still reject.'],
    ['paper_ml_max_hours_out','Paper New ML horizon h','Book-native-v2 Paper singles: regular <=24h. LIVE still rechecks <=4h.'],
    ['paper_ml_crypto_max_hours_out','Paper New ML crypto h','Book-native-v2 Paper crypto singles: <=6h. LIVE still rechecks <=2h.'],
    ['parlay_min_leg_pwin','Combo min leg p_win (0–1)','R70: minimum calibrated win-probability per combo LEG. Raise for safer (fewer) combos, lower for more longshot legs. Default 0.55.']]],
  ['Entry filters & auto-invert',[
    ['auto_invert_min_trades','Legacy inverse sample setting','Display/research compatibility only. R165 forbids Paper history from automatically flipping a side.'],
    ['auto_invert_banded','Banded inverse diagnostic (legacy 1/0)','Discovery-only. It may flag a candidate opposite hypothesis, but cannot flip Paper/LIVE without a separately named current trigger and route proof.'],
    ['min_entry_price','Longshot filter (min entry, 0–1)','Skip any auto bet priced below this on the side we\'d buy. Edge finder: sub-35¢ entries win only 11–22% across every signal — pure bleed. A source whose own per-contract net-EV 95% lower bound is positive at n≥20 has EARNED its longshots and passes anyway. 0 = off. ~0.35 recommended.'],
    ['min_ev_per_contract','Min ML EV / contract (EVGATE)','Model-score threshold. Historical unscored fallback values came from modeled Paper net/contract and are research-only—not authenticated exchange profit evidence. This setting alone cannot promote or authorize LIVE; current route proof remains required.'],
    ['model_ev_haircut','Paper model-EV haircut (0–1)','Research-only Paper-simulation sensitivity input. The historical ~0.70 ratio came from modeled Paper outcomes, not authenticated exchange fills. It cannot promote, size, or authorize LIVE; a future cash rule needs its own forward authenticated cohort.'],
    ['ml_implausibility_guard','Model-implausibility guard (1/0)','R79: the "p_win ≥ 3× a sub-10¢ live price → skip" rule on the ML buy loops (real + shadow books) and the live-proposal near-expiry guard. 0 = OFF (operator default — the rule is removed). 1 = restore it with one click. History: it exists because the ML book once bought $25 of a FINISHED game at 1¢ on garbage p_win, and sub-10¢ edges measured ~2× optimistic. The 5–95¢ live-price sanity band is separate and always on.'],
    ['ml_min_p_win','ML p_win floor (0–1)','R84 (operator-confirmed): the ML paper book AND the live ML-proposal funnel refuse any pick whose model win probability (p_win) is below this floor, whatever its EV score says. Evidence (2026-07-05 book autopsy): p_win<0.50 picks were 55% of ML book losses and realized a 14% win rate vs the 35% their prices implied — high-EV longshots were cooking the bankroll. Rejections log as "p_win below floor" (Logs → Rejects: ML + the Live funnel). The SHADOW book stays ungated on purpose — it is the all-gross control the floor is measured against. Default 0.50; set 0 to disable. The sidecar re-reads config.json every cycle, so this applies without a sidecar restart.']]],
  ['Strategies on / off (1=on · 0=off)',[
    ['consensus_enabled','Master consensus','Master switch for ALL the copy/flow strategies below.'],
    ['consensus_kalshi','Kalshi flow','Follow strongly one-sided aggressive money on Kalshi.'],
    ['consensus_poly','Poly leaderboard','Copy when several elite profit-leaderboard traders pile onto one side.'],
    ['consensus_cross','Cross-platform','Bet when Kalshi and Poly both price the same game as the favorite.'],
    ['consensus_poly_flow','Poly whale flow','Follow ranked Polymarket whales fresh aggressive buys.'],
    ['consensus_polyus_flow','Poly US flow','Follow the Poly US own aggressive-money tape (live games first).'],
    ['consensus_arb','Two-leg arb','Buy both sides across exchanges when the prices lock a guaranteed gap (off by default).'],
    ['consensus_kalshi_needs_poly','Kalshi needs Poly confirm','Require Polymarket to agree before ANY Kalshi flow bet (stricter).'],
    ['maker_first','Low-fee maker orders','ON = post-only/maker entries (rest at the bid) to pay the lowest fee: Kalshi 25% of taker, Polymarket $0. OFF = submit a marketable taker/IOC order at the current ask; it can still zero-fill if the book moves or liquidity disappears. 1=on, 0=off.'],
    ['consensus_crypto','Kalshi crypto (kcrypto)','Bet Kalshi\'s own 15M mid-priced crypto favorite, cross-confirmed by Poly. 1=on, 0=off.'],
    ['consensus_pcrypto','Poly crypto (pcrypto)','Bet Polymarket\'s own crypto up/down (info-only paper). 1=on, 0=off.'],
    ['consensus_xmatch','Crypto cross-match','Bet Kalshi 15M when Poly\'s 15m signal says Kalshi underprices it. 1=on, 0=off.'],
    ['consensus_kthresh','Kalshi threshold ladder','Bet the mispriced strike on a coin\'s KX{COIN}D ladder. 1=on, 0=off.'],
    ['consensus_pmatch','Consensus bridge → Kalshi','Poly 5–10%-conviction consensus → bet the SAME event on Kalshi (the legal bridge). 1=on, 0=off.'],
    ['consensus_confluence','Confluence engine','When ≥2 of {Kalshi, Poly-int, Poly US} price the same side as the favorite, bet that side on the CHEAPEST tradeable venue, sized up by how many agree. 1=on, 0=off.'],
    ['consensus_favlong','Favorite-longshot (favlong)','Bet the FAVORITE side (priced 0.60–0.92) of liquid non-crypto markets, but ONLY when the ML says it\'s underpriced. Exploits the structural longshot bias. 1=on, 0=off.'],
    ['consensus_xvlag','Cross-venue lag (xvlag)','When Kalshi & Poly US price the same outcome and ONE venue just moved while the other lagged, bet the lagging (cheaper) venue to catch up to the leader. Thresholds in “New signals & gates”. 1=on, 0=off.'],
    ['consensus_meanrev','Mean-reversion (meanrev · log-only)','LOG-ONLY discovery: flag a side that just SPIKED to a price extreme on a sharp move. Research tables can nominate a forward test; they cannot prove exchange profit or authorize cash. 1=on, 0=off.'],
    ['consensus_basket','Basket (log-only)','QUALITY-BASKET consensus: log a signal when ≥ (basket min) distinct consistent wallets buy the same side. Research-only; historical Edge-finder results cannot prove exchange profit or authorize cash. 1=on, 0=off.'],
    ['consensus_basket_min','Basket min wallets','How many distinct basket wallets must agree on a side before the basket signal fires (≥2).'],
    ['kflow_books_enabled','kflow twin books (1/0)','R72-B experiment (R81: was API-only): two Go-side paper mini-books split kflow by context — kflow_pre takes every PRE-GAME kflow signal, kflow_live the IN-PLAY ones ($10 flat, maker-fee model, $500 each, settled off signal_log). Pure evidence books: never real money, never the main bankrolls. 1=on (default), 0=off.'],
    ['auto_invert_on_loss','Inverse hypothesis diagnostic (legacy 1/0)','Discovery-only. Negative emitted-side evidence can start a separate opposite-control experiment; it cannot mechanically flip an order.'],
    ['ml_driven_directional','ML owns directional (one portfolio)','ONE-PORTFOLIO mode: the ML executor places every DIRECTIONAL bet (its top calibrated net-EV picks, source auto-ml, into the SAME paper book), while auto keeps only what the ML can\'t do — arb, dutch books, cross-venue gaps. Consensus/follower signals stop autobetting (rejected as ml-routed, still logged for the Edge finder). 1=on, 0=off.'],
    ['honest_fills','Honest maker fills','Paper maker entries count as FILLED only after an opposite-aggressor trade print proves execution; Kalshi queue-known posts must first consume visible queue ahead. Midpoint movement alone never fills. 1=on, 0=off.'],
    ['maker_adverse_guard','Maker-post policy (option c)','R105: resting/maker posts are allowed ONLY when conditions are safe — visible book depth above the floor AND ~5m momentum not running against the posted side. Unsafe candidates TAKE at the ask through the normal gates instead of posting (was: skip outright). Every diversion is stamped in maker_fill_stats + tagged on the fill so the auditor can grade it. 1=on (default), 0=off (posts freely, counterfactual still logged).'],
    ['maker_min_depth','Maker min depth','R105 (option c): minimum visible touch depth (contracts/shares) required to allow a resting post. Books thinner than this — or with no visible book at all — divert to taker. 0 = default 10. The logged mfs depth distributions are the tuning data.'],
    ['maker_mom_gate_c','Maker momentum gate ¢','R105 (option c): a resting post is diverted to taker when ~5-minute momentum runs at least this many ¢ AGAINST the posted side (YES side: price falling; NO side: price rising). 0 = default 1.0¢ (the R101-logged threshold).']]],
  ['Risk caps',[
    ['max_per_market_usd','Max per order $','Most a single buy order can cost (fat-finger guard).'],
    ['max_daily_loss_usd','Max daily loss $','Once the realized loss for the day hits this, new buys are blocked.'],
    ['max_order_contracts','Max order contracts','Hard ceiling on contracts in one order.']]],
  ['Bet sizing (Kelly)',[
    ['kelly_frac','Kelly fraction (0=flat)','KELLYSIZE: 0 = flat % sizing (stake_pct). Above 0 = size each bet by EDGE — stake = bankroll x (edge/(1-price)) x this. 0.25 (quarter-Kelly) is the safe default; a bigger edge means a bigger bet. Capped at 25% of the venue bankroll per bet.'],
    ['kelly_edge','Kelly edge source','Sizing-model input: 0=model estimate, 1=modeled Paper history, 2=the more conservative of those two. Paper history is not exchange-profit proof and cannot itself authorize LIVE; any cash route still requires separate authenticated fill-conditioned proof.'],
    ['auto_retire','Legacy Paper retirement (disabled)','Display/research compatibility only. R165 forbids simulated history from activating or retiring execution routes.']]],
  ['Exits & hold-to-settle',[
    ['consensus_hold_to_settle','Hold to settle (1/0)','R70: the EXITLEAK master switch. 1 = ride auto positions to SETTLEMENT / take-profit (disables auto-sell-exit + auto-SL; keeps TP, the max-hours backstop and settlement) — early exits netted −$8.7k in export 20, and the R71 exit grids confirmed ride beats every TP/SL ladder tested. 0 = allow the early-exit machinery. Default 1 (the effective suite-wide default).'],
    ['exit_scratch_ev','Dynamic-EV scratch (E9)','EXIT rule, not an entry gate: each sweep re-scores every open non-crypto AUTO position with the ML at the live mark, and closes it (auto-scratch) when p_win − current price < MINUS this — i.e. the model now calls the hold clearly negative-EV. Runs even in hold-to-settle mode (the one belief-based exit kept); crypto 15m and near-settled marks (≤3¢/≥97¢) are skipped. Default 0 = OFF, and the live file runs 0 — the R71 exit study found no family where early exits clearly beat riding, so arm ~0.06 only deliberately, to cut model-abandoned holds.'],
    ['exit_trail_cents','Trailing TP ¢ (E10)','EXIT rule: once a non-crypto AUTO position is at least +10¢ over entry it becomes trail-armed — its peak mark is tracked, and a retrace of this many ¢ from that peak closes it (auto-trail), locking a runner\'s gain instead of round-tripping it. Crypto 15m excluded (rides to settlement). Default 0 = OFF (shipped and live) — evidence says ride/hold-to-settle beats every take-profit ladder tested (R71 grids), so leave 0 until a family proves otherwise.']]],
  ['Sharpline (Pinnacle anchor · log-only)',[
    ['sharpline_enabled','Sharpline on (1/0)','R70: de-vigged Pinnacle h2h fair values vs Kalshi moneylines → log-only "sharpline" signals for the Edge finder. Needs the Odds API key below. 1=on, 0=off.'],
    ['sharpline_minutes','Sharpline every (min)','R70: minutes between sharpline sweeps (each sweep = one request per tracked league — budget your Odds API plan). Default 30.'],
    ['odds_api_key','Odds API key','R70: the-odds-api.com key (paste it here — this box is what the config comment always promised). Empty = sharpline inert.']]],
  ['Auto stops, feeds & briefing',[
    ['sltp_mode','SL/TP mode','STOPMODES — the master switch for how every auto stop/target is set. 0=ride (no stops, ride to resolution — best for 15-min crypto, where a stop just locks in noise + doubles fees). 1=offset (TP/SL a fixed ¢ DELTA from entry, using the two ¢ fields below). 2=abs (TP/SL at ABSOLUTE price levels regardless of entry — TP field 90 = exit at 90¢, SL field 20 = exit at 20¢). 3=ratio (scaled to price: TP=p+r(1−p), SL=p−r·p, r=the Ratio field — right for sports / longer holds). 4=auto (same ratio math but r is self-tuned live from realized paths). Default 4.'],
    ['sl_cents','Auto SL −¢','Used by modes 1 (offset) & 2 (abs). OFFSET: stop-loss this many ¢ BELOW entry. ABS: the absolute SL level (20 = exit at 20¢). Ignored in ride/ratio/auto. 0 = off. (The Auto TP +¢ twin was REMOVED in R100: inert since 06-29, and the r26 exit study measured fixed profit-takers as EV-negative — 74/74 auto-TP closes were settled winners anyway. Gate/manual per-order TPs are unaffected.)'],
    ['sltp_ratio','Ratio r (modes 3/4)','The scale-to-price stop/target: TP = p + r·(1−p), SL = p − r·p (r=0.4 on a 50¢ entry → TP 70¢ / SL 30¢). Read ONLY in RATIO (3) and AUTO (4) modes; in AUTO the self-tuner overwrites it, so pin mode 3 to set r by hand. r = 0 places NO stops — that IS ride. Default 0, and the live tuner keeps landing on 0: the R71 exit grids showed ride/hold-to-settle beats every fixed TP/SL ratio tested.'],
    ['sltp_auto','Ratio self-tuner (1/0)','SAFETYNET enable — the only thing this gates is whether Ratio r above keeps being RE-LEARNED (SL/TP mode auto (4) only; other modes ignore it; this is separate from the Research-tab replay AUTOTUNE). Every ~30 min it replays candidate ratios INCLUDING 0=ride against ≥50 realized post-entry price paths (with exit slippage) and applies a new r only when it clearly beats riding (hysteresis). 1 = keep learning (default); 0 = freeze r where it is (auto degrades to fixed-ratio). Evidence so far: it keeps choosing ride (r stays 0), matching the R71 exit study.'],
    ['kalshi_rate_limit_per_sec','Kalshi rate/sec','How many Kalshi API calls per second (restart to apply). 20 = Basic-tier cap; set 30 only on Advanced.'],
    ['kalshi_request_timeout_ms','Kalshi timeout ms','R81 (was config.json-only): per-request HTTP timeout for the Kalshi client. Default 8000. Persists from here, applies on RESTART (the clients are built at boot) — same contract as the rate limit.'],
    ['kalshi_book_ws_cap','Book WS cap','R142: how many Kalshi markets receive full multi-level depth at once — held/resting money first, then genuinely live, every market inside the shared AUTO/Combo horizon (regular ≤4h, crypto ≤2h), explicit proposals, then value/volume. The full board still receives ticker-WS BBO. 0 = default 600; clamped 50–1000. /api/live reports any required-band shortfall instead of claiming exhaustive depth. Applies LIVE at the next subscription sync. Bigger = more queue/depth truth but a heavier resnapshot storm after a WS gap.'],
    ['markets_max_pull','Board max (retired)','Compatibility-only setting: the Kalshi crawler now follows the venue cursor to exhaustion so every current market is retained. Changing this old value has no effect; the refresh cadence below is the live load control.'],
    ['markets_refresh_s','Board refresh s','R73 (R81: was API-only): seconds between board refreshes (the client cache TTL rides it at +0.5s). 0 = default 4; clamped 2–60. At the defaults this is ~4–5 req/s of the 20/s budget. Applies live within one tick.'],
    ['messenger_mode','Messenger mode','R82/R83 (operator: telegram only): which channel carries the briefings — a STRING: telegram (DEFAULT — the 2-min Telegram loop is the sole messenger; the ntfy .md drops stop, so a forwarder has nothing to send), ntfy (legacy — .md drops on, Telegram sends suppressed), both (pre-R82 behavior). Unknown/empty normalizes to telegram. R83 removed the ambiguous 0/1/2 wire code — API GET/POST use the word (legacy numeric posts still accepted). Briefing TEXT is generated identically in every mode. Note: build-suite.bat no longer builds/starts the forwarder — un-comment its block there if you return to ntfy.'],
    ['briefing_enabled','ntfy on (1/0)','R67i: turn the phone (ntfy) briefing on or off — applies LIVE on the next 30s tick, no restart. R82: the .md drops ALSO require Messenger mode ntfy/both — in telegram mode (default) this switch is inert. The Telegram briefing is separate (fixed 5-min cadence whenever creds are set — R92).'],
    ['briefing_every_minutes','ntfy every (min)','R67i: how often the ntfy briefing fires — applies LIVE, no restart. Telegram ignores this (its own 5-min ticker — R92).'],
    ['briefing_dir','Briefing folder','R81 (was API-only): the folder the app-generated market-briefing .md files are written to (the ntfy forwarder watches it). Free text; applies on the next briefing tick. Leave it unless you move the forwarder.'],
    ['telegram_bot_token','Telegram bot token','R65/R67i: bot API token from @BotFather. When BOTH this and the chat id below are set, the briefing posts to that Telegram chat EVERY 5 MINUTES (its own ticker, independent of ntfy — R92, was 2 min). Clear either box to stop sends on the next tick — no restart in either direction; ntfy is unaffected.'],
    ['telegram_chat_id','Telegram chat id','R65: where the bot posts — your numeric chat id (message the bot once, then read it from getUpdates) or a channel id like -100xxxxxxxxxx (add the bot as admin). Empty = Telegram off.'],
    ['telegram_heartbeat','TG heartbeat (1/0)','R83: when a 5-min Telegram briefing tick has nothing to send, still send a minimal suite-up heartbeat line at most every 30 minutes — so a silent phone MEANS the suite is down, never that the loop quietly skipped (the 2026-07-05 failure mode). 1 = on (DEFAULT — an absent config key also means on), 0 = off. Applies live on the next tick. Every skip reason is also audited in category telegram regardless of this switch.']]],
  ['ML model & gates (R114 — every config tunable now lives in Settings)',[
    ['ml_pwin_min','ML band p_win min (0–1)','R90 ML borders: live-proposal funnel refuses picks below this calibrated win probability. Default 0.05. Same key the ⚙ gate popover edits. Applies live.'],
    ['ml_pwin_max','ML band p_win max (0–1)','R90 ML borders: refuse picks ABOVE this p_win (too-good-to-be-true guard). Default 0.95. Applies live.'],
    ['ml_ev_min_cents','ML band EV min ¢','R90 ML borders: minimum fee-adjusted model EV per contract in cents for the live funnel. Default 2. Applies live.'],
    ['ml_ev_max_cents','ML band EV max ¢','R90 ML borders: maximum plausible EV per contract in cents — above this the edge is treated as model error. Default 20. Applies live.'],
    ['realization_haircut','Realization haircut (1/0)','R90 edge 29: scale each family\'s gate EV by its measured realized/predicted ratio (shrunk, floored at 0, capped at 1). 0 = log-only validation mode. Default 1.'],
    ['ml_decay_half_life_days','Training recency half-life (days)','R114: exponential time-decay sample weighting for sidecar training — a row half-life days old counts half. Data-picked default 14 (OOS-AUC .7769 vs .7750 unweighted on the 07-08 comparison; 1d was too aggressive). 0 = the default 14; the sidecar hot-reads it each cycle. Re-tune as history grows.']]],
  ['Maker bands & experiment books (R114)',[
    ['ml_maker_min_px_c','ML maker band floor ¢','R114 maker autopsy: minimum price for an ML-book resting maker post. Epoch-3 evidence: settled sub-50¢ ml-book maker fills won only 12.5–25% and 71% of fills saw adverse 5-minute drift (avg −5.8¢) — cheap resting orders fill mainly when informed flow runs them over. 0 = default 50; negative = off.'],
    ['cluster_exposure_cap','Correlated-cluster cap $','R111/R112: max summed open stake across CORRELATED-DISTINCT variants of one underlying (F3/F5/F7, spread/strike ladders) for auto sources. 0 = OFF (R112 default — metric still measured + surfaced on the ML tab).'],
    ['alloc_rawflow','RawFlow fraction (0–1)','R103: equity fraction for the RawFlow experiment book (kalshi-flow only, 10–50¢ band, ride-to-settle). 0 = book off.'],
    ['alloc_weather','Weather fraction (0–1)','R106: equity fraction for the Weather experiment book. 0 = book off.'],
    ['weather_band_lo_c','Weather band low ¢','R106: Weather book entry band floor in cents. 0 = default 20.'],
    ['weather_band_hi_c','Weather band high ¢','R106: Weather book entry band ceiling in cents. 0 = default 40.'],
    ['kflow_books_frozen','kflow twins frozen (1/0)','R98: 1 = the concluded kflow pre/live twin experiment keeps its final record — reset P&L skips re-epoching them; open lots settle out naturally. 0 = twins re-epoch with everything else.'],
    ['shadow_book_retired','Shadow book retired (1/0)','R98: 1 = the all-gross control book is CONCLUDED — exempt from resets, record frozen readable, sidecar stops feeding it. 0 = shadow runs like every other book.'],
    ['parlay_lab_enabled','Combo Lab (1/0)','The log-only Combo Lab represents 2–6-leg combinations, samples them prospectively, probes venue legality, and grades them at settlement. It never places. Default 1.'],
    ['parlay_lab_min_ev_net','Combo Lab leg EV floor $','Per-leg net-EV floor ($/contract) for lab candidates. Default 0.005.'],
    ['parlay_lab_max_per_cycle','Legacy lab row cap','Compatibility setting for the retired pre-R132 full-space row materializer. Current manifest mode represents every accepted subset without materializing the full space; only a separately bounded prospective grade sample becomes rows.'],
    ['parlay_lab_max_legs','Combo Lab max legs (2-6)','Combo Lab uses the same hard six-leg ceiling as every placement path. A lower value is allowed; 0 is normalized to 6. Older 8/10-leg rows remain labeled history but no new ones are generated.'],
    ['parlay_lab_combo_floor','Combo Lab EV floor $/$1','Fee-net EV per $1 a combo must clear to enter the prospective sample. Default 0 = any +EV after fees; negative admits study space.'],
    ['go_eval_enabled','Go tick re-scorer (1\0)','R123: re-score held/actionable ML p_win in microseconds on every WS price tick from the sidecar\'s parity-gated model export. 0 = file-based p_win everywhere (pre-R123 behavior).'],
    ['go_eval_drift_warn','Go-eval drift alarm','R123: WARN + red /api/ready when the median same-vector |go − sidecar| p_win drift exceeds this (version-skew detector). 0 = default 0.02.']]],
  ['Live-money rails (real-$ proposals — caps in force whenever ARMED)',[
    ['live_prospective_allocation','Adaptive Allocation Model (1/0)','Master opt-in for current accepted Paper signals whose exact venue/side/taker route clears the distinct settled-contract fee-net lower bound. Canonical event clusters remain correlation diagnostics only and never authorize money. Each destination also has its own LIVE Systems switch below. This is not promotion or sealed proof. Quantity compounds from current venue balance, then exact depth and all risk rails can only reduce it. LIVE still requires ARM + AUTO. Default 0.'],
    ['live_system_kalshi','LIVE Systems: Kalshi (1/0)','Additional destination switch for proof-qualified Kalshi System singles. Requires the master Adaptive Allocation switch, ARM, LIVE AUTO, an exact allowlisted identity, fresh signal/input feeds, current book/fee/depth, at least the configured distinct-contract sample (hard floor 60), and a contract-equal fee-net lower bound of at least 0.5¢. Default 0.'],
    ['live_system_polyus','LIVE Systems: PolyUS (1/0)','Additional destination switch for proof-qualified PolyUS System singles. Uses the same exact route/evidence contract as Kalshi plus current PolyUS lifecycle, tick, minimum quantity, fee, private account, and full-book checks. Default 0.'],
    ['live_system_allowlist','LIVE System exact identities','Up to 6 comma-separated entries. Exact format: venue|family|YES-or-NO|maker-or-taker, for example kalshi|spotlag|YES|taker. No wildcards. Empty or invalid means no System order can submit, even with proof. New ML uses separate switches and is never entered here.'],
    ['live_system_canary_allowlist','LIVE one-contract canaries','A subset of the LIVE System identities. Only Spot-lag YES/NO and raw Kalshi-flow YES/NO taker lanes are accepted. If normal confidence proof does not pass, a listed lane may collect one real IOC contract while every current book, fee, account, conflict and risk check remains mandatory. Receipts are labelled UNPROVEN_CANARY.'],
    ['live_new_ml_kalshi','LIVE New ML: Kalshi (1/0)','Additional Kalshi switch for New ML. It cannot bypass ARM, LIVE AUTO, frozen untouched model validation, compatible current-epoch Paper evidence, or the current 3¢ model/route lower-bound floor and venue checks. Default 0.'],
    ['live_new_ml_polyus','LIVE New ML: PolyUS (1/0)','Additional PolyUS switch for New ML. It cannot bypass ARM, LIVE AUTO, frozen untouched model validation, compatible current-epoch Paper evidence, or the current 3¢ model/route lower-bound floor and venue checks. Default 0.'],
    ['live_allocation_min_markets','Allocation min settled contracts','Minimum distinct settled venue+ticker contracts in the exact system+destination+side+taker lane. The engine enforces a hard floor of 60 even if a smaller value is posted; the exact LIVE allowlist remains a separate final gate.'],
    ['live_allocation_min_edge','Allocation fee-net lower bound','Minimum confidence lower bound per contract after current executable price, fee, spread/depth and deterioration. This is explicit and separate from normal AUTO MinEV; the engine enforces at least $0.005.'],
    ['live_exposure_cap_usd','Live exposure cap $','Optional tighter dollar ceiling on outstanding REAL-money exposure across venues. 0 = use the Adaptive Allocation percentage-of-current-NAV rail; there is no $10 fallback.'],
    ['live_crypto_cap_pct','Aggregate crypto exposure','Maximum share of current LIVE equity that may be exposed to crypto across all enabled venues. Default 0.50 = 50%. This is a ceiling, not a reserved allocation; stronger independent non-crypto opportunities remain eligible.'],
    ['live_min_vol_24h_usd','Live min 24h volume $','Live-proposal gate: only propose real bets on markets with at least this much 24h volume. 0 = default 20000.'],
    ['live_px_band_min_c','Live price band min ¢','Live proposals only inside this price band (¢). 0 = default 2.'],
    ['live_px_band_max_c','Live price band max ¢','Live proposals only inside this band (¢). 0 = default 98.'],
    ['kalshi_live_bankroll','Kalshi live bankroll pin $','R103: pin the Kalshi live bankroll for sizing. 0 = use venue-reported balance (the default).'],
    ['polyus_live_bankroll','PolyUS live bankroll pin $','R103: pin the PolyUS live bankroll for sizing. 0 = WS balance when available.'],
    ['live_kalshi_cap_usd','Kalshi live cap $','R103: per-venue live exposure ceiling. 0 = combined cap only.'],
    ['live_polyus_cap_usd','PolyUS live cap $','R103: per-venue live exposure ceiling. 0 = combined cap only.']]],
  ['Latency, probes & transport (R114)',[
    ['latency_warn_rest_ms','REST warn ms','R107: Kalshi REST p95 latency that flags latency health after sustained breaches (Telegram warn latched 6h — R112). 0 = default 2000. Applies live.'],
    ['latency_warn_ws_age_s','WS age warn s','R107: max acceptable book-WS staleness seconds before the latency health flags. 0 = default 120. Applies live.'],
    ['latency_warn_db_p95_ms','DB p95 warn ms','R107: DB write p95 latency warning threshold. 0 = default 500. Applies live.'],
    ['rfq_sim_margin_c','RFQ sim margin ¢','R106 edge 36: would-quote margin floor for the RFQ simulator, cents. 0 = default 3.'],
    ['rfq_sim_margin_per_leg_c','RFQ sim per-leg ¢','R106: additional would-quote margin per combo leg, cents. 0 = default 1.5.'],
    ['rfq_sim_max_usd','RFQ sim max $','R106: would-quote size cap in dollars. 0 = default 5.'],
    ['orders_poll_ms','Orders poll ms','R90 bug 134: ARMED stale-order sweep poll interval. 0 = default 1000. Applies on RESTART (the client is built at boot).'],
    ['reset_on_start','Reset books on start (1/0)','Re-epoch the paper books automatically at boot. The header chip edits the same key.'],
    ['consensus_kalshi_flow','Kalshi flow family (1/0)','The kalshi-flow signal family switch (the Signals tab edits the same key — mirrored here so the Settings inventory is complete).']]],
  ['Credentials — key files (restart to apply)',[
    ['kalshi_key_file','Kalshi key file path','R87: path to the Kalshi RSA private key file (PEM with BEGIN/END markers; an optional first line key_id: <uuid> inside the file is honored). When the file EXISTS it is the credential source and WINS over the encrypted store + KALSHI_SUITE_PASSPHRASE; clear this box to disable the file source. The signer is built at BOOT, so changes apply on the next restart. The key material is never shown or logged — only a sha256 fingerprint prefix. Default C:\\Projects\\Keys\\Kalshi.txt.'],
    ['kalshi_key_id','Kalshi API key id','R87: the Kalshi API key UUID (kalshi.com account → API keys). REQUIRED when the key file is PEM-only (no key_id: line inside it) — without it auth boots RED with the fix text "key file found but kalshi_key_id not set (Settings)". A key_id: line inside the file overrides this box. Applies on restart. Ignored when the encrypted store is the source (the store bundles its own key id).'],
    ['polyus_key_file','PolyUS secret file path','R87: path to the Polymarket US secret file — the single base64 one-liner (ends in =) from polymarket.us/developer. When the file EXISTS it WINS over the POLY_US_SECRET(_FILE) env vars; the key id comes from polyus_key_id below (R88) or the POLY_US_KEY_ID env var (file present with neither = RED polyus_auth). Applies on restart. Never shown or logged — only a sha256 fingerprint prefix. Default C:\\Projects\\Keys\\Poly.txt.'],
    ['polyus_key_id','PolyUS key id','R88: the Polymarket US key UUID (polymarket.us/developer) that labels the Ed25519 signatures. Pairs with the secret file above; this config/Settings value WINS over the POLY_US_KEY_ID env var, so a headless boot never depends on the launching shell env (the 2026-07-05 polyus-RED failure). Applies on restart. Empty with the env var also unset while a secret file exists = RED polyus_auth with the fix text.']]]
];
function openSettings(){uiTab('settings');}
function loadSettingsView(){var sm=document.getElementById("settingsMsg");if(sm)sm.textContent="";
  fetch("/api/settings").then(function(r){return r.json();}).then(function(d){
    window.SETTINGS_INFO={};
    var h='<div class="muted" style="margin:0 0 8px;font-size:12px;">Click the ⓘ on any setting — its explanation pops up right under that section.</div>';
    // Fee model selector (moved here from the header — R6): taker / maker / hybrid.
    // R77 item 8 FIX: selection used to key off the LEGACY maker_first flag first — a diverged
    // config (maker_first=false + fee_maker_share=1, exactly what the pre-R77 auto-tune apply
    // wrote) showed TAKER selected while every fee path blended MAKER. fee_maker_share is now the
    // single source of truth: the toggle SELECTS from it and WRITES only it (Taker=0 · Maker=1 ·
    // Hybrid=the measured maker fill share). Default/effective mode is MAKER (share 1.0, R67h).
    var share=(d.fee_maker_share!=null)?d.fee_maker_share:1;
    window._measShare=(d.measured_maker_share!=null)?d.measured_maker_share:0.5;
    var feeMode=(share<=0.001)?"taker":((share>=0.999)?"maker":"hybrid");
    h+='<div style="font-weight:700;color:var(--text);margin:2px 0 4px">Fee model (legacy Paper simulation)</div>'
     +'<div style="margin:0 0 4px">'
     +'<button class="modebtn'+(feeMode==="taker"?" on":"")+'" onclick="setFeeMode(\'taker\');setTimeout(openSettings,300)" title="TAKER: model a marketable order at the ask with the full estimated fee. Legacy Paper may assume a fill; a real IOC can zero-fill. Writes fee_maker_share=0.">Taker</button> '
     +'<button class="modebtn'+(feeMode==="maker"?" on":"")+'" onclick="setFeeMode(\'maker\');setTimeout(openSettings,300)" title="MAKER: rest at the bid = maker fee; a fill needs a real opposite trade print (plus queue consumption when known). Writes fee_maker_share=1.">Maker</button> '
     +'<button class="modebtn'+(feeMode==="hybrid"?" on":"")+'" onclick="setFeeMode(\'hybrid\');setTimeout(openSettings,300)" title="HYBRID: blend at the MEASURED maker fill share (fraction of maker-intent orders that actually rested as maker — MakerFillSummary; 0.5 until enough fills accrue). Writes fee_maker_share='+window._measShare+'.">Hybrid</button>'
     +' <span class="muted" style="font-size:11.5px">drives fee_maker_share — current '+share+' · measured maker share '+Math.round(window._measShare*100)+'%</span></div>';
    window.SETTINGS_SEC={};
    SETTINGS_SPEC.forEach(function(g,gi){
      // R63 4c: auto-fill GRID instead of flex-wrap — the form now fills the FULL panel width with
      // as many columns as fit (~7 at 1680px) instead of cramming into the left half.
      h+='<div style="font-weight:700;color:var(--text);margin:12px 0 4px">'+escapeHtml(g[0])+'</div><div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:8px 14px;">';
      g[1].forEach(function(f){
        window.SETTINGS_INFO[f[0]]=[f[1],f[2]||''];
        window.SETTINGS_SEC[f[0]]=gi;
        var v=(d&&d[f[0]]!=null)?d[f[0]]:'';
        var inp;var SEL={sltp_mode:['ride','offset','abs','ratio','auto'],kelly_edge:['ml','realized','hybrid'],messenger_mode:['telegram','ntfy','both']}; // R83: messenger_mode is a STRING enum end-to-end (was an ambiguous 0/1/2 code); sltp_mode/kelly_edge still post numeric codes
        var TXT={telegram_bot_token:1,telegram_chat_id:1,odds_api_key:1,briefing_dir:1,kalshi_key_file:1,kalshi_key_id:1,polyus_key_file:1,polyus_key_id:1,live_system_allowlist:1,live_system_canary_allowlist:1}; // free-TEXT settings; LIVE allowlists use exact comma-separated identities
        if(SEL[f[0]]){
          var opts=SEL[f[0]],os='',asStr=(f[0]==='messenger_mode'); // R83: string-enum select posts/reads the WORD ("telegram"...), never a bare int
          for(var oi=0;oi<opts.length;oi++){var ov=asStr?opts[oi]:oi,ol=asStr?opts[oi]:(oi+' · '+opts[oi]);os+='<option value="'+ov+'"'+(String(v)===String(ov)?' selected':'')+'>'+ol+'</option>';}
          inp='<select id="set_'+f[0]+'" style="width:100%;max-width:220px">'+os+'</select>';
        }else if(TXT[f[0]]){
          inp='<input id="set_'+f[0]+'" type="text" value="'+escapeHtml(String(v))+'" autocomplete="off" spellcheck="false" style="width:100%;max-width:220px">';
          // R80: live delivery check right next to the creds — probes getMe → getChat → ONE real
          // sendMessage with whatever is typed in the boxes (unsaved values included), result inline.
          if(f[0]==='telegram_chat_id'){inp+='<button type="button" onclick="tgTest(this)" style="margin-top:5px;max-width:220px;width:100%">Send test message</button>';}
        }else{
          inp='<input id="set_'+f[0]+'" type="number" step="any" value="'+v+'" style="width:100%;max-width:220px">';
        }
        h+='<label class="fld">'+escapeHtml(f[1])+' <span onclick="si(\''+f[0]+'\')" style="cursor:pointer;color:var(--accent);font-weight:700" title="what this does">ⓘ</span>'+inp+'</label>';
      });
      h+='</div><div id="setinfo_'+gi+'" class="muted" style="display:none;margin:4px 0 2px;padding:7px 10px;background:var(--card2);border:1px solid var(--line);border-radius:8px;font-size:12.5px;"></div>';
    });
    document.getElementById("settingsBody").innerHTML=h;
  }).catch(function(){document.getElementById("settingsBody").textContent="Could not load settings.";});}
function si(k){var gi=(window.SETTINGS_SEC||{})[k];var m=(window.SETTINGS_INFO||{})[k];if(gi==null||!m)return;var e=document.getElementById('setinfo_'+gi);if(e){e.style.display='block';e.innerHTML='<b>'+escapeHtml(m[0])+':</b> '+escapeHtml(m[1]);}}
// R80: Settings → "Send test message" — POST /api/telegram/test with the CURRENT box values (so
// unsaved creds are testable before hitting Save); server probes getMe/getChat then sends ONE real
// message and reports message_id (delivered) or the exact failing stage + Telegram error.
function tgTest(btn){
  var sm=document.getElementById('settingsMsg');btn.disabled=true;var old=btn.textContent;btn.textContent='Sending…';
  var tok=document.getElementById('set_telegram_bot_token'),ch=document.getElementById('set_telegram_chat_id');
  fetch('/api/telegram/test',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({token:(tok?tok.value.trim():''),chat_id:(ch?ch.value.trim():'')})})
  .then(function(r){return r.json();}).then(function(d){
    btn.disabled=false;btn.textContent=old;if(!sm)return;
    if(d.ok){sm.innerHTML='<span style="color:var(--good)">Telegram OK — delivered (message_id '+escapeHtml(String(d.message_id))+(d.chat_type?(', chat type '+escapeHtml(String(d.chat_type))):'')+(d.bot?(', bot @'+escapeHtml(String(d.bot))):'')+')</span>';}
    else{sm.innerHTML='<span style="color:var(--bad)">Telegram FAILED at '+escapeHtml(String(d.stage||'?'))+': '+escapeHtml(String(d.error||'unknown'))+'</span>';}
  }).catch(function(e){btn.disabled=false;btn.textContent=old;if(sm)sm.textContent='Telegram test error: '+e;});
}
window.REPLAY_INFO={
 stake:['Flat stake $ (default 50)','Dollars per bet for ONLY the per-signal NET buckets at the bottom (kalshi-flow / poly-crypto … net-$ rows). It does NOT affect the Concurrent / Sequential Kelly tables — those size by fraction of bankroll, so a flat stake cannot change a growth multiple.'],
 kelly:['Custom Kelly × (default blank)','Adds one extra row to BOTH Kelly tables at this fraction (e.g. 0.75 = 75% Kelly), on top of flat / ¼ / ½ / full. Blank = none.'],
 bank:['Starting bank $ (default 500)','Both sims start from this bankroll — set to the real book ($500) so the backtest\'s dollars are the dollars you\'d actually see. Growth is a multiple either way, but per-bet caps and the capacity ceiling bind differently at different bank sizes.'],
 pbcap:['Concurrent per-bet cap (default 0.025)','CONCURRENT table only. Max fraction of CURRENT equity on any single bet. Lower = more, smaller, diversified bets → smoother compounding (lower drawdown). 0.025 = 2.5%.'],
 maxexp:['Concurrent max-exposure (default 0.94)','CONCURRENT table only. Max TOTAL exposure across all simultaneously-open bets, as a fraction of equity. Higher = more bets allowed at once (more diversification). 0.94 = up to 94% deployed.'],
 cap:['Sequential capacity $/bet (default 3000)','SEQUENTIAL table only. Max $ a single thin market can absorb at a good price — caps each one-at-a-time bet so growth cannot pretend to push huge size into an illiquid market.'],
 maxhold:['Sequential max-hold hrs (default 0.12)','SEQUENTIAL table only. Only take bets that RESOLVE within this many hours, so the one-at-a-time bankroll turns over fast. Lower = mostly 15-min markets = faster compounding. 0.12h ≈ 7 minutes.'],
 slip:['Taker slippage ¢ (default 2)','TAKER execution: how many cents WORSE than the signal price you actually fill (you cross the spread chasing the move). Applies to both Kelly tables. This is the single biggest reason replay edges shrink in real life — our 585% "edge" collapsed to a coin-flip once filled. Ignored when FOKM is on (makers do not cross the spread).'],
 fokm:['FOKM — fill-or-kill maker (default off)','Switches execution from TAKER to posting a MAKER order at the touch: NO slippage and the cheaper maker fee (¼ of taker on Kalshi), BUT only a fraction of your orders fill (see fill rate) — the rest are killed and skipped. Models "I only post maker fill-or-kills." The honest trade-off vs taker.'],
 fill:['FOKM base fill rate (default 0.85)','When FOKM is on, the base fraction of maker orders that fill. Lowered from 0.95 — real maker fills on fast 15-min markets run well below that. The other side is killed → no bet.'],
 adv:['FOKM adverse selection (default 0.15)','The honest maker tax: you get filled MORE when you are about to be WRONG and MISSED when you are right. Losers fill at (rate+adverse), winners at (rate−adverse). 0.15 → winners fill ~70%, losers ~100%. Set 0 to turn it off (and see how much it was flattering FOKM).']
};
function bi(k){var e=document.getElementById('replayInfo');var m=(window.REPLAY_INFO||{})[k];if(e&&m)e.innerHTML='<b>'+escapeHtml(m[0])+':</b> '+escapeHtml(m[1]);}
function saveSettings(){
  var body={};
  SETTINGS_SPEC.forEach(function(g){g[1].forEach(function(f){var el=document.getElementById("set_"+f[0]);if(!el)return;
    if(el.type==="text"){body[f[0]]=el.value.trim();} // R65: string settings post as strings — an EMPTY box still posts (clearing creds turns Telegram off)
    else if(f[0]==="messenger_mode"){if(el.value!=="")body[f[0]]=el.value;} // R83: string enum — posts "telegram"/"ntfy"/"both" (server still accepts the legacy numeric code)
    else if(el.value!=="")body[f[0]]=parseFloat(el.value);});});
  fetch("/api/settings",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)}).then(function(r){return r.json();}).then(function(){
    document.getElementById("settingsMsg").innerHTML='<span style="color:var(--good)">Saved — applies on the next auto/AI tick (and persisted to config.json).</span>';
  }).catch(function(){document.getElementById("settingsMsg").textContent="error saving";});
}
function loadAuto(){
  fetch("/api/auto").then(function(r){return r.json();}).then(function(d){
    window._auto=d;
    var m=d.mode||(d.on?"ai":"off");if(m==="gate")m="ai";
    // R70 (audit §a P3): the modeAI / feeTaker / feeMaker / feeHybrid lookups targeted ids removed
    // rounds ago — silent no-ops every poll. Only the two live header buttons remain.
    var btns={off:document.getElementById("modeOff"),autobet:document.getElementById("modeAuto")};
    Object.keys(btns).forEach(function(k){var b=btns[k];if(!b)return;b.className="modebtn"+(k===m?(" on"+(k==="off"?"":" live")):"");});
    var fr=(d.feeds_ready!==false);var mn=document.getElementById("modenote");
    if(mn){if(m==="off"){mn.textContent="";}else if(fr){mn.textContent="● feeds live";mn.style.color="var(--good)";}else{mn.textContent="⏳ waiting for live feeds…";mn.style.color="var(--warn)";}}
  }).catch(function(){});
}
function setMode(mode){
  fetch("/api/auto",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({mode:mode})}).then(function(r){return r.json();}).then(function(d){window._auto=d;loadAuto();}).catch(function(){});
}
// Global-invert UI REMOVED (operator: redundant with per-signal inverts + the EV-aware auto-invert;
// the blanket flip predates both). The server field still exists for the API but has no button.
// Fee model — R77 item 8: ONE key. fee_maker_share drives every fee path (paper blendedFee, ML
// books, backtests, the sidecar mirror); maker_first is a server-derived legacy mirror (share>0).
//   taker  → fee_maker_share 0   (full taker fee)
//   maker  → fee_maker_share 1.0 (all-maker fee — the R67 default execution reality)
//   hybrid → fee_maker_share = the MEASURED maker fill share (server measured_maker_share; 0.5 fallback)
function setFeeMode(mode){
  var body={fee_maker_share:(mode==="taker")?0:((mode==="maker")?1:((window._measShare>0&&window._measShare<1)?window._measShare:0.5))};
  fetch("/api/settings",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)}).then(function(r){return r.json();}).then(function(){loadAuto();}).catch(function(){});
}
// Per-signal control rows: [key,label,onoff_key,invert_key]
// R25 CANONICAL SIGNAL REGISTRY (operator: "signals are all over the place — make them cohesive").
// One row per signal: [key, human label, on-flag, invert-flag, trade source (verdict join), venue].
// The SAME source string names it in Net-by-signal, Curves, Backtest, Edge, exports. Full map with
// raw signal_log types lives in SIGNALMAP.md.
var SIGNALS=[
 ['crypto','kcrypto — Kalshi 15M crypto (mid-favorite)','consensus_crypto','invert_crypto','auto-cons-kcrypto','🟩'],
 ['pcrypto','pcrypto — Poly-int 15m crypto 📡','consensus_pcrypto','invert_pcrypto','auto-cons-pcrypto','📡'],
 ['xmatch','xmatch — Poly→Kalshi crypto divergence','consensus_xmatch','invert_xmatch','auto-cons-xmatch','🟩'],
 ['kthresh','kthresh — Kalshi threshold ladder (fade)','consensus_kthresh','invert_kthresh','auto-cons-kthresh','🟩'],
 ['pmatch','pmatch — Poly conviction → Kalshi twin','consensus_pmatch','invert_pmatch','auto-cons-pmatch','🟩'],
 ['confluence','confluence — multi-platform agreement','consensus_confluence','invert_confluence','auto-cons-confluence','🟩'],
 ['kflow','kflow — Kalshi flow-only consensus (clean split; places since R79)','consensus_kalshi_flow','invert_kflow','auto-cons-kflow','🟩'],
 ['kalshi','kalshi — Kalshi whale flow (R79: policy-governed)','consensus_kalshi','invert_kalshi','auto-cons-kalshi','🟩'],
 ['poly','poly — Poly-int leaderboard 📡','consensus_poly','invert_poly','auto-cons-poly','📡'],
 ['pflow','pflow — Poly-int whale flow 📡','consensus_poly_flow','invert_pflow','auto-cons-pflow','📡'],
 ['pusflow','pusflow — Poly US flow','consensus_polyus_flow','invert_pusflow','auto-cons-pusflow','🇺🇸'],
 ['cross','cross — cross-platform consensus','consensus_cross','invert_cross','auto-cons-cross','🟩'],
 ['arb','arb — two-leg arb','consensus_arb','invert_arb','auto-cons-arb','🟩']
];
function closeSignals(){setTab(WTAB);}
// R25: plain-language names for EVERY logged signal type (operator: "rename them to be better
// understood" + "do ALL signals"). Used by the Signals tab's research section and anywhere a raw
// type shows. 📡 = poly-int (research venue, signal-only).
var SIGNAMES={
 "kalshi-whale":"🟩 Kalshi whales — big aggressive prints",
 "kalshi-flow":"🟩 Kalshi flow — sustained one-sided buying",
 "kflow":"🟩 Kalshi flow-only consensus — the clean split of the retired blend (places since R79; policy-governed)",
 "polyus-whale":"🇺🇸 PolyUS whales — big aggressive prints",
 "polyus-flow":"🇺🇸 PolyUS flow — sustained one-sided buying",
 "polyus-consensus":"🇺🇸 PolyUS consensus — crowd leaning one way",
 "poly-whale":"📡 Poly-int whales — big prints (research)",
 "poly-consensus":"📡 Poly-int top-trader agreement (research)",
 "pflow":"📡 Poly-int flow (research)",
 "pcrypto":"📡 Poly-int 15m crypto favorite (research)",
 "kcrypto":"🟩 Kalshi 15M crypto — mid-priced favorite",
 "kthresh":"🟩 Threshold ladder — fade the mispriced strike",
 "xmatch":"🟩 Crypto divergence — Poly leads, Kalshi lags",
 "pmatch":"🟩 Conviction bridge — Poly conviction → Kalshi twin",
 "pbridge":"🟩 Concentration bridge — Poly whales → Kalshi twin",
 "confluence":"🟩 Confluence — 2–3 venues agree",
 "cross":"🟩 Cross-venue consensus",
 "favlong":"🟩 Favorite-longshot tilt — buy underpriced favorites",
 "basket":"📡 Poly-int basket — skilled wallets clustered on one outcome (research)",
 "fade":"📡 Fade the losers — bet against consistently-losing wallets (research)",
 "divergence":"📡 Smart-vs-dumb split — skilled money one side, crowd the other (research)",
 "insider":"📡 Insider watch — unknown wallet, big money, deep longshot (research)",
 "skillbuy":"📡 Skilled-wallet pile-in — 3+ proven wallets buying the same outcome (research)",
 "whale-exit":"📡 Smart money leaving — skilled wallets dumping a side (research)",
 "whale-exit-hold-bridge":"🟩/🇺🇸 Whale-exit hold bridge — buy the sold outcome on a certified tradeable twin",
 "meanrev":"🟩/🇺🇸 Mean reversion — snapback after a spike to an extreme",
 "xvlag":"🟩/🇺🇸 Cross-venue lag — one venue moved, the other is still cheap",
 "xvgap":"🟩/🇺🇸 Cross-venue gap — same game priced ≥3¢ apart; buy the cheap venue (R93 log-only)",
 "spotlag":"🟩 Spot lag — the coin moved hard, the contract hasn't repriced",
 "sharpline":"🟩 Sharp sportsbook line vs market",
 "arb":"🟩 Two-leg arb — both sides for under $1",
 "pfbridge":"🟩/🇺🇸 Flow bridge — Poly-int whale flow → tradeable twin",
 "fbridge":"🟩/🇺🇸 Fade bridge — bet against losing wallets, on the tradeable twin",
 "freshlist":"🟩 Fresh listing — first price on a brand-new market",
 "freshfade":"🟥 Fade the fresh listing — NO side of a brand-new market (listings open overpriced)",
 "xinv-pcrypto":"🟥 Inverted crypto bridge — the opposite of pcrypto's call, on the anchor-verified Kalshi twin (log-only)",
 "bookskew":"🟩 Book skew — resting orders piled on one side",
 "fundtilt":"🟩 Funding tilt — crowded perp positioning vs 15m crypto",
 "wxedge":"🟩 Weather edge — NWS forecast vs the temp ladder",
 "poly-pred-kalshi":"🟩 Poly prediction mapped to Kalshi",
 "xvgap2":"🇺🇸 Cross-venue gap — mirrored/NO-side PolyUS expression",
 "xvgapk":"🟩 Cross-venue gap — Kalshi-side expression",
 "notail":"🟩🇺🇸 NO-tail harvester — broad-board 90–99¢ NO candidates",
 "auto-ml":"🤖 ML executor picks (retired — tracked in ML books)",
 "manual":"✋ Manual bets"
};
function renderSignals(){
  Promise.all([fetch("/api/settings").then(function(r){return r.json();}),
               fetch("/api/curves").then(function(r){return r.json();}).catch(function(){return {};}),
               fetch("/api/backtest").then(function(r){return r.json();}).catch(function(){return {};})])
  .then(function(a){
    var d=a[0]||{},cv=a[1]||{},bt=a[2]||{};
    // R25 (operator: "signals are all over the place — make them cohesive"): ONE registry. The
    // toggle rows join their realized VERDICT (same source names as Net-by-signal/Curves); below,
    // EVERY logged signal type with its plain-language name + net EV/ct (same numbers as Backtest).
    var vmap={};((cv&&cv.by_source)||[]).forEach(function(r){vmap[r.source]={v:r.verdict,net:r.net||0,n:r.n||0};});
    var h='<div class="muted" style="margin:0 0 10px;font-size:12.5px">One research registry — the SAME names appear in Paper simulation, replay, Edge, and exports. These toggles control Paper collection only. Modeled labels and synthetic inversion are not exchange profit evidence and cannot promote, size, or authorize LIVE. Full map: SIGNALMAP.md.</div>';
    h+='<div style="margin:0 0 12px;padding:8px 10px;border:1px solid var(--line);border-radius:8px;display:flex;align-items:center;gap:10px;flex-wrap:wrap">'
     +'<b>Legacy Paper auto-invert · research only</b><span class="muted" style="font-size:11.5px;flex:1;min-width:150px">synthetically flips a losing Paper row. It does not observe the opposite executable ask, depth, fee, or exchange fill and can never justify an opposite cash order.</span>'
     +'<button class="modebtn'+(d.auto_invert_on_loss?" on live":"")+'" onclick="toggleSig(\'auto_invert_on_loss\','+(d.auto_invert_on_loss?0:1)+')">'+(d.auto_invert_on_loss?"PAPER AUTO-INVERT ON":"PAPER AUTO-INVERT OFF")+'</button></div>';
    h+='<div style="font-weight:700;margin:4px 0 2px">Paper-simulation signal toggles <span class="muted" style="font-weight:400;font-size:12px">— modeled collection only; no LIVE authority</span></div>';
    h+='<table style="width:100%;border-collapse:collapse"><tr style="text-align:left;color:var(--muted);font-size:12px"><th style="padding:6px">Strategy</th><th>Paper On / Off</th><th>Synthetic side</th><th>Paper-model label</th><th class="r" style="padding-right:6px">Modeled net · n</th></tr>';
    SIGNALS.forEach(function(s){
      var on=!!d[s[2]],iv=!!d[s[3]];
      var vd=vmap[s[4]]||null;
      var vcell=vd?vpill(vd.v):'<span class="muted" style="font-size:11px">no closed trades</span>';
      var ncell=vd?('<span style="color:'+((vd.net||0)>=0?'var(--good)':'var(--bad)')+'">'+((vd.net||0)>=0?'+':'')+'$'+vd.net.toFixed(0)+'</span> <span class="muted" style="font-size:10.5px">n'+vd.n+'</span>'):'—';
      h+='<tr style="border-top:1px solid var(--line)"><td style="padding:8px 6px">'+s[5]+' '+escapeHtml(s[1])+'</td>'
       +'<td><button class="modebtn'+(on?" on":"")+'" onclick="toggleSig(\''+s[2]+'\','+(on?0:1)+')">'+(on?"PAPER ON":"PAPER OFF")+'</button></td>'
       +'<td><button class="modebtn'+(iv?" on live":"")+'" onclick="toggleSig(\''+s[3]+'\','+(iv?0:1)+')">'+(iv?"SIM INVERTED":"SIM DIRECT")+'</button></td>'
       +'<td>'+vcell+'</td><td class="r" style="padding-right:6px">'+ncell+'</td></tr>';
    });
    h+='</table><div style="margin-top:14px;display:flex;gap:8px;flex-wrap:wrap"><button onclick="onlySig(\'consensus_crypto\')" title="Paper simulation: enable only Kalshi crypto">★ Paper: only Kalshi crypto</button><button onclick="allSig(1)">Paper all on</button><button onclick="allSig(0)">Paper all off</button></div>';
    // R77 item 6 — "All logged models": EVERY SIGNAMES family renders here, including the
    // ones that have never logged a row (they used to be simply INVISIBLE on this tab because the
    // list was built only from bt.signals). Zero-state families get the same honest status chips
    // as the Coverage matrix (LOG-ONLY / NEEDS KEY / never + the SIGZS reason), built from the
    // SAME data the matrix uses (curves by_source_venue verdicts + sig_log_venue counts) — no new
    // endpoint. Logged families keep the Backtest table's EV numbers.
    var btBy={};((bt&&bt.signals)||[]).forEach(function(s2){if(s2&&s2.signal_type)btBy[s2.signal_type]=s2;});
    var vTrades={},vCounts={}; // family -> aggregated trades/log-counts across venues (Coverage matrix data)
    ((cv&&cv.by_source_venue)||[]).forEach(function(v){if(!v||!v.source)return;var k2=String(v.source).replace(/^auto-cons-/,'');var t=vTrades[k2]=vTrades[k2]||{n:0,net:0,verdict:null};t.n+=(v.n||0);t.net+=(v.net||0);if(!t.verdict||v.verdict==='promote'||(v.verdict==='retire'&&t.verdict==='track'))t.verdict=v.verdict||'track';});
    ((cv&&cv.sig_log_venue)||[]).forEach(function(c){if(!c)return;var k2=c.signal_type||'';var e=vCounts[k2]=vCounts[k2]||{lg:0,rs:0,per:[]};e.lg+=(c.logged||0);e.rs+=(c.resolved||0);e.per.push(platLabel(c.platform)+': logged '+(c.logged||0)+' · resolved '+(c.resolved||0));});
    var fams={};Object.keys(SIGNAMES).forEach(function(k2){fams[k2]=1;});Object.keys(btBy).forEach(function(k2){fams[k2]=1;});
    var rows=Object.keys(fams).map(function(k2){var b=btBy[k2]||null;return {k:k2,bt:b,ev:(b&&b.ev_net!=null)?b.ev_net:null};})
      .sort(function(x,y){return ((y.ev!=null?y.ev:-9))-((x.ev!=null?x.ev:-9));});
    function sigChip(t,bg,tip){return '<span title="'+escapeHtml(tip||'')+'" style="font-size:10px;font-weight:700;color:#0d1117;background:'+bg+';border-radius:3px;padding:1px 5px;cursor:default;white-space:nowrap">'+t+'</span>';}
    h+='<div style="font-weight:700;margin:18px 0 2px">All logged models <span class="muted" style="font-weight:400;font-size:12px">— signal-log coverage plus modeled assumed-fill EV/ct · discovery only, never exchange proof</span></div>';
    h+='<table style="width:100%;border-collapse:collapse"><tr style="text-align:left;color:var(--muted);font-size:12px"><th style="padding:6px">Model</th><th>Research status</th><th class="r">Logged</th><th class="r">Resolved</th><th class="r">Modeled replay EV/ct</th></tr>';
    rows.forEach(function(r2){
      var k2=r2.k,s2=r2.bt,c2=vCounts[k2]||null,t2=vTrades[k2]||null;
      var lg=(s2&&s2.total)||(c2&&c2.lg)||0,rs=(s2&&s2.resolved)||(c2&&c2.rs)||0;
      var st2;
      if(t2&&t2.n>0){st2=vpill(t2.verdict)+' <span class="muted" style="font-size:10.5px" title="closed paper trades across venues">T '+t2.n+'</span>';}
      else if(lg>0){st2=sigChip('LOG-ONLY','#6ba3f8','logged '+lg+' · resolved '+rs+' · no paper trades (log-only family / research venue / retired — placement off by design)'+(c2&&c2.per.length?('\n'+c2.per.join('\n')):''));}
      else if(k2==='sharpline'){st2=sigChip('NEEDS KEY','var(--warn)',SIGZS.sharpline);}
      else{st2='<span class="muted" title="'+escapeHtml('never logged on any venue'+(SIGZS[k2]?(' — '+SIGZS[k2]):' — the detector has not fired yet'))+'" style="cursor:help">never</span>';}
      var evc=(s2&&s2.ev_net!=null)?('<b style="color:'+((s2.ev_net||0)>=0?'var(--good)':'var(--bad)')+'">'+((s2.ev_net||0)>=0?'+':'')+((s2.ev_net||0)*100).toFixed(1)+'¢</b>'):'—';
      var nm=SIGNAMES[k2]?SIGNAMES[k2]:srcLbl(k2);
      h+='<tr style="border-top:1px solid var(--line)'+(lg>0?'':';opacity:.55')+'"><td style="padding:6px" title="'+escapeHtml(k2)+'">'+escapeHtml(nm)+'</td><td>'+st2+'</td><td class="r">'+lg+'</td><td class="r">'+rs+'</td><td class="r">'+evc+'</td></tr>';
    });
    h+='</table>';
    document.getElementById("signalsBody").innerHTML=h;
    // R77 item 6: cold curves/backtest snapshots answer {building:true} — re-poll while the tab/widget
    // is actually showing so the status chips + EV numbers fill in instead of freezing at "never".
    if(((cv&&cv.building)||(bt&&bt.building))&&(document.getElementById("signalscard").style.display==="block"||wOn('signals')))setTimeout(renderSignals,2500);
  }).catch(function(){document.getElementById("signalsBody").textContent="Could not load signals.";});
}
function toggleSig(key,val){var b={};b[key]=val;fetch("/api/settings",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(b)}).then(function(r){return r.json();}).then(function(){renderSignals();}).catch(function(){});}
var SIGKEYS=['consensus_crypto','consensus_pcrypto','consensus_xmatch','consensus_kthresh','consensus_pmatch','consensus_confluence','consensus_kalshi_flow','consensus_kalshi','consensus_poly','consensus_poly_flow','consensus_polyus_flow','consensus_cross','consensus_arb'];
function allSig(v){var b={};SIGKEYS.forEach(function(k){b[k]=v;});fetch("/api/settings",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(b)}).then(function(r){return r.json();}).then(function(){renderSignals();}).catch(function(){});}
function onlySig(keep){var b={};SIGKEYS.forEach(function(k){b[k]=(k===keep)?1:0;});fetch("/api/settings",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(b)}).then(function(r){return r.json();}).then(function(){renderSignals();}).catch(function(){});}
function closePos(platform,ticker,side){
  fetch("/api/paper/close",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({platform:platform,ticker:ticker,side:side})}).then(function(r){return r.json();}).then(function(d){
    var m=document.getElementById("paperMsg");
    if(d.error){m.innerHTML='<span style="color:var(--bad)">'+escapeHtml(d.error)+'</span>';return;}
    m.innerHTML='<span style="color:var(--good)">Closed '+Math.round(d.closed_contracts||0)+' @ '+Math.round((d.price||0)*100)+'¢.</span>';
    loadPaper();
  }).catch(function(){document.getElementById("paperMsg").textContent="error";});
}
// VARIABLE SHARE SELLING — sell PART of a position (scale out). Accepts a contract count, or "25%".
function sellPos(platform,ticker,side){
  var raw=prompt("Sell how much of this position?\nEnter a number of contracts, or a percent like 25%");
  if(raw==null)return;
  raw=String(raw).trim();var body={platform:platform,ticker:ticker,side:side};
  if(raw.indexOf("%")>=0){var f=parseFloat(raw)/100;if(!(f>0&&f<=1)){alert("Enter 1–100%");return;}body.mode="fraction";body.fraction=f;}
  else{var n=parseFloat(raw);if(!(n>0)){alert("Enter a positive number");return;}body.mode="qty";body.contracts=n;}
  postSell(body);
}
// FREE-ROLL — sell exactly enough to recover the full cost; the rest rides at zero net cost (can't lose).
function freeRollPos(platform,ticker,side){postSell({platform:platform,ticker:ticker,side:side,mode:"freeroll"});}
function postSell(body){
  document.getElementById("paperMsg").textContent="Selling…";
  fetch("/api/paper/sell",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)}).then(function(r){return r.json();}).then(function(d){
    var m=document.getElementById("paperMsg");
    if(d.error){m.innerHTML='<span style="color:var(--bad)">'+escapeHtml(d.error)+'</span>';return;}
    m.innerHTML='<span style="color:var(--good)">Sold '+Math.round(d.sold||0)+' @ '+Math.round((d.price||0)*100)+'¢ (proceeds $'+money(d.proceeds||0)+'). '+Math.round(d.remaining||0)+' left'+((body.mode==="freeroll")?' — now riding free.':'.')+'</span>';
    loadPaper();
  }).catch(function(){document.getElementById("paperMsg").textContent="error";});
}
// Manual paper placement is retired; only management/closure controls for existing or historical
// positions remain on this page.
function toggleROS(){ // R30: "reset P&L on start" switch (persists to config via /api/settings) — now the ↺RST chip in bar1, left of RESET
  var v=window._rosOn?0:1;window._rosOn=(v===1);paintROS();
  fetch("/api/settings",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({reset_on_start:v})}).catch(function(){});
}
function paintROS(){var b=document.getElementById("rosBtn");if(!b)return;b.textContent=window._rosOn?"↺RST:ON":"↺RST:OFF";b.className="modebtn"+(window._rosOn?" on":"");}
fetch("/api/settings").then(function(r){return r.json();}).then(function(d){window._rosOn=!!(d&&d.reset_on_start);paintROS();}).catch(function(){}); // R30: init the switch from config once at load
function toggleKill(){
  // KILL SWITCH MUST NEVER WAIT ON AN RTT (audit §4): the old code GET /api/status (a live 6s
  // upstream call) BEFORE posting. Now: use the last-known state and POST immediately; a RESET (the
  // dangerous direction) asks for confirmation, a TRIP fires instantly.
  var on=window._ksTripped===true;
  if(on&&!confirm("Kill switch is TRIPPED. Reset it and re-enable trading?"))return;
  fetch("/api/killswitch",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({action:on?"reset":"trip",reason:"dashboard button"})})
    .then(function(r){return r.json();}).then(function(st){window._ksTripped=!!(st&&st.tripped);refresh();}).catch(refresh);
}
function loadPolyMarkets(){
  if(!tabArmed('markets'))return; // R98 lazy tabs: no fetch until Markets first opened
  jget("/api/polymarkets").then(function(d){
    var list=(d&&d.markets)||[];window.lastPM=list;
    document.getElementById("pmcount").textContent=list.length?(list.length+" markets · most active first"):"";
    var tb=document.getElementById("pmrows");
    if(list.length===0){tb.innerHTML='<tr><td colspan="11" class="muted">No live Polymarket markets right now.</td></tr>';return;}
    var h=""; // R63 item 4: 11-col uniform union · R67g: grouped by EVENT (shared event URL/slug)
    var pRow=function(m,i,gid,isSub,caret){
      return '<tr'+(isSub?grpSubRow(gid):'')+'><td class="name ell"'+(isSub?' style="padding-left:18px"':'')+' title="'+escapeHtml(m.question)+'">'+(caret||'')+'<a class="go" href="'+escapeHtml(m.url||"#")+'" target="_blank" rel="noopener">'+escapeHtml(m.question)+'</a></td>'+
        pxCell(m.bid)+pxCell(m.ask)+
        '<td class="c">'+pill(m.yes_pct)+'</td>'+
        sigCellU(null)+
        dCell(m.move||0)+
        '<td class="r vol">'+fmtVol(m.vol)+'</td>'+
        '<td class="r when">'+(m.resolve?fmtLocalISO(m.resolve):"—")+'</td>'+
        liveDotCell(m.live,'LIVE — venue is accepting orders (gamma active+acceptingOrders) AND showing activity: game in play (started ≤6h ago), ≥$25k traded in 24h, or a trade printed ≤2 min ago. Halted/suspended books never light.')+
        bookCell('showBets('+i+')','who bet this — recorded ★ smart-money bets on this market')+ /* R67f: ≣ = whale/bets popup */
        goCell(m.url)+'</tr>';};
    grpBuild(list,function(m){return m.url||m.slug;},function(m){return m.vol;}).forEach(function(g){
      var gid=grpId('p',g.key);
      h+=pRow(list[g.mi],g.mi,gid,false,g.subs.length?grpCaret(gid,g.subs.length):'');
      g.subs.forEach(function(si){h+=pRow(list[si],si,gid,true,'');});
    });
    withScroll(tb,h);
    applyLiveFilter();
  }).catch(function(e){document.getElementById("pmrows").innerHTML='<tr><td colspan="11" class="muted">Could not load Polymarket ('+escapeHtml((e&&e.message)||'fetch failed')+') — retrying on the next poll.</td></tr>';});
}
function fmtLocalISO(iso){return tFmt(iso);} // R60: 12-hour (server sends UTC ISO; the BROWSER's zone renders it)
function pmusPx(p){return Math.round((p||0)*100)+"¢";}
function pmusWhen(ts){return tFmt(ts);} // R60: 12-hour
function pmusMove(mv){mv=mv||0;if(Math.abs(mv)<0.0005)return '<span class="muted">—</span>';var c=mv>0?"var(--good)":"var(--bad)",a=mv>0?"▲":"▼";return '<span style="color:'+c+'" title="24h price move — '+(mv>0?('▲ up by '+(Math.abs(mv)*100).toFixed(1)+'¢'):('▼ down by '+(Math.abs(mv)*100).toFixed(1)+'¢'))+'">'+a+' '+(Math.abs(mv)*100).toFixed(1)+'¢</span>';}
function pmusImb(v){v=v||0;if(Math.abs(v)<0.02)return '<span class="muted">~</span>';var c=v>0?"var(--good)":"var(--bad)";return '<span style="color:'+c+'" title="resting order-flow pressure (>0 = buy)">'+(v>0?"+":"")+Math.round(v*100)+'%</span>';}
function pmusState(m){if(m.live)return '<span style="color:var(--warn);font-weight:700">LIVE '+escapeHtml(m.score||"")+' '+escapeHtml(m.period||"")+'</span>';if((m.period||"")==="NS"||!m.score)return '<span class="muted">'+escapeHtml(pmusWhen(m.start))+'</span>';return '<span class="muted">'+escapeHtml(m.score||"")+'</span>';}
// pmusAggro renders the recent aggressive-money side + strength (the Poly US "flow") — its own taker tape.
function pmusAggro(m){if(!m.flow_side)return '<span class="muted">~</span>';var c=m.flow_side==="YES"?"var(--good)":"var(--bad)";return '<span style="color:'+c+';font-weight:700" title="recent aggressive money: '+(m.flow_notional?('$'+fmtVol(m.flow_notional)):'')+'">'+m.flow_side+' '+Math.round((m.flow_strength||0)*100)+'%</span>';}
// pmusSig: a market's live signal = aggressive money if there's taker flow, else the recent price move.
function pmusSig(m){if(m.flow_side&&(m.flow_strength||0)>=0.5)return pmusAggro(m);if(Math.abs(m.move||0)>=0.0005)return pmusMove(m.move);return pmusImb(m.imbalance);}
// pmusVol: traded volume (USD) — Poly US volume is only known for markets whose book we fetched this
// pass (the public API doesn't return it in the list), so show "—" when it's not measured, not a fake 0.
function pmusVol(v){v=v||0;return v>0?('$'+fmtVol(v)):'<span class="muted">—</span>';}
function loadPolyUS(){
  if(!tabArmed('whales','flow','markets'))return; // R98 lazy tabs: PolyUS feed renders on Whales/Flow/Markets only
  jget("/api/polyus/markets").then(function(d){
    var list=(d&&d.markets)||[];window.lastPMUS=list;
    var c=document.getElementById("pmuscount");if(c)c.textContent=list.length?(list.length+" live markets"):"";
    var tb=document.getElementById("pmusrows");if(!tb)return;
    if(list.length===0){tb.innerHTML='<tr><td colspan="11" class="muted">No Poly US markets yet (feed warms up ~45s after start).</td></tr>';return;}
    var h=""; // R63 item 4: 11-col uniform union · CHECKED: the PolyUS payload has game START but no
    // resolve time (public feed doesn't publish one) → the RESOLVES cell stays blank by design.
    var uRow=function(m,i,gid,isSub,caret){ // R67g: grouped by EVENT (event_id — one game's moneyline + team markets)
      // R75: sub-markets (spreads/totals/props — kind != winner) label with their own QUESTION
      // ("Spread: STL (+3.5)"); 24 rows all titled "Cardinals vs. Cubs" would be unreadable.
      var lbl=(m.kind&&m.kind!=='winner'&&m.question)?m.question:(m.game||prettySlug(m.slug)||"");
      var nm=lbl+((m.team)?(' '+(m.team||'').toUpperCase()):'');
      var url=posURL({platform:'polyus',ticker:m.slug,title:m.game});
      var ltip=m.live?(('LIVE '+(m.score||'')+' '+(m.period||'')).replace(/\s+/g,' ')):((m.start?('starts '+pmusWhen(m.start)):''));
      return '<tr'+(isSub?grpSubRow(gid):'')+'><td class="name ell"'+(isSub?' style="padding-left:18px"':'')+' title="'+escapeHtml(nm+(m.start?(' · starts '+pmusWhen(m.start)):''))+'">'+(caret||'')+'<a class="go" href="'+escapeHtml(url)+'" target="_blank" rel="noopener">'+escapeHtml(lbl)+'</a> <span class="muted">'+escapeHtml((m.team||"").toUpperCase())+'</span></td>'+
        pxCell(m.bid)+pxCell(m.ask)+
        '<td class="c">'+pill(Math.round((m.yes||0)*100))+'</td>'+
        sigCellU({flow_side:m.flow_side,flow_strength:m.flow_strength,flow_notional:m.flow_notional})+
        dCell((m.move||0)*100)+
        '<td class="r vol" title="point-in-time notionalTraded snapshot from the book fetch — NOT a 24h window; blank = book not sampled this pass">'+pmusVol(m.volume)+'</td>'+ /* R67m: label the semantics */
        '<td class="r when muted">—</td>'+
        liveDotCell(m.live,ltip||'live')+
        bookCell('showPusBets('+i+')','whale prints on this market (Poly US has no public order book)')+ /* R67f: ≣ = whale/bets popup */
        goCell(url)+'</tr>';};
    grpBuild(list,function(m){return m.event_id||m.game||m.slug;},function(m){return m.volume;}).forEach(function(g){
      var gid=grpId('u',g.key);
      h+=uRow(list[g.mi],g.mi,gid,false,g.subs.length?grpCaret(gid,g.subs.length):'');
      g.subs.forEach(function(si){h+=uRow(list[si],si,gid,true,'');});
    });
    withScroll(tb,h);
  }).catch(function(e){var tb=document.getElementById("pmusrows");if(tb)tb.innerHTML='<tr><td colspan="11" class="muted">Could not load Poly US ('+escapeHtml((e&&e.message)||'fetch failed')+') — retrying on the next poll.</td></tr>';});
  jget("/api/polyus/flow").then(function(d){
    var list=(d&&d.flow)||[];window.lastPMUSF=list;var el=document.getElementById("pmusflow");if(!el)return;
    if(list.length===0){el.innerHTML='<span class="muted">No Poly US flow yet (feed warms up ~45s after start).</span>';return;}
    var hs='<table class="mkt"><colgroup><col><col style="width:104px"><col style="width:48px"><col style="width:62px"><col style="width:58px"></colgroup>'+
      '<thead><tr><th>Market</th><th>Aggressive money</th><th class="r">Yes</th><th class="r">Move</th><th class="r">Vol</th></tr></thead><tbody>';
    list.forEach(function(m,i){
      hs+='<tr><td><a class="go" href="'+escapeHtml(posURL({platform:'polyus',ticker:m.slug,title:m.game}))+'" target="_blank" rel="noopener">'+escapeHtml(m.game||prettySlug(m.slug)||"")+'</a> <span class="muted">'+escapeHtml(m.league||"")+'</span>'+(m.live?' <span style="color:var(--warn)">●</span>':'')+'</td>'+
        '<td>'+pmusAggro(m)+'</td>'+
        '<td class="r">'+pmusPx(m.yes)+'</td>'+
        '<td class="r">'+pmusMove(m.move)+'</td>'+
        '<td class="r vol">'+pmusVol(m.flow_notional||m.volume)+'</td></tr>';});
    withScroll(el,hs+'</tbody></table>');
  }).catch(function(){var el=document.getElementById("pmusflow");if(el)el.textContent="Could not load Poly US flow.";});
  jget("/api/polyus/whales").then(function(d){
    var list=(d&&d.whales)||[];var el=document.getElementById("pmuswhales");if(!el)return;
    var minU=parseFloat((document.getElementById("minPus")||{}).value)||0; // R57: standardized min-$ control
    list=list.filter(function(t){return (t.notional||0)>=minU;});
    if(window.pusWhaleSort==='amt'){list=list.slice().sort(function(a,b){return (b.notional||0)-(a.notional||0);});}
    else{list=list.slice().sort(function(a,b){return (b.at||0)-(a.at||0);});}
    markSort();
    var pc=document.getElementById("puswCount");if(pc)pc.textContent=list.length?(list.length+" prints"):"";
    window.lastPUSW=list; // cached for the per-market bets popup (showPusBets) — the smart-money PUS $ column is SERVER-joined since R69
    if(list.length===0){el.innerHTML='<span class="muted">'+(minU>0?('No Poly US prints over $'+fmtVol(minU)):'No large Poly US taker prints in the last ~20 min')+'.</span>';return;}
    var hs=ufHead('setPusSort'); // R57 unified feed schema · R63 1d: sortable Time/Size headers
    list.forEach(function(t){var age=ageUnix(t.at);
      var full=String(t.game||t.slug||'')+' '+String(t.team||t.league||'').toUpperCase()+(t.live?' · LIVE':'');
      hs+='<tr>'+ufTime(t.at,age)+ufSide(t.side)+
        '<td class="mkt" title="'+escapeHtml(full)+'"><div class="mrow"><span class="mtxt"><a class="go" href="'+escapeHtml(posURL({platform:'polyus',ticker:t.slug,title:t.game}))+'" target="_blank" rel="noopener">'+escapeHtml(t.game||prettySlug(t.slug)||"")+'</a> <span class="muted">'+escapeHtml((t.team||t.league||"").toUpperCase())+'</span></span>'+(t.live?'<span style="color:var(--warn)" title="game is live">●</span>':'')+'</div></td>'+
        '<td class="r">'+Math.round((t.price||0)*100)+'¢</td>'+
        '<td class="r vol">$'+fmtVol(t.notional)+'</td>'+
        '<td class="r muted">—</td></tr>';});
    withScroll(el,hs+'</tbody></table>');
  }).catch(function(){var el=document.getElementById("pmuswhales");if(el)el.textContent="Could not load Poly US whales.";});
}
// ================= R60 WIDGET SYSTEM (variant 6 "GRID") =================
// A user-configurable 12-col × 90px-row grid replaces the fixed paper/live zones. Every widget is a
// variant-6 pane (2px colored left edge, 20px uppercase title bar) WRAPPING an existing renderer
// target ID, so the legacy loaders keep painting whether or not a widget is on the visible tab.
// Registry: id → title, edge color, default span [w,h], body markup (the legacy target elements).
var WREG={
 'paper-positions':{t:'POSITIONS · PAPER BOOK',e:'#3b82f6',w:8,h:6,body:'<div id="paperSummary" class="wsum">Loading…</div><div id="properPaperW" class="wsum">Loading Brier, Log, and Spherical Paper portfolios…</div><div id="riskBox" class="muted" style="margin:1px 4px;"></div><div id="paperPositions" style="padding:0 2px;"></div><div id="paperMsg" class="muted" style="margin:2px 4px;"></div>'},
 'ml-book':{t:'ML BOOK · SIDECAR',e:'#a855f7',w:4,h:3,body:'<div id="mlBookW" class="muted" style="padding:2px 4px;">Loading…</div>'},
 'ml-picks':{t:'ML PICKS',e:'#a855f7',w:4,h:4,body:'<div id="mlPicksW" class="muted" style="padding:2px 4px;">Loading…</div>'},
 'ml-combo-picks':{t:'COMBO PICKS',e:'#d946ef',w:4,h:4,body:'<div id="mlComboPicksW" class="muted" style="padding:2px 4px;">Loading…</div>'},
 'shadow-book':{t:'SHADOW BOOK',e:'#94a3b8',w:4,h:3,body:'<div id="shadowBookW" class="muted" style="padding:2px 4px;">Waiting for the ML sidecar shadow book…</div>'},
 'rawflow-book':{t:'RAWFLOW BOOK',e:'#0ea5e9',w:4,h:3,body:'<div id="rawflowBookW" class="muted" style="padding:2px 4px;">Waiting for the RawFlow book…</div>'}, // R105: addable like the other book widgets
 'weather-book':{t:'WEATHER BOOK',e:'#38bdf8',w:4,h:3,body:'<div id="weatherBookW" class="muted" style="padding:2px 4px;">Waiting for the Weather book…</div>'}, // R106: wxedge paper book widget
 'freshinv-book':{t:'FRESHINV BOOK',e:'#22c55e',w:4,h:3,body:'<div id="freshinvBookW" class="muted" style="padding:2px 4px;">Waiting for the FreshInv book (promotion #1)…</div>'}, // R117: freshlist-fade paper book (auto-promotion pipeline)
 'promotions':{t:'AUTO-PROMOTIONS',e:'#f43f5e',w:5,h:3,body:'<div id="promotionsW" class="muted" style="padding:2px 4px;">Loading promotion pipeline…</div>',load:'loadPromotions'}, // R117: pipeline states/allocs/reserve (paper only, forever)
 'rfq-sim':{t:'RFQ WOULD-QUOTE SIM',e:'#a855f7',w:4,h:3,body:'<div id="rfqSimW" class="muted" style="padding:2px 4px;">Loading would-quote case…</div>',load:'loadRFQSim'}, // R106 (edge 36): log-only quote sim P&L
 'verdicts':{t:'SYSTEM VERDICTS',e:'#ec4899',w:5,h:4,body:'<div id="verdictsW" class="muted" style="padding:2px 4px;">Loading system verdicts…</div>',load:'loadVerdicts'}, // R115: always-valid CS verdict engine — every experiment's state
 'session-pnl':{t:'SESSION P&L',e:'#f59e0b',w:4,h:3,body:'<div id="portSessChart" style="padding:2px 4px;height:100%;box-sizing:border-box;" title="net since this suite start (15s samples)"></div>'},
 'live-pnl':{t:'LIVE P&L',e:'#f59e0b',w:4,h:2,body:'<div id="livePnlW" class="muted" style="padding:2px 4px;height:100%;box-sizing:border-box;">Waiting for live P&L samples (15s cadence, ~5h ring)…</div>'},
 'live-go':{t:'LIVE AUTO SAFETY',e:'#22c55e',w:12,h:2,body:'<div id="liveGoW" style="padding:4px;">Checking current runtime safety…</div>'},
 'live-strip':{t:'LIVE',e:'#ea3943',w:12,h:1,body:'<div id="pipestrip" class="pipestrip" style="padding:1px 4px;"></div><div id="liveHead" style="padding:1px 4px;">Loading…</div>'},
 'proposed':{t:'PROPOSED ORDERS',e:'#3b82f6',w:7,h:3,gear:1,body:'<div id="liveProps" style="padding:0 2px;">Loading…</div>'},
 'combos':{t:'$1 COMBOS RFQ',e:'#a855f7',w:7,h:3,combo:1,body:'<div id="liveCombos" style="padding:0 2px;"></div>'},
 'live-positions':{t:'POSITIONS · LIVE',e:'#16c784',w:5,h:3,body:'<div id="livePosGrid" style="padding:0 2px;"></div>'},
 'resting':{t:'RESTING',e:'#f59e0b',w:5,h:3,body:'<div id="liveOrdersGrid" style="padding:0 2px;"></div>'},
 'history':{t:'HISTORY · FILLS',e:'#16c784',w:6,h:3,copy:'hist',body:'<div id="liveHistGrid" style="padding:0 2px;"></div>'},
 'log':{t:'ORDER LOG',e:'#64748b',w:6,h:3,copy:'log',body:'<div id="liveLogGrid" style="padding:0 2px;"></div>'},
 // R63 6e: EVERY tab panel is also a widget. These ADOPT the single-instance panel DOM (adopt:
 // element id) — while such a widget is on a layout, the panel's home tab shows without it
 // (widget takes precedence; IDs are never duplicated). Removing the widget returns the panel home.
 'whales-kal':{t:'WHALES · KALSHI',e:'#16c784',w:6,h:3,adopt:'fcKal'},
 'whales-pus':{t:'WHALES · POLY US',e:'#38bdf8',w:6,h:3,adopt:'fcPus'},
 'whales-int':{t:'WHALES · POLY-INT',e:'#3b82f6',w:6,h:3,adopt:'fcInt'},
 'flow-kal':{t:'FLOW · KALSHI',e:'#16c784',w:4,h:3,adopt:'flKal'},
 'flow-pus':{t:'FLOW · POLY US',e:'#38bdf8',w:4,h:3,adopt:'flPus'},
 'flow-int':{t:'FLOW · POLY-INT',e:'#3b82f6',w:4,h:3,adopt:'flInt'},
 'cross-venue':{t:'CROSS-VENUE',e:'#a855f7',w:8,h:4,adopt:'both',load:'loadArb'},
 'markets-kal':{t:'MARKETS · KALSHI',e:'#84cc16',w:8,h:4,adopt:'mkSecK',load:'loadMarkets'},
 'markets-int':{t:'MARKETS · POLY-INT',e:'#84cc16',w:8,h:4,adopt:'mkSecI',load:'loadPolyMarkets'},
 'markets-pus':{t:'MARKETS · POLY US',e:'#84cc16',w:8,h:4,adopt:'mkSecP',load:'loadPolyUS'},
 'orders':{t:'ORDER JSON',e:'#f97316',w:6,h:4,adopt:'ordersBody',load:'loadOrders'},
 'signals':{t:'SIGNALS',e:'#22d3ee',w:6,h:4,adopt:'signalsBody',load:'renderSignals'},
  // The remaining operator panels stay available as widgets. Legacy signal/replay backtests are
  // intentionally absent: Systems is the sole promotion evidence surface; Combo Lab stays separate.
 'ml-top-scored':{t:'ML · TOP SCORED',e:'#ec4899',w:6,h:4,adopt:'mlTopScored',load:'loadML'},
 'history-closed':{t:'HISTORY · CLOSED BETS',e:'#10b981',w:6,h:4,adopt:'histTrades',load:'loadHistoryView'},
 'combo-suggest':{t:'COMBO SUGGESTIONS',e:'#d946ef',w:6,h:4,adopt:'parlaySuggest',load:'loadParlay'},
 'logs':{t:'LOGS',e:'#64748b',w:6,h:4,adopt:'logsBody',load:'loadLogsW'}
};
function loadLogsW(){showLogSub(window._logSub||'live');} // R63 3e: named loader for the logs widget
// Default layouts {id:[col,row,w,h]} — PAPER tab feel (positions + ML book + split picks + session
// graph); the LIVE tab carries the blotter widgets (strip, proposed, combos, positions, resting,
// history, log). 12 columns; rows are 90px.
var WDEF={
 paper:{'paper-positions':[1,1,8,6],'ml-picks':[1,7,4,4],'ml-combo-picks':[5,7,4,4],'session-pnl':[9,1,4,3],'ml-book':[9,4,4,7]},
 live:{'live-go':[1,1,12,2],'live-strip':[1,3,12,1],'proposed':[1,4,7,3],'live-positions':[8,4,5,3],'combos':[1,7,7,3],'resting':[8,7,5,3],'history':[1,10,6,3],'log':[7,10,6,3]}
};
var WTAB='paper',WLAY={paper:{},live:{}};
var WCW={paper:{},live:{}}; // R63 6d: per-widget-table column widths (px), persisted in the layout blob
function wSave(){ // persisted on EVERY layout change: pms_layout_v1 (both tabs) + per-tab keys
  if(window._winMode)return;
  try{
    localStorage.setItem('pms_layout_v1',JSON.stringify({tab:WTAB,paper:WLAY.paper,live:WLAY.live,colw:WCW}));
    localStorage.setItem('pms_layout_paper',JSON.stringify(WLAY.paper));
    localStorage.setItem('pms_layout_live',JSON.stringify(WLAY.live));
  }catch(e){}
}
function wLoad(){
  var ok=false;
  try{
    var v=JSON.parse(localStorage.getItem('pms_layout_v1')||'null');
    if(v&&v.paper&&v.live){WLAY.paper=v.paper;WLAY.live=v.live;if(v.tab==='live')WTAB='live';ok=true;
      // R70 (audit §a P2): the colw blob used to restore with NO validation — a shape change (or a
      // renamed widget) applied garbage widths silently and orphan keys lived forever. Keep only
      // entries keyed to a KNOWN widget id whose value is an array of finite non-negative numbers.
      if(v.colw&&v.colw.paper&&v.colw.live){
        var cw={paper:{},live:{}};
        ['paper','live'].forEach(function(tb){
          Object.keys(v.colw[tb]||{}).forEach(function(k){
            var wid=String(k).split('#')[0],ws=v.colw[tb][k];
            if(!WREG[wid]||!Array.isArray(ws)||!ws.length||ws.length>40)return;
            for(var i=0;i<ws.length;i++){if(typeof ws[i]!=='number'||!isFinite(ws[i])||ws[i]<0||ws[i]>4000)return;}
            cw[tb][k]=ws;
          });
        });
        WCW=cw;
      }}
    if(!ok){
      var p=JSON.parse(localStorage.getItem('pms_layout_paper')||'null'),l=JSON.parse(localStorage.getItem('pms_layout_live')||'null');
      if(p||l){WLAY.paper=p||{};WLAY.live=l||{};ok=true;}
    }
  }catch(e){}
  if(!ok){WLAY.paper=JSON.parse(JSON.stringify(WDEF.paper));WLAY.live=JSON.parse(JSON.stringify(WDEF.live));}
  ['paper','live'].forEach(function(tb){ // sanity: drop unknown ids, clamp geometry to the grid
    var L=WLAY[tb],out={};
    Object.keys(L).forEach(function(id){
      if(!WREG[id])return;var g=L[id];if(!g||(g.length!==4&&g.length!==5))return;
      var c=Math.max(1,Math.min(12,Math.round(g[0])||1)),r=Math.max(1,Math.round(g[1])||1);
      var w=Math.max(2,Math.min(12,Math.round(g[2])||2)),h=Math.max(1,Math.min(30,Math.round(g[3])||1));
      if(c+w>13)c=13-w;
      // R63 3a: element 5 (optional) = the FREE pixel size {pw,ph} kept when a resize released
      // off-gridline — restored through wPlace so widgets reopen at their exact pixel size.
      var px=(g[4]&&g[4].pw>0&&g[4].ph>0)?{pw:Math.round(g[4].pw),ph:Math.round(g[4].ph)}:null;
      out[id]=px?[c,r,w,h,px]:[c,r,w,h];
    });
    WLAY[tb]=out;
    if(!Object.keys(WLAY[tb]).length)WLAY[tb]=JSON.parse(JSON.stringify(WDEF[tb])); // an emptied tab heals to defaults
  });
  // Mandatory safety status: add it above older persisted custom LIVE layouts too.
  if(!WLAY.live['live-go']){
    Object.keys(WLAY.live).forEach(function(id){WLAY.live[id][1]+=2;});
    WLAY.live['live-go']=[1,1,12,2];
  }
}
function wPlace(id){ // apply the ACTIVE layout's geometry (display:none when not on this tab)
  var el=document.getElementById('w_'+id);if(!el)return;
  var g=WLAY[WTAB][id];
  if(!g){el.style.display='none';return;}
  el.style.display='flex';
  el.style.gridColumn=g[0]+' / span '+g[2];
  el.style.gridRow=g[1]+' / span '+g[3];
  // R63 3a: a free (off-gridline) release keeps its exact pixel box — the spans above reserve the
  // covering grid area, the inline size renders the true pixels. Snapped widgets clear the inline.
  var px=g[4];
  el.style.width=(px&&px.pw>0)?(px.pw+'px'):'';
  el.style.height=(px&&px.ph>0)?(px.ph+'px'):'';
}
function wEnsure(id){ // create the widget DOM once — BOTH tabs share the node, renderers keep painting
  if(document.getElementById('w_'+id))return;
  var R=WREG[id];if(!R)return;
  var grid=document.getElementById('wgrid');if(!grid)return;
  var el=document.createElement('section');
  el.className='wdg';el.id='w_'+id;el.style.setProperty('--edge',R.e);
  var ctl='';
  if(R.gear)ctl+='<button class="wbtn" onclick="toggleGates(event)" title="proposal gate tunables — EV floor + horizon (saved to config)">⚙</button>';
  if(R.combo)ctl+='<button class="wbtn" onclick="loadCombos()" title="refresh combos now (auto every 10s)">↻</button>';
  if(R.copy==='hist')ctl+='<button class="wbtn" onclick="copyLiveHist(this)" title="copy Kalshi fills + PolyUS history as JSON">⧉ Copy all</button>';
  if(R.copy==='log')ctl+='<button class="wbtn" onclick="copyLiveLogJSON(this)" title="copy the full order log as JSON">⧉ Copy all</button>';
  var body=R.body||'<div class="wadopt" style="min-height:100%"></div>'; // R63 6e: adopt-slot widgets have no static body
  el.innerHTML='<header class="wh" data-wid="'+id+'" title="drag to move · corner handles resize"><span class="wt">'+R.t+'</span><span class="spacer"></span>'+ctl+'<button class="wbtn wx" onclick="removeWidget(\''+id+'\')" title="remove from this layout (re-add via ＋Widgets)">✕</button></header><div class="wb">'+body+'</div>'
    +'<div class="wrs" data-wid="'+id+'" data-corner="se" title="drag to resize — free pixels, snaps near gridlines"></div>'
    +'<div class="wrs nw" data-wid="'+id+'" data-corner="nw" title="drag to resize"></div>'
    +'<div class="wrs ne" data-wid="'+id+'" data-corner="ne" title="drag to resize"></div>'
    +'<div class="wrs sw" data-wid="'+id+'" data-corner="sw" title="drag to resize"></div>'; // R63 6b: all four corners
  grid.appendChild(el);
}
// R63 6e ADOPTION SYNC: a widget with adopt:<elid> MOVES that single-instance panel DOM into its
// body while the widget exists on either layout, and returns it to its recorded home when the
// widget is removed from both. IDs are never duplicated — the widget takes precedence over the tab.
function wAdoptSync(){
  Object.keys(WREG).forEach(function(id){
    var R=WREG[id];if(!R.adopt)return;
    var tgt=document.getElementById(R.adopt);if(!tgt)return;
    var w=document.getElementById('w_'+id);
    var active=!!(WLAY.paper[id]||WLAY.live[id]);
    if(active&&w){
      var slot=w.querySelector('.wadopt');
      if(slot&&tgt.parentNode!==slot){
        if(!tgt.__hp){tgt.__hp=tgt.parentNode;tgt.__hn=tgt.nextSibling;}
        slot.appendChild(tgt);
      }
      // R77 item 4: a research group may have hidden some .rsec sections inside this container —
      // a widget shows the WHOLE panel, so un-hide them all while adopted.
      if(tgt.querySelectorAll){var rs=tgt.querySelectorAll('.rsec');for(var ri=0;ri<rs.length;ri++)rs[ri].style.display='';}
    }else if(tgt.__hp&&tgt.parentNode&&tgt.parentNode.classList&&tgt.parentNode.classList.contains('wadopt')){
      try{
        if(tgt.__hn&&tgt.__hn.parentNode===tgt.__hp)tgt.__hp.insertBefore(tgt,tgt.__hn);
        else tgt.__hp.appendChild(tgt);
      }catch(e){}
    }
  });
}
function renderWidgets(){
  Object.keys(WREG).forEach(function(id){
    if(WLAY.paper[id]||WLAY.live[id])wEnsure(id);
    wPlace(id);
  });
  wAdoptSync();          // R63 6e
  if(typeof applyColWidths==='function')applyColWidths(); // R63 6d: re-apply persisted column widths
  uiMarkTabs();
}
function setTab(t,noSave){if(t!=='paper'&&t!=='live')return;WTAB=t;UITAB=t;
  var g=document.getElementById('wgrid');if(g)g.style.display='grid'; // R62: grid tabs hide the panel tabs
  Object.keys(UIPANELS).forEach(function(k){var el=document.getElementById(UIPANELS[k]);if(el)el.style.display='none';});
  renderWidgets();if(!noSave)wSave();
  if(typeof redrawPnlCharts==='function')redrawPnlCharts();}
function wFree(w,h){ // first free [col,row] that fits w×h in the active layout (top-left scan)
  var L=WLAY[WTAB];
  function hit(c,r){var k=Object.keys(L);for(var i=0;i<k.length;i++){var g=L[k[i]];
    if(c<g[0]+g[2]&&c+w>g[0]&&r<g[1]+g[3]&&r+h>g[1])return true;}return false;}
  for(var r=1;r<60;r++)for(var c=1;c<=13-w;c++)if(!hit(c,r))return [c,r];
  return [1,60];
}
// R62 item 13: simple auto-reflow — every widget rises to fill gaps (stable top-left order).
// Runs after any move/resize/add/remove; layouts stay persisted via the wSave that follows.
function wPack(){
  var L=WLAY[WTAB],ids=Object.keys(L);
  ids.sort(function(a,b){return (L[a][1]-L[b][1])||(L[a][0]-L[b][0]);});
  function hit(id,c,r,w,h){
    for(var i=0;i<ids.length;i++){var o=ids[i];if(o===id)continue;var g=L[o];
      if(c<g[0]+g[2]&&c+w>g[0]&&r<g[1]+g[3]&&r+h>g[1])return true;}
    return false;
  }
  ids.forEach(function(id){
    var g=L[id],r=g[1];
    while(r>1&&!hit(id,g[0],r-1,g[2],g[3]))r--;
    if(r!==g[1]){g[1]=r;wPlace(id);}
  });
}
function addWidget(id){ // ＋Widgets click = INSTANTLY added at the first free slot (no open button)
  var R=WREG[id];if(!R||WLAY[WTAB][id])return;
  var at=wFree(R.w,R.h);
  WLAY[WTAB][id]=[at[0],at[1],R.w,R.h];
  wEnsure(id);wPack();renderWidgets();wSave();
  if(R.load){try{window[R.load]();}catch(e){}} // R63 6e: panel widgets load their data on add
  var el=document.getElementById('w_'+id);
  if(el){el.scrollIntoView({block:'nearest'});el.style.boxShadow='0 0 0 1px var(--accent)';setTimeout(function(){el.style.boxShadow='';},700);}
}
function removeWidget(id){delete WLAY[WTAB][id];wPack();renderWidgets();wSave();}
function buildWidgetMenu(){ // rebuilt on open: everything not already on the active tab
  var m=document.getElementById('dd_widgets');if(!m)return;
  var h='<div class="ddver" style="border-top:0;margin-top:0;">click = added to the '+WTAB.toUpperCase()+' layout</div>';
  Object.keys(WREG).forEach(function(id){
    var on=!!WLAY[WTAB][id];
    h+='<button '+(on?'disabled':'onclick="closeDD();addWidget(\''+id+'\')"')+'><span style="color:'+WREG[id].e+'">▎</span> '+WREG[id].t+(on?' <span class="muted">· on</span>':'')+'</button>';
  });
  m.innerHTML=h;
}
// R63 6a/6b/6c WIDGET DRAG v3 (pointer events): the title bar MOVES; ALL FOUR corner handles
// RESIZE with CONTINUOUS pixel feedback (inline width/height) that SNAPS when within ~8px of a
// gridline. Spans track the pixel box live, wPack reflows neighbors on a throttled pointermove
// (push/shrink live), and the release commits spans + persists (pixel overrides cleared).
(function(){
  var st=null;
  function cell(){var g=document.getElementById('wgrid');var r=g?g.getBoundingClientRect():{width:1200};return {w:(r.width-11)/12,h:91};}
  function packSoon(){if(window._packT)return;window._packT=setTimeout(function(){window._packT=null;wPack();},90);} // 6c: neighbor-aware, throttled
  function onDown(e){
    var t=e.target;if(!t||!t.closest)return;
    if(t.closest('.wbtn')||t.closest('input')||t.closest('select'))return; // title-bar buttons still click
    var rs=t.closest('.wrs'),hd=t.closest('.wh');
    if(!rs&&!hd)return;
    var id=(rs||hd).getAttribute('data-wid');if(!id||!WLAY[WTAB][id])return;
    var el=document.getElementById('w_'+id);
    st={id:id,mode:(rs?'rs':'mv'),corner:(rs?(rs.getAttribute('data-corner')||'se'):''),
        x:e.clientX,y:e.clientY,g:WLAY[WTAB][id].slice(),
        pw:(el?el.offsetWidth:0),ph:(el?el.offsetHeight:0)};
    if(el)el.classList.add('drag');
    e.preventDefault();
  }
  function onMove(e){
    if(!st)return;
    var c=cell(),g=WLAY[WTAB][st.id];if(!g)return;
    var dx=e.clientX-st.x,dy=e.clientY-st.y;
    if(st.mode==='rs'){
      var sx=(st.corner==='nw'||st.corner==='sw')?-1:1;
      var sy=(st.corner==='nw'||st.corner==='ne')?-1:1;
      var pw=Math.max(c.w*2-1,st.pw+sx*dx), ph=Math.max(c.h-1,st.ph+sy*dy); // free pixels (min 2×1 cells)
      function snap(px,unit){var n=Math.max(1,Math.round(px/unit));var tgt=n*unit-1;return (Math.abs(px-tgt)<=8)?tgt:px;} // 3a: snap ONLY within ~8px of a gridline
      pw=snap(pw,c.w);ph=snap(ph,c.h);
      st.pxW=pw;st.pxH=ph; // remembered for the release decision (keep free px vs commit spans)
      // Spans COVER the pixel box (ceil, tiny epsilon for float noise) so neighbors reflow around
      // the true size; the pixel override on the layout entry is what actually renders.
      var w=Math.max(2,Math.min(12,Math.ceil((pw+1)/c.w-0.02))),h=Math.max(1,Math.min(30,Math.ceil((ph+1)/c.h-0.02)));
      var x=g[0],y=g[1];
      if(st.corner==='nw'||st.corner==='sw')x=Math.max(1,st.g[0]+(st.g[2]-w)); // left-edge corners keep the RIGHT edge pinned
      if(st.corner==='nw'||st.corner==='ne')y=Math.max(1,st.g[1]+(st.g[3]-h)); // top-edge corners keep the BOTTOM pinned
      if(x+w>13)w=13-x;
      g[0]=x;g[1]=y;g[2]=w;g[3]=h;g[4]={pw:pw,ph:ph}; // live pixel box rides the layout so wPlace renders it exactly
      wPlace(st.id);
      packSoon(); // 3d: neighbors reflow live (throttled) while the pointer drags
      if(!window._pnlRszT)window._pnlRszT=setTimeout(function(){window._pnlRszT=null;if(typeof redrawPnlCharts==='function')redrawPnlCharts();},120);
    }else{
      var mdx=Math.round(dx/c.w),mdy=Math.round(dy/c.h);
      var x2=Math.max(1,Math.min(13-st.g[2],st.g[0]+mdx)),y2=Math.max(1,st.g[1]+mdy);
      if(x2!==g[0]||y2!==g[1]){g[0]=x2;g[1]=y2;wPlace(st.id);packSoon();}
    }
  }
  function onUp(){if(!st)return;
    var el=document.getElementById('w_'+st.id);
    if(el)el.classList.remove('drag');
    var g=WLAY[WTAB][st.id];
    if(g&&st.mode==='rs'&&st.pxW!=null){ // R63 3a: release ON a gridline → clean spans; off-grid → the free pixel size PERSISTS
      var c2=cell();
      var onW=Math.abs(st.pxW-(Math.round((st.pxW+1)/c2.w)*c2.w-1))<=0.5;
      var onH=Math.abs(st.pxH-(Math.round((st.pxH+1)/c2.h)*c2.h-1))<=0.5;
      if(onW&&onH){if(g.length>4)g.length=4;}
      else{g[4]={pw:Math.round(st.pxW),ph:Math.round(st.pxH)};}
    }
    st=null;
    wPack();renderWidgets();wSave(); // wPlace re-applies (or clears) the pixel box from the layout entry
    if(typeof redrawPnlCharts==='function')redrawPnlCharts();}
  document.addEventListener('pointerdown',onDown,true);
  document.addEventListener('pointermove',onMove,true);
  document.addEventListener('pointerup',onUp,true);
}());
// R63 6d TABLE COLUMN RESIZER — drag near a th's right edge in ANY widget table; widths persist
// per widget-table (key = widget#tableIndex#colCount) inside the pms_layout_v1 blob and re-apply
// after re-renders (renderWidgets + a slow interval, since panels rebuild via innerHTML).
function wTblKey(wid,idx,ncols){return wid+'#'+idx+'#'+ncols;}
function applyColWidths(){
  var store=WCW[WTAB]||{};
  Object.keys(WLAY[WTAB]||{}).forEach(function(wid){
    var w=document.getElementById('w_'+wid);if(!w||w.style.display==='none')return;
    var tables=w.querySelectorAll('.wb table');
    Array.prototype.forEach.call(tables,function(tb,idx){
      var ths=tb.querySelectorAll('thead th');if(!ths.length)return;
      var ws=store[wTblKey(wid,idx,ths.length)];if(!ws)return;
      var cg=tb.querySelector('colgroup');
      if(!cg){cg=document.createElement('colgroup');for(var i=0;i<ths.length;i++)cg.appendChild(document.createElement('col'));tb.insertBefore(cg,tb.firstChild);}
      var cols=cg.querySelectorAll('col');
      for(var i=0;i<cols.length&&i<ws.length;i++){if(ws[i]>0)cols[i].style.width=ws[i]+'px';}
      tb.style.tableLayout='fixed';
    });
  });
}
setInterval(function(){if(document.hidden||uiIdle()||Date.now()<uiHold)return;try{applyColWidths();}catch(e){}},1500); // R70 (audit §a P3): gated like every other repaint loop
(function(){
  var cs=null;
  function thAt(e){var t=e.target;return (t&&t.closest)?t.closest('.wdg .wb table thead th'):null;}
  document.addEventListener('pointermove',function(e){
    if(cs){
      var nw=Math.max(24,cs.w0+(e.clientX-cs.x));
      cs.col.style.width=nw+'px';cs.tb.style.tableLayout='fixed';
      e.preventDefault();return;
    }
    var th=thAt(e);if(!th)return;
    var r=th.getBoundingClientRect();
    th.style.cursor=(r.right-e.clientX<=6)?'col-resize':'';
  },true);
  document.addEventListener('pointerdown',function(e){
    var th=thAt(e);if(!th)return;
    var r=th.getBoundingClientRect();if(r.right-e.clientX>6)return;
    var tb=th.closest('table'),wdg=th.closest('.wdg');if(!tb||!wdg)return;
    var head=th.parentNode,ci=Array.prototype.indexOf.call(head.children,th);
    var cg=tb.querySelector('colgroup');
    if(!cg){cg=document.createElement('colgroup');for(var i=0;i<head.children.length;i++)cg.appendChild(document.createElement('col'));tb.insertBefore(cg,tb.firstChild);}
    var cols=cg.querySelectorAll('col');if(ci>=cols.length)return;
    var tbs=wdg.querySelectorAll('.wb table'),tIdx=Array.prototype.indexOf.call(tbs,tb);
    cs={x:e.clientX,w0:th.offsetWidth,col:cols[ci],tb:tb,wid:wdg.id.replace(/^w_/,''),tIdx:tIdx,nc:head.children.length};
    uiHold=Date.now()+60000; // freeze panel re-renders while dragging a column (innerHTML would kill the table)
    e.preventDefault();e.stopPropagation();
  },true);
  document.addEventListener('pointerup',function(){
    if(!cs)return;
    var ws=[];Array.prototype.forEach.call(cs.tb.querySelectorAll('thead th'),function(t2){ws.push(t2.offsetWidth||0);});
    (WCW[WTAB]=WCW[WTAB]||{})[wTblKey(cs.wid,cs.tIdx,cs.nc)]=ws;
    wSave();
    uiHold=Date.now()+800;
    cs=null;
  },true);
}());
// R63 4a: ONE fullscreen toggle used by BOTH the F11 handler and the new bar1 ⛶ button — WebView2
// swallows the F11 chrome shortcut, so an in-page requestFullscreen path must always exist.
// R67c: the API call now runs DIRECTLY in the click gesture, its rejection is surfaced to a toast,
// and a rejecting host (WebView2 without element-fullscreen) falls back to GET /api/fullscreen —
// the server toggles the real app window via user32 (borderless maximize).
function uiToast(msg){
  var t=document.getElementById('uitoast');
  if(!t){t=document.createElement('div');t.id='uitoast';
    t.style.cssText='position:fixed;bottom:16px;left:50%;transform:translateX(-50%);z-index:500;background:#24272d;border:1px solid #33373e;color:var(--text);padding:6px 12px;font-size:10.5px;border-radius:2px;box-shadow:0 10px 30px rgba(0,0,0,.6);max-width:72vw;display:none;';
    document.body.appendChild(t);}
  t.textContent=String(msg||'');t.style.display='block';
  clearTimeout(window._toastT);window._toastT=setTimeout(function(){t.style.display='none';},4000);
}
function fsFallback(why){
  if(why)uiToast('fullscreen API rejected ('+why+') — toggling the app window instead');
  fetch('/api/fullscreen').then(function(r){return r.json();}).then(function(d){
    if(d&&d.error)uiToast('fullscreen: '+d.error);
  }).catch(function(){uiToast('fullscreen: server fallback unreachable');});
}
function toggleFullscreen(){
  try{
    if(document.fullscreenElement){
      var xp=document.exitFullscreen?document.exitFullscreen():null;
      if(xp&&xp.catch)xp.catch(function(){fsFallback('');});
      return;
    }
    var el=document.documentElement;
    if(el.requestFullscreen){
      var p=el.requestFullscreen(); // called synchronously inside the user gesture (R67c i)
      if(p&&p.then){p.then(function(){},function(err){fsFallback((err&&err.message)||'not allowed');});}
      return;
    }
    fsFallback('no requestFullscreen in this host');
  }catch(err){fsFallback((err&&err.message)||'exception');}
}
function initWidgets(){ // boot: restore layouts, honor ?pms_tab=<any tab>, wire P/L keys + F11, start the clock
  wLoad();
  var qs=new URLSearchParams(location.search),qt=qs.get('pms_tab');
  if(qt==='live'||qt==='paper')WTAB=qt;
  UITAB=WTAB;
  renderWidgets();buildWidgetMenu();
  if(qt&&UIPANELS[qt])uiTab(qt); // explicit deep link wins
  else if(qt==='paper'||qt==='live')setTab(qt,true);
  else uiTab('overview'); // R138: begin with the human research digest, not an execution ledger
  window.addEventListener('keydown',function(e){
    if(e.key==='F11'){ // R62 item 8 / R63 4a: F11 + the ⛶ button share toggleFullscreen()
      e.preventDefault();
      toggleFullscreen();
      return;
    }
    var t=e.target;if(t&&(t.tagName==='INPUT'||t.tagName==='SELECT'||t.tagName==='TEXTAREA'))return;
    if(e.key==='p'||e.key==='P')setTab('paper');
    else if(e.key==='l'||e.key==='L')setTab('live');
  });
  window.addEventListener('resize',function(){ // R62 item 12: P&L charts refit on window resize
    if(window._pnlRedrawT)clearTimeout(window._pnlRedrawT);
    window._pnlRedrawT=setTimeout(function(){if(typeof redrawPnlCharts==='function')redrawPnlCharts();},150);
  });
  // R70 (audit §a P1 accessibility): the tab strip was onclick <span>s with ZERO keyboard path.
  // Every bar2 tab/action becomes focusable (tab role, tabindex) and Enter/Space activates it —
  // one loop instead of 16 hand-edited spans; aria-selected tracks the active tab via uiMarkTabs.
  document.querySelectorAll('#bar2 .mtab, #bar2 .ni').forEach(function(el){
    el.setAttribute('tabindex','0');
    if(el.classList.contains('mtab'))el.setAttribute('role','tab');else el.setAttribute('role','button');
    el.addEventListener('keydown',function(e){
      if(e.key==='Enter'||e.key===' '){e.preventDefault();el.click();}
    });
  });
  setInterval(tickClk,1000);tickClk();
}
// R60 C8 EDITABLE TUNABLES: any numeric chip → click → inline input → Enter POSTs {key:value} to
// the existing /api/settings endpoint (exactly the Settings tab's save path). Esc/blur cancels.
function editTunable(key,cur,el){
  if(!el||el.querySelector('input'))return;
  var old=el.innerHTML;
  el.innerHTML='<input type="number" step="any" style="width:64px;font-size:10px;" value="'+cur+'">';
  var inp=el.querySelector('input');inp.focus();inp.select();
  var doneOnce=false;
  function done(save){
    if(doneOnce)return;doneOnce=true;
    if(save){
      var v=parseFloat(inp.value);
      if(!isNaN(v)){
        var b={};b[key]=v;
        fetch('/api/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(b)})
          .then(function(r){return r.json();})
          .then(function(){window._livePropsHTML=null;loadPaper();loadRisk();loadLive();}).catch(function(){});
      }
    }
    el.innerHTML=old;
  }
  inp.addEventListener('keydown',function(e){e.stopPropagation();if(e.key==='Enter')done(true);else if(e.key==='Escape')done(false);});
  inp.addEventListener('blur',function(){done(false);});
}
function echip(key,val,disp,tip){return '<span class="echip" title="'+escapeHtml(tip||('click to edit — saves '+key))+'" onclick="editTunable(\''+key+'\','+(Number(val)||0)+',this)">'+disp+'</span>';}
// R60 C8 gate popover (⚙ on PROPOSED): the live-proposal gate tunables, saved via /api/settings.
// min-24h-volume + the price band are config fields now too (live_min_vol_24h_usd,
// live_px_band_min_c/_max_c) — 0 restores the historical defaults ($20k, 2–98¢).
function toggleGates(ev){
  if(ev)ev.stopPropagation();
  var pop=document.getElementById('gatePop');
  if(pop){pop.remove();return;}
  var host=document.getElementById('w_proposed');if(!host)return;
  pop=document.createElement('div');pop.id='gatePop';pop.className='gatepop';
  pop.innerHTML='Loading…';
  host.appendChild(pop);
  jget('/api/settings').then(function(d){
    d=d||{};
    function row(k,lbl,tip){return '<label class="fld" title="'+tip+'">'+lbl+'<input id="gp_'+k+'" type="number" step="any" value="'+(d[k]!=null?d[k]:'')+'" style="width:96px"></label>';}
    pop.innerHTML='<div class="muted" style="margin-bottom:4px;">PROPOSAL GATES · saved to config via /api/settings</div>'
      +'<div style="display:flex;gap:8px;flex-wrap:wrap;align-items:flex-end;">'
      +row('min_ev_per_contract','EV floor $/ct','model edge floor AND the net-EV floor at the LIVE price: a proposal needs p_win − live price − fee ≥ this (0.01 = +1¢)')
      +row('consensus_max_hours_out','Max hours out','only propose markets resolving within this many hours (non-crypto)')
      +row('consensus_crypto_max_hours_out','Crypto max h','the same horizon gate for crypto windows')
      +row('live_min_vol_24h_usd','Min 24h vol $','Kalshi liquidity floor: min 24h volume for a market to propose (0 = default 20000)')
      +row('live_px_band_min_c','Px band lo ¢','only propose inside this price band — low end in cents, applies to model AND live prices on both venues (0 = default 2)')
      +row('live_px_band_max_c','Px band hi ¢','price band high end in cents (0 = default 98)')
      +row('ml_pwin_min','ML p_win min','R90 plausibility band floor: reject ML picks whose model p_win is below this (default 0.05; 0 disables)')
      +row('ml_pwin_max','ML p_win max','R90 plausibility band ceiling: reject picks above this (default 0.95; 0 disables)')
      +row('ml_ev_min_cents','ML EV min ¢','R90 border floor: net-of-fees EV at the live price must be ≥ this many cents, applied AFTER the realization haircut (default 2; 0 disables)')
      +row('ml_ev_max_cents','ML EV max ¢','R90 too-good ceiling: net EV above this QUARANTINES the pick (audit category mlquarantine, never bet; default 20; 0 disables)')
      +'</div><div class="muted" style="margin:4px 0;">volume floor + price band show their EFFECTIVE values; set 0 to restore the defaults ($20k · 2–98¢). ML borders (R90) apply after the per-family realization haircut.</div>'
      +'<button class="mini" onclick="saveGates()">Save</button> <button class="mini" onclick="toggleGates()">Close</button><span id="gateMsg" class="muted" style="margin-left:6px;"></span>';
  }).catch(function(){pop.innerHTML='<span style="color:var(--bad)">could not load settings</span> <button class="mini" onclick="toggleGates()">Close</button>';});
}
function saveGates(){
  var body={};
  ['min_ev_per_contract','consensus_max_hours_out','consensus_crypto_max_hours_out','live_min_vol_24h_usd','live_px_band_min_c','live_px_band_max_c','ml_pwin_min','ml_pwin_max','ml_ev_min_cents','ml_ev_max_cents'].forEach(function(k){
    var e=document.getElementById('gp_'+k);if(e&&e.value!=='')body[k]=parseFloat(e.value);});
  fetch('/api/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)})
    .then(function(r){return r.json();}).then(function(){var m=document.getElementById('gateMsg');if(m)m.textContent='saved ✓';window._livePropsHTML=null;loadLive();})
    .catch(function(){var m=document.getElementById('gateMsg');if(m)m.textContent='save failed';});
}
// R60 C7 copy-all buttons (JSON, like copyLogSub).
function copyLiveHist(b){
  var d=window._liveData||{};
  var out={kalshi_fills:d.kalshi_fills||[],polyus_activities:(d.polyus&&d.polyus.activities)||[]};
  navigator.clipboard.writeText(JSON.stringify(out,null,1)).then(function(){if(b){b.textContent='✓ copied';setTimeout(function(){b.textContent='⧉ Copy all';},1200);}});
}
function copyLiveLogJSON(b){
  navigator.clipboard.writeText(JSON.stringify(((window._liveData||{}).log)||[],null,1)).then(function(){if(b){b.textContent='✓ copied';setTimeout(function(){b.textContent='⧉ Copy all';},1200);}});
}
// R60 ml-picks widget: top ML predictions (/api/ml, cached by loadPaper) + the live $1 combo
// proposals (/api/live/combos, cached by loadCombos) — "ml picks and combo picks" in one pane.
function renderMlPicks(){ // R63 6e: SPLIT into TWO widgets — ML PICKS (mlPicksW) | COMBO PICKS (mlComboPicksW)
  var el=document.getElementById('mlPicksW');
  var ml=window._mlData,pr=ml&&ml.predictions;
  if(el){
    var h='<div class="wsec">ML PICKS <span class="muted" style="font-weight:400;letter-spacing:0;text-transform:none;">+EV at the CURRENT price (R98 live-priced ranking; widget body scrolls)</span></div>';
    // R98: same live-priced actionability as the ML tab — rank/filter by fee-net EV at the
    // current venue price; picks that already ran drop off instead of flexing stale +EV.
    var evLiveW=function(x){return (x.ev_net_live!=null)?x.ev_net_live:((x.ev_net!=null)?x.ev_net:(x.ev_per_contract||0));};
    var ps=((pr&&pr.predictions)||[]).filter(function(x){return evLiveW(x)>0;}).sort(function(a,b){return evLiveW(b)-evLiveW(a);});
    if(!ps.length){h+='<div class="muted" style="padding:1px 4px;">No +EV ML picks right now (the sidecar warms up a few minutes after start).</div>';}
    else{
      h+='<table class="mkt"><colgroup><col><col style="width:38px"><col style="width:34px"><col style="width:44px"><col style="width:48px"><col style="width:40px"></colgroup><thead><tr><th>Market</th><th>Side</th><th class="r" title="current venue price when live-quoted (signal price otherwise)">@¢</th><th class="r">p_win</th><th class="r" title="fee-net EV/ct at the CURRENT price (~ = no live quote, signal-time EV)">EV/ct</th><th class="r">ROI</th></tr></thead><tbody>';
      ps.forEach(function(x){var isLive=(x.ev_net_live!=null);var evc=evLiveW(x);var pxc=(x.live_price!=null)?x.live_price:(x.price||0);var roiW=isLive?(x.roi_live||0):(x.roi||0);
        h+='<tr><td class="ell" title="'+escapeHtml(x.title||x.ticker||'')+'"><a class="go" href="'+posURL(x)+'" target="_blank" rel="noopener">'+escapeHtml((x.title||x.ticker||''))+'</a></td><td>'+escapeHtml(x.side||'')+'</td><td class="r">'+Math.round(pxc*100)+'</td><td class="r">'+(x.p_win||0).toFixed(2)+'</td><td class="r" style="color:'+(evc>=0?'var(--good)':'var(--bad)')+'"><b>'+(isLive?'':'~')+(evc>=0?'+':'')+(evc*100).toFixed(1)+'¢</b></td><td class="r muted">'+Math.round(roiW*100)+'%</td></tr>';});
      h+='</tbody></table>';
    }
    withScroll(el,h);
  }
  var el2=document.getElementById('mlComboPicksW');
  if(el2){
    var cs=(window._liveCombos||[]).slice(0,8);
    var h2='<div class="wsec">COMBO PICKS <span class="muted" style="font-weight:400;letter-spacing:0;text-transform:none;">live $1 RFQ proposals — accept in the COMBOS RFQ widget</span></div>';
    if(!cs.length){h2+='<div class="muted" style="padding:1px 4px;">No live combo proposals this moment (crypto legs roll every 15 min).</div>';}
    else{
      h2+='<table class="mkt"><colgroup><col><col style="width:24px"><col style="width:34px"><col style="width:40px"><col style="width:44px"><col style="width:46px"></colgroup><thead><tr><th>Legs</th><th class="r">#</th><th class="r">@¢</th><th class="r">Pay×</th><th class="r">p_win</th><th class="r">EV/ct</th></tr></thead><tbody>';
      cs.forEach(function(c){var evc=(c.p_win||0)-(c.price||0);var plainNames=(c.legs||[]).map(function(l){return (l.outcome||l.title||l.ticker||'').slice(0,22);}).join(' + ');
        var names=(c.legs||[]).map(function(l){return '<a class="go" href="'+posURL({platform:'kalshi',ticker:l.ticker,title:l.title})+'" target="_blank" rel="noopener">'+escapeHtml((l.outcome||l.title||l.ticker||'').slice(0,22))+'</a>';}).join(' + '); // R62 item 5: every leg clickable
        h2+='<tr><td title="'+escapeHtml(plainNames)+'">'+names+'</td><td class="r">'+(c.n_legs||(c.legs||[]).length)+'</td><td class="r">'+Math.round((c.price||0)*100)+'</td><td class="r">'+((c.payout_mult||0)).toFixed(1)+'×</td><td class="r">'+(c.p_win||0).toFixed(2)+'</td><td class="r" style="color:'+(evc>=0?'var(--good)':'var(--bad)')+'"><b>'+(evc>=0?'+':'')+(evc*100).toFixed(1)+'¢</b></td></tr>';});
      h2+='</tbody></table>';
    }
    withScroll(el2,h2);
  }
}
// R63 6e SHADOW BOOK widget — renders from the /api/ml payload loadPaper already caches (_mlData).
function renderShadowBook(){
  var el=document.getElementById('shadowBookW');if(!el)return;
  var sh=window._mlData&&window._mlData.shadow;
  function mny(v){v=v||0;return (v<0?'-$':'$')+Math.abs(v).toFixed(2);}
  function col(v){return (v||0)>=0?'var(--good)':'var(--bad)';}
  if(!sh||!sh.stats){withScroll(el,'<div class="muted" style="padding:2px 4px;">ML sidecar shadow book not available yet.</div>');return;}
  var ss=sh.stats;
  var h='<div class="wsum">net <b style="color:'+col(ss.net)+'">'+mny(ss.net)+'</b> · closed '+(ss.closed||0)+' · open '+(ss.open_n||0)+' · <span title="R64: buys EVERY pick with positive GROSS edge at the live price (p_win − px &gt; 0) — no fee subtraction, no EV floors; only the live-price + 5–95¢ integrity guards remain. Flat-stake, $800 bank.">all +EV, gross of fees · $800</span></div>';
  var sop=((sh.open)||[]).slice().sort(function(a,b){return (b.unrealized||0)-(a.unrealized||0);}).slice(0,15);
  if(!sop.length){h+='<div class="muted" style="padding:2px 4px;">No open shadow picks.</div>';}
  else{
    h+='<table class="mkt"><colgroup><col><col style="width:34px"><col style="width:56px"><col style="width:56px"><col style="width:54px"></colgroup><thead><tr><th>Pick</th><th>Side</th><th class="r">@→Now¢</th><th class="r">Bought</th><th class="r">P&amp;L</th></tr></thead><tbody>';
    sop.forEach(function(p){var cur=(p.cur_price!=null)?p.cur_price:null;
      h+='<tr><td class="ell" title="'+escapeHtml(p.title||p.ticker||'')+'"><a class="go" href="'+posURL(p)+'" target="_blank" rel="noopener">'+escapeHtml(p.title||p.ticker||'')+'</a></td><td>'+escapeHtml(p.side||'')+'</td><td class="r">'+Math.round((p.price||0)*100)+'→'+(cur!=null?(Math.round(cur*100)+'¢'):'—')+'</td>'+boughtCell(p.opened)+'<td class="r" style="color:'+col(p.unrealized)+'">'+(cur!=null?mny(p.unrealized):'—')+'</td></tr>';});
    h+='</tbody></table>';
  }
  withScroll(el,h);
}
// R105 RAWFLOW BOOK widget — renders from the same /api/ml cache (_mlData.rawflow); the ML-tab
// panel shows the full card, this is the compact addable-widget form (shadow-book pattern).
function renderRawflowBook(){
  var el=document.getElementById('rawflowBookW');if(!el)return;
  var rb=window._mlData&&window._mlData.rawflow;
  function mny(v){v=v||0;return (v<0?'-$':'$')+Math.abs(v).toFixed(2);}
  function col(v){return (v||0)>=0?'var(--good)':'var(--bad)';}
  if(!rb||rb.enabled===false){withScroll(el,'<div class="muted" style="padding:2px 4px;">RawFlow book not enabled (alloc_rawflow=0 or legacy flat bankrolls).</div>');return;}
  var rn=(rb.closed||0);var rwr=(rn>0)?(Math.round((rb.win_rate||0)*100)+'%'):'—';
  var h='<div class="wsum" title="'+escapeHtml(rb.rules||'')+'">net <b style="color:'+col(rb.net)+'">'+mny(rb.net)+'</b> · closed '+rn+' · open '+(rb.open||0)+' · win '+rwr+' · <span title="raw kalshi-flow signals, band '+escapeHtml(rb.band||'10–50¢')+', realized-only Kelly on own equity, no ML">band '+escapeHtml(rb.band||'10–50¢')+' · eq '+mny(rb.equity!=null?rb.equity:(rb.bank||0))+'</span></div>';
  var rop=((rb.open_lots)||[]).slice().sort(function(a,b){return (b.unrealized||0)-(a.unrealized||0);}).slice(0,15);
  if(!rop.length){h+='<div class="muted" style="padding:2px 4px;">No open RawFlow lots.</div>';}
  else{
    h+='<table class="mkt"><colgroup><col><col style="width:34px"><col style="width:56px"><col style="width:56px"><col style="width:54px"></colgroup><thead><tr><th>Lot</th><th>Side</th><th class="r">@→Now¢</th><th class="r">Bought</th><th class="r">P&amp;L</th></tr></thead><tbody>';
    rop.forEach(function(p){var cur=(p.cur_price!=null)?p.cur_price:null;
      h+='<tr><td class="ell" title="'+escapeHtml(p.title||p.ticker||'')+'"><a class="go" href="'+posURL({platform:'kalshi',ticker:p.ticker,title:p.title})+'" target="_blank" rel="noopener">'+escapeHtml(p.title||p.ticker||'')+'</a></td><td>'+escapeHtml(p.side||'')+'</td><td class="r">'+Math.round((p.price||0)*100)+'→'+(cur!=null?(Math.round(cur*100)+'¢'):'—')+'</td>'+boughtCell(p.opened)+'<td class="r" style="color:'+col(p.unrealized)+'">'+(cur!=null?mny(p.unrealized):'—')+'</td></tr>';});
    h+='</tbody></table>';
  }
  withScroll(el,h);
}
function loadRFQSim(){ // R106 (edge 36): the would-quote simulator panel — /api/rfqsim
  var el=document.getElementById('rfqSimW');if(!el)return;
  jget('/api/rfqsim').then(function(d){
    var el2=document.getElementById('rfqSimW');if(!el2)return;
    function mny(v){v=v||0;return (v<0?'-$':'$')+Math.abs(v).toFixed(2);}
    function col(v){return (v||0)>=0?'var(--good)':'var(--bad)';}
    var st=d.stats||{};
    var h='<div class="wsum" title="'+escapeHtml(d.note||'')+'">adverse P&L <b style="color:'+col(st.pnl_adverse)+'">'+mny(st.pnl_adverse)+'</b> · graded '+(st.graded||0)+'/'+(st.quoted||0)+' quoted · open '+(d.open||0)+'</div>';
    h+='<div class="muted" style="font-size:11.5px;padding:1px 4px">if only YES-hit '+mny(st.pnl_yes_hit)+' · if only NO-hit '+mny(st.pnl_no_hit)+' · margin '+((d.margin&&d.margin.floor_c)||3)+'¢ floor · log-only, no orders</div>';
    var gs=(d.grades||[]).slice(-8).reverse();
    if(gs.length){
      h+='<table class="mkt"><thead><tr><th>Combo</th><th class="r">quote</th><th class="r">settle</th><th class="r">adverse</th></tr></thead><tbody>';
      gs.forEach(function(g){h+='<tr><td class="name ell" title="'+escapeHtml(g.ticker||'')+'">'+escapeHtml((g.ticker||'').slice(0,26))+'</td><td class="r">'+Math.round((g.yes_bid||0)*100)+'/'+Math.round((g.no_bid||0)*100)+'¢</td><td class="r">'+Math.round((g.settle_val||0)*100)+'¢</td><td class="r" style="color:'+col(g.pnl_adverse)+'">'+mny(g.pnl_adverse)+'</td></tr>';});
      h+='</tbody></table>';
    } else { h+='<div class="muted" style="padding:2px 4px">No settled quotes yet — grades land as combos resolve.</div>'; }
    withScroll(el2,h);
  }).catch(function(){});
}
function loadVerdicts(){ // R145: System-result validation panel — /api/verdicts (always-valid confidence sequences)
  var el=document.getElementById('verdictsW');if(!el)return;
  jget('/api/verdicts').then(function(d){
    var el2=document.getElementById('verdictsW');if(!el2)return;
    var vs=d.systems||d.experiments||[];
    function sc(st){return st==='PROVEN+'?'var(--good)':(st==='PROVEN-'?'var(--bad)':(st==='RESEARCH+'?'#6ba3f8':(st==='RESEARCH-'?'#f59e0b':'var(--muted)')));}
    var h='<div class="wsum" title="'+escapeHtml((d.method||'')+' · '+(d.profit_contract||''))+'">'+vs.length+' systems · confidence math over each stated evidence class</div>';
    h+='<div class="muted" style="font-size:11px;padding:2px 4px"><b>EXCHANGE PROFIT</b> requires an authenticated, fill-conditioned exchange cohort. <b>RESEARCH ONLY</b> keeps the raw modeled/replay diagnostic but cannot promote, size, or authorize LIVE. A confidence range can be mathematically valid for a simulation without proving cash profit.</div>';
    var none=(d.depth_src_24h&&d.depth_src_24h.none)||0,tot=0;for(var k in (d.depth_src_24h||{}))tot+=d.depth_src_24h[k];
    if(tot>0)h+='<div class="muted" style="font-size:11px;padding:1px 4px" title="Part-3 coverage: posts whose book we could not observe at decision time (target <5%)">blind posts 24h: '+(100*none/tot).toFixed(1)+'% ('+none+'/'+tot+')</div>';
    if(vs.length){
      h+='<table class="mkt"><thead><tr><th>System + evidence</th><th class="r">n</th><th class="r" title="raw fee-net metric for the row’s stated evidence class; research rows are not exchange P&amp;L">class metric</th><th class="r" title="always-valid range for this observation class; it proves exchange profit only when the row says EXCHANGE PROFIT">95% class range</th><th class="r" title="standard errors from zero inside this evidence class (context only)">SDs</th><th class="r">operator state</th></tr></thead><tbody>';
      vs.forEach(function(v){
        var rng=(isFinite(v.ci_lo)&&isFinite(v.ci_hi)&&Math.abs(v.ci_lo)<100&&Math.abs(v.ci_hi)<100)?(v.ci_lo.toFixed(3)+'…'+v.ci_hi.toFixed(3)):'—';
        var exchange=v.profit_evidence===true&&v.fill_conditioned===true;
        var badge=exchange?'<span style="font-size:9px;color:var(--good);font-weight:700">EXCHANGE PROFIT</span>':'<span style="font-size:9px;color:var(--muted);font-weight:700">RESEARCH ONLY</span>';
        var evidence=String(v.evidence_tier||'unclassified').replaceAll('_',' ');
        var detail=v.group+' · '+v.unit+' · '+evidence+' · fill-conditioned '+(v.fill_conditioned===true?'yes':'no')+' · profit evidence '+(v.profit_evidence===true?'yes':'no')+' · LIVE authority '+(v.live_authorizes===true?'yes':'no')+(v.void_reason?(' · '+v.void_reason):'')+(v.simulation_state?(' · raw research state '+v.simulation_state):'')+(v.venue_locked?' · VENUE-LOCKED':'');
        h+='<tr'+(!exchange?' style="opacity:.78"':'')+'><td class="name ell" title="'+escapeHtml(detail)+'">'+systemLayerBadge(v.origin_layer||v.group)+' '+escapeHtml(v.family)+(v.venue_locked?' <b style="color:#f59e0b">VENUE-LOCKED</b>':'')+'<div>'+badge+' <span class="muted" style="font-size:9px">'+escapeHtml(evidence)+'</span></div></td><td class="r">'+v.n+'</td><td class="r" style="color:'+(exchange?((v.mean||0)>=0?'var(--good)':'var(--bad)'):'var(--muted)')+'">'+(v.mean>=0?'+':'')+(v.mean||0).toFixed(3)+'</td><td class="r">'+rng+'</td><td class="r">'+(v.sds_from_zero||0).toFixed(1)+'</td><td class="r"><b style="color:'+sc(v.state)+'">'+escapeHtml(v.state)+'</b><div class="muted" style="font-size:9px">LIVE '+(v.live_authorizes===true?'eligible by this row':'no')+'</div></td></tr>';});
      h+='</tbody></table>';
    } else { h+='<div class="muted" style="padding:2px 4px">No systems with graded results yet.</div>'; }
    withScroll(el2,h);
  }).catch(function(){});
}
function renderWeatherBook(){ // R106: the weather-book widget (renderRawflowBook pattern, /api/ml "weather" block)
  var el=document.getElementById('weatherBookW');if(!el)return;
  var wb=window._mlData&&window._mlData.weather;
  function mny(v){v=v||0;return (v<0?'-$':'$')+Math.abs(v).toFixed(2);}
  function col(v){return (v||0)>=0?'var(--good)':'var(--bad)';}
  if(!wb||wb.enabled===false){withScroll(el,'<div class="muted" style="padding:2px 4px;">Weather book not enabled (alloc_weather=0 or legacy flat bankrolls).</div>');return;}
  var wn=(wb.closed||0);var wwr=(wn>0)?(Math.round((wb.win_rate||0)*100)+'%'):'—';
  var h='<div class="wsum" title="'+escapeHtml(wb.rules||'')+'">net <b style="color:'+col(wb.net)+'">'+mny(wb.net)+'</b> · closed '+wn+' · open '+(wb.open||0)+' · win '+wwr+' · <span title="wxedge signals (NWS-anchored temp ladders), band '+escapeHtml(wb.band||'20–40¢')+', realized-only Kelly on own equity, no ML">band '+escapeHtml(wb.band||'20–40¢')+' · eq '+mny(wb.equity!=null?wb.equity:(wb.bank||0))+'</span></div>';
  var wop=((wb.open_lots)||[]).slice().sort(function(a,b){return (b.unrealized||0)-(a.unrealized||0);}).slice(0,15);
  if(!wop.length){h+='<div class="muted" style="padding:2px 4px;">No open Weather lots.</div>';}
  else{
    h+='<table class="mkt"><colgroup><col><col style="width:34px"><col style="width:56px"><col style="width:56px"><col style="width:54px"></colgroup><thead><tr><th>Lot</th><th>Side</th><th class="r">@→Now¢</th><th class="r">Bought</th><th class="r">P&amp;L</th></tr></thead><tbody>';
    wop.forEach(function(p){var cur=(p.cur_price!=null)?p.cur_price:null;
      h+='<tr><td class="name ell" title="'+escapeHtml(p.title||p.ticker||'')+'">'+escapeHtml(p.title||p.ticker||'')+'</td><td>'+escapeHtml(p.side||'')+'</td><td class="r">'+Math.round((p.price||0)*100)+(cur!=null?('→'+Math.round(cur*100)):'')+'</td><td class="r">'+mny((p.price||0)*(p.contracts||0))+'</td><td class="r" style="color:'+col(p.unrealized)+'">'+(p.unrealized!=null?mny(p.unrealized):'—')+'</td></tr>';});
    h+='</tbody></table>';
  }
  withScroll(el,h);
}
function renderFreshinvBook(){ // R117: the freshinv-book widget (renderRawflowBook pattern, /api/ml "freshinv" block)
  var el=document.getElementById('freshinvBookW');if(!el)return;
  var fb=window._mlData&&window._mlData.freshinv;
  function mny(v){v=v||0;return (v<0?'-$':'$')+Math.abs(v).toFixed(2);}
  function col(v){return (v||0)>=0?'var(--good)':'var(--bad)';}
  if(!fb||fb.enabled===false){withScroll(el,'<div class="muted" style="padding:2px 4px;">FreshInv Paper simulation is inactive in the legacy research roster (state: '+escapeHtml((fb&&fb.state)||'—')+'). This state never authorizes cash.</div>');return;}
  var fn=(fb.closed||0);var fwr=(fn>0)?(Math.round((fb.win_rate||0)*100)+'%'):'—';
  var h='<div class="wsum" title="'+escapeHtml(fb.rules||'')+'">net <b style="color:'+col(fb.net)+'">'+mny(fb.net)+'</b> · closed '+fn+' · open '+(fb.open||0)+' · win '+fwr+' · <span title="Kalshi fresh listings are faded; PolyUS fresh listings are followed; each uses same-venue evidence">bank '+mny(fb.bank||0)+' · eq '+mny(fb.equity!=null?fb.equity:(fb.bank||0))+(fb.winding_down?' · winding down':'')+'</span></div>';
  var fop=((fb.open_lots)||[]).slice().sort(function(a,b){return (b.unrealized||0)-(a.unrealized||0);}).slice(0,15);
  if(!fop.length){h+='<div class="muted" style="padding:2px 4px;">No open FreshInv lots.</div>';}
  else{
    h+='<table class="mkt"><colgroup><col><col style="width:34px"><col style="width:56px"><col style="width:44px"><col style="width:54px"></colgroup><thead><tr><th>Lot</th><th>Side</th><th class="r">@→Now¢</th><th class="r">Venue</th><th class="r">P&amp;L</th></tr></thead><tbody>';
    fop.forEach(function(p){var cur=(p.cur_price!=null)?p.cur_price:null;
      h+='<tr><td class="ell" title="'+escapeHtml(p.title||p.ticker||'')+'"><a class="go" href="'+posURL({platform:p.platform||'kalshi',ticker:p.ticker,title:p.title})+'" target="_blank" rel="noopener">'+escapeHtml(p.title||p.ticker||'')+'</a></td><td>'+escapeHtml(p.side||'')+'</td><td class="r">'+Math.round((p.price||0)*100)+'→'+(cur!=null?(Math.round(cur*100)+'¢'):'—')+'</td><td class="r muted">'+escapeHtml((p.platform||'kal').slice(0,3))+'</td><td class="r" style="color:'+col(p.unrealized)+'">'+(cur!=null?mny(p.unrealized):'—')+'</td></tr>';});
    h+='</tbody></table>';
  }
  withScroll(el,h);
}
function loadPromotions(){ // R117: the auto-promotion pipeline panel — /api/promotions
  var el=document.getElementById('promotionsW');if(!el)return;
  jget('/api/promotions').then(function(d){
    var el2=document.getElementById('promotionsW');if(!el2)return;
    function sc(st){return (st==='PROMOTED'||st==='GROWN')?'var(--good)':(st==='RETIRED'?'var(--bad)':'var(--muted)');}
    var recs=d.records||[];
    var h='<div class="wsum" title="'+escapeHtml(d.note||'')+'">sim reserve <b>$'+(d.reserve_remaining||0).toFixed(0)+'</b> · simulated NAV $'+(d.nav||0).toFixed(0)+' (sim peak $'+(d.nav_peak||0).toFixed(0)+') · modeled drawdown '+(d.drawdown_pct||0).toFixed(1)+'%'+(d.drawdown_paused?' <b style="color:var(--warn)">PAPER PAUSED</b>':'')+'</div>';
    h+='<div class="muted" style="font-size:11px;padding:1px 4px">PAPER simulations only. Historical PROVEN labels and portfolio growth are research history, not exchange profit evidence and not LIVE authority.</div>';
    var main=recs.filter(function(r){return r.state!=='ELIGIBLE-WATCH'&&r.state!=='ELIGIBLE-NO-EXECUTOR';});
    var watch=recs.length-main.length;
    if(main.length){
      h+='<table class="mkt"><thead><tr><th>Strategy</th><th class="r">Paper-model state</th><th class="r">sim alloc</th><th class="r">ckpts</th></tr></thead><tbody>';
      main.forEach(function(r){var label={PROMOTED:'PAPER MODEL ON',GROWN:'PAPER MODEL GROWN',RETIRED:'PAPER MODEL OFF'}[r.state]||('PAPER '+String(r.state||'RESEARCH'));h+='<tr><td class="name ell" title="'+escapeHtml((r.reason||'')+(r.promoted_at?(' · legacy research timestamp '+r.promoted_at):''))+'">'+escapeHtml(r.family)+'</td><td class="r"><b style="color:'+sc(r.state)+'">'+escapeHtml(label)+'</b></td><td class="r">$'+(r.alloc_usd||0).toFixed(0)+'</td><td class="r muted">'+(r.checkpoints||0)+'</td></tr>';});
      h+='</tbody></table>';
    } else { h+='<div class="muted" style="padding:2px 4px">Nothing in the Paper research roster yet. Research nomination never authorizes LIVE.</div>'; }
    if(watch>0)h+='<div class="muted" style="font-size:11px;padding:1px 4px">'+watch+' more strategies on eligibility watch.</div>';
    withScroll(el2,h);
  }).catch(function(){});
}
var uiHold=0; // re-renders pause until this time — set on any click so panels don't rebuild mid-click
document.addEventListener("mousedown",function(){uiHold=Date.now()+1200;},true);
// Also freeze re-renders while SCROLLING or TYPING — rebuilding big tables mid-scroll is what makes
// the page feel laggy/janky. A short hold means panels rebuild only once the user pauses.
document.addEventListener("wheel",function(){uiHold=Date.now()+800;},{passive:true,capture:true});
document.addEventListener("touchmove",function(){uiHold=Date.now()+800;},{passive:true,capture:true});
document.addEventListener("keydown",function(){uiHold=Date.now()+1500;},true);
var lastStatsView=0,lastHistView=0,lastStatusView=0,lastFeedView=0,lastOrdersView=0,lastResearchView=0;
// R63 6e: is a widget live on the CURRENT grid tab (grid visible + widget on this layout)?
function wOn(id){return UITAB===WTAB&&!!(WLAY[WTAB]&&WLAY[WTAB][id]);}
// Idle pause: after 30 min with no interaction, STOP the UI polling/rendering (the DOM/SVG churn
// is what grows memory and eventually OOMs Chrome on a tab left open all night). The Go server —
// feeds, settlement, the PAPER autobet — keeps running untouched; only the browser UI pauses.
// Any mouse/key/scroll resumes it instantly. Backgrounding the tab pauses it too.
var lastActivity=Date.now(),IDLE_MS=30*60*1000;
function uiIdle(){return Date.now()-lastActivity>IDLE_MS;}
function refresh(){
  if(document.hidden||uiIdle()){var u=document.getElementById("updated");if(u){u.style.color="";u.textContent=document.hidden?"Paused (tab backgrounded) · autobet still running":"Paused (idle 30m — move the mouse to resume) · autobet still running";}return;}
  if(Date.now()<uiHold){document.getElementById("updated").textContent="Live · paused (interacting)";return;}
  // Status (env/connection badge + kill-switch) changes rarely and hits a live Kalshi REST call, so
  // poll it every ~8s instead of every tick — keeps it off the hot path.
  if(Date.now()-lastStatusView>8000){lastStatusView=Date.now();loadStatus();loadNetworth();loadReady();
    if(Date.now()-(window._lastLatView||0)>30000){window._lastLatView=Date.now();loadLatency();}} // R107: latency chip/panel ~30s (matches probe cadence)
  // R75: /api/markets polls ONLY while a markets surface is visible (the MARKETS tab or the
  // markets-kal widget on the current grid) AND only re-fetches rows the user already loaded —
  // loadMarkets() itself no-ops into the Load-button state when nothing is loaded. Every other
  // tab now makes ZERO /api/markets requests (this was the only caller besides the tab loader).
  if(UITAB==='markets'||wOn('markets-kal'))loadMarkets();
  if(UITAB==='markets')gtMaybeRefresh(); // R102: game tree re-fetches ≤1/30s, tab-visible only
  if(openTicker)loadBook();
  // UIFAST: the heavy secondary feed panels (whales / Kalshi consensus / Poly-int / Poly US / arb)
  // each rebuild a big scrolling table. Redraw them at ~1.4/s (every other 700ms tick) instead of
  // every tick — roughly halves DOM churn so scroll/clicks stay smooth. Server-side feeds and the
  // paper autobet are unaffected; this is purely the browser's redraw cadence.
  // R98 lazy tabs: each of these five loaders now NO-OPS until its tab has been opened once
  // (TABSEEN gate inside the function), so a fresh dashboard boot fires ZERO of these requests.
  if(Date.now()-lastFeedView>1400){lastFeedView=Date.now();loadWhales();loadPoly();loadPolyMarkets();loadPolyUS();loadArb();}
  if(Date.now()-lastResearchView>15000){lastResearchView=Date.now();var researchLoaders={overview:loadResearchOverview,systems:loadResearchSystemsPage,experiments:loadResearchExperimentsPage,evidence:loadResearchEvidencePage,data:loadResearchDataPage,operations:loadResearchOperationsPage};var researchLoader=researchLoaders[UITAB];if(researchLoader)researchLoader();}
  loadPaper(); // R59: the PAPER zone is a permanent shell row — repaint on the backstop cadence (SSE "paper" hints repaint it instantly between polls)
  // Stats has the heaviest render (P&L chart SVG + 3 fetches); its data only changes ~1/s, so
  // throttle it to ~1.5s instead of the 7/s screen rate — big cut in allocation/GC churn.
  if((document.getElementById("statscard").style.display==="block"||wOn('stats-backtest')||wOn('stats-netby'))&&Date.now()-lastStatsView>1500){lastStatsView=Date.now();loadStatsView();} // R63 6e: stats widgets keep polling
  if((document.getElementById("historycard").style.display==="block"||wOn('history-closed'))&&Date.now()-lastHistView>2000){lastHistView=Date.now();loadHistoryView();} // R63 3e: the history widget polls too
  if(((document.getElementById("logscard").style.display==="block"&&window._logSub==='orders')||wOn('orders'))&&Date.now()-lastOrdersView>2000){lastOrdersView=Date.now();loadOrders();} // R63 6e · R67e · R70 (audit §a P2): 2s throttle like stats/history
  if(wOn('combo-suggest')&&Date.now()-(window._lastParlayW||0)>5000){window._lastParlayW=Date.now();loadParlay();} // R63 3e: combo-suggestions widget
  loadAuto();
  // R77 audit: STALENESS-AWARE header — age computed from the last SUCCESSFUL /api/live load
  // (stamped in loadLive), NOT the render tick; a data wedge now visibly ages amber (>=30s) then
  // red (>=5min) instead of permanently claiming "updated now".
  (function(){var u=document.getElementById("updated");if(!u)return;
    if(!window._pageT0)window._pageT0=Date.now();
    var ts=window._lastDataTs||0,ageS=Math.max(0,Math.round((Date.now()-(ts||window._pageT0))/1000));
    u.textContent=ts?("Live · updated "+(ageS<2?"now":(ageS<120?(ageS+"s ago"):(Math.floor(ageS/60)+"m ago")))):("Live · waiting for data ("+ageS+"s)");
    u.style.color=(ageS>=300)?"var(--bad)":((ageS>=30)?"var(--warn)":"");
  })();
}
loadNetworth();loadReady();setTimeout(loadLatency,1500); // R107: first latency paint shortly after boot
initWidgets(); // R60: build the widget grid — restores pms_layout_v1, honors ?pms_tab=live, P/L keys, clock
initWinMode(); // R19/R60: ?win=<panel> renders ONE widget full-window (pop-outs for multi-monitor)
openLive(); // the live widgets poll from boot — 1s live snapshot + 10s combos — whichever tab shows
refresh();
// SSE + BroadcastChannel (audit §4): ONE window (the leader, elected via navigator.locks) holds the
// /api/events stream; every refresh hint is relayed on a BroadcastChannel so sibling windows update
// instantly WITHOUT their own sockets. The interval poll below drops from 700ms to a 4s BACKSTOP —
// the push stream is what makes panels feel instant now, not the polling.
(function(){
  var bc=null;try{bc=new BroadcastChannel("kalshi-suite");}catch(e){}
  // R98 lazy tabs: SSE snapshot-completion pushes repaint ONLY armed surfaces — an unopened
  // Research/Stats tab no longer fetches /api/edge//api/curves//api/backtest on every rebuild.
  var panelFns={edge:function(){if(resArmed())loadEdge();},
    curves:function(){if(resArmed())loadCurves();},
    stoplab:function(){if(resArmed())loadCurves();},
    kellylab:function(){if(resArmed())loadCurves();},
    backtest:function(){ // R77 item 3: the backtest snapshot completing must also resolve the
      // Curves "All logged signal types" block + the Research BACKTEST table, not just Stats.
      if(tabArmed('stats')||wOn('stats-backtest')||wOn('stats-netby'))loadStatsView();
      if(resArmed()&&document.getElementById("allSigEV"))loadAllSigEV();
      var bt=document.getElementById("resBT");if(resArmed()&&bt&&bt.style.display!=='none')loadResBTSignals();
    },
    harness:function(){var lb=document.getElementById("resLabs");if(resArmed()&&lb&&lb.style.display!=='none')loadHarness();}, // R77 item 4: Labs harness repaints on snapshot completion
    paper:loadPaper,live:loadLive,ml:loadML}; // ml: instant repaint when the sidecar finishes a cycle
  function onPanel(p){
    if(document.hidden)return; // R77 audit: skip push-driven repaints while hidden (the SSE socket itself stays open); visibilitychange refires the loaders once on return
    var fn=panelFns[p];
    if(fn){try{fn();}catch(e){}}
  }
  if(bc){bc.onmessage=function(ev){if(ev&&ev.data)onPanel(ev.data);};}
  function startStream(){
    try{
      var es=new EventSource("/api/events");
      es.onmessage=function(ev){
        if(!ev.data||ev.data==="connected")return;
        onPanel(ev.data);
        if(bc){try{bc.postMessage(ev.data);}catch(e){}}
      };
      es.onerror=function(){es.close();setTimeout(startStream,5000);};
    }catch(e){setTimeout(startStream,10000);}
  }
  if(navigator.locks&&navigator.locks.request){
    // leader election: the lock holder owns the single EventSource; when it closes, the next
    // window acquires the lock and takes over — one socket total across all windows.
    navigator.locks.request("kalshi-suite-sse",function(){startStream();return new Promise(function(){});});
  }else{startStream();}
}());
setInterval(refresh,4000); // 4s BACKSTOP poll (was 700ms) — SSE pushes make panels instant; this just catches missed events
// UIFAST: loadArb is now folded into refresh()'s throttled feed block (one cadence that also
// respects the interaction pause) — it used to run on its own 1500ms interval that fired mid-scroll.
// Resume instantly on focus or any interaction after being idle.
document.addEventListener("visibilitychange",function(){if(!document.hidden){lastActivity=Date.now();refresh();loadArb();loadLive();loadCombos();loadPipeStrip();}}); // R77 audit: the visibility-gated loops refire IMMEDIATELY once on return-to-visible
['mousemove','mousedown','keydown','wheel','touchstart'].forEach(function(ev){document.addEventListener(ev,function(){var wasIdle=uiIdle();lastActivity=Date.now();if(wasIdle){refresh();loadArb();}},{passive:true});});
// TABCLOSE: when the suite stops answering (you closed it), close this tab. App windows (chrome/edge
// --app, how the suite opens it) can self-close; a normal tab can't, so we fall back to a clear banner.
(function(){var hbFails=0;function hbCheck(){if(hbFails<3)return;try{window.close();}catch(e){}setTimeout(function(){try{document.title="■ Suite stopped";document.body.innerHTML="<div style=\'font:600 18px system-ui,sans-serif;color:#9aa;height:100vh;display:flex;align-items:center;justify-content:center;text-align:center\'>Suite stopped — you can close this tab.</div>";}catch(e){}},400);}
function hbBeat(){fetch("/health",{cache:"no-store"}).then(function(r){if(r&&r.ok){hbFails=0;}else{hbFails++;hbCheck();}}).catch(function(){hbFails++;hbCheck();});}
setInterval(hbBeat,2000);}());
</script>
</body>
</html>`
