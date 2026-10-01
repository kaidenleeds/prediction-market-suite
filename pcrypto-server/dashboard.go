package main

// dashboardHTML is the standalone live dashboard. It polls /api/state and renders the summary, open
// positions (with a color-scaled confidence column), recent settled trades, and the activity log.
// NOTE: this is a Go raw-string literal (backtick-delimited) so it must contain NO backticks — all JS
// strings use single/double quotes and concatenation, never template literals.
const dashboardHTML = `<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>pcrypto-server</title>
<style>
:root{--bg:#0b0e13;--card:#141922;--line:#232a36;--text:#e6edf3;--muted:#8b97a7;--good:#3fb950;--bad:#f85149;--accent:#58a6ff}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);font:14px/1.5 -apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif}
.wrap{max-width:1040px;margin:0 auto;padding:18px}
h1{font-size:18px;margin:0 0 2px}
.sub{color:var(--muted);font-size:12.5px;margin:0 0 14px}
.bar{display:flex;flex-wrap:wrap;gap:10px;margin:0 0 16px}
.kpi{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:10px 14px;min-width:120px}
.kpi .l{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.04em}
.kpi .v{font-size:18px;font-weight:700;margin-top:2px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px 14px;margin:0 0 16px}
.card h2{font-size:13px;color:var(--muted);text-transform:uppercase;letter-spacing:.04em;margin:0 0 8px}
table{width:100%;border-collapse:collapse}
th,td{text-align:left;padding:6px 8px;font-size:13px;border-bottom:1px solid var(--line)}
th{color:var(--muted);font-weight:600;font-size:11.5px}
td.r,th.r{text-align:right}
.pill{display:inline-block;padding:1px 7px;border-radius:999px;font-size:11px;font-weight:700}
.mode-live{color:#000;background:#f0b429}
.mode-paper{color:#000;background:#3fb950}
.log{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:12px;color:var(--muted);max-height:240px;overflow:auto;white-space:pre-wrap}
.muted{color:var(--muted)}
</style></head>
<body><div class="wrap">
<h1>pcrypto-server <span id="mode" class="pill"></span></h1>
<p class="sub">Standalone Polymarket-crypto edge — Up/Down across durations, Kalshi-15m cross-confirmed, paper-traded with live confidence. Auto-refreshes.</p>
<div class="bar" id="kpis"></div>
<div class="card"><h2>Open positions</h2><div id="open"></div></div>
<div class="card"><h2>Recent settled</h2><div id="hist"></div></div>
<div class="card"><h2>Backtest — Kelly sizing (compounds $1,000 over settled history)</h2>
<div style="margin-bottom:8px"><label class="muted">Custom Kelly &times;<input id="btk" type="number" step="0.05" min="0" placeholder="e.g. 0.75" style="width:80px;margin:0 6px;background:#0b0e13;color:var(--text);border:1px solid var(--line);border-radius:6px;padding:3px 6px"></label><button onclick="loadBT()" style="background:var(--accent);color:#000;border:0;border-radius:6px;padding:4px 12px;font-weight:700;cursor:pointer">Run</button></div>
<div id="bt"></div></div>
<div class="card"><h2>Settings (live — applies without restart)</h2><div id="settings"></div></div>
<div class="card"><h2>Activity</h2><div class="log" id="log"></div></div>
</div>
<script>
function confColor(v){v=Math.max(0,Math.min(1,v||0));var h=Math.max(0,Math.min(1,(v-0.35)/0.30))*120;return 'hsl('+h.toFixed(0)+',72%,55%)';}
function spark(path){
  if(!path||path.length<2)return '<span class="muted">—</span>';
  var w=72,h=18,n=path.length,mn=Math.min.apply(null,path),mx=Math.max.apply(null,path),rng=(mx-mn)||1;
  var pts=path.map(function(v,i){return ((i/(n-1))*w).toFixed(1)+','+(h-((v-mn)/rng)*h).toFixed(1);}).join(' ');
  var up=path[path.length-1]>=path[0];
  return '<svg width="'+w+'" height="'+h+'" style="vertical-align:middle"><polyline fill="none" stroke="'+(up?'var(--good)':'var(--bad)')+'" stroke-width="1.5" points="'+pts+'"/></svg>';
}
function money(v){v=v||0;return (v<0?'-$':'$')+Math.abs(v).toFixed(2);}
function esc(s){return String(s==null?'':s).replace(/[&<>"]/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c];});}
function kpi(l,v,color){return '<div class="kpi"><div class="l">'+l+'</div><div class="v"'+(color?(' style="color:'+color+'"'):'')+'>'+v+'</div></div>';}
function col(v){return (v||0)>=0?'var(--good)':'var(--bad)';}
function load(){
  fetch('/api/state').then(function(r){return r.json();}).then(function(d){
    var m=document.getElementById('mode');
    m.textContent=d.mode; m.className='pill '+((d.mode||'').indexOf('LIVE')>=0?'mode-live':'mode-paper');
    var k='';
    k+=kpi('Equity','$'+Math.round((d.equity!=null?d.equity:d.bankroll)||0));
    k+=kpi('Net (after fees)',money(d.net),col(d.net));
    k+=kpi('Unrealized',money(d.unrealized),col(d.unrealized));
    k+=kpi('Realized',money(d.realized),col(d.realized));
    k+=kpi('Fees',money(-(d.fees||0)),'var(--bad)');
    // WR KPI removed (operator directive + audit Q1#13: this panel LED with win rate while the book
    // lost money after fees). Net/bet is the profiting metric; WR stays tracked server-side.
    k+=kpi('Net/bet (EV)',(d.settled?money((d.net||0)/d.settled):'—')+' ('+(d.settled||0)+')',col(d.net));
    k+=kpi('Open',((d.open||[]).length)+' · $'+Math.round(d.open_cost||0));
    k+=kpi('Bankroll','$'+Math.round(d.bankroll||0));
    document.getElementById('kpis').innerHTML=k;
    var op=d.open||[];
    if(!op.length){document.getElementById('open').innerHTML='<span class="muted">No open positions yet.</span>';}
    else{
      op.sort(function(a,b){return (b.confidence||0)-(a.confidence||0);});
      var h='<table><thead><tr><th>Coin</th><th>Dur</th><th>Side</th><th class="r">Entry&rarr;Cur</th><th class="r">Size</th><th class="r">Value</th><th class="r">P&amp;L</th><th class="r">Conf</th><th>Confirm</th><th>Path</th></tr></thead><tbody>';
      op.forEach(function(p){
        var up=p.last_up||0;var cs=(p.side==='Down')?(1-up):up;var marked=up>0;
        var val=marked?(p.contracts||0)*cs:0;var pnl=marked?(p.contracts||0)*(cs-(p.entry||0)):0;
        var mu=p.slug?('https://polymarket.com/event/'+encodeURIComponent(p.slug)):'';
        var coinCell=mu?('<a href="'+mu+'" target="_blank" rel="noopener" style="color:var(--accent)">'+esc((p.coin||'').toUpperCase())+'</a>'):esc((p.coin||'').toUpperCase());
        h+='<tr><td>'+coinCell+'</td><td>'+esc(p.dur)+'</td><td>'+esc(p.side)+'</td>'+
          '<td class="r">'+Math.round((p.entry||0)*100)+'¢'+(marked?('&rarr;'+Math.round(cs*100)+'¢'):'')+'</td>'+
          '<td class="r">'+Math.round(p.contracts||0)+'</td>'+
          '<td class="r">'+(marked?money(val):'<span class="muted">—</span>')+'</td>'+
          '<td class="r" style="color:'+col(pnl)+'">'+(marked?money(pnl):'<span class="muted">—</span>')+'</td>'+
          '<td class="r" style="font-weight:700;color:'+confColor(p.confidence)+'">'+(p.confidence||0).toFixed(2)+'</td>'+
          '<td>'+(p.confirmed?'<span style="color:var(--accent)">Kalshi 2x</span>':'<span class="muted">solo</span>')+'</td>'+
          '<td>'+spark(p.path)+'</td></tr>';
      });
      document.getElementById('open').innerHTML=h+'</tbody></table>';
    }
    var hs=d.history||[];
    if(!hs.length){document.getElementById('hist').innerHTML='<span class="muted">No settled trades yet.</span>';}
    else{
      var hh='<table><thead><tr><th>Coin</th><th>Dur</th><th>Side</th><th class="r">Entry</th><th class="r">Conf</th><th>Result</th><th class="r">P&amp;L</th></tr></thead><tbody>';
      hs.slice(0,40).forEach(function(c){
        hh+='<tr><td>'+esc((c.coin||'').toUpperCase())+'</td><td>'+esc(c.dur)+'</td><td>'+esc(c.side)+'</td>'+
          '<td class="r">'+Math.round((c.entry||0)*100)+'¢</td>'+
          '<td class="r" style="color:'+confColor(c.confidence)+'">'+(c.confidence||0).toFixed(2)+'</td>'+
          '<td style="color:'+(c.won?'var(--good)':'var(--bad)')+'">'+(c.won?'WON':'LOST')+'</td>'+
          '<td class="r" style="color:'+col(c.pnl)+'">'+money(c.pnl)+'</td></tr>';
      });
      document.getElementById('hist').innerHTML=hh+'</tbody></table>';
    }
    document.getElementById('log').textContent=(d.log||[]).join('\n');
  }).catch(function(){});
}
function loadBT(){
  var k=parseFloat(document.getElementById('btk').value)||0;
  var u='/api/backtest'+(k>0?('?kelly='+k):'');
  document.getElementById('bt').innerHTML='<span class="muted">Running…</span>';
  fetch(u).then(function(r){return r.json();}).then(function(d){
    var s=d.sweep||[];
    if(!s.length){document.getElementById('bt').innerHTML='<span class="muted">No settled trades yet to backtest.</span>';return;}
    var h='<table><thead><tr><th>Strategy</th><th class="r">Final</th><th class="r">Growth</th><th class="r">Max DD</th><th class="r">Bets</th></tr></thead><tbody>';
    s.forEach(function(x){
      h+='<tr><td>'+esc(x.label)+'</td>'+
        '<td class="r" style="color:'+col((x.final||0)-(d.start||1000))+'">$'+Math.round(x.final||0)+'</td>'+
        '<td class="r" style="font-weight:700;color:'+col((x.growth||0)-1)+'">'+(x.growth||0).toFixed(2)+'x</td>'+
        '<td class="r" style="color:var(--bad)">'+Math.round((x.max_dd||0)*100)+'%</td>'+
        '<td class="r">'+(x.bets||0)+'</td></tr>';
    });
    h+='</tbody></table><div class="muted" style="margin-top:6px;font-size:11.5px">'+esc(d.note||'')+' (n='+(d.settled||0)+')</div>';
    document.getElementById('bt').innerHTML=h;
  }).catch(function(){document.getElementById('bt').innerHTML='<span class="muted">backtest failed</span>';});
}
function num(id){var v=parseFloat(document.getElementById(id).value);return isNaN(v)?null:v;}
function loadSettings(){
  fetch('/api/settings').then(function(r){return r.json();}).then(function(d){
    function inp(id,v,step){return '<input id="'+id+'" type="number" step="'+step+'" value="'+v+'" style="width:84px;background:#0b0e13;color:var(--text);border:1px solid var(--line);border-radius:6px;padding:3px 6px">';}
    var h='<table><tbody>';
    h+='<tr><td>Use Kelly sizing</td><td><input id="s_kelly" type="checkbox"'+(d.kelly?' checked':'')+'></td></tr>';
    h+='<tr><td>Bet floor on no-edge bands (vs skip)</td><td><input id="s_nef" type="checkbox"'+(d.no_edge_floor?' checked':'')+'></td></tr>';
    h+='<tr><td>Kelly multiplier</td><td>'+inp('s_km',d.kelly_mult,'0.05')+'</td></tr>';
    h+='<tr><td>Kelly cap (frac / bet)</td><td>'+inp('s_kc',d.kelly_cap,'0.01')+'</td></tr>';
    h+='<tr><td>Max exposure (frac of bankroll)</td><td>'+inp('s_me',d.max_exposure,'0.05')+'</td></tr>';
    h+='<tr><td>Flat stake (cold-start)</td><td>'+inp('s_sp',d.stake_pct,'0.01')+'</td></tr>';
    h+='<tr><td>Min confidence (0=off)</td><td>'+inp('s_mc',d.min_conf,'0.05')+'</td></tr>';
    h+='<tr><td>Min entry price</td><td>'+inp('s_min',d.min_entry,'0.01')+'</td></tr>';
    h+='<tr><td>Max entry price</td><td>'+inp('s_max',d.max_entry,'0.01')+'</td></tr>';
    h+='<tr><td>Solo min entry (unconfirmed bar · 0=off)</td><td>'+inp('s_solo',d.solo_min_entry,'0.01')+'</td></tr>';
    h+='<tr><td>Cross-confirm size ×</td><td>'+inp('s_cx',d.cross_confirm_x,'0.5')+'</td></tr>';
    h+='<tr><td>Bankroll $</td><td>'+inp('s_bank',d.bankroll,'50')+'</td></tr>';
    h+='</tbody></table><button onclick="saveSettings()" style="margin-top:8px;background:var(--good);color:#000;border:0;border-radius:6px;padding:5px 14px;font-weight:700;cursor:pointer">Save</button> <span id="s_msg" class="muted"></span>';
    document.getElementById('settings').innerHTML=h;
  }).catch(function(){});
}
function saveSettings(){
  var body={kelly:document.getElementById('s_kelly').checked,no_edge_floor:document.getElementById('s_nef').checked,kelly_mult:num('s_km'),kelly_cap:num('s_kc'),max_exposure:num('s_me'),stake_pct:num('s_sp'),min_conf:num('s_mc'),min_entry:num('s_min'),max_entry:num('s_max'),solo_min_entry:num('s_solo'),cross_confirm_x:num('s_cx'),bankroll:num('s_bank')};
  fetch('/api/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}).then(function(r){return r.json();}).then(function(){var m=document.getElementById('s_msg');if(m){m.textContent='saved ✓';setTimeout(function(){if(m)m.textContent='';},2000);}}).catch(function(){var m=document.getElementById('s_msg');if(m)m.textContent='save failed';});
}
load(); loadBT(); loadSettings(); setInterval(load,2000); // MINIFAST: poll cur price + path every 2s
// TABCLOSE: when the mini-server stops answering (suite closed), close this app-window tab (or banner).
(function(){var hf=0;function chk(){if(hf<3)return;try{window.close();}catch(e){}setTimeout(function(){try{document.title='■ mini stopped';document.body.innerHTML='<div style="font:600 18px system-ui,sans-serif;color:#9aa;height:100vh;display:flex;align-items:center;justify-content:center;text-align:center">pcrypto mini stopped — you can close this tab.</div>';}catch(e){}},400);}
function beat(){fetch('/api/state',{cache:'no-store'}).then(function(r){if(r&&r.ok){hf=0;}else{hf++;chk();}}).catch(function(){hf++;chk();});}
setInterval(beat,2000);}());
</script>
</body></html>`
