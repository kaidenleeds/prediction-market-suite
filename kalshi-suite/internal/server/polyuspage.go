package server

import "net/http"

// handlePolyUSPage serves the Poly US own-data view as a standalone browser window: a FLOW panel
// (game moneylines sorted by recent YES-price move) and a LIVE MARKETS section (all current Poly US
// game moneylines with live score/period + bid/ask). Data: /api/polyus/flow + /api/polyus/markets.
// Self-contained (own raw string) so it never touches the main dashboard's minified body.
func (s *Server) handlePolyUSPage(w http.ResponseWriter, _ *http.Request) {
	s.servePage(w, polyUSPageHTML)
}

const polyUSPageHTML = `<!doctype html>
<html><head><meta charset="utf-8"><title>Poly US - Kalshi Suite</title>
<style>
:root{--bg:#0b0e14;--line:#222a38;--text:#e6eaf2;--muted:#8a93a6;--good:#4ade80;--bad:#f87171;--live:#f59e0b;}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);font:14px/1.5 system-ui,Segoe UI,Roboto,sans-serif;padding:16px}
h1{font-size:16px;margin:0 0 4px}
.sub{color:var(--muted);font-size:12px;margin:0 0 14px}
.sec{margin-top:18px;font-weight:700}
table{width:100%;border-collapse:collapse;margin-top:6px}
th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--line);font-size:13px;white-space:nowrap}
th.r,td.r{text-align:right}
td.m{color:var(--muted)}
.muted{color:var(--muted)}
.live{color:var(--live);font-weight:700}
.tag{font-size:11px;color:var(--muted);text-transform:uppercase}
button{background:#1b2230;color:var(--text);border:1px solid var(--line);border-radius:7px;padding:4px 10px;cursor:pointer}
</style></head>
<body>
<h1>Poly US <span class="tag">own-data feed</span> <button onclick="load()" style="margin-left:8px">&#8635; Refresh</button></h1>
<div class="sub">Polymarket US game moneylines from gateway.polymarket.us. Price = live book mid (bid/ask shown). Server polls ~45s.</div>

<div class="sec">Flow &mdash; biggest recent moves</div>
<div id="flow"></div>

<div class="sec">Live markets</div>
<div id="markets"></div>

<script>
function esc(s){return (s==null?"":String(s)).replace(/[&<>"]/g,function(c){return {"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;"}[c];});}
function px(p){return ((p||0)*100).toFixed(1)+"&cent;";}
function moveCell(mv){mv=mv||0;if(Math.abs(mv)<0.0005)return '<span class="muted">&mdash;</span>';var c=mv>0?"var(--good)":"var(--bad)",a=mv>0?"&#9650;":"&#9660;";return '<span style="color:'+c+'">'+a+' '+(Math.abs(mv)*100).toFixed(1)+'&cent;</span>';}
function when(ts){if(!ts)return "-";var d=new Date(ts);if(isNaN(d.getTime()))return "-";return (d.getMonth()+1)+"/"+d.getDate()+" "+d.getHours()+":"+("0"+d.getMinutes()).slice(-2);}
function state(m){if(m.live)return '<span class="live">LIVE '+esc(m.score)+' '+esc(m.period)+'</span>';if((m.period||"")==="NS"||!m.score)return '<span class="muted">'+when(m.start)+'</span>';return '<span class="muted">'+esc(m.score||"")+'</span>';}
function spread(m){return (((m.ask||0)-(m.bid||0))*100).toFixed(1)+"&cent;";}
// R70-B #1: resting size at the touch (live WS book levels) — the maker fill-odds column.
function tobSz(m){if(!(m.bid_sz>0||m.ask_sz>0))return '<span class="muted">&mdash;</span>';return Math.round(m.bid_sz||0)+'/'+Math.round(m.ask_sz||0);}
function rows(list){
  var h='<table><thead><tr><th>League</th><th>Game</th><th>Side</th><th class="r">Yes</th><th class="r">Bid/Ask</th><th class="r" title="resting contracts at the best bid/ask (live WS book) - the queue a maker order joins">Size b/a</th><th class="r">Spread</th><th>State</th></tr></thead><tbody>';
  list.forEach(function(m){
    h+='<tr><td class="tag">'+esc(m.league)+'</td><td>'+esc(m.game)+'</td><td>'+esc((m.team||"").toUpperCase())+'</td>'
     +'<td class="r" style="font-weight:700">'+px(m.yes)+'</td>'
     +'<td class="r m">'+px(m.bid)+'/'+px(m.ask)+'</td>'
     +'<td class="r m">'+tobSz(m)+'</td>'
     +'<td class="r m">'+spread(m)+'</td>'
     +'<td>'+state(m)+'</td></tr>';
  });
  return h+'</tbody></table>';
}
function flowRows(list){
  var h='<table><thead><tr><th>Game</th><th>Side</th><th class="r">Yes</th><th class="r">Move</th><th>State</th></tr></thead><tbody>';
  list.forEach(function(m){
    h+='<tr><td>'+esc(m.game)+' <span class="tag">'+esc(m.league)+'</span></td><td>'+esc((m.team||"").toUpperCase())+'</td>'
     +'<td class="r" style="font-weight:700">'+px(m.yes)+'</td>'
     +'<td class="r">'+moveCell(m.move)+'</td>'
     +'<td>'+state(m)+'</td></tr>';
  });
  return h+'</tbody></table>';
}
function load(){
  fetch("/api/polyus/flow").then(function(r){return r.json();}).then(function(d){
    var f=(d&&d.flow)||[],el=document.getElementById("flow");
    el.innerHTML=f.length?flowRows(f):'<div class="muted" style="padding:6px 0">No Poly US markets yet (feed warms up ~45s after start).</div>';
  }).catch(function(){document.getElementById("flow").textContent="Could not load flow.";});
  fetch("/api/polyus/markets").then(function(r){return r.json();}).then(function(d){
    var m=(d&&d.markets)||[],el=document.getElementById("markets");
    el.innerHTML=m.length?rows(m):'<div class="muted" style="padding:6px 0">No markets.</div>';
  }).catch(function(){document.getElementById("markets").textContent="Could not load markets.";});
}
load();setInterval(function(){if(document.hidden)return;load();},5000); // hidden-pause (audit §4)
</script>
</body></html>`
