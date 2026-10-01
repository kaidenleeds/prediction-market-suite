package server

import "net/http"

// handleHistoryPage serves History as a standalone browser WINDOW (popup) so it can
// sit alongside the dashboard instead of overlaying it. Same data as /api/history +
// /api/pnl-series: P&L-over-time, by-signal performance, and every closed bet —
// including the dollars put in (contracts x entry), not just the P&L.
func (s *Server) handleHistoryPage(w http.ResponseWriter, _ *http.Request) {
	s.servePage(w, historyPageHTML)
}

const historyPageHTML = `<!doctype html>
<html><head><meta charset="utf-8"><title>History - Kalshi Suite</title>
<style>
:root{--bg:#0b0e14;--line:#222a38;--text:#e6eaf2;--muted:#8a93a6;--good:#4ade80;--bad:#f87171;}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);font:14px/1.5 system-ui,Segoe UI,Roboto,sans-serif;padding:16px}
h1{font-size:16px;margin:0 0 8px}
.sum{margin:6px 0 14px;color:var(--muted)}
.sum b{color:var(--text)}
table{width:100%;border-collapse:collapse;margin-top:6px}
th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--line);font-size:13px;white-space:nowrap}
th.r,td.r{text-align:right}
td.m{color:var(--muted)}
.sec{margin-top:18px;font-weight:700}
.muted{color:var(--muted)}
button{background:#1b2230;color:var(--text);border:1px solid var(--line);border-radius:7px;padding:4px 10px;cursor:pointer}
#chart{margin:6px 0 4px}
</style></head>
<body>
<h1>History <button onclick="load()" style="margin-left:8px">&#8635; Refresh</button> <button onclick="copyHist()" style="margin-left:8px">&#128203; Copy all</button> <button onclick="clr()" style="color:#f87171">Clear history</button></h1>
<div id="sum" class="sum">Loading...</div>
<div class="muted" style="font-size:12px">Net P&amp;L over time (after fees)</div>
<div id="chart"></div>
<div class="sec">By signal</div>
<div id="bysrc"></div>
<div class="sec">Closed bets - newest first</div>
<div id="trades"></div>
<script>
function esc(s){return (s==null?"":String(s)).replace(/[&<>"]/g,function(c){return {"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;"}[c];});}
function col(v){return (v||0)>=0?"var(--good)":"var(--bad)";}
function money(v){v=v||0;return (v<0?"-":"")+Math.abs(v).toFixed(2);}
function px(p){return Math.round((p||0)*100)+"¢";}
function when(ts){if(!ts)return "-";var d=new Date(ts);if(isNaN(d.getTime()))return "-";return (d.getMonth()+1)+"/"+d.getDate()+" "+d.getHours()+":"+("0"+d.getMinutes()).slice(-2);}
function chart(series){
  var el=document.getElementById("chart");
  if(!series||series.length<2){el.innerHTML='<div class="muted" style="padding:6px 0">No P&amp;L history yet.</div>';return;}
  if(series.length>500){var stp=Math.ceil(series.length/500),ds=[];for(var i=0;i<series.length;i+=stp)ds.push(series[i]);ds.push(series[series.length-1]);series=ds;}
  var W=980,H=200,pl=54,pr=14,pt=24,pb=22;
  var xs=series.map(function(p){return Date.parse(p.ts);}),ys=series.map(function(p){return p.pnl;});
  var x0=Math.min.apply(null,xs),x1=Math.max.apply(null,xs),y0=Math.min.apply(null,ys),y1=Math.max.apply(null,ys);
  if(y0>0)y0=0;if(y1<0)y1=0;if(y1===y0)y1=y0+1;if(x1===x0)x1=x0+1;var pd=(y1-y0)*0.08;y0-=pd;y1+=pd;
  function X(t){return pl+(t-x0)/(x1-x0)*(W-pl-pr);}function Y(v){return H-pb-(v-y0)/(y1-y0)*(H-pt-pb);}
  var d="";series.forEach(function(p,i){d+=(i?"L":"M")+X(xs[i]).toFixed(1)+" "+Y(ys[i]).toFixed(1)+" ";});
  var last=ys[ys.length-1],lc=last>=0?"#4ade80":"#f87171",zy=Y(0).toFixed(1);
  var s='<svg viewBox="0 0 '+W+' '+H+'" width="100%" style="background:rgba(255,255,255,.025);border-radius:8px">';
  s+='<line x1="'+pl+'" y1="'+zy+'" x2="'+(W-pr)+'" y2="'+zy+'" stroke="rgba(255,255,255,.18)" stroke-dasharray="4 4"/>';
  s+='<path d="'+d+'" fill="none" stroke="'+lc+'" stroke-width="2"/>';
  s+='<text x="'+(W-pr)+'" y="16" fill="'+lc+'" font-size="14" font-weight="700" text-anchor="end">net $'+last.toFixed(2)+'</text>';
  s+='</svg>';el.innerHTML=s;
}
function srcLbl(s){s=String(s==null?"-":s);var M={'auto-cons-kcrypto':'Kalshi crypto','auto-cons-xmatch':'Crypto cross-match','auto-cons-kthresh':'Kalshi threshold','auto-cons-xcrypto':'Kalshi crypto (old)','auto-cons-pcrypto':'Poly crypto','auto-cons-kalshi':'Kalshi whale flow','auto-cons-poly':'Poly-int leaderboard','auto-cons-pflow':'Poly-int whale flow','auto-cons-pusflow':'Poly US flow','auto-cons-x':'Cross-platform','auto-arb':'Two-leg arb','gate':'AI','manual':'Manual','auto-tp':'take-profit','auto-sl':'stop-loss','settled':'settled','gate-close':'AI close','auto-arb-rollback':'arb rollback'};return M[s]||s.replace(/gate/gi,"AI");}
function platLabel(p){return {kalshi:"Kalshi",polyus:"Poly US",polymarket:"Poly-int"}[p]||(p||"other");}
function clr(){
  // CONFIRM (audit §4: destructive one-click clears with no confirm = accidental data loss)
  if(!confirm("Clear ALL closed-bet history? This permanently deletes the History records."))return;
  fetch("/api/history/clear",{method:"POST"}).then(function(r){return r.json();}).then(function(){load();}).catch(function(){});
}
// Copy the full export (all history + per-platform/per-source stats + net-P&L curve) to the clipboard
// as JSON so it can be pasted into an analysis tool.
function copyHist(){
  var s=document.getElementById("sum");var prev=s?s.innerHTML:"";if(s)s.innerHTML="Copying...";
  fetch("/api/export").then(function(r){return r.text();}).then(function(t){
    var done=function(){if(s)s.innerHTML='<b style="color:var(--good)">Copied '+t.length+' chars to clipboard — ready for analysis.</b>';};
    if(navigator.clipboard&&navigator.clipboard.writeText){navigator.clipboard.writeText(t).then(done,function(){hcFallback(t,done);});}else{hcFallback(t,done);}
  }).catch(function(){if(s)s.innerHTML=prev;});
}
function hcFallback(t,done){var ta=document.createElement("textarea");ta.value=t;ta.style.position="fixed";ta.style.left="-9999px";document.body.appendChild(ta);ta.focus();ta.select();try{document.execCommand("copy");done();}catch(e){}ta.remove();}
function load(){
  fetch("/api/pnl-series").then(function(r){return r.json();}).then(function(d){chart((d&&d.series)||[]);}).catch(function(){});
  fetch("/api/history").then(function(r){return r.json();}).then(function(d){
    // WR REMOVED from the headline (operator directive): profiting = NET EV, not win rate — an
    // 88%-WR source here lost $1.58/bet. Net per bet is the number that decides anything.
    var net=(d.realized||0)-(d.fees||0),perBet=(d.closed||0)>0?net/d.closed:0;
    document.getElementById("sum").innerHTML='<b>'+(d.closed||0)+'</b> closed bets &middot; net <b style="color:'+col(net)+'">$'+money(net)+'</b> &middot; net/bet <b style="color:'+col(perBet)+'">$'+money(perBet)+'</b> &middot; realized <b style="color:'+col(d.realized)+'">$'+money(d.realized)+'</b> &middot; fees <b style="color:var(--bad)">-$'+(d.fees||0).toFixed(2)+'</b>';
    var bs=(d.by_source||[]),es=document.getElementById("bysrc");
    if(!bs.length){es.innerHTML='<span class="muted">No closed trades yet.</span>';}
    else{var by={};bs.forEach(function(x){(by[x.platform]||(by[x.platform]=[])).push(x);});
      var order=['kalshi','polyus','polymarket'];Object.keys(by).forEach(function(p){if(order.indexOf(p)<0)order.push(p);});
      var hb='';
      order.forEach(function(pl){var rows=by[pl];if(!rows||!rows.length)return;var tot=0,totf=0;rows.forEach(function(s){tot+=(s.realized||0);totf+=(s.fees||0);});var pnet=tot-totf;
        hb+='<div class="sec" style="margin-top:10px">'+platLabel(pl)+' <span style="color:'+col(pnet)+'">net $'+money(pnet)+'</span> <span class="muted" style="font-size:11px">(realized $'+money(tot)+' − fees $'+money(totf)+')</span></div>'+
          '<table><thead><tr><th>Signal that opened it</th><th class="r">Closed</th><th class="r" title="net (realized − fees) per closed bet — the profiting metric (WR hidden: high win% can still lose money)">Net/bet</th><th class="r">Realized</th><th class="r">Fees</th><th class="r">Net</th></tr></thead><tbody>';
        rows.forEach(function(x){var n=(x.realized||0)-(x.fees||0);var nb=(x.closed||0)>0?n/x.closed:0;hb+='<tr><td>'+esc(srcLbl(x.source))+'</td><td class="r">'+x.closed+'</td><td class="r" style="color:'+col(nb)+'">$'+money(nb)+'</td><td class="r" style="color:'+col(x.realized)+'">$'+money(x.realized)+'</td><td class="r" style="color:var(--bad)">-$'+money(x.fees||0)+'</td><td class="r" style="color:'+col(n)+'">$'+money(n)+'</td></tr>';});
        hb+='</tbody></table>';});
      es.innerHTML=hb;}
    var tr=(d.trades||[]),et=document.getElementById("trades");
    if(!tr.length){et.innerHTML='<div class="muted" style="padding:6px 0">No closed bets yet.</div>';return;}
    var h='<table><thead><tr><th>Closed</th><th>Venue</th><th>Market</th><th>Signal in</th><th>Exit</th><th class="r">Contracts</th><th class="r">In $</th><th class="r">Entry&rarr;Exit</th><th class="r">P&amp;L</th></tr></thead><tbody>';
    tr.forEach(function(t){var inUSD=(t.contracts||0)*(t.entry_price||0);
      h+='<tr><td class="m">'+when(t.closed_at)+'</td><td class="m">'+platLabel(t.platform)+'</td><td>'+esc(t.title||t.ticker||"")+' <span class="muted">'+esc(t.side||"")+'</span></td><td>'+esc(srcLbl(t.source||"-"))+'</td><td>'+esc(srcLbl(t.exit_source||"-"))+'</td><td class="r">'+(t.contracts||0).toFixed(0)+'</td><td class="r">$'+inUSD.toFixed(2)+'</td><td class="r">'+px(t.entry_price)+'&rarr;'+px(t.exit_price)+'</td><td class="r" style="color:'+col(t.realized)+';font-weight:700">$'+money(t.realized)+'</td></tr>';});
    et.innerHTML=h+'</tbody></table>';
  }).catch(function(){document.getElementById("sum").textContent="Could not load history.";});
}
load();setInterval(function(){if(document.hidden)return;load();},4000); // hidden-pause (audit §4: background popups kept triggering full-table scans)
</script>
</body></html>`
