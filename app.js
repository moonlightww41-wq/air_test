/* Transport Fare Calculator
 * v11: Reliability-first rebuild (data load, normalization, dropdown population, fare search)
 * - Auto loads data/transport.csv (preferred) or data/fare_source.tsv (fallback)
 * - Supports manual local file import (CSV/TSV) to bypass hosting/path issues
 * - Normalizes headers & place names, builds route index, and searches by date
 */
const APP_VERSION = "v11.2-2026-02-12";

const DB = {
  meta: {
    source: "",
    sourceUrl: "",
    fares: 0,
    routes: 0,
    places: 0,
    updatedAt: "",
    rawRows: 0,
  },
  faresRows: [],        // normalized fare rows
  routeMap: new Map(),  // key: from||to (normalized) -> rows[]
  places: [],           // unique places (canonical display strings)
  aliasToCanon: new Map(), // key: normalized alias -> canonical display string
  loadLog: [],
};

const $ = (sel) => document.querySelector(sel);

function safeText(idOrEl, text){
  const el = typeof idOrEl === "string" ? $(idOrEl) : idOrEl;
  if (el) el.textContent = text;
}

function nowStamp(){
  const d = new Date();
  const s = d.toISOString().slice(0,19).replace("T"," ");
  return s;
}

function stripBOM(s){
  return (s ?? "").toString().replace(/^\uFEFF/, "");
}

function norm(s){
  return (s ?? "").toString().trim()
    .replace(/[\s\u3000]+/g,"")
    .replace(/[‐‑–—−]/g,"-")
    .toLowerCase();
}

// Place key: stronger than norm() (drops dots/brackets too)
function normKey(s){
  return norm(s)
    .replace(/[・･]/g, "")
    .replace(/[()（）\[\]【】]/g, "");
}

function normHeaderKey(k){
  return stripBOM(k)
    .replace(/[\u3000\s]+/g,"")
    .replace(/[()（）\[\]【】]/g,"")
    .replace(/[‐‑–—―ー]/g,"-")
    .toLowerCase();
}

function canonicalKey(k){
  const nk = normHeaderKey(k);

  const has = (arr) => arr.some(x => nk.includes(normHeaderKey(x)));

  if (has(["出発地","発地","出発","from","origin","出発場所","発駅","乗車地"])) return "from";
  if (has(["到着地","着地","到着","to","destination","到着場所","着駅","降車地"])) return "to";
  if (has(["運賃","金額","料金","fare","price","運賃額"])) return "fare";
  if (has(["価格タイプ","価格ﾀｲﾌﾟ","シーズン","season","type","区分"])) return "priceType";

  if (has(["搭乗期間開始","搭乗開始","boardfrom","validfrom","搭乗期間from"])) return "wholeFrom";
  if (has(["搭乗期間終了","搭乗終了","boardto","validto","搭乗期間to"])) return "wholeTo";

  // If data provides explicit valid-from/valid-to columns
  if (has(["価格適用期間開始","適用開始","periodfrom","farefrom","価格適用開始"])) return "validFrom";
  if (has(["価格適用期間終了","適用終了","periodto","fareto","価格適用終了"])) return "validTo";

  // String range (e.g., "2025-06-01〜2025-06-30 / ...")
  if (has(["価格適用期間","適用期間","validrange","period","range"])) return "validRange";

  // Alias mapping sheet
  if (has(["alias","別名","入力","候補","表記ゆれ","表記揺れ","synonym"])) return "alias";
  if (has(["canonical","正規","正規名","統一名","正式名称"])) return "canonical";

  // Optional
  if (has(["根拠","備考","rule","注記","参照"])) return "rule";

  return nk; // fallback
}

function normalizeRowKeys(row){
  const out = {};
  for (const [k,v] of Object.entries(row || {})){
    const ck = canonicalKey(k);
    const val = (v ?? "").toString().trim();
    if (!(ck in out) || (out[ck] === "" && val !== "")) out[ck] = val;
  }
  return out;
}

// -----------------------------
// Parsing helpers
// -----------------------------
function parseDateLoose(s){
  const t = (s ?? "").toString().trim();
  if (!t) return null;

  // YYYY-MM-DD
  let m = t.match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/);
  if (m) return new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));

  // YYYY/MM/DD
  m = t.match(/^(\d{4})\/(\d{1,2})\/(\d{1,2})$/);
  if (m) return new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));

  // M/D or M-D (assume current year)
  m = t.match(/^(\d{1,2})[\/\-](\d{1,2})$/);
  if (m) {
    const y = new Date().getFullYear();
    return new Date(y, Number(m[1]) - 1, Number(m[2]));
  }

  return null;
}

function ymd(d){
  if (!d) return "";
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2,"0");
  const day = String(d.getDate()).padStart(2,"0");
  return `${y}-${m}-${day}`;
}

function money(n){
  if (n == null || !Number.isFinite(n)) return "-";
  return Math.trunc(n).toLocaleString("ja-JP");
}

function parseTSV(text){
  const lines = (text ?? "").replace(/\r/g,"").split("\n").filter(l => l.trim() !== "");
  if (!lines.length) return [];
  const header = lines[0].split("\t").map(h => h.trim());
  const rows = [];
  for (let i=1; i<lines.length; i++){
    const cols = lines[i].split("\t");
    const r = {};
    for (let j=0; j<header.length; j++){
      r[header[j]] = (cols[j] ?? "").trim();
    }
    rows.push(r);
  }
  return rows;
}

function parseCSV(text){
  // Minimal CSV parser (handles quotes + commas). Enough for this dataset.
  const rows = [];
  let cur = [];
  let cell = "";
  let inQ = false;

  const pushCell = () => { cur.push(cell); cell = ""; };
  const pushRow  = () => { rows.push(cur); cur = []; };

  for (let i=0; i<text.length; i++){
    const ch = text[i];
    const next = text[i+1];

    if (inQ){
      if (ch === '"' && next === '"'){ cell += '"'; i++; }
      else if (ch === '"'){ inQ = false; }
      else { cell += ch; }
      continue;
    }

    if (ch === '"'){ inQ = true; continue; }
    if (ch === ','){ pushCell(); continue; }
    if (ch === '\n'){ pushCell(); pushRow(); continue; }
    if (ch === '\r'){ continue; }

    cell += ch;
  }
  if (cell.length || cur.length) { pushCell(); pushRow(); }

  if (!rows.length) return [];
  const header = rows[0].map(h => stripBOM((h ?? "").toString()).trim());
  const out = [];

  for (let i=1; i<rows.length; i++){
    if (rows[i].every(x => (x ?? "").toString().trim() === "")) continue;
    const obj = {};
    for (let j=0; j<header.length; j++){
      obj[header[j]] = (rows[i][j] ?? "").toString().trim();
    }
    out.push(obj);
  }
  return out;
}

function looksLikeHTML(text){
  const t = (text ?? "").toString().trim().slice(0, 300).toLowerCase();
  return t.startsWith("<!doctype") || t.startsWith("<html") || t.includes("<head") || t.includes("<body");
}

// "2025-06-01〜2025-06-30 / 2025-09-01〜2025-10-25"
function parsePeriodRanges(periodStr){
  const t = (periodStr ?? "").toString().trim();
  if (!t) return [];

  const parts = t.split("/").map(p => p.trim()).filter(Boolean);
  const ranges = [];

  for (const p of parts){
    // allow YYYY-MM-DD or YYYY/MM/DD
    const m = p.match(/(\d{4}[\/-]\d{1,2}[\/-]\d{1,2})\s*[〜～~\-–—―]\s*(\d{4}[\/-]\d{1,2}[\/-]\d{1,2})/);
    if (!m) continue;
    const a = parseDateLoose(m[1]);
    const b = parseDateLoose(m[2]);
    if (a && b) ranges.push({ from: a, to: b, raw: p });
  }
  return ranges;
}

function hasFareShape(rows){
  if (!Array.isArray(rows) || rows.length === 0) return { ok:false, reason:"no rows" };
  const n = normalizeRowKeys(rows[0]);
  const ok = (n.from && n.to && (n.fare !== undefined));
  return ok ? { ok:true } : { ok:false, reason:`headers=${Object.keys(rows[0] || {}).join(",")}` };
}

// -----------------------------
// Fetch helpers
// -----------------------------
function bustCache(url){
  try{
    const u = new URL(url, location.href);
    // keep stable for same session: still avoid CDN stale
    u.searchParams.set("_ts", String(Date.now()));
    return u.toString();
  } catch {
    // fallback (shouldn't happen)
    const sep = url.includes("?") ? "&" : "?";
    return url + sep + "_ts=" + Date.now();
  }
}

async function fetchTextFirstOk(urls){
  let lastErr = null;
  for (const raw of urls){
    const u = bustCache(raw);
    try{
      const res = await fetch(u, { cache: "no-store" });
      if (!res.ok){
        lastErr = new Error(`${raw} => HTTP ${res.status}`);
        DB.loadLog.push(`NG: ${raw} (HTTP ${res.status})`);
        continue;
      }
      const txt = await res.text();
      DB.loadLog.push(`OK: ${raw} (${txt.length} chars)`);
      return { url: raw, text: txt, status: res.status };
    } catch (e){
      lastErr = e;
      DB.loadLog.push(`NG: ${raw} (${e?.message || e})`);
    }
  }
  throw lastErr || new Error("fetch failed");
}

// -----------------------------
// Alias handling
// -----------------------------
function buildAliasDefaultsFromPlaces(places){
  const m = new Map();
  for (const p of places){
    m.set(normKey(p), p);
  }

  for (const p of places){
    const variants = [
      p.replace(/(都|道|府|県)$/,""),
      p.replace(/空港$/,""),
    ].filter(v => v && v !== p);
    for (const v of variants){
      const k = normKey(v);
      if (!m.has(k)) m.set(k, p);
    }
  }

  const common = [
    ["羽田","東京"],
    ["成田","東京"],
    ["那覇","沖縄"],
  ];
  for (const [alias, canon] of common){
    if (places.includes(canon) && !m.has(normKey(alias))) m.set(normKey(alias), canon);
  }
  return m;
}

function resolvePlace(name){
  const k = normKey(name);
  if (!k) return "";
  return DB.aliasToCanon.get(k) || (name ?? "").toString().trim();
}

// -----------------------------
// Core search
// -----------------------------
function inRange(d, a, b){
  const x = d.getTime();
  return x >= a.getTime() && x <= b.getTime();
}

function pickBest(rows, date){
  const cands = (rows || []).filter(r => r.validFrom && r.validTo && inRange(date, r.validFrom, r.validTo));
  if (!cands.length) return null;

  // prefer narrowest valid window, then "ピーク" (business rule), then lower fare (stable tie-break)
  cands.sort((a,b)=>{
    const lenA = a.validTo.getTime() - a.validFrom.getTime();
    const lenB = b.validTo.getTime() - b.validFrom.getTime();
    if (lenA !== lenB) return lenA - lenB;
    const peakA = (a.priceType === "ピーク") ? 0 : 1;
    const peakB = (b.priceType === "ピーク") ? 0 : 1;
    if (peakA !== peakB) return peakA - peakB;
    return a.fare - b.fare;
  });
  return cands[0];
}

function findFare(date, from, to){
  const f = resolvePlace(from);
  const t = resolvePlace(to);

  const keyFT = normKey(f) + "||" + normKey(t);
  const keyTF = normKey(t) + "||" + normKey(f);

  const listFT = DB.routeMap.get(keyFT) || [];
  const listTF = DB.routeMap.get(keyTF) || [];

  const hasAnyRoute = (listFT.length > 0) || (listTF.length > 0);

  let best = pickBest(listFT, date);
  if (best){
    return { hit:true, row:best, from:f, to:t, tried:[`${f}→${t}`], hasAnyRoute, usedReverse:false };
  }

  best = pickBest(listTF, date);
  if (best){
    // Reverse-direction fallback is allowed, but we do not display any note in the UI.
    return { hit:true, row:best, from:f, to:t, tried:[`${f}→${t}`, `${t}→${f}`], hasAnyRoute, usedReverse:true };
  }

  return { hit:false, row:null, from:f, to:t, tried:[`${f}→${t}`, `${t}→${f}`], hasAnyRoute };
}

// -----------------------------
// Itinerary parsing + UI
// -----------------------------
function parseItineraryLines(text){
  const lines = (text ?? "").toString().split(/\r?\n/).map(l => l.trim()).filter(Boolean);
  const legs = [];
  const errors = [];

  for (const line of lines){
    const m = line.match(/^([0-9]{4}[\/-][0-9]{1,2}[\/-][0-9]{1,2}|[0-9]{1,2}[\/-][0-9]{1,2})\s*(.+)$/);
    if (!m){ errors.push(`日付が読み取れません: ${line}`); continue; }

    const d = parseDateLoose(m[1]);
    if (!d){ errors.push(`日付形式が不正: ${line}`); continue; }

    const rest = m[2].trim();

    // unify arrows
    const s = rest
      .replace(/→/g,"->")
      .replace(/⇒/g,"->")
      .replace(/～/g,"〜");

    const m2 = s.match(/^(.+?)\s*(?:->|〜|~|-)\s*(.+)$/);
    if (!m2){ errors.push(`出発地/到着地が読み取れません: ${line}`); continue; }

    const from = m2[1].trim();
    const to = m2[2].trim();

    if (!from || !to){ errors.push(`出発地/到着地が空です: ${line}`); continue; }

    legs.push({ date: d, from, to, raw: line });
  }

  return { legs, errors };
}

function setSelectLoading(){
  const ids = ["#fromSelect","#toSelect","#via1Select","#via2Select"];
  for (const id of ids){
    const sel = $(id);
    if (!sel) continue;
    sel.innerHTML = `<option value="">（データ読み込み中...）</option>`;
    sel.disabled = true;
  }
}

function renderSelectOptions(){
  const fromSel = $("#fromSelect");
  const toSel = $("#toSelect");
  const via1Sel = $("#via1Select");
  const via2Sel = $("#via2Select");

  const prev = {
    from: fromSel?.value || "",
    to: toSel?.value || "",
    via1: via1Sel?.value || "",
    via2: via2Sel?.value || "",
  };

  const opts = (DB.places || []).slice().sort((a,b)=>a.localeCompare(b,"ja"));

  function fill(sel, includeBlank, blankLabel){
    if (!sel) return;
    sel.innerHTML = "";
    if (includeBlank){
      const ob = document.createElement("option");
      ob.value = "";
      ob.textContent = blankLabel || "（未指定）";
      sel.appendChild(ob);
    }
    for (const p of opts){
      const o = document.createElement("option");
      o.value = p;
      o.textContent = p;
      sel.appendChild(o);
    }
    sel.disabled = false;
  }

  fill(fromSel, true, "（出発地を選択）");
  fill(toSel, true, "（到着地を選択）");
  fill(via1Sel, true, "（未指定）");
  fill(via2Sel, true, "（未指定）");

  if (prev.from && opts.includes(prev.from)) fromSel.value = prev.from;
  if (prev.to && opts.includes(prev.to)) toSel.value = prev.to;
  if (prev.via1 && opts.includes(prev.via1)) via1Sel.value = prev.via1;
  if (prev.via2 && opts.includes(prev.via2)) via2Sel.value = prev.via2;

  // convenience defaults
  if (!fromSel.value && opts.includes("東京")) fromSel.value = "東京";
  if (!toSel.value && opts.includes("沖縄")) toSel.value = "沖縄";
}

function renderLegs(){
  const wrap = $("#legsList");
  if (!wrap) return;

  wrap.innerHTML = "";
  const legs = window.__legs || [];
  if (!legs.length){
    wrap.innerHTML = `<div class="msg">まだ旅程がありません。</div>`;
    return;
  }

  legs.forEach((leg, idx) => {
    const el = document.createElement("div");
    el.className = "legItem";
    el.innerHTML = `
      <div class="meta">
        <div class="m1">${ymd(leg.date)}　${leg.from} → ${leg.to}</div>
        <div class="m2">${leg.raw || ""}</div>
      </div>
      <div class="actions">
        <button class="btn ghost" data-act="up" data-idx="${idx}">↑</button>
        <button class="btn ghost" data-act="down" data-idx="${idx}">↓</button>
        <button class="btn" data-act="del" data-idx="${idx}">削除</button>
      </div>
    `;
    wrap.appendChild(el);
  });

  wrap.querySelectorAll("button").forEach(btn => {
    btn.addEventListener("click", () => {
      const act = btn.getAttribute("data-act");
      const idx = Number(btn.getAttribute("data-idx"));
      if (act === "del") window.__legs.splice(idx, 1);
      if (act === "up" && idx > 0){
        const t = window.__legs[idx-1];
        window.__legs[idx-1] = window.__legs[idx];
        window.__legs[idx] = t;
      }
      if (act === "down" && idx < window.__legs.length - 1){
        const t = window.__legs[idx+1];
        window.__legs[idx+1] = window.__legs[idx];
        window.__legs[idx] = t;
      }
      renderLegs();
      runSearch();
    });
  });
}

function recalcTotalsFromTable(){
  const meta = window.__resultMeta || { totalHits: 0, totalMisses: 0 };
  const checks = Array.from(document.querySelectorAll('#resultTable tbody input.rowInclude'));
  let sum = 0;
  let selectedHits = 0;

  for (const ch of checks){
    const fare = Number(ch.dataset.fare || "0");
    const tr = ch.closest("tr");
    if (ch.checked && Number.isFinite(fare)){
      sum += Math.trunc(fare);
      selectedHits++;
      if (tr) tr.classList.remove("excluded");
    } else {
      if (tr) tr.classList.add("excluded");
    }
  }

  safeText("#sumFare", selectedHits ? money(sum) : "-");
  safeText("#hitCount", meta.totalHits ? `${selectedHits}/${meta.totalHits}` : "0");
  safeText("#missCount", String(meta.totalMisses));
}

function renderResults(rows, misses){
  const tbody = $("#resultTable tbody");
  if (!tbody) return;

  tbody.innerHTML = "";

  const totalHits = (rows || []).filter(r => r.hit && Number.isFinite(r.row?.fare)).length;
  const totalMisses = (rows || []).length - totalHits;

  window.__resultMeta = { totalHits, totalMisses };

  for (const r of (rows || [])){
    const tr = document.createElement("tr");

    const chk = r.hit
      ? `<input type="checkbox" class="rowInclude" data-fare="${Number(r.row?.fare || 0)}" checked />`
      : `<input type="checkbox" disabled />`;

    const status = r.hit
      ? `${ymd(r.row.validFrom)}〜${ymd(r.row.validTo)}`
      : (r.hasAnyRoute ? `<span class="pill amber">期間外</span>` : `<span class="pill red">未登録</span>`);

    tr.innerHTML = `
      <td class="chk">${chk}</td>
      <td>${ymd(r.leg.date)}</td>
      <td>${r.from}</td>
      <td>${r.to}</td>
      <td>${r.hit ? (r.row.priceType || "-") : `<span class="pill red">未ヒット</span>`}</td>
      <td class="num">${r.hit ? money(r.row.fare) : "-"}</td>
      <td>${status}</td>
    `;
    tbody.appendChild(tr);
  }

  // Wire checkbox events (recalculate totals in-place)
  tbody.querySelectorAll("input.rowInclude").forEach(ch => {
    ch.addEventListener("change", recalcTotalsFromTable);
  });

  // Update totals + counts
  recalcTotalsFromTable();

  // Diagnostics: misses (no candidate hints; keep it minimal)
  const missLines = (misses || []).map(m => {
    const reason = m.hasAnyRoute ? "期間外" : "未登録";
    return `- ${ymd(m.leg.date)} ${m.from}→${m.to} (${reason})`;
  }).join("\n");
  safeText("#diagMisses", missLines || "（未ヒットなし）");

  // Diagnostics: alias sample + load log (compact)
  const aliasSample = Array.from(DB.aliasToCanon.entries()).slice(0, 120).map(([k,v]) => `${k} => ${v}`).join("\n");
  const log = DB.loadLog.slice(-30).map(s => `• ${s}`).join("\n");
  const diag = (aliasSample ? aliasSample : "（aliasなし）") + "\n\n---- load log (last 30) ----\n" + (log || "（logなし）");
  safeText("#diagAliases", diag);
}

function runSearch(){
  const legs = window.__legs || [];

  if (!DB.faresRows || DB.faresRows.length === 0){
    renderResults([], []);
    safeText("#parseMsg", "DB未読み込み（運賃データ0件）です。右下の「データ更新日時」を確認してください。");
    return;
  }
  if (!legs.length){
    renderResults([], []);
    safeText("#parseMsg", "旅程がありません。テキスト貼り付けかフォームで追加してください。");
    return;
  }

  safeText("#parseMsg", "");

  const results = [];
  const misses = [];

  for (const leg of legs){
    const res = findFare(leg.date, leg.from, leg.to);
    results.push({ ...res, leg });
    if (!res.hit) misses.push({ ...res, leg });
  }

  renderResults(results, misses);
}

// -----------------------------
// DB build
// -----------------------------
function resetDB(){
  DB.faresRows = [];
  DB.routeMap = new Map();
  DB.places = [];
  DB.aliasToCanon = new Map();
  DB.meta = {
    source: "",
    sourceUrl: "",
    fares: 0,
    routes: 0,
    places: 0,
    updatedAt: "",
    rawRows: 0,
  };
}

function setDbMeta(){
  const m = DB.meta;
  const el = $("#dbMeta");
  if (!el) return;

  if (!m.source){
    el.textContent = "-";
    return;
  }

  el.textContent = `DB: ${m.source} / routes=${m.routes} / fares=${m.fares} / places=${m.places} / updated=${m.updatedAt}`;
}

function setDbLoadMsg(msg){
  safeText("#dbLoadMsg", msg || "");
}

function parseFareRowsFromText(text, hintedName){
  // decide CSV/TSV
  const name = (hintedName || "").toLowerCase();
  const isTSV = name.endsWith(".tsv") || (!name.endsWith(".csv") && (text.includes("\t") && !text.includes(",")));
  const rows = isTSV ? parseTSV(text) : parseCSV(text);
  const shape = hasFareShape(rows);
  if (!shape.ok){
    throw new Error(`データ形式が不正です（出発地/到着地/運賃が見つかりません）。${shape.reason ? shape.reason : ""}`);
  }
  return { rows, format: isTSV ? "tsv" : "csv" };
}

function shiftYear(d, plus){
  const x = new Date(d);
  x.setFullYear(x.getFullYear() + plus);
  return x;
}

// Align year for cross-year boarding windows
function alignToWholeRange(pFrom, pTo, wholeFrom, wholeTo){
  let from = pFrom;
  let to = pTo;
  if (!(from instanceof Date) || !(to instanceof Date)) return null;

  if (wholeFrom instanceof Date && wholeTo instanceof Date){
    const crosses = wholeTo.getFullYear() > wholeFrom.getFullYear();
    if (crosses){
      const startY = wholeFrom.getFullYear();
      const startM = wholeFrom.getMonth();

      // Common: Jan-Mar written with startY; shift to +1 year
      if (from.getFullYear() === startY && from.getMonth() < startM) from = shiftYear(from, +1);
      if (to.getFullYear() === startY && to.getMonth() < startM) to = shiftYear(to, +1);

      if (to < wholeFrom){
        from = shiftYear(from, +1);
        to = shiftYear(to, +1);
      }
    }

    // clamp
    const vf = new Date(Math.max(from.getTime(), wholeFrom.getTime()));
    const vt = new Date(Math.min(to.getTime(), wholeTo.getTime()));
    if (vf > vt) return null;
    return { from: vf, to: vt };
  }

  return { from, to };
}

function buildDBFromRows(fareRows, sourceName, sourceUrl, aliasRows){
  resetDB();

  const fares = [];
  const placesSet = new Set();
  const seen = new Set();

  const keyOf = (from,to,ptype,fromD,toD) => `${from}||${to}||${ptype}||${ymd(fromD)}||${ymd(toD)}`;

  for (const r of fareRows){
    const rr = normalizeRowKeys(r);

    const fromRaw = (rr.from ?? "").toString().trim();
    const toRaw = (rr.to ?? "").toString().trim();
    if (!fromRaw || !toRaw) continue;

    const priceType = ((rr.priceType ?? "").toString().trim() || "通常");
    const fare = parseInt(((rr.fare ?? "0").toString()).replace(/[^0-9-]/g,""), 10);
    const fareNum = Number.isFinite(fare) ? fare : 0;

    const wholeFrom = parseDateLoose(rr.wholeFrom);
    const wholeTo = parseDateLoose(rr.wholeTo);

    // periods
    let periods = [];

    // 1) explicit from/to columns
    const vf = parseDateLoose(rr.validFrom);
    const vt = parseDateLoose(rr.validTo);
    if (vf && vt) periods.push({ from: vf, to: vt, raw: `${rr.validFrom}〜${rr.validTo}` });

    // 2) string range column
    if (!periods.length){
      periods = parsePeriodRanges(rr.validRange);
    }

    // 3) fallback: use whole boarding range if no specific periods
    if (!periods.length && wholeFrom && wholeTo){
      periods = [{ from: wholeFrom, to: wholeTo, raw: `${ymd(wholeFrom)}〜${ymd(wholeTo)}` }];
    }

    for (const p of periods){
      const aligned = alignToWholeRange(p.from, p.to, wholeFrom, wholeTo);
      if (!aligned) continue;

      const uniq = keyOf(fromRaw, toRaw, priceType, aligned.from, aligned.to);
      if (seen.has(uniq)) continue;
      seen.add(uniq);

      fares.push({
        from: fromRaw,
        to: toRaw,
        priceType,
        fare: fareNum,
        validFrom: aligned.from,
        validTo: aligned.to,
        source: sourceName,
      });

      placesSet.add(fromRaw);
      placesSet.add(toRaw);
    }
  }

  const places = Array.from(placesSet).sort((a,b)=>a.localeCompare(b,"ja"));
  const aliasToCanon = buildAliasDefaultsFromPlaces(places);

  // merge external aliases (optional)
  for (const a of (aliasRows || [])){
    const aa = normalizeRowKeys(a);
    const alias = (aa.alias ?? "").toString().trim();
    const canon = (aa.canonical ?? "").toString().trim();
    if (!alias || !canon) continue;
    aliasToCanon.set(normKey(alias), canon);
  }

  // route index
  const map = new Map();
  for (const row of fares){
    const k = normKey(row.from) + "||" + normKey(row.to);
    if (!map.has(k)) map.set(k, []);
    map.get(k).push(row);
  }
  for (const [k, arr] of map.entries()){
    arr.sort((a,b)=>a.validFrom - b.validFrom || a.validTo - b.validTo || a.fare - b.fare);
  }

  DB.faresRows = fares;
  DB.routeMap = map;
  DB.places = places;
  DB.aliasToCanon = aliasToCanon;
  DB.meta = {
    source: sourceName,
    sourceUrl: sourceUrl || "",
    fares: fares.length,
    routes: map.size,
    places: places.length,
    updatedAt: nowStamp(),
    rawRows: Array.isArray(fareRows) ? fareRows.length : 0,
  };

  setDbMeta();

  if (DB.faresRows.length === 0 || DB.places.length === 0){
    setDbLoadMsg(`⚠ DB読込は成功しましたが、検索用データが0件です。期間列（価格適用期間）が想定形式か確認してください。rawRows=${DB.meta.rawRows}`);
  } else {
    setDbLoadMsg(`✅ DB読込完了: ${DB.meta.source}（rows=${DB.meta.rawRows} / fares=${DB.meta.fares} / places=${DB.meta.places}）`);
  }
}

async function loadAliasRowsRemote(){
  const urls = [
    new URL("./data/place_aliases.csv", location.href).toString(),
    "data/place_aliases.csv",
    "./data/place_aliases.csv",
  ];
  try{
    const got = await fetchTextFirstOk(urls);
    if (looksLikeHTML(got.text)) return [];
    return parseCSV(got.text);
  } catch {
    return [];
  }
}

async function loadDBRemote(){
  DB.loadLog = [];
  setDbLoadMsg("DB読み込み中...");

  // candidate URLs - robust for GitHub Pages (subdir) & Netlify
  const csvUrls = [
    new URL("./data/transport.csv", location.href).toString(),
    "data/transport.csv",
    "./data/transport.csv",
  ];
  const tsvUrls = [
    new URL("./data/fare_source.tsv", location.href).toString(),
    "data/fare_source.tsv",
    "./data/fare_source.tsv",
  ];

  let fareRows = null;
  let sourceName = "";
  let sourceUrl = "";

  // 1) try transport.csv
  try{
    const got = await fetchTextFirstOk(csvUrls);
    if (looksLikeHTML(got.text)) throw new Error("transport.csv がHTMLを返しています（SPA fallback等）");
    const parsed = parseFareRowsFromText(got.text, "transport.csv");
    fareRows = parsed.rows;
    sourceName = "transport.csv";
    sourceUrl = got.url;
  } catch (eCsv){
    DB.loadLog.push(`WARN: transport.csv failed: ${eCsv?.message || eCsv}`);

    // 2) fallback tsv
    const got = await fetchTextFirstOk(tsvUrls);
    if (looksLikeHTML(got.text)) throw new Error("fare_source.tsv がHTMLを返しています（SPA fallback等）");
    const parsed = parseFareRowsFromText(got.text, "fare_source.tsv");
    fareRows = parsed.rows;
    sourceName = "fare_source.tsv";
    sourceUrl = got.url;
  }

  const aliasRows = await loadAliasRowsRemote();
  buildDBFromRows(fareRows, sourceName, sourceUrl, aliasRows);

  // populate dropdowns
  renderSelectOptions();

  // If itinerary already exists, re-run
  runSearch();
}

async function loadDBFromLocalFile(file){
  const text = await file.text();
  DB.loadLog = [`LOCAL: ${file.name} (${text.length} chars)`];

  const parsed = parseFareRowsFromText(text, file.name);
  const aliasRows = await loadAliasRowsRemote(); // keep same alias sheet if deployed
  buildDBFromRows(parsed.rows, `local:${file.name}`, "", aliasRows);

  renderSelectOptions();
  runSearch();
}

// -----------------------------
// Event wiring
// -----------------------------
function bindUI(){
  window.__legs = [];

// Date quick actions (②フォーム)
$("#btnDateToday")?.addEventListener("click", (e) => {
  e.preventDefault();
  const el = $("#legDate");
  if (el) el.value = ymd(new Date());
});

$("#btnDatePlus1")?.addEventListener("click", (e) => {
  e.preventDefault();
  const el = $("#legDate");
  const base = el?.value ? parseDateLoose(el.value) : new Date();
  const d = base ? new Date(base) : new Date();
  d.setDate(d.getDate() + 1);
  if (el) el.value = ymd(d);
});


  $("#btnParse")?.addEventListener("click", () => {
    const { legs, errors } = parseItineraryLines($("#itineraryText")?.value || "");
    if (errors.length){
      safeText("#parseMsg", errors.slice(0, 8).join(" / "));
    } else {
      safeText("#parseMsg", `解析OK：${legs.length}件`);
    }
    window.__legs = legs;
    renderLegs();
    runSearch();
  });

  $("#btnClear")?.addEventListener("click", () => {
    const t = $("#itineraryText");
    if (t) t.value = "";
    safeText("#parseMsg", "");
  });

  $("#btnAddLeg")?.addEventListener("click", () => {
    const d = $("#legDate")?.value ? parseDateLoose($("#legDate").value) : null;
    const from = $("#fromSelect")?.value || "";
    const to = $("#toSelect")?.value || "";
    const via1 = $("#via1Select")?.value || "";
    const via2 = $("#via2Select")?.value || "";

    if (!d || !from || !to){
      safeText("#parseMsg", "日付・出発地・到着地を指定してください。");
      return;
    }

    const legsToAdd = [];

    // If via specified, add both: direct candidate + segmented via legs (so you can compare)
    if (via1 || via2){
      legsToAdd.push({ date: d, from, to, raw: `${ymd(d)} ${from}→${to}（直行候補）`, tag: "direct" });

      if (via1 && via2){
        legsToAdd.push({ date: d, from, to: via1, raw: `${ymd(d)} ${from}→${via1}（経由①）`, tag: "via" });
        legsToAdd.push({ date: d, from: via1, to: via2, raw: `${ymd(d)} ${via1}→${via2}（経由②）`, tag: "via" });
        legsToAdd.push({ date: d, from: via2, to, raw: `${ymd(d)} ${via2}→${to}（経由③）`, tag: "via" });
      } else if (via1){
        legsToAdd.push({ date: d, from, to: via1, raw: `${ymd(d)} ${from}→${via1}（経由①）`, tag: "via" });
        legsToAdd.push({ date: d, from: via1, to, raw: `${ymd(d)} ${via1}→${to}（経由②）`, tag: "via" });
      } else if (via2){
        legsToAdd.push({ date: d, from, to: via2, raw: `${ymd(d)} ${from}→${via2}（経由①）`, tag: "via" });
        legsToAdd.push({ date: d, from: via2, to, raw: `${ymd(d)} ${via2}→${to}（経由②）`, tag: "via" });
      }
    } else {
      legsToAdd.push({ date: d, from, to, raw: `${ymd(d)} ${from}→${to}`, tag: "" });
    }

    window.__legs.push(...legsToAdd);
    renderLegs();
    runSearch();
  });

  $("#btnResetLegs")?.addEventListener("click", () => {
    window.__legs = [];
    renderLegs();
    runSearch();
  });

  // DB reload
  $("#btnReloadDb")?.addEventListener("click", async () => {
    try{
      await loadDBRemote();
    } catch (e){
      console.error(e);
      setDbLoadMsg("❌ DB再読み込み失敗: " + (e?.message || e));
      safeText("#dbMeta", "読み込み失敗");
    }
  });

  // Local file import
  $("#btnLoadLocalDb")?.addEventListener("click", async () => {
    const inp = $("#dbFileInput");
    const file = inp?.files?.[0];
    if (!file){
      setDbLoadMsg("ローカルファイルを選択してください（.csv / .tsv）。");
      return;
    }
    try{
      await loadDBFromLocalFile(file);
    } catch (e){
      console.error(e);
      setDbLoadMsg("❌ ローカルDB読み込み失敗: " + (e?.message || e));
    }
  });
}

async function boot(){
  safeText("#dbMeta", "-");
  safeText("#dbLoadMsg", "");
  setSelectLoading();
  bindUI();

  // UI defaults
  const ld = $("#legDate");
  if (ld && !ld.value) ld.value = ymd(new Date());
  safeText("#topMeta", APP_VERSION);

  renderLegs();
  runSearch();

  try{
    await loadDBRemote();
  } catch (e){
    console.error(e);
    safeText("#dbMeta", "読み込み失敗");
    setDbLoadMsg("❌ 自動DB読み込みに失敗: " + (e?.message || e) + "（右のローカル読み込みで回避できます）");
  }
}

if (document.readyState === "loading"){
  document.addEventListener("DOMContentLoaded", boot);
} else {
  boot();
}
