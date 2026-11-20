"use strict";
/**
 * MCBE Proximity Voice - เซิร์ฟเวอร์กลาง
 *  - /api/mc/sync  : แอดออนในเกมส่งตำแหน่ง/ค่าตั้งของผู้เล่นมาที่นี่ (ต้องมี x-api-key)
 *  - /ws           : เว็บแอปของผู้เล่นเชื่อมมาส่ง/รับเสียง
 *  เสียงถูกส่งต่อ (relay) เฉพาะคนที่อยู่ในระยะเท่านั้น
 */
const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const API_KEY = process.env.API_KEY || "";
const SIM = process.env.SIM === "1"; // โหมดทดสอบโดยไม่ต้องเปิดเกม
// ระยะเสียงสูงสุด (บล็อก) ที่ผู้เล่นตั้งได้ แอดมินปรับได้ด้วยตัวแปร MAX_RANGE (ค่าเริ่มต้น 30)
const MAX_RANGE = Math.min(500, Math.max(5, Math.round(Number(process.env.MAX_RANGE) || 30)));
const SECRET = process.env.TOKEN_SECRET || crypto.createHash("sha256").update("vc:" + API_KEY).digest("hex");
// ไฟล์ทั้งหมดอยู่ในโฟลเดอร์เดียวกัน (อัปโหลดง่ายบนมือถือ) เสิร์ฟเฉพาะ 2 ไฟล์นี้เท่านั้น
const FILES = { "/": "index.html", "/index.html": "index.html", "/manifest.webmanifest": "manifest.webmanifest", "/icon-192.png": "icon-192.png", "/icon-512.png": "icon-512.png", "/config.json": "config.json" };

if (!API_KEY || API_KEY.length < 12) {
  console.error("!! ตั้งค่า API_KEY (ยาวอย่างน้อย 12 ตัวอักษร) ใน Environment ก่อนรัน");
  if (!SIM) process.exit(1);
}

/* ---------- state ---------- */
const mc = new Map(); // name -> {x,y,z,d,yaw,mic,deaf,range,seen,sim?,code?,codeExp?}
const clients = new Set(); // {ws,id,name,ready,hear,audience,lastVoice}
const byName = new Map();
const codes = new Map(); // code -> name
const pendingApply = {}; // name -> {mic,deaf,range,gain,agc}
const calls = new Map(); // id -> {id,a,b,st:"ringing"|"active",t}
const callOf = new Map(); // name -> call
const notes = {}; // name -> [ข้อความสั้นๆ ให้แอดออนแจ้งผู้เล่น]
let callSeq = 0;
let nextId = 1;

const clamp = (v, a, b) => Math.min(b, Math.max(a, v));
const num = (v, d = 0) => (Number.isFinite(+v) ? +v : d);

/* ---------- token ---------- */
const b64u = (s) => Buffer.from(s, "utf8").toString("base64url");
const sign = (p) => crypto.createHmac("sha256", SECRET).update(p).digest("base64url");
function makeToken(name) { const p = b64u(name); return p + "." + sign(p); }
function readToken(t) {
  if (typeof t !== "string") return null;
  const [p, s] = t.split(".");
  if (!p || !s) return null;
  const good = sign(p);
  if (s.length !== good.length || !crypto.timingSafeEqual(Buffer.from(s), Buffer.from(good))) return null;
  return Buffer.from(p, "base64url").toString("utf8");
}

/* ---------- link code ---------- */
function codeFor(name, ps) {
  const now = Date.now();
  if (ps.code && ps.codeExp > now) return ps.code;
  if (ps.code) codes.delete(ps.code);
  let c;
  do { c = String(crypto.randomInt(100000, 1000000)); } while (codes.has(c));
  codes.set(c, name);
  ps.code = c;
  ps.codeExp = now + 30 * 60 * 1000;
  return c;
}

/* ---------- rate limit ---------- */
const hits = new Map();
function limited(ip, max = 10, windowMs = 60000) {
  const now = Date.now();
  const h = (hits.get(ip) || []).filter((t) => now - t < windowMs);
  h.push(now);
  hits.set(ip, h);
  return h.length > max;
}
const ipOf = (req) => (req.headers["x-forwarded-for"] || "").split(",")[0].trim() || req.socket.remoteAddress;

/* ---------- โทรศัพท์ในเกม ---------- */
const RING_MS = 30000;
function note(n, msg) { (notes[n] = notes[n] || []).push(msg); }
function endCall(c, noteA, noteB) {
  calls.delete(c.id); callOf.delete(c.a); callOf.delete(c.b);
  if (noteA) note(c.a, noteA);
  if (noteB) note(c.b, noteB);
}
function callInfo(n) {
  const c = callOf.get(n);
  if (!c) return null;
  return { with: c.a === n ? c.b : c.a, st: c.st === "active" ? "active" : c.a === n ? "out" : "in" };
}
function handleEvent(ev) {
  const n = ev && ev.n;
  if (typeof n !== "string" || !mc.has(n)) return;
  const cur = callOf.get(n);
  if (ev.t === "call") {
    const to = String(ev.to || "");
    if (!to || to === n || !mc.has(to)) return note(n, "โทรไม่ได้ ไม่พบผู้เล่นปลายสายในเซิร์ฟเวอร์");
    if (cur) return note(n, "คุณกำลังอยู่ในสายอยู่แล้ว");
    if (callOf.has(to)) return note(n, to + " ไม่ว่าง (กำลังอยู่ในสาย)");
    const c = { id: ++callSeq, a: n, b: to, st: "ringing", t: Date.now() };
    calls.set(c.id, c); callOf.set(n, c); callOf.set(to, c);
  } else if (!cur) {
    return;
  } else if (ev.t === "answer") {
    if (cur.st === "ringing" && cur.b === n) { cur.st = "active"; cur.t = Date.now(); }
  } else if (ev.t === "decline") {
    if (cur.st === "ringing" && cur.b === n) endCall(cur, n + " ไม่รับสาย", null);
  } else if (ev.t === "hangup") {
    if (cur.st === "ringing") endCall(cur, cur.a === n ? null : n + " ไม่รับสาย", cur.a === n ? "สายที่โทรเข้าถูกยกเลิก" : null);
    else endCall(cur, cur.a === n ? null : "ปลายสายวางสายแล้ว", cur.a === n ? "ปลายสายวางสายแล้ว" : null);
  }
}
function tickCalls() {
  const now = Date.now();
  for (const c of [...calls.values()]) {
    if (!mc.has(c.a) || !mc.has(c.b)) endCall(c, "สายหลุด (ปลายสายออกจากเกม)", "สายหลุด (ปลายสายออกจากเกม)");
    else if (c.st === "ringing" && now - c.t > RING_MS) endCall(c, "ไม่มีคนรับสาย", "พลาดสายจาก " + c.a);
  }
}

/* ---------- proximity mix ---------- */
function recompute() {
  const list = [...clients].filter((c) => c.name && c.ready);
  for (const c of list) { c.hear = {}; c.audience = []; }
  for (const s of list) {
    const ps = mc.get(s.name);
    if (!ps || !ps.mic || ps.deaf) continue;
    for (const l of list) {
      if (l === s) continue;
      const pl = mc.get(l.name);
      if (!pl || pl.deaf || pl.d !== ps.d) continue;
      const dx = ps.x - pl.x, dy = ps.y - pl.y, dz = ps.z - pl.z;
      const dist = Math.hypot(dx, dy, dz);
      if (dist > ps.range) continue;
      const near = ps.range * 0.25;
      const vol = dist <= near ? 1 : 1 - (dist - near) / (ps.range - near);
      // pan: ซ้าย -1 .. ขวา +1 ตามทิศที่ผู้ฟังหันหน้า (yaw ของ MC: 0 = หัน +Z)
      const yaw = (pl.yaw * Math.PI) / 180;
      const hd = Math.hypot(dx, dz);
      let pan = 0;
      if (hd > 0.5) pan = clamp(((dx * -Math.cos(yaw) + dz * -Math.sin(yaw)) / hd) * 0.85, -1, 1);
      l.hear[s.id] = [+vol.toFixed(2), +pan.toFixed(2)];
      s.audience.push(l);
    }
  }
  // สายโทรศัพท์ที่รับแล้ว: คู่สายได้ยินกันเต็มเสียงไม่จำกัดระยะ (คนข้างๆ ได้ยินแค่เสียงของเราตามระยะปกติ)
  for (const cl of calls.values()) {
    if (cl.st !== "active") continue;
    const A = byName.get(cl.a), B = byName.get(cl.b);
    for (const [s, l] of [[A, B], [B, A]]) {
      if (!s || !l || !s.ready || !l.ready) continue;
      const ps = mc.get(s.name), pl = mc.get(l.name);
      if (!ps || !pl || !ps.mic || ps.deaf || pl.deaf) continue;
      l.hear[s.id] = [1, 0, 1];
      if (!s.audience.includes(l)) s.audience.push(l);
    }
  }
  for (const c of clients) {
    if (!c.name || c.ws.readyState !== 1) continue;
    const ps = mc.get(c.name);
    const names = {};
    for (const id of Object.keys(c.hear || {})) {
      const o = [...clients].find((x) => x.id === +id);
      if (o) names[id] = o.name;
    }
    c.ws.send(JSON.stringify({
      t: "mix",
      max: MAX_RANGE,
      v: c.hear || {},
      names,
      me: ps
        ? { online: true, mic: ps.mic, deaf: ps.deaf, range: ps.range, gain: ps.gain, agc: ps.agc, call: callInfo(c.name), speaking: Date.now() - c.lastVoice < 500 }
        : { online: false },
    }));
  }
}

/* ---------- HTTP ---------- */
const MIME = { ".html": "text/html; charset=utf-8", ".webmanifest": "application/manifest+json", ".png": "image/png", ".json": "application/json; charset=utf-8" };
function body(req, max = 256 * 1024) {
  return new Promise((res, rej) => {
    let b = "";
    req.on("data", (d) => { b += d; if (b.length > max) { rej(new Error("big")); req.destroy(); } });
    req.on("end", () => { try { res(JSON.parse(b || "{}")); } catch (e) { rej(e); } });
  });
}
const send = (res, code, obj) => { res.writeHead(code, { "content-type": "application/json" }); res.end(JSON.stringify(obj)); };

const server = http.createServer(async (req, res) => {
  const url = req.url.split("?")[0];
  try {
    if (req.method === "POST" && url === "/api/mc/sync") {
      if (req.headers["x-api-key"] !== API_KEY) return send(res, 401, { ok: false });
      const data = await body(req);
      const now = Date.now();
      const seen = new Set();
      for (const p of data.players || []) {
        if (!p || typeof p.n !== "string") continue;
        seen.add(p.n);
        const old = mc.get(p.n) || {};
        mc.set(p.n, {
          ...old,
          x: num(p.x), y: num(p.y), z: num(p.z), d: String(p.d || "overworld"), yaw: num(p.yaw),
          mic: p.mic !== false, deaf: p.deaf === true, range: clamp(Math.round(num(p.range, 15)), 1, MAX_RANGE),
          gain: clamp(num(p.gain, 1), 0.25, 4), agc: p.agc !== false, seen: now,
        });
      }
      for (const [n, ps] of mc) if (!ps.sim && !seen.has(n)) { if (ps.code) codes.delete(ps.code); mc.delete(n); }
      for (const ev of Array.isArray(data.events) ? data.events.slice(0, 50) : []) handleEvent(ev);
      tickCalls();
      const out = {};
      for (const n of seen) {
        const ps = mc.get(n);
        const c = byName.get(n);
        const connected = !!(c && c.ready);
        out[n] = { c: connected ? 1 : 0, s: connected && Date.now() - c.lastVoice < 500 ? 1 : 0 };
        if (!connected) out[n].code = codeFor(n, ps);
        const ci = callInfo(n);
        if (ci) out[n].call = ci;
        if (notes[n] && notes[n].length) { out[n].note = notes[n].splice(0, 5); }
      }
      const apply = JSON.parse(JSON.stringify(pendingApply));
      for (const k of Object.keys(pendingApply)) delete pendingApply[k];
      recompute();
      return send(res, 200, { ok: true, p: out, apply, max: MAX_RANGE });
    }
    if (req.method === "POST" && url === "/api/link") {
      if (limited(ipOf(req))) return send(res, 429, { ok: false, error: "ลองบ่อยเกินไป รอสักครู่" });
      const data = await body(req);
      if (SIM && typeof data.sim === "string" && data.sim.trim()) {
        const name = data.sim.trim().slice(0, 32);
        return send(res, 200, { ok: true, name, token: makeToken(name) });
      }
      const code = String(data.code || "").replace(/\D/g, "");
      const name = codes.get(code);
      if (!name || !mc.has(name)) return send(res, 404, { ok: false, error: "รหัสไม่ถูกต้องหรือหมดอายุ (ต้องอยู่ในเกมระหว่างกรอก)" });
      const ps = mc.get(name);
      codes.delete(code); ps.code = null;
      return send(res, 200, { ok: true, name, token: makeToken(name) });
    }
    if (url === "/api/info") return send(res, 200, { ok: true, sim: SIM, players: mc.size, max: MAX_RANGE });
    if (req.method === "GET" && FILES[url]) {
      const full = path.join(__dirname, FILES[url]);
      if (fs.existsSync(full)) {
        res.writeHead(200, { "content-type": MIME[path.extname(full)] || "application/octet-stream", "cache-control": "no-cache", "access-control-allow-origin": "*" });
        return fs.createReadStream(full).pipe(res);
      }
    }
    res.writeHead(404); res.end("not found");
  } catch (e) {
    send(res, 400, { ok: false });
  }
});

/* ---------- WebSocket ---------- */
const wss = new WebSocketServer({ server, path: "/ws", maxPayload: 16 * 1024 });
wss.on("connection", (ws, req) => {
  const c = { ws, id: 0, name: null, ready: false, hear: {}, audience: [], lastVoice: 0, alive: true };
  const kill = setTimeout(() => { if (!c.name) ws.close(4000, "auth timeout"); }, 8000);
  ws.on("pong", () => (c.alive = true));
  ws.on("message", (data, isBinary) => {
    if (isBinary) {
      if (!c.ready || data.length < 64 || data.length > 4096) return;
      const ps = mc.get(c.name);
      if (!ps || !ps.mic || ps.deaf) return;
      c.lastVoice = Date.now();
      const out = Buffer.allocUnsafe(2 + data.length);
      out.writeUInt16BE(c.id, 0);
      data.copy(out, 2);
      for (const l of c.audience) if (l.ws.readyState === 1 && l.ws.bufferedAmount < 200000) l.ws.send(out, { binary: true });
      return;
    }
    let m;
    try { m = JSON.parse(data.toString()); } catch { return; }
    if (m.t === "auth") {
      if (limited("ws:" + ipOf(req), 60)) return ws.close(4003, "rate");
      const name = readToken(m.token);
      if (!name) return ws.close(4002, "bad token");
      const old = byName.get(name);
      if (old && old !== c) old.ws.close(4001, "replaced");
      c.name = name; c.id = nextId++; if (nextId > 65000) nextId = 1;
      byName.set(name, c); clients.add(c); clearTimeout(kill);
      ws.send(JSON.stringify({ t: "hello", name, sim: SIM, max: MAX_RANGE }));
    } else if (!c.name) {
      return;
    } else if (m.t === "ready") {
      c.ready = true;
    } else if (m.t === "set") {
      const set = {};
      if (typeof m.mic === "boolean") set.mic = m.mic;
      if (typeof m.deaf === "boolean") set.deaf = m.deaf;
      if (m.range !== undefined) set.range = clamp(Math.round(num(m.range, 15)), 1, MAX_RANGE);
      if (m.gain !== undefined) set.gain = clamp(num(m.gain, 1), 0.25, 4);
      if (typeof m.agc === "boolean") set.agc = m.agc;
      const ps = mc.get(c.name);
      if (SIM) {
        if (!ps) mc.set(c.name, { x: 0, y: 64, z: 0, d: "overworld", yaw: 0, mic: true, deaf: false, range: 15, gain: 1, agc: true, sim: true, ...set });
        else Object.assign(ps, set);
      } else {
        pendingApply[c.name] = { ...(pendingApply[c.name] || {}), ...set };
        if (ps) Object.assign(ps, set);
      }
    } else if (m.t === "callact") {
      if (m.act === "hangup") handleEvent({ n: c.name, t: "hangup" });
      else if (SIM && m.act === "call") handleEvent({ n: c.name, t: "call", to: m.to });
      else if (SIM && (m.act === "answer" || m.act === "decline")) handleEvent({ n: c.name, t: m.act });
    } else if (m.t === "pos" && SIM) {
      const ps = mc.get(c.name) || { d: "overworld", mic: true, deaf: false, range: 15, gain: 1, agc: true };
      mc.set(c.name, { ...ps, sim: true, x: num(m.x), y: 64, z: num(m.z), yaw: num(m.yaw), seen: Date.now() });
    }
  });
  ws.on("close", () => {
    clearTimeout(kill);
    clients.delete(c);
    if (c.name && byName.get(c.name) === c) byName.delete(c.name);
  });
});
setInterval(() => {
  for (const c of clients) {
    if (!c.alive) { c.ws.terminate(); continue; }
    c.alive = false; c.ws.ping();
  }
  tickCalls();
  if (SIM) recompute();
}, 20000);
if (SIM) setInterval(() => { tickCalls(); recompute(); }, 250);

server.listen(PORT, () => console.log(`Voice server พร้อมที่พอร์ต ${PORT} | ระยะเสียงสูงสุด ${MAX_RANGE} บล็อก${SIM ? " (โหมดทดสอบ SIM)" : ""}`));
