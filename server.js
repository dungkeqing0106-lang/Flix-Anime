// Anime Vietsub API — wrapper cho AniMapper (https://animapper.net/docs/tutorial)
// Chạy: npm install && npm start   (Node >= 18)

const express = require("express");
const cors = require("cors");
const dns = require("dns").promises;
const net = require("net");
const { Readable } = require("stream");

const PORT = process.env.PORT || 3000;
const UPSTREAM = process.env.UPSTREAM || "https://api.animapper.net/api/v1";
const PUBLIC_URL = process.env.PUBLIC_URL || ""; // vd: https://api.domain.com (để trống = tự nhận)
const CLIENT_LIMIT = Number(process.env.CLIENT_LIMIT || 120); // req/phút/IP
const ALLOWED_HOSTS = (process.env.ALLOWED_HOSTS || "")
  .split(",").map(s => s.trim()).filter(Boolean); // giới hạn proxy (tuỳ chọn)

const app = express();
app.set("trust proxy", 1);
app.use(cors());
app.use(express.static(require("path").join(__dirname, "public"))); // giao diện web

/* ---------------- Cache đơn giản (giảm tải rate limit 60 req/phút của upstream) ---------------- */
const cache = new Map();
const TTL = {
  search: 5 * 60e3,
  autocomplete: 10 * 60e3,
  metadata: 60 * 60e3,
  episodes: 30 * 60e3,
  servers: 30 * 60e3,
  source: 5 * 60e3,
};
function cacheGet(k) {
  const e = cache.get(k);
  if (!e) return null;
  if (e.exp < Date.now()) { cache.delete(k); return null; }
  return e.val;
}
function cacheSet(k, val, ttl) {
  if (cache.size > 2000) cache.delete(cache.keys().next().value);
  cache.set(k, { val, exp: Date.now() + ttl });
}

/* ---------------- Rate limit theo IP ---------------- */
const hits = new Map();
setInterval(() => hits.clear(), 60e3).unref();
app.use((req, res, next) => {
  const n = (hits.get(req.ip) || 0) + 1;
  hits.set(req.ip, n);
  if (n > CLIENT_LIMIT)
    return res.status(429).json({ success: false, code: "RATE_LIMITED", message: "Quá nhiều request, thử lại sau." });
  next();
});

/* ---------------- Gọi AniMapper ---------------- */
class ApiError extends Error {
  constructor(status, code, message) { super(message); this.status = status; this.code = code; }
}

async function upstream(path, params = {}, { ttl = 0 } = {}) {
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(params))
    if (v !== undefined && v !== null && v !== "") qs.set(k, v);
  const url = `${UPSTREAM}${path}?${qs}`;

  if (ttl) {
    const hit = cacheGet(url);
    if (hit) return hit;
  }
  const r = await fetch(url, { headers: { Accept: "application/json" }, signal: AbortSignal.timeout(15000) });
  const text = await r.text();
  let data;
  try { data = JSON.parse(text); } catch { data = { message: text }; }

  if (!r.ok) {
    throw new ApiError(r.status, data.code || "UPSTREAM_ERROR", data.message || `Upstream ${r.status}`);
  }
  if (ttl) cacheSet(url, data, ttl);
  return data;
}

const wrap = fn => (req, res) =>
  fn(req, res).catch(err => {
    const status = err.status || 500;
    if (status >= 500) console.error(err);
    res.status(status).json({
      success: false,
      code: err.code || "INTERNAL_ERROR",
      message: err.message || "Lỗi máy chủ",
    });
  });

const need = (v, name) => {
  if (!v) throw new ApiError(400, "MISSING_PARAM", `Thiếu tham số: ${name}`);
  return v;
};

/* ---------------- Proxy HLS (Referer / CORS) ---------------- */
const b64e = o => Buffer.from(JSON.stringify(o)).toString("base64url");
const b64d = s => { try { return JSON.parse(Buffer.from(s, "base64url").toString()); } catch { return {}; } };
const SAFE_HEADERS = ["referer", "origin", "user-agent"];

function baseUrl(req) {
  return PUBLIC_URL || `${req.protocol}://${req.get("host")}`;
}
function proxify(req, absUrl, headers) {
  let u = `${baseUrl(req)}/api/proxy?u=${encodeURIComponent(absUrl)}`;
  if (headers && Object.keys(headers).length) u += `&h=${b64e(headers)}`;
  return u;
}

function isPrivateIp(ip) {
  if (net.isIPv4(ip)) {
    const [a, b] = ip.split(".").map(Number);
    return a === 10 || a === 127 || a === 0 || (a === 169 && b === 254) ||
      (a === 172 && b >= 16 && b <= 31) || (a === 192 && b === 168) || a >= 224;
  }
  const l = ip.toLowerCase();
  return l === "::1" || l === "::" || l.startsWith("fc") || l.startsWith("fd") ||
    l.startsWith("fe80") || l.startsWith("::ffff:127.") || l.startsWith("::ffff:10.") ||
    l.startsWith("::ffff:192.168.");
}
async function assertSafeTarget(u) {
  let url;
  try { url = new URL(u); } catch { throw new ApiError(400, "BAD_URL", "URL không hợp lệ"); }
  if (!["http:", "https:"].includes(url.protocol)) throw new ApiError(400, "BAD_URL", "Chỉ hỗ trợ http/https");
  if (ALLOWED_HOSTS.length && !ALLOWED_HOSTS.some(h => url.hostname === h || url.hostname.endsWith("." + h)))
    throw new ApiError(403, "HOST_NOT_ALLOWED", "Host không nằm trong danh sách cho phép");
  const addrs = net.isIP(url.hostname) ? [{ address: url.hostname }] : await dns.lookup(url.hostname, { all: true });
  if (addrs.some(a => isPrivateIp(a.address))) throw new ApiError(403, "FORBIDDEN_TARGET", "Địa chỉ không được phép");
  return url;
}

function rewriteM3u8(req, body, base, headers) {
  const abs = ref => proxify(req, new URL(ref, base).toString(), headers);
  return body.split(/\r?\n/).map(line => {
    const t = line.trim();
    if (!t) return line;
    if (t.startsWith("#"))
      return line.replace(/URI="([^"]+)"/g, (_, uri) => `URI="${abs(uri)}"`);
    return abs(t);
  }).join("\n");
}

app.get("/api/proxy", wrap(async (req, res) => {
  const target = await assertSafeTarget(need(req.query.u, "u"));
  const given = b64d(req.query.h || "");
  const headers = {};
  for (const k of Object.keys(given))
    if (SAFE_HEADERS.includes(k.toLowerCase())) headers[k] = String(given[k]);
  if (req.headers.range) headers.Range = req.headers.range;

  const r = await fetch(target, { headers, redirect: "follow", signal: AbortSignal.timeout(30000) });
  if (!r.ok && r.status !== 206) throw new ApiError(r.status === 404 ? 404 : 502, "PROXY_FAILED", `Nguồn trả về ${r.status}`);

  const ct = r.headers.get("content-type") || "";
  const isM3u8 = /mpegurl/i.test(ct) || target.pathname.endsWith(".m3u8");

  res.set("Access-Control-Allow-Origin", "*");
  if (isM3u8) {
    const text = await r.text();
    if (text.trimStart().startsWith("#EXTM3U")) {
      res.set("Content-Type", "application/vnd.apple.mpegurl");
      res.set("Cache-Control", "no-cache");
      return res.send(rewriteM3u8(req, text, target.toString(), given));
    }
    res.set("Content-Type", ct || "text/plain");
    return res.send(text);
  }
  res.status(r.status);
  for (const h of ["content-type", "content-length", "content-range", "accept-ranges"])
    if (r.headers.get(h)) res.set(h, r.headers.get(h));
  res.set("Cache-Control", "public, max-age=3600");
  Readable.fromWeb(r.body).on("error", () => res.end()).pipe(res);
}));

/* ---------------- API công khai ---------------- */

// Tìm kiếm: /api/search?title=attack&mediaType=ANIME&limit=10 (các tham số khác được chuyển tiếp)
app.get("/api/search", wrap(async (req, res) => {
  res.json(await upstream("/search", { mediaType: "ANIME", ...req.query }, { ttl: TTL.search }));
}));

// Gợi ý tìm kiếm
app.get("/api/autocomplete", wrap(async (req, res) => {
  res.json(await upstream("/autocomplete", req.query, { ttl: TTL.autocomplete }));
}));

// Thông tin anime + providers có sẵn
app.get("/api/anime/:id", wrap(async (req, res) => {
  res.json(await upstream("/metadata", { id: req.params.id }, { ttl: TTL.metadata }));
}));

// Danh sách server của provider
app.get("/api/anime/:id/servers", wrap(async (req, res) => {
  res.json(await upstream("/stream/episodes/servers",
    { id: req.params.id, provider: need(req.query.provider, "provider") }, { ttl: TTL.servers }));
}));

// Danh sách tập: ?provider=ANIMEVIETSUB&limit=&offset=&server=
app.get("/api/anime/:id/episodes", wrap(async (req, res) => {
  const { provider, limit, offset, server } = req.query;
  res.json(await upstream("/stream/episodes",
    { id: req.params.id, provider: need(provider, "provider"), limit, offset, server }, { ttl: TTL.episodes }));
}));

// Lấy nguồn phát theo episodeData (episodeId lấy từ /episodes)
async function getSource(req, { episodeData, provider, server, proxy }) {
  const src = await upstream("/stream/source", { episodeData, provider, server }, { ttl: TTL.source });
  const out = { ...src };
  if (proxy !== "0" && src.type === "HLS" && src.url) {
    out.originalUrl = src.url;
    out.url = proxify(req, src.url, src.proxyHeaders || {});
    out.corsProxyRequired = false;
  }
  return out;
}

// ?episodeData=...&provider=...&server=DU&proxy=0 (proxy=0 để lấy URL gốc)
app.get("/api/watch", wrap(async (req, res) => {
  const { episodeData, provider, server, proxy } = req.query;
  res.json(await getSource(req, {
    episodeData: need(episodeData, "episodeData"), provider: need(provider, "provider"), server, proxy,
  }));
}));

// Tiện lợi: xem thẳng theo số tập — /api/anime/16498/watch/1?provider=ANIMEVIETSUB&server=DU
app.get("/api/anime/:id/watch/:ep", wrap(async (req, res) => {
  const { provider, server, proxy } = req.query;
  need(provider, "provider");
  const list = await upstream("/stream/episodes", { id: req.params.id, provider, server }, { ttl: TTL.episodes });
  const eps = list.episodes || [];
  const ep = eps.find(e => String(e.episodeNumber) === String(req.params.ep));
  if (!ep) throw new ApiError(404, "EPISODE_NOT_FOUND", `Không có tập ${req.params.ep}`);
  const idx = eps.indexOf(ep);
  const source = await getSource(req, { episodeData: ep.episodeId, provider, server: server || ep.server, proxy });
  res.json({
    mediaId: Number(req.params.id),
    provider,
    episode: ep,
    prev: eps[idx - 1]?.episodeNumber ?? null,
    next: eps[idx + 1]?.episodeNumber ?? null,
    source,
  });
}));

// Một lệnh từ A–Z: tự chọn provider đầu tiên có tập, trả danh sách tập + stream của tập chỉ định
// /api/anime/16498/auto?ep=1
app.get("/api/anime/:id/auto", wrap(async (req, res) => {
  const meta = await upstream("/metadata", { id: req.params.id }, { ttl: TTL.metadata });
  const providers = Object.keys(meta?.result?.streamingProviders || {});
  const prefer = req.query.provider ? [req.query.provider] : providers;
  if (!prefer.length) throw new ApiError(404, "NO_PROVIDER", "Anime này chưa có provider streaming");

  for (const provider of prefer) {
    try {
      const list = await upstream("/stream/episodes", { id: req.params.id, provider, server: req.query.server }, { ttl: TTL.episodes });
      const eps = list.episodes || [];
      if (!eps.length) continue;
      const ep = eps.find(e => String(e.episodeNumber) === String(req.query.ep || eps[0].episodeNumber)) || eps[0];
      const source = await getSource(req, { episodeData: ep.episodeId, provider, server: req.query.server || ep.server, proxy: req.query.proxy });
      return res.json({ media: meta.result, provider, providers, episodes: eps, episode: ep, source });
    } catch (e) {
      if (e.status && e.status < 500 && e.status !== 404) throw e; // thử provider kế tiếp nếu 404/5xx
    }
  }
  throw new ApiError(404, "EPISODES_NOT_FOUND", "Không provider nào có tập phim");
}));

app.get("/health", (_, res) => res.json({ ok: true, upstream: UPSTREAM, cached: cache.size }));

app.get("/", (_, res) => res.json({
  name: "Anime Vietsub API",
  endpoints: [
    "GET /api/search?title=&mediaType=ANIME&limit=",
    "GET /api/autocomplete?title=",
    "GET /api/anime/:id",
    "GET /api/anime/:id/servers?provider=",
    "GET /api/anime/:id/episodes?provider=&limit=&offset=&server=",
    "GET /api/anime/:id/watch/:ep?provider=&server=",
    "GET /api/anime/:id/auto?ep=&provider=&server=",
    "GET /api/watch?episodeData=&provider=&server=",
    "GET /api/proxy?u=&h=",
  ],
}));

app.use((req, res) => res.status(404).json({ success: false, code: "NOT_FOUND", message: "Endpoint không tồn tại" }));

app.listen(PORT, () => console.log(`Anime API chạy tại http://localhost:${PORT}`));
