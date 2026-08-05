// ============================================================================
// SOPERT RELAY — Render
// Chain: Frontend → Render (this) → localhost.run → VAIO :3000
//
// Env vars to set in Render dashboard:
//   TUNNEL_URL     = (auto-updated by VAIO tunnel monitor on each start)
//   RELAY_SECRET   = g7as078sa0hga0af0w78s07gb0nns8907fgdga8a08gf90ag09
// ============================================================================

const express   = require("express");
const https     = require("https");
const http      = require("http");
const rateLimit = require("express-rate-limit");
const { createProxyMiddleware } = require("http-proxy-middleware");

const TUNNEL_URL   = (process.env.TUNNEL_URL || "").replace(/\/$/, "");
const RELAY_SECRET = process.env.RELAY_SECRET;

if (!RELAY_SECRET || !TUNNEL_URL) {
  console.error("FATAL: Missing TUNNEL_URL or RELAY_SECRET environment variables.");
  process.exit(1);
}

const app = express();
app.set("trust proxy", 1);

// ── Log every incoming request ────────────────────────────────────────────────
app.use((req, res, next) => {
  console.log(`[relay] ${req.method} ${req.path} | origin: ${req.headers.origin || "none"}`);
  next();
});

// NOTE: No CORS middleware here — VAIO already sets its own CORS headers on
// every response and they pass through the proxy untouched.

// ── SSE: kill buffering so events stream through immediately ──────────────────
app.use((req, res, next) => {
  if (req.headers.accept === "text/event-stream") {
    res.setHeader("X-Accel-Buffering", "no");
    res.setHeader("Cache-Control", "no-cache");
    console.log(`[sse] stream detected: ${req.path}`);
  }
  next();
});

// ── Health / status ───────────────────────────────────────────────────────────
app.get("/relay-status", (_req, res) =>
  res.json({ ok: true, tunnel: TUNNEL_URL })
);

// ── Ping: fast VAIO reachability check, returns 999 instantly if tunnel down ──
app.get("/ping", (req, res) => {
  const url = `${TUNNEL_URL}/ping`;
  const lib = url.startsWith("https") ? https : http;

  const abort = setTimeout(() => {
    if (!res.headersSent) {
      console.warn("[ping] VAIO unreachable — timeout");
      res.status(999).json({ ok: false, error: "VAIO unreachable" });
    }
    request.destroy();
  }, 4000);

  const request = lib.get(url, {
    headers: { "x-relay-secret": RELAY_SECRET }
  }, (vaioRes) => {
    clearTimeout(abort);
    vaioRes.resume();
    if (res.headersSent) return;
    if (vaioRes.statusCode === 200) {
      res.status(200).json({ ok: true });
    } else {
      console.warn(`[ping] VAIO returned ${vaioRes.statusCode}`);
      res.status(200).json({ ok: true, informations: "VAIO unhealthy" });
    }
  });

  request.on("error", (e) => {
    clearTimeout(abort);
    if (res.headersSent) return;
    console.warn(`[ping] VAIO connection error: ${e.message}`);
    res.status(999).json({ ok: false, error: "VAIO unreachable" });
  });

  request.end();
});

// ── Rate limit ────────────────────────────────────────────────────────────────
app.use(rateLimit({ windowMs: 60_000, max: 200 }));

// ── Proxy → localhost.run → VAIO ──────────────────────────────────────────────
const proxy = createProxyMiddleware({
  target: TUNNEL_URL,
  changeOrigin: true,
  ws: true,
  headers: {
    "x-relay-secret": RELAY_SECRET,
  },
  on: {
    proxyReq: (proxyReq, req) => {
      console.log(`[proxy] → ${req.method} ${TUNNEL_URL}${req.path}`);
    },
    proxyRes: (proxyRes, req) => {
      console.log(`[proxy] ← ${proxyRes.statusCode} ${req.path}`);
      if (proxyRes.statusCode === 403)
        console.error(`[proxy] 403 from VAIO — is x-relay-secret correct?`);
    },
    error: (err, req, res) => {
      console.error(`[proxy] error on ${req.path}:`, err.message);
      if (res && !res.headersSent)
        res.status(502).json({ error: "Tunnel unreachable — is VAIO running?" });
    },
  },
});

app.use("/", proxy);

// ── Start ─────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 10000;
const server = app.listen(PORT, () =>
  console.log(`[relay] Listening on :${PORT} → ${TUNNEL_URL}`)
);

// ── WebSocket upgrades must be wired here ─────────────────────────────────────
server.on("upgrade", proxy.upgrade);
