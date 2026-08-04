// ============================================================================
// SOPERT RELAY — Render
// Chain: Frontend → Render (this) → ngrok → VAIO :3000
//
// Env vars to set in Render dashboard:
//   TUNNEL_URL     = https://latticed-hunting-causing.ngrok-free.dev
//   RELAY_SECRET   = g7as078sa0hga0af0w78s07gb0nns8907fgdga8a08gf90ag09
//   ALLOWED_ORIGIN = https://totallyrandom001.github.io  (unused now, kept for reference)
// ============================================================================

const express   = require("express");
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
// every response and they pass through the proxy untouched. Adding relay CORS
// headers on top caused duplicate Access-Control-Allow-Origin which browsers
// hard-reject.

// ── SSE: kill buffering so events stream through immediately ──────────────────
app.use((req, res, next) => {
  if (req.headers.accept === "text/event-stream") {
    res.setHeader("X-Accel-Buffering", "no");
    res.setHeader("Cache-Control", "no-cache");
    console.log(`[sse] stream detected: ${req.path}`);
  }
  next();
});

// ── Health check ──────────────────────────────────────────────────────────────
app.get("/relay-status", (_req, res) =>
  res.json({ ok: true, tunnel: TUNNEL_URL })
);

// ── Rate limit ────────────────────────────────────────────────────────────────
app.use(rateLimit({ windowMs: 60_000, max: 200 }));

// ── Proxy → ngrok → VAIO ─────────────────────────────────────────────────────
const proxy = createProxyMiddleware({
  target: TUNNEL_URL,
  changeOrigin: true,
  ws: true,
  headers: {
    "ngrok-skip-browser-warning": "true",   // skip ngrok interstitial
    "x-relay-secret": RELAY_SECRET,         // VAIO auth check
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

// WebSocket upgrades must be wired here
server.on("upgrade", proxy.upgrade);
