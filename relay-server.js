// ============================================================================
// SOPERT RELAY — Render
// Chain: Frontend → Render (this) → ngrok → VAIO :3000
//
// Env vars to set in Render dashboard:
//   TUNNEL_URL     = https://latticed-hunting-causing.ngrok-free.dev
//   RELAY_SECRET   = (your secret)
//   ALLOWED_ORIGIN = https://totallyrandom001.github.io
// ============================================================================

const express    = require("express");
const rateLimit  = require("express-rate-limit");
const { createProxyMiddleware } = require("http-proxy-middleware");

const TUNNEL_URL     = (process.env.TUNNEL_URL || "").replace(/\/$/, "");
const RELAY_SECRET   = process.env.RELAY_SECRET;
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || "https://totallyrandom001.github.io";

if (!RELAY_SECRET || !TUNNEL_URL) {
  console.error("FATAL: Missing TUNNEL_URL or RELAY_SECRET environment variables.");
  process.exit(1);
}

const app = express();
app.set("trust proxy", 1);

const ALLOWED_ORIGINS = new Set([
  ALLOWED_ORIGIN,
  "http://localhost:3000",
  "http://127.0.0.1:3000",
]);

// ── CORS ────────────────────────────────────────────────────────────────────
app.use((req, res, next) => {
  const origin = req.headers.origin;
  if (ALLOWED_ORIGINS.has(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Vary", "Origin");
  }
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

// ── SSE: kill buffering so events stream through immediately ─────────────────
app.use((req, res, next) => {
  if (req.headers.accept === "text/event-stream") {
    res.setHeader("X-Accel-Buffering", "no");
    res.setHeader("Cache-Control", "no-cache");
  }
  next();
});

// ── Health / status ──────────────────────────────────────────────────────────
app.get("/relay-status", (_req, res) =>
  res.json({ ok: true })
);

app.get("/ping", async (_req, res) => {
  try {
    const response = await fetch(TUNNEL_URL + "/ping");
    if (response.ok) return res.status(200).json({ ok: true });
    return res.status(999).json({ ok: false });
  } catch {
    return res.status(999).json({ ok: false });
  }
});

// ── Rate limit ───────────────────────────────────────────────────────────────
app.use(rateLimit({ windowMs: 60_000, max: 200 }));

// ── Proxy everything → ngrok tunnel ─────────────────────────────────────────
const proxy = createProxyMiddleware({
  target: TUNNEL_URL,
  changeOrigin: true,
  ws: true,                     // enable WebSocket proxying
  headers: {
    "ngrok-skip-browser-warning": "true",  // bypass ngrok interstitial page
  },
  on: {
    error: (err, req, res) => {
      console.error("[relay] proxy error:", err.message);
      if (res && !res.headersSent)
        res.status(999).json({ error: "Upstream unreachable." });
    },
  },
});

app.use("/", proxy);

// ── Start server ─────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 10000;
const server = app.listen(PORT, () =>
  console.log(`[relay] Listening on :${PORT}`)
);

// WebSocket upgrades MUST be wired here — middleware alone doesn't catch them
server.on("upgrade", proxy.upgrade);
