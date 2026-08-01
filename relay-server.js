// ============================================================================
// SOPERT RELAY — deployed on Render.
// Browser (GitHub Pages) -> this relay -> whatever Cloudflare quick-tunnel
// URL the VAIO last registered -> VAIO's local server.
//
// Env vars required on Render:
//   RELAY_SECRET     shared secret the VAIO uses to register its tunnel URL
//   ALLOWED_ORIGIN    (optional) defaults to https://totallyrandom001.github.io
// ============================================================================

const express = require("express");
const rateLimit = require("express-rate-limit");
const { createProxyMiddleware } = require("http-proxy-middleware");

const PORT          = process.env.PORT || 10000;
const RELAY_SECRET  = process.env.RELAY_SECRET;
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || "https://totallyrandom001.github.io";
const STALE_MS       = 6 * 60 * 1000; // if VAIO hasn't re-registered in 6 min, treat as offline

if (!RELAY_SECRET) {
  console.error("FATAL: RELAY_SECRET env var must be set on Render.");
  process.exit(1);
}

let tunnelUrl = null;
let lastRegistered = 0;

const app = express();
app.set("trust proxy", 1);

// ---------------------------------------------------------------------------
// CORS — set on every response (including errors) so the browser always
// gets a readable response instead of an opaque CORS failure.
// ---------------------------------------------------------------------------
app.use((req, res, next) => {
  const origin = req.headers.origin;
  if (origin === ALLOWED_ORIGIN || origin === "http://localhost:3000" || origin === "http://127.0.0.1:3000") {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Vary", "Origin");
  }
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

// ---------------------------------------------------------------------------
// Tunnel registration (called by run.js on the VAIO every time it (re)starts,
// and every 4 minutes while running)
// ---------------------------------------------------------------------------
app.post("/register-tunnel", express.json(), (req, res) => {
  const { url, secret } = req.body || {};
  if (secret !== RELAY_SECRET) return res.status(403).json({ error: "bad secret" });
  if (!url || !/^https:\/\/[a-z0-9-]+\.trycloudflare\.com\/?$/.test(url)) {
    return res.status(400).json({ error: "bad url" });
  }
  tunnelUrl = url.replace(/\/$/, "");
  lastRegistered = Date.now();
  console.log("[relay] tunnel registered ->", tunnelUrl);
  res.json({ ok: true });
});

app.get("/relay-status", (req, res) => {
  const connected = !!tunnelUrl && Date.now() - lastRegistered < STALE_MS;
  res.json({
    connected,
    lastRegisteredSecondsAgo: tunnelUrl ? Math.floor((Date.now() - lastRegistered) / 1000) : null,
  });
});

// ---------------------------------------------------------------------------
// Rate limit everything below this point
// ---------------------------------------------------------------------------
app.use(rateLimit({
  windowMs: 60 * 1000,
  max: 120,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: "Çok fazla istek, yavaşlayın." },
}));

// ---------------------------------------------------------------------------
// Refuse to proxy if we don't have a live tunnel
// ---------------------------------------------------------------------------
app.use((req, res, next) => {
  if (!tunnelUrl || Date.now() - lastRegistered > STALE_MS) {
    return res.status(503).json({ error: "VAIO şu anda bağlı değil" });
  }
  next();
});

// ---------------------------------------------------------------------------
// Proxy everything else to the current tunnel URL
// ---------------------------------------------------------------------------
app.use("/", createProxyMiddleware({
  router: () => tunnelUrl,
  changeOrigin: true,
  ws: false,
  logLevel: "warn",
  onProxyRes: (proxyRes) => {
    // strip upstream CORS headers so ours (set above) are the only ones the browser sees
    delete proxyRes.headers["access-control-allow-origin"];
    delete proxyRes.headers["access-control-allow-methods"];
    delete proxyRes.headers["access-control-allow-headers"];
    delete proxyRes.headers["vary"];
  },
  onError: (err, req, res) => {
    console.error("[relay] proxy error:", err.message);
    if (!res.headersSent) res.status(502).json({ error: "VAIO bağlantısı başarısız" });
  },
}));

app.listen(PORT, () => console.log(`[relay] listening on :${PORT}`));
