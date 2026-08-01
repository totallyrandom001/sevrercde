// ============================================================================
// SOPERT RELAY — deployed on Render.
// Browser (GitHub Pages) -> this relay -> whatever Cloudflare quick-tunnel
// URL the VAIO last registered -> VAIO's local server.
//
// Env vars required on Render:
//   RELAY_SECRET     shared secret the VAIO uses to register its tunnel URL
//   ALLOWED_ORIGIN    (optional) defaults to https://totallyrandom001.github.io
// ============================================================================

const express  = require("express");
const https    = require("https");
const http     = require("http");
const rateLimit = require("express-rate-limit");
const { createProxyMiddleware } = require("http-proxy-middleware");

const PORT           = process.env.PORT || 10000;
const RELAY_SECRET   = process.env.RELAY_SECRET;
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || "https://totallyrandom001.github.io";
const STALE_MS       = 6 * 60 * 1000; // if VAIO hasn't re-registered in 6 min, treat as offline

if (!RELAY_SECRET) {
  console.error("FATAL: RELAY_SECRET env var must be set on Render.");
  process.exit(1);
}

let tunnelUrl      = null;
let lastRegistered = 0;

const app = express();
app.set("trust proxy", 1);

// ---------------------------------------------------------------------------
// CORS — set on every response (including errors) so the browser always
// gets a readable response instead of an opaque CORS failure.
// ---------------------------------------------------------------------------
function setCors(req, res) {
  const origin = req.headers.origin;
  if (origin === ALLOWED_ORIGIN || origin === "http://localhost:3000" || origin === "http://127.0.0.1:3000") {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Vary", "Origin");
  }
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
}

app.use((req, res, next) => {
  setCors(req, res);
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
  tunnelUrl      = url.replace(/\/$/, "");
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
  legacyHeaders:   false,
  message: { error: "Çok fazla istek, yavaşlayın." },
}));

// ---------------------------------------------------------------------------
// Refuse to proxy if we don't have a live tunnel
// ---------------------------------------------------------------------------
function requireTunnel(req, res, next) {
  if (!tunnelUrl || Date.now() - lastRegistered > STALE_MS) {
    return res.status(503).json({ error: "VAIO şu anda bağlı değil" });
  }
  next();
}

// ---------------------------------------------------------------------------
// SSE route — proxied manually so we can flush every chunk immediately.
//
// http-proxy-middleware buffers response bodies before forwarding them.
// For SSE this is fatal: frames accumulate in the proxy's buffer, the browser
// never receives them, Render's LB sees an idle connection and kills it.
// The fix is to bypass the proxy middleware entirely for SSE requests and
// pipe the VAIO response chunk-by-chunk with an explicit flush after each one.
// ---------------------------------------------------------------------------
app.get("/api/stream", requireTunnel, (req, res) => {
  // FIX: previously this was built with a fragile string-slice on req.url
  // ("/api/stream" + req.url.slice(req.url.indexOf("?") - 1)) which was off
  // by one character and produced "/api/streamm?token=..." — a path that
  // does not exist on the VAIO. Every SSE request 404'd silently, so the
  // stream "connected" but never delivered a single frame (no I-FRAME, no
  // pings, no P-FRAMEs). Building the URL properly with the URL API avoids
  // this class of bug entirely.
  const target = new URL("/api/stream", tunnelUrl);
  target.search = new URLSearchParams({ token: req.query.token || "" }).toString();

  const transport = target.protocol === "https:" ? https : http;

  // Forward all original headers except host (replaced by the tunnel host).
  const proxyHeaders = { ...req.headers, host: target.hostname };

  console.log(`[relay] SSE proxy → ${target.href}`);

  const proxyReq = transport.request(
    {
      hostname: target.hostname,
      port:     target.port || (target.protocol === "https:" ? 443 : 80),
      path:     target.pathname + target.search,
      method:   "GET",
      headers:  proxyHeaders,
    },
    (proxyRes) => {
      // Build response headers: keep upstream headers, strip upstream CORS
      // (our middleware already set the correct CORS headers above), and
      // add explicit no-buffering directives for Render's infrastructure.
      const headers = { ...proxyRes.headers };
      delete headers["access-control-allow-origin"];
      delete headers["access-control-allow-methods"];
      delete headers["access-control-allow-headers"];
      delete headers["vary"];
      // Belt-and-suspenders: tell every caching/buffering layer to stand down.
      headers["cache-control"]      = "no-cache";
      headers["x-accel-buffering"]  = "no";  // nginx / Render infrastructure
      headers["connection"]         = "keep-alive";
      headers["content-type"]       = "text/event-stream; charset=utf-8";

      res.writeHead(proxyRes.statusCode, headers);

      // Forward each SSE chunk the moment it arrives and flush immediately.
      proxyRes.on("data", (chunk) => {
        if (res.writableEnded) return;
        try {
          res.write(chunk);
          // Flush through any buffering layer in the Render→browser path.
          if (typeof res.flush === "function") res.flush();
          if (res.socket && !res.socket.destroyed) res.socket.uncork?.();
        } catch (e) {
          console.error("[relay] SSE write error:", e.message);
          proxyReq.destroy();
        }
      });

      proxyRes.on("end", () => {
        if (!res.writableEnded) res.end();
      });

      proxyRes.on("error", (e) => {
        console.error("[relay] SSE upstream error:", e.message);
        if (!res.writableEnded) res.end();
      });
    },
  );

  proxyReq.on("error", (e) => {
    console.error("[relay] SSE proxy request error:", e.message);
    if (!res.headersSent) {
      setCors(req, res);
      res.status(502).json({ error: "VAIO bağlantısı başarısız" });
    } else if (!res.writableEnded) {
      res.end();
    }
  });

  // If the browser closes the tab / navigates away, tear down the upstream too.
  req.on("close", () => {
    console.log("[relay] SSE browser disconnected — aborting upstream");
    proxyReq.destroy();
  });

  proxyReq.end();
});

// ---------------------------------------------------------------------------
// Proxy everything else to the current tunnel URL (non-SSE routes)
// ---------------------------------------------------------------------------
app.use("/", requireTunnel, createProxyMiddleware({
  router: () => tunnelUrl,
  changeOrigin: true,
  ws: false,
  logLevel: "warn",
  onProxyRes: (proxyRes) => {
    // Strip upstream CORS headers so ours (set above) are the only ones the
    // browser sees.
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
