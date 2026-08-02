// ============================================================================
// SOPERT RELAY — deployed on Render.
// Browser (GitHub Pages) -> this relay -> the VAIO's Tailscale Funnel hostname
// (fixed, doesn't rotate) -> VAIO's local server.
//
// Env vars required on Render:
//   TUNNEL_URL        your Tailscale Funnel hostname, e.g.
//                      https://your-machine.your-tailnet.ts.net
//   RELAY_SECRET      shared secret, still used to auth the optional manual
//                      /register-tunnel override below (kept for convenience
//                      if you ever need to point at a different URL without
//                      redeploying, but TUNNEL_URL is the normal source of truth)
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

if (!RELAY_SECRET) {
  console.error("FATAL: RELAY_SECRET env var must be set on Render.");
  process.exit(1);
}
if (!process.env.TUNNEL_URL) {
  console.error("FATAL: TUNNEL_URL env var must be set on Render (your ngrok static domain).");
  process.exit(1);
}

// Fixed by default — ngrok's free static domain doesn't rotate, so there's
// no staleness/expiry concept anymore. /register-tunnel can still override
// this at runtime if you ever need to (e.g. temporarily testing a different
// tunnel), but nothing goes "stale" the way quick-tunnel URLs used to.
let tunnelUrl = process.env.TUNNEL_URL.replace(/\/$/, "");
console.log("[relay] using fixed tunnel ->", tunnelUrl);

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
// Manual override (optional) — normally unused now that TUNNEL_URL is fixed.
// Useful if you ever need to hot-swap the tunnel without a Render redeploy.
// ---------------------------------------------------------------------------
app.post("/register-tunnel", express.json(), (req, res) => {
  const { url, secret } = req.body || {};
  if (secret !== RELAY_SECRET) return res.status(403).json({ error: "bad secret" });
  if (!url || !/^https:\/\/[a-z0-9.-]+$/.test(url.replace(/\/$/, ""))) {
    return res.status(400).json({ error: "bad url" });
  }
  tunnelUrl = url.replace(/\/$/, "");
  console.log("[relay] tunnel manually overridden ->", tunnelUrl);
  res.json({ ok: true });
});

app.get("/relay-status", (req, res) => {
  // With a fixed ngrok static domain there's no registration heartbeat to
  // measure staleness against — this just reports what URL is configured.
  // Actual reachability is only known when a real proxied request succeeds
  // or fails (see the 502 handlers below).
  res.json({ connected: true, tunnelUrl });
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
// Refuse to proxy if we don't have a tunnel URL configured at all.
// Actual reachability failures (tunnel down, VAIO offline) surface as 502s
// from the proxy error handlers below, not from this check.
// ---------------------------------------------------------------------------
function requireTunnel(req, res, next) {
  if (!tunnelUrl) {
    return res.status(503).json({ error: "VAIO tüneli yapılandırılmamış" });
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
      // If the tunnel itself is down, ngrok/the network layer returns a
      // non-2xx status with an HTML error body instead of a real SSE stream.
      // Piping that through as if it were valid would leave the browser's
      // EventSource silently stuck (exactly what happened with Cloudflare's
      // error 530 earlier) — so bail out with a clean JSON error instead.
      if (proxyRes.statusCode < 200 || proxyRes.statusCode >= 300) {
        console.error(`[relay] SSE upstream returned non-2xx: ${proxyRes.statusCode}`);
        proxyRes.resume(); // drain and discard the error body
        setCors(req, res);
        res.status(502).json({ error: "VAIO tüneli şu anda ulaşılamıyor" });
        return;
      }

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

const httpServer = app.listen(PORT, () =>
  console.log(`[relay] listening on :${PORT}`)
);

// ── WebSocket bridge (/api/client-stream) ───────────────────────────────────
// http-proxy-middleware cannot proxy WebSocket connections in this config
// (ws: false, and Render's infrastructure strips the Upgrade header anyway).
// We bridge manually: accept the upgrade from the browser, open a matching WS
// to the VAIO tunnel, and pipe frames both ways.
const wssRelay = new WebSocketServer({ noServer: true });

httpServer.on('upgrade', (req, socket, head) => {
  if (!req.url?.startsWith('/api/client-stream')) {
    socket.write('HTTP/1.1 404 Not Found\r\n\r\n');
    socket.destroy();
    return;
  }

  // CORS check — same origins the HTTP middleware allows
  const origin = req.headers.origin;
  const allowed = [ALLOWED_ORIGIN, 'http://localhost:3000', 'http://127.0.0.1:3000'];
  if (!allowed.includes(origin)) {
    socket.write('HTTP/1.1 403 Forbidden\r\n\r\n');
    socket.destroy();
    return;
  }

  if (!tunnelUrl) {
    socket.write('HTTP/1.1 503 Service Unavailable\r\n\r\n');
    socket.destroy();
    return;
  }

  // Build the upstream WS URL
  const vaioWsUrl = tunnelUrl.replace(/^http/, 'ws') + req.url;
  console.log(`[relay] WS bridge → ${vaioWsUrl}`);

  const upstream = new WsClient(vaioWsUrl, {
    headers: {
      ...req.headers,
      host: new URL(tunnelUrl).hostname,
    },
  });

  wssRelay.handleUpgrade(req, socket, head, (clientWs) => {
    // Wait for upstream to open before completing the handshake with the browser
    upstream.on('open', () => {
      clientWs.on('message', (data, isBinary) => {
        if (upstream.readyState === WsClient.OPEN) upstream.send(data, { binary: isBinary });
      });
      upstream.on('message', (data, isBinary) => {
        if (clientWs.readyState === WsClient.OPEN) clientWs.send(data, { binary: isBinary });
      });

      const closeUpstream = (code, reason) => {
        if (upstream.readyState < WsClient.CLOSING) upstream.close(code, reason);
      };
      const closeClient = (code, reason) => {
        if (clientWs.readyState < WsClient.CLOSING) clientWs.close(code, reason);
      };

      clientWs.on('close', (code, reason) => closeUpstream(code, reason));
      upstream.on('close',  (code, reason) => closeClient(code, reason));
      clientWs.on('error',  () => closeUpstream(1011, 'Client error'));
      upstream.on('error',  (err) => {
        console.error('[relay] WS upstream error:', err.message);
        closeClient(1011, 'Upstream error');
      });
    });

    upstream.on('error', (err) => {
      console.error('[relay] WS upstream failed to open:', err.message);
      if (clientWs.readyState < WsClient.CLOSING) clientWs.close(1011, 'Upstream unreachable');
    });
  });
});
