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

const express   = require("express");
const https     = require("https");
const http      = require("http");
const rateLimit = require("express-rate-limit");
const { createProxyMiddleware }      = require("http-proxy-middleware");
const { WebSocketServer, WebSocket: WsClient } = require("ws");   // npm install ws

const PORT           = process.env.PORT || 10000;
const RELAY_SECRET   = process.env.RELAY_SECRET;
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || "https://totallyrandom001.github.io";

if (!RELAY_SECRET) {
  console.error("FATAL: RELAY_SECRET env var must be set on Render.");
  process.exit(1);
}
if (!process.env.TUNNEL_URL) {
  console.error("FATAL: TUNNEL_URL env var must be set on Render (your Tailscale Funnel hostname).");
  process.exit(1);
}

let tunnelUrl = process.env.TUNNEL_URL.replace(/\/$/, "");
console.log("[relay] using fixed tunnel ->", tunnelUrl);

const app = express();
app.set("trust proxy", 1);

// ---------------------------------------------------------------------------
// CORS — set on every response (including errors) so the browser always
// gets a readable response instead of an opaque CORS failure.
// ---------------------------------------------------------------------------
const ALLOWED_ORIGINS = new Set([
  ALLOWED_ORIGIN,
  "http://localhost:3000",
  "http://127.0.0.1:3000",
]);

function setCors(req, res) {
  const origin = req.headers.origin;
  if (ALLOWED_ORIGINS.has(origin)) {
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
// Manual tunnel override (optional) — hot-swap without a Render redeploy.
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
  res.json({ connected: true, tunnelUrl });
});

// ---------------------------------------------------------------------------
// Rate limit everything below this point.
// Note: WebSocket upgrades bypass Express middleware entirely (they go through
// httpServer's 'upgrade' event) so this only covers HTTP routes.
// ---------------------------------------------------------------------------
app.use(rateLimit({
  windowMs: 60 * 1000,
  max:      120,
  standardHeaders: true,
  legacyHeaders:   false,
  message: { error: "Çok fazla istek, yavaşlayın." },
}));

function requireTunnel(req, res, next) {
  if (!tunnelUrl) return res.status(503).json({ error: "VAIO tüneli yapılandırılmamış" });
  next();
}

// ---------------------------------------------------------------------------
// SSE route — proxied manually so we can flush every chunk immediately.
//
// http-proxy-middleware buffers response bodies before forwarding them.
// For SSE this is fatal: frames accumulate in the proxy's buffer, the browser
// never receives them, and Render's LB sees an idle connection and kills it.
// ---------------------------------------------------------------------------
app.get("/api/stream", requireTunnel, (req, res) => {
  const target = new URL("/api/stream", tunnelUrl);
  target.search = new URLSearchParams({ token: req.query.token || "" }).toString();

  const transport = target.protocol === "https:" ? https : http;
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
      if (proxyRes.statusCode < 200 || proxyRes.statusCode >= 300) {
        console.error(`[relay] SSE upstream returned non-2xx: ${proxyRes.statusCode}`);
        proxyRes.resume();
        setCors(req, res);
        res.status(502).json({ error: "VAIO tüneli şu anda ulaşılamıyor" });
        return;
      }

      const headers = { ...proxyRes.headers };
      delete headers["access-control-allow-origin"];
      delete headers["access-control-allow-methods"];
      delete headers["access-control-allow-headers"];
      delete headers["vary"];
      headers["cache-control"]     = "no-cache";
      headers["x-accel-buffering"] = "no";
      headers["connection"]        = "keep-alive";
      headers["content-type"]      = "text/event-stream; charset=utf-8";

      res.writeHead(proxyRes.statusCode, headers);

      proxyRes.on("data", (chunk) => {
        if (res.writableEnded) return;
        try {
          res.write(chunk);
          if (typeof res.flush === "function") res.flush();
          if (res.socket && !res.socket.destroyed) res.socket.uncork?.();
        } catch (e) {
          console.error("[relay] SSE write error:", e.message);
          proxyReq.destroy();
        }
      });

      proxyRes.on("end",   () => { if (!res.writableEnded) res.end(); });
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

  req.on("close", () => {
    console.log("[relay] SSE browser disconnected — aborting upstream");
    proxyReq.destroy();
  });

  proxyReq.end();
});

// ---------------------------------------------------------------------------
// Proxy everything else to the tunnel (non-SSE HTTP routes).
// ---------------------------------------------------------------------------
app.use("/", requireTunnel, createProxyMiddleware({
  router:      () => tunnelUrl,
  changeOrigin: true,
  ws:           false,
  logLevel:     "warn",
  onProxyRes: (proxyRes) => {
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

// ---------------------------------------------------------------------------
// Boot
// ---------------------------------------------------------------------------
const httpServer = app.listen(PORT, () =>
  console.log(`[relay] listening on :${PORT}`)
);

// ---------------------------------------------------------------------------
// WebSocket bridge (/api/client-stream)
//
// http-proxy-middleware cannot proxy WebSocket upgrades in this config
// (ws: false, and Render's infra strips the Upgrade header anyway).
// We bridge manually: accept the upgrade from the browser, open a WS to the
// VAIO tunnel, and pipe frames both ways.
//
// KEY DESIGN POINT — buffering client messages before upstream opens:
//   handleUpgrade() completes the browser-side handshake synchronously,
//   which fires ws.onopen on the ClientStream immediately. The client sends
//   its I-FRAME auth message right then. But the upstream WS connection to
//   the VAIO is still being established (async TCP + TLS handshake). If we
//   only wire up clientWs.on('message') inside upstream.on('open'), that
//   I-FRAME arrives in the gap and is silently dropped — the VAIO never sees
//   it, never sends I-FRAME-ACK, and the auth timeout fires 8 s later.
//   Fix: buffer all client messages received before the upstream opens, then
//   flush them in order the moment the upstream connection is ready.
// ---------------------------------------------------------------------------
const wssRelay = new WebSocketServer({ noServer: true });

httpServer.on("upgrade", (req, socket, head) => {
  if (!req.url?.startsWith("/api/client-stream")) {
    socket.write("HTTP/1.1 404 Not Found\r\n\r\n");
    socket.destroy();
    return;
  }

  const origin = req.headers.origin;
  if (!ALLOWED_ORIGINS.has(origin)) {
    socket.write("HTTP/1.1 403 Forbidden\r\n\r\n");
    socket.destroy();
    return;
  }

  if (!tunnelUrl) {
    socket.write("HTTP/1.1 503 Service Unavailable\r\n\r\n");
    socket.destroy();
    return;
  }

  const vaioWsUrl = tunnelUrl.replace(/^http/, "ws") + req.url;
  console.log(`[relay] WS bridge → ${vaioWsUrl}`);

  const upstream = new WsClient(vaioWsUrl, {
    headers: { ...req.headers, host: new URL(tunnelUrl).hostname },
  });

  wssRelay.handleUpgrade(req, socket, head, (clientWs) => {
    // Buffer for messages that arrive from the client before the upstream
    // WS connection is open. Typically just the I-FRAME auth message.
    const clientQueue = [];

    // Wire up the client message handler immediately so no messages are missed.
    clientWs.on("message", (data, isBinary) => {
      if (upstream.readyState === WsClient.OPEN) {
        upstream.send(data, { binary: isBinary });
      } else {
        // Upstream still connecting — queue for ordered flush on open.
        clientQueue.push({ data, isBinary });
      }
    });

    upstream.on("open", () => {
      // Flush any messages that arrived before the upstream was ready.
      for (const { data, isBinary } of clientQueue) {
        upstream.send(data, { binary: isBinary });
      }
      clientQueue.length = 0;

      // Now wire up the upstream→client direction.
      upstream.on("message", (data, isBinary) => {
        if (clientWs.readyState === WsClient.OPEN) {
          clientWs.send(data, { binary: isBinary });
        }
      });
    });

    // ── teardown helpers ──────────────────────────────────────────────────
    function closeUpstream(code, reason) {
      if (upstream.readyState < WsClient.CLOSING) upstream.close(code, reason);
    }
    function closeClient(code, reason) {
      if (clientWs.readyState < WsClient.CLOSING) clientWs.close(code, reason);
    }

    clientWs.on("close", (code, reason) => closeUpstream(code, reason));
    upstream.on("close",  (code, reason) => closeClient(code, reason));

    clientWs.on("error", (err) => {
      console.error("[relay] WS client error:", err.message);
      closeUpstream(1011, "Client error");
    });
    upstream.on("error", (err) => {
      console.error("[relay] WS upstream error:", err.message);
      closeClient(1011, "Upstream error");
    });

    // If the upstream never opens (e.g. VAIO is down), close the client cleanly.
    // This is handled by upstream.on('error') above — the 'error' event fires
    // before 'close' when the connection is refused, so closeClient() runs there.
  });
});
