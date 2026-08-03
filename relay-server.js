// ============================================================================
// SOPERT RELAY — deployed on Render.
// ============================================================================

const express   = require("express");
const https     = require("https");
const http      = require("http");
const tls       = require("tls");
const net       = require("net");
const dns       = require("dns");
const rateLimit = require("express-rate-limit");
const { createProxyMiddleware }      = require("http-proxy-middleware");
const { WebSocketServer, WebSocket: WsClient } = require("ws");

// Force IPv4 first to bypass Render's lack of IPv6 egress routes
if (typeof dns.setDefaultResultOrder === "function") {
  dns.setDefaultResultOrder("ipv4first");
  console.log("[relay] DNS result order forced to ipv4first");
}

if (!process.env.RELAY_SECRET || !process.env.TUNNEL_URL) {
  console.error("FATAL: RELAY_SECRET and TUNNEL_URL env vars must be set on Render.");
  process.exit(1);
}

let tunnelUrl = process.env.TUNNEL_URL.replace(/\/$/, "");
const TUNNEL_HOSTNAME = new URL(tunnelUrl).hostname;
console.log("[relay] Using fixed tunnel ->", tunnelUrl);

// Base known IPs + dynamic discovery pool
const KNOWN_FUNNEL_IPS = ["176.58.88.108", "176.58.88.82", "176.58.92.199"];
let currentProbeIps = [...KNOWN_FUNNEL_IPS];

const TLS_COMBOS = [
  { label: "TLS1.3-only, ALPN h1", minVersion: "TLSv1.3", maxVersion: "TLSv1.3", ALPNProtocols: ["http/1.1"] },
  { label: "TLS1.2-only, ALPN h1", minVersion: "TLSv1.2", maxVersion: "TLSv1.2", ALPNProtocols: ["http/1.1"] },
  { label: "TLS1.2-1.3, ALPN h1",  minVersion: "TLSv1.2", maxVersion: "TLSv1.3", ALPNProtocols: ["http/1.1"] },
  { label: "TLS1.2-only, no ALPN", minVersion: "TLSv1.2", maxVersion: "TLSv1.2", ALPNProtocols: undefined },
  { label: "TLS1.3-only, no ALPN", minVersion: "TLSv1.3", maxVersion: "TLSv1.3", ALPNProtocols: undefined },
  { label: "default negotiation",  minVersion: undefined, maxVersion: undefined, ALPNProtocols: undefined },
];

let bestConnection  = null;
let probeInProgress = false;

// DNS Patch to enforce our discovered/working IP globally across the Node process
const _realDnsLookup = dns.lookup.bind(dns);
dns.lookup = function patchedLookup(hostname, options, callback) {
  if (typeof options === "function") { callback = options; options = {}; }
  options = options || {};

  if (hostname === TUNNEL_HOSTNAME && bestConnection) {
    if (options.all) return callback(null, [{ address: bestConnection.ip, family: 4 }]);
    return callback(null, bestConnection.ip, 4);
  }
  return _realDnsLookup(hostname, options, callback);
};

function probeOnce(ip, combo, cb) {
  let done = false;
  const socket = tls.connect({
    host: ip,
    port: 443,
    servername: TUNNEL_HOSTNAME,
    minVersion: combo.minVersion,
    maxVersion: combo.maxVersion,
    ALPNProtocols: combo.ALPNProtocols,
    timeout: 6000,
  });

  const finish = (ok, err) => {
    if (done) return;
    done = true;
    try { socket.destroy(); } catch {}
    cb(ok, err);
  };

  socket.once("secureConnect", () => {
    socket.write(`GET / HTTP/1.1\r\nHost: ${TUNNEL_HOSTNAME}\r\nConnection: close\r\n\r\n`);
    let body = "";
    socket.on("data", (chunk) => { body += chunk.toString(); });
    socket.on("end", () => finish(body.length > 0, null));
  });

  socket.once("timeout", () => finish(false, "TIMEOUT"));
  socket.once("error",   (err) => finish(false, err.code || err.message));
}

function executeProbes(onComplete) {
  const attempts = [];
  for (const ip of currentProbeIps) {
    for (const combo of TLS_COMBOS) attempts.push({ ip, combo });
  }

  let i = 0;
  function next() {
    if (i >= attempts.length) {
      probeInProgress = false;
      console.error("[relay] ALL connection combinations failed.");
      return onComplete(null);
    }
    const { ip, combo } = attempts[i++];
    probeOnce(ip, combo, (ok, err) => {
      if (ok) {
        console.log(`[relay] WORKING combination found: ip=${ip} tls="${combo.label}"`);
        bestConnection = { ip, combo };
        probeInProgress = false;
        return onComplete(bestConnection);
      }
      next();
    });
  }
  next();
}

function probeAllCombinations(onComplete) {
  if (probeInProgress) return;
  probeInProgress = true;

  // [FIX] Dynamically fetch Tailscale's CURRENT IPs to prevent hardcoded IPs from rotting
  dns.resolve4(TUNNEL_HOSTNAME, (err, addrs) => {
    if (!err && addrs && addrs.length > 0) {
      const uniqueIps = new Set([...addrs, ...KNOWN_FUNNEL_IPS]);
      currentProbeIps = Array.from(uniqueIps);
      console.log(`[relay] Target IP pool updated via DNS: ${currentProbeIps.join(", ")}`);
    } else {
      console.warn(`[relay] DNS resolve4 failed, falling back to hardcoded IPs.`);
    }
    executeProbes(onComplete);
  });
}

probeAllCombinations(() => {});

class AdaptiveHttpsAgent extends https.Agent {
  createConnection(options, callback) {
    const chosen = bestConnection || { ip: currentProbeIps[0], combo: TLS_COMBOS[0] };
    const socket = tls.connect({
      ...options,
      host: chosen.ip,
      servername: TUNNEL_HOSTNAME,
      minVersion: chosen.combo.minVersion,
      maxVersion: chosen.combo.maxVersion,
      ALPNProtocols: chosen.combo.ALPNProtocols,
    });
    socket.once("error", () => { if (!probeInProgress) probeAllCombinations(() => {}); });
    if (callback) socket.once("secureConnect", () => callback(null, socket));
    return socket;
  }
}
const adaptiveAgent = new AdaptiveHttpsAgent({ keepAlive: true, maxSockets: 20 });

const PORT           = process.env.PORT || 10000;
const RELAY_SECRET   = process.env.RELAY_SECRET;
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || "https://totallyrandom001.github.io";

const app = express();
app.set("trust proxy", 1);

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

// Manual tunnel override
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

app.get("/relay-status", (req, res) => res.json({ connected: true, tunnelUrl }));

// Rate limiter for standard HTTP endpoints
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
// SSE Route
// ---------------------------------------------------------------------------
app.get("/api/stream", requireTunnel, (req, res) => {
  const target = new URL("/api/stream", tunnelUrl);
  target.search = new URLSearchParams({ token: req.query.token || "" }).toString();

  const transport = target.protocol === "https:" ? https : http;
  const proxyHeaders = { ...req.headers, host: target.hostname };

  const proxyReq = transport.request(
    {
      hostname: target.hostname,
      port:     target.port || (target.protocol === "https:" ? 443 : 80),
      path:     target.pathname + target.search,
      method:   "GET",
      headers:  proxyHeaders,
      family:   4,
      agent:    target.protocol === "https:" ? adaptiveAgent : undefined,
    },
    (proxyRes) => {
      if (proxyRes.statusCode < 200 || proxyRes.statusCode >= 300) {
        proxyRes.resume();
        setCors(req, res);
        return res.status(502).json({ error: "VAIO tüneli şu anda ulaşılamıyor" });
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
    }
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

  req.on("close", () => proxyReq.destroy());
  proxyReq.end();
});

// ---------------------------------------------------------------------------
// Proxy General Routes
// ---------------------------------------------------------------------------
app.use("/", requireTunnel, createProxyMiddleware({
  router:      () => tunnelUrl,
  changeOrigin: true,
  ws:           false, // We handle WS manually below
  logLevel:     "warn",
  agent:        adaptiveAgent,
  onProxyRes: (proxyRes) => {
    delete proxyRes.headers["access-control-allow-origin"];
    delete proxyRes.headers["access-control-allow-methods"];
    delete proxyRes.headers["access-control-allow-headers"];
    delete proxyRes.headers["vary"];
  },
  onError: (err, req, res) => {
    console.error("[relay] proxy error:", err.code || err.message);
    if (!probeInProgress) probeAllCombinations(() => {});
    if (!res.headersSent) res.status(502).json({ error: "VAIO bağlantısı başarısız" });
  },
}));

// Boot HTTP
const httpServer = app.listen(PORT, () => console.log(`[relay] listening on :${PORT}`));

// ---------------------------------------------------------------------------
// WebSocket Bridge [FIXED: Added Adaptive Agent]
// ---------------------------------------------------------------------------
const wssRelay = new WebSocketServer({ noServer: true });

httpServer.on("upgrade", (req, socket, head) => {
  if (!req.url?.startsWith("/api/client-stream")) {
    socket.write("HTTP/1.1 404 Not Found\r\n\r\n");
    return socket.destroy();
  }

  const origin = req.headers.origin;
  if (!ALLOWED_ORIGINS.has(origin)) {
    socket.write("HTTP/1.1 403 Forbidden\r\n\r\n");
    return socket.destroy();
  }

  if (!tunnelUrl) {
    socket.write("HTTP/1.1 503 Service Unavailable\r\n\r\n");
    return socket.destroy();
  }

  const vaioWsUrl = tunnelUrl.replace(/^http/, "ws") + req.url;
  
  // [FIX] Apply the exact same adaptive TLS agent to WebSockets to bypass the Render drop
  const upstream = new WsClient(vaioWsUrl, {
    headers: { ...req.headers, host: new URL(tunnelUrl).hostname },
    agent: vaioWsUrl.startsWith("wss:") ? adaptiveAgent : undefined,
  });

  wssRelay.handleUpgrade(req, socket, head, (clientWs) => {
    const clientQueue = [];

    clientWs.on("message", (data, isBinary) => {
      if (upstream.readyState === WsClient.OPEN) {
        upstream.send(data, { binary: isBinary });
      } else {
        clientQueue.push({ data, isBinary });
      }
    });

    upstream.on("open", () => {
      for (const { data, isBinary } of clientQueue) {
        upstream.send(data, { binary: isBinary });
      }
      clientQueue.length = 0;

      upstream.on("message", (data, isBinary) => {
        if (clientWs.readyState === WsClient.OPEN) {
          clientWs.send(data, { binary: isBinary });
        }
      });
    });

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
      if (!probeInProgress) probeAllCombinations(() => {});
    });
  });
});
