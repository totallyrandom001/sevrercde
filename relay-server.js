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
const dns       = require("dns");           // [DEBUG] for diagnostic endpoint
const rateLimit = require("express-rate-limit");
const { createProxyMiddleware }      = require("http-proxy-middleware");
const { WebSocketServer, WebSocket: WsClient } = require("ws");   // npm install ws

// ============================================================================
// [FIX] ENETUNREACH root cause: the tunnel hostname resolves to both IPv4 and
// IPv6 addresses, and Render's outbound network does not route IPv6 traffic.
// Node's default DNS behavior can return/prefer the IPv6 address first, so
// http-proxy-middleware (and any raw https.request) ends up trying to dial
// an address with literally no outbound route -> ENETUNREACH.
//
// Forcing ipv4first here makes dns.lookup() -- which is what Node's http/https
// modules use internally to resolve a hostname before connecting -- always
// return IPv4 addresses ahead of IPv6 ones, so every outbound connection
// (the manual SSE request, the WS bridge, and http-proxy-middleware's
// internal requests) picks an address Render can actually route to.
// ============================================================================
if (typeof dns.setDefaultResultOrder === "function") {
  dns.setDefaultResultOrder("ipv4first");
  console.log("[relay] DNS result order forced to ipv4first (fix for ENETUNREACH on IPv6-less egress)");
} else {
  console.warn("[relay] dns.setDefaultResultOrder unavailable on this Node version — IPv6 ENETUNREACH risk remains");
}

// ============================================================================
// [FIX] ENOTFOUND root cause, confirmed via /debug-connectivity: Render's own
// DNS infrastructure cannot resolve *.ts.net hostnames at all — dns.lookup,
// dns.resolve4, AND dns.resolve6 all fail identically and instantly (2ms,
// meaning it never even reaches the network — it's a resolver-level dead
// end on Render's side specifically). Public checkers (whatsmydns.net)
// confirm the hostname resolves consistently worldwide to a fixed set of
// IPs, so the record is fine — Render's resolver just can't/won't reach
// Tailscale's authoritative nameservers for this TLD.
//
// Workaround: bypass DNS resolution for this one hostname entirely by
// monkey-patching dns.lookup to return a hardcoded, known-good IP whenever
// it's asked to resolve the tunnel hostname specifically. Everything else
// (any other hostname) still goes through the real resolver untouched.
//
// This is safe for TLS: we are NOT skipping certificate validation — we're
// only substituting which IP the TCP connection dials. The TLS handshake
// still sends the real hostname via SNI (Node does this automatically based
// on the `host`/`servername` used in the request, not the resolved IP), so
// the certificate Tailscale Funnel presents is still checked against
// "desktop-hbhqdgt.tail966c62.ts.net" as normal, and the Host header is
// still sent correctly for Funnel's own internal routing.
//
// CAVEAT: if Tailscale ever rotates the Funnel edge IPs, this hardcoded list
// needs updating. Re-check via whatsmydns.net (#A/<your-hostname>) if this
// workaround suddenly stops working after previously succeeding.
// ============================================================================
const TUNNEL_HOSTNAME = new URL(process.env.TUNNEL_URL.replace(/\/$/, "")).hostname;
const KNOWN_FUNNEL_IPS = ["176.58.88.108", "176.58.88.82", "176.58.92.199"];
let _funnelIpRotation = 0;

const _realDnsLookup = dns.lookup.bind(dns);
dns.lookup = function patchedLookup(hostname, options, callback) {
  // Normalize the (hostname, options, callback) vs (hostname, callback) overload.
  if (typeof options === "function") {
    callback = options;
    options = {};
  }
  options = options || {};

  if (hostname === TUNNEL_HOSTNAME) {
    const ip = KNOWN_FUNNEL_IPS[_funnelIpRotation % KNOWN_FUNNEL_IPS.length];
    _funnelIpRotation++;
    console.log(`[relay] DNS bypass — serving hardcoded IP for ${hostname} -> ${ip}`);
    if (options.all) {
      return callback(null, KNOWN_FUNNEL_IPS.map((addr) => ({ address: addr, family: 4 })));
    }
    return callback(null, ip, 4);
  }

  return _realDnsLookup(hostname, options, callback);
};
console.log(`[relay] DNS bypass installed for ${TUNNEL_HOSTNAME} -> [${KNOWN_FUNNEL_IPS.join(", ")}]`);

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
// [DEBUG] Diagnostic endpoint — no shell access on Render's free tier, so
// this lets us see EXACTLY what Node/Render's network sees when it tries to
// resolve and reach the tunnel host, without guessing from outside anymore.
//
// GET /debug-connectivity
// Returns: DNS lookup result (or the exact error code), plus the result of
// an actual HTTPS request to the tunnel's health check route ("/").
//
// This is deliberately unauthenticated read-only diagnostic info (hostname/IP
// only, no secrets) so it's safe to hit directly from a browser while
// debugging. Remove or protect behind RELAY_SECRET once the issue is found.
// ---------------------------------------------------------------------------
app.get("/debug-connectivity", (req, res) => {
  const target = new URL(tunnelUrl);
  const report = {
    tunnelUrl,
    hostname: target.hostname,
    nodeVersion: process.version,
    timestamp: new Date().toISOString(),
    dnsLookup: null,
    dnsResolve4: null,
    dnsResolve6: null,
    httpsRequest: null,
  };

  const finish = () => res.json(report);

  // Step 1: dns.lookup (uses OS resolver — this is what most Node HTTP
  // clients use internally, including what http-proxy-middleware relies on).
  dns.lookup(target.hostname, { all: true }, (err, addresses) => {
    report.dnsLookup = err
      ? { error: err.code || err.message }
      : { addresses };

    // Step 2: dns.resolve4 (uses the DNS protocol directly, bypassing the
    // OS's /etc/hosts and other resolver quirks — narrows down whether this
    // is an OS-level resolver issue vs a network-level block).
    dns.resolve4(target.hostname, (err4, addrs4) => {
      report.dnsResolve4 = err4 ? { error: err4.code || err4.message } : { addresses: addrs4 };

      dns.resolve6(target.hostname, (err6, addrs6) => {
        report.dnsResolve6 = err6 ? { error: err6.code || err6.message } : { addresses: addrs6 };

        // Step 3: actual HTTPS request to the tunnel's health check.
        const healthUrl = new URL("/", tunnelUrl);
        const startedAt = Date.now();
        const request = https.get(healthUrl, { timeout: 8000 }, (upstreamRes) => {
          let body = "";
          upstreamRes.on("data", (chunk) => { body += chunk; });
          upstreamRes.on("end", () => {
            report.httpsRequest = {
              statusCode: upstreamRes.statusCode,
              bodySnippet: body.slice(0, 200),
              tookMs: Date.now() - startedAt,
            };
            finish();
          });
        });
        request.on("timeout", () => {
          request.destroy();
          report.httpsRequest = { error: "TIMEOUT", tookMs: Date.now() - startedAt };
          finish();
        });
        request.on("error", (err) => {
          report.httpsRequest = {
            error: err.code || err.message,
            tookMs: Date.now() - startedAt,
          };
          finish();
        });
      });
    });
  });
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
      family:   4, // [FIX] explicit IPv4-only, in addition to the global dns.setDefaultResultOrder fix above
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
    });
  });
});
