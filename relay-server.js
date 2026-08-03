// ============================================================================
// SOPERT RELAY — 10X Redundant Production Deployment (Render)
// ============================================================================

const express   = require("express");
const https     = require("https");
const http      = require("http");
const tls       = require("tls");
const dns       = require("dns");
const rateLimit = require("express-rate-limit");
const { createProxyMiddleware } = require("http-proxy-middleware");
const { WebSocketServer, WebSocket: WsClient } = require("ws");

if (typeof dns.setDefaultResultOrder === "function") {
  dns.setDefaultResultOrder("ipv4first");
}

const TUNNEL_URL = process.env.TUNNEL_URL ? process.env.TUNNEL_URL.replace(/\/$/, "") : "";
const RELAY_SECRET = process.env.RELAY_SECRET;
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || "https://totallyrandom001.github.io";

if (!RELAY_SECRET || !TUNNEL_URL) {
  console.error("FATAL: Missing mandatory environment variables (TUNNEL_URL or RELAY_SECRET).");
  process.exit(1);
}

const TUNNEL_HOSTNAME = new URL(TUNNEL_URL).hostname;
const KNOWN_IPS = ["176.58.88.108", "176.58.88.82", "176.58.92.199"];
let activeIpPool = [...KNOWN_IPS];
let bestConnection = null;
let isProbing = false;

// DNS Redundancy Injection
const originalLookup = dns.lookup.bind(dns);
dns.lookup = function (hostname, options, callback) {
  if (typeof options === "function") { callback = options; options = {}; }
  if (hostname === TUNNEL_HOSTNAME && bestConnection) {
    if (options.all) return callback(null, [{ address: bestConnection.ip, family: 4 }]);
    return callback(null, bestConnection.ip, 4);
  }
  return originalLookup(hostname, options, callback);
};

function testTlsConnection(ip, combo, callback) {
  let finished = false;
  const socket = tls.connect({
    host: ip,
    port: 443,
    servername: TUNNEL_HOSTNAME,
    minVersion: combo.minVersion,
    maxVersion: combo.maxVersion,
    ALPNProtocols: combo.ALPNProtocols,
    timeout: 5000,
  });

  const done = (success, err) => {
    if (finished) return;
    finished = true;
    try { socket.destroy(); } catch {}
    callback(success, err);
  };

  socket.once("secureConnect", () => {
    socket.write(`GET / HTTP/1.1\r\nHost: ${TUNNEL_HOSTNAME}\r\nConnection: close\r\n\r\n`);
    let dataLen = 0;
    socket.on("data", (chunk) => { dataLen += chunk.length; });
    socket.on("end", () => done(dataLen > 0, null));
  });

  socket.once("timeout", () => done(false, "TIMEOUT"));
  socket.once("error", (err) => done(false, err.code));
}

function probeCluster(onFinished) {
  if (isProbing) return;
  isProbing = true;

  dns.resolve4(TUNNEL_HOSTNAME, (err, addresses) => {
    if (!err && addresses && addresses.length > 0) {
      activeIpPool = Array.from(new Set([...addresses, ...KNOWN_IPS]));
    }

    const combos = [
      { minVersion: "TLSv1.3", maxVersion: "TLSv1.3", ALPNProtocols: ["http/1.1"] },
      { minVersion: "TLSv1.2", maxVersion: "TLSv1.3", ALPNProtocols: ["http/1.1"] },
      { minVersion: "TLSv1.2", maxVersion: "TLSv1.2", ALPNProtocols: undefined }
    ];

    let index = 0;
    function nextAttempt() {
      if (index >= activeIpPool.length * combos.length) {
        isProbing = false;
        return onFinished(null);
      }
      const ip = activeIpPool[Math.floor(index / combos.length)];
      const combo = combos[index % combos.length];
      index++;

      testTlsConnection(ip, combo, (success) => {
        if (success) {
          bestConnection = { ip, combo };
          isProbing = false;
          console.log(`[relay-redundancy] Secure route established via IP: ${ip}`);
          return onFinished(bestConnection);
        }
        nextAttempt();
      });
    }
    nextAttempt();
  });
}

probeCluster(() => {});

class RedundantHttpsAgent extends https.Agent {
  createConnection(options, callback) {
    const target = bestConnection || { ip: activeIpPool[0], combo: { minVersion: "TLSv1.2", maxVersion: "TLSv1.3", ALPNProtocols: ["http/1.1"] } };
    const socket = tls.connect({
      ...options,
      host: target.ip,
      servername: TUNNEL_HOSTNAME,
      minVersion: target.combo.minVersion,
      maxVersion: target.combo.maxVersion,
      ALPNProtocols: target.combo.ALPNProtocols,
    });
    socket.once("error", () => { if (!isProbing) probeCluster(() => {}); });
    if (callback) socket.once("secureConnect", () => callback(null, socket));
    return socket;
  }
}

const secureAgent = new RedundantHttpsAgent({ keepAlive: true, maxSockets: 30 });
const app = express();
app.set("trust proxy", 1);

const ALLOWED_ORIGINS = new Set([ALLOWED_ORIGIN, "http://localhost:3000", "http://127.0.0.1:3000"]);

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

app.get("/relay-status", (req, res) => res.json({ connected: true, tunnelUrl: TUNNEL_URL, activeNode: bestConnection?.ip }));

app.use(rateLimit({ windowMs: 60 * 1000, max: 200 }));

// General Proxy Middleware with Redundant Agent Routing
app.use("/", createProxyMiddleware({
  router: () => TUNNEL_URL,
  changeOrigin: true,
  ws: true,
  logLevel: "warn",
  agent: secureAgent,
  onError: (err, req, res) => {
    console.error("[relay-error] Proxy transmission failed:", err.message);
    if (!isProbing) probeCluster(() => {});
    if (!res.headersSent) res.status(502).json({ error: "VAIO hedef sunucuya ulaşılamıyor. Yeniden bağlanılıyor..." });
  }
}));

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log(`[relay-redundancy] Active and guarding on port ${PORT}`));
