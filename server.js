// ============================================================================
// SOPERT — Render relay
// This server does ONE thing: forward every request to the Vaio via the
// Cloudflare Tunnel URL stored in the VAIO_URL environment variable.
// No logic, no database, no state. Just a proxy.
//
// SSE streams (/api/stream) are piped through with proper chunked streaming
// so the browser's EventSource connection stays alive end-to-end.
// ============================================================================

const express = require("express");
const http    = require("http");
const https   = require("https");

const PORT     = process.env.PORT || 3000;
const VAIO_URL = (process.env.VAIO_URL || "").replace(/\/+$/, "");

if (!VAIO_URL) {
  // On first deploy VAIO_URL might not be set yet — don't crash, just warn.
  console.warn("WARNING: VAIO_URL env var is not set. All requests will return 503 until it is.");
}

const app = express();

// ---------------------------------------------------------------------------
// CORS — same list as original server
// ---------------------------------------------------------------------------
const ALLOWED_ORIGINS = [
  "https://totallyrandom001.github.io",
  "http://localhost:3000",
  "http://127.0.0.1:3000",
];
app.use((req, res, next) => {
  const origin = req.headers.origin;
  if (ALLOWED_ORIGINS.includes(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Vary", "Origin");
  }
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

// ---------------------------------------------------------------------------
// Proxy — pipe every request straight to the Vaio
// ---------------------------------------------------------------------------
app.use((req, res) => {
  const vaioUrl = process.env.VAIO_URL
    ? process.env.VAIO_URL.replace(/\/+$/, "")
    : VAIO_URL;

  if (!vaioUrl) {
    return res.status(503).json({ error: "Vaio tunnel URL not configured. Run run.bat on the Vaio first." });
  }

  const target = new URL(vaioUrl);
  const isSSE  = req.headers.accept === "text/event-stream";
  const isHttps = target.protocol === "https:";

  // Build the options for the outgoing request to the Vaio
  const options = {
    hostname: target.hostname,
    port:     target.port || (isHttps ? 443 : 80),
    path:     req.url,
    method:   req.method,
    headers: {
      ...req.headers,
      host: target.hostname,       // replace browser's host with tunnel host
      "x-forwarded-for": req.ip,  // pass real IP through for logging
    },
  };

  // Don't forward connection/encoding headers that confuse the tunnel
  delete options.headers["connection"];
  delete options.headers["transfer-encoding"];

  const transport = isHttps ? https : http;

  const proxyReq = transport.request(options, (proxyRes) => {
    // Copy status and headers from Vaio response
    res.status(proxyRes.statusCode);
    for (const [k, v] of Object.entries(proxyRes.headers)) {
      // Skip headers that express/node will set itself
      if (["transfer-encoding", "connection"].includes(k.toLowerCase())) continue;
      res.setHeader(k, v);
    }

    if (isSSE) {
      // SSE: flush immediately so the browser gets events in real time
      res.setHeader("Content-Type", "text/event-stream");
      res.setHeader("Cache-Control", "no-cache");
      res.setHeader("Connection", "keep-alive");
      res.setHeader("X-Accel-Buffering", "no");
      res.flushHeaders();
      proxyRes.on("data", chunk => {
        res.write(chunk);
        // res.flush exists if compression middleware is present, safe to call
        if (typeof res.flush === "function") res.flush();
      });
      proxyRes.on("end", () => res.end());
    } else {
      // Normal request: pipe response body straight through
      proxyRes.pipe(res);
    }
  });

  proxyReq.on("error", (err) => {
    console.error("Proxy error:", err.message);
    if (!res.headersSent) {
      res.status(502).json({ error: "Could not reach Vaio. Is the tunnel running? " + err.message });
    }
  });

  // Pipe request body (POST/PUT) to Vaio
  if (req.method !== "GET" && req.method !== "HEAD") {
    req.pipe(proxyReq);
  } else {
    proxyReq.end();
  }
});

// ---------------------------------------------------------------------------
// Boot
// ---------------------------------------------------------------------------
app.listen(PORT, () => {
  console.log(`SOPERT Render relay on port ${PORT}`);
  console.log(`Forwarding to Vaio: ${VAIO_URL || "(not set yet)"}`);
});
