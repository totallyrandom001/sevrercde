const express = require('express');
const cors = require('cors');

const app = express();
const PORT = process.env.PORT || 3000;

const MAX_MESSAGE_CHARS = 20000;
const MAX_IMAGE_BASE64_CHARS = 1500000;

app.use(cors());
app.options('*', cors());
app.use(express.json({ limit: '5mb' }));

let clients = [];

// Helper function to query Cloudflare D1 via REST API
async function queryD1(sql, params = []) {
  const accountId = process.env.CF_ACCOUNT_ID;
  const databaseId = process.env.CF_DATABASE_ID;
  const apiToken = process.env.CF_API_TOKEN;

  if (!accountId || !databaseId || !apiToken) {
    throw new Error("Missing Cloudflare Environment Variables on Render.");
  }

  const response = await fetch(
    `https://api.cloudflare.com/client/v4/accounts/${accountId}/d1/database/${databaseId}/query`,
    {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${apiToken}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ sql, params })
    }
  );

  const data = await response.json();
  if (!data.success) {
    throw new Error(data.errors?.[0]?.message || "Cloudflare D1 Query Failed");
  }

  const firstResult = data.result?.[0] || {};
  return {
    results: firstResult.results || [],
    lastRowId: firstResult.meta?.last_row_id || null
  };
}

// Broadcast new message to all connected SSE clients
function broadcast(data) {
  const payload = `data: ${JSON.stringify(data)}\n\n`;
  clients.forEach(client => {
    try {
      client.write(payload);
    } catch (e) {
      // Ignore dead connection writes
    }
  });
}

// Keep-alive ping every 20 seconds
setInterval(() => {
  clients.forEach(client => {
    try {
      client.write(': ping\n\n');
    } catch (e) {}
  });
}, 20000);

app.get('/', (req, res) => {
  res.json({ status: "ok", message: "Server is running!" });
});

// SSE Real-Time Stream Endpoint
app.get('/api/stream', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no'); // Prevents Render proxy from buffering SSE messages
  res.flushHeaders();

  clients.push(res);

  req.on('close', () => {
    clients = clients.filter(client => client !== res);
  });
});

// POST /api/send
app.post('/api/send', async (req, res, next) => {
  try {
    const { name: rawName, message: rawMsg, image: rawImg } = req.body || {};
    
    const name = (rawName || '').toString().trim().slice(0, 50);
    const message = (rawMsg || '').toString();
    const image = rawImg ? rawImg.toString() : null;

    if (!name) return res.status(400).json({ error: 'İsim gerekli.' });
    if (!message && !image) return res.status(400).json({ error: 'Mesaj veya görsel gerekli.' });
    if (message.length > MAX_MESSAGE_CHARS) return res.status(400).json({ error: 'Mesaj çok uzun.' });
    if (image && image.length > MAX_IMAGE_BASE64_CHARS) return res.status(400).json({ error: 'Görsel çok büyük.' });

    const createdAt = Date.now();
    
    // Save to D1 and capture generated last_row_id
    const { lastRowId } = await queryD1(
      "INSERT INTO messages (name, message, image, created_at) VALUES (?, ?, ?, ?)",
      [name, message, image, createdAt]
    );

    const newMsg = {
      id: lastRowId,
      name,
      message,
      image,
      created_at: createdAt
    };

    // Push new message instantly with its valid ID
    broadcast(newMsg);

    return res.json({ ok: true });
  } catch (err) {
    next(err);
  }
});

// GET /api/messages (Initial message history load)
app.get('/api/messages', async (req, res, next) => {
  try {
    const limit = Math.min(parseInt(req.query.limit || '50', 10) || 50, 200);
    const { results } = await queryD1(
      "SELECT id, name, message, image, created_at FROM messages ORDER BY id DESC LIMIT ?",
      [limit]
    );
    return res.json({ messages: results.reverse() });
  } catch (err) {
    next(err);
  }
});

app.use((err, req, res, next) => {
  console.error("Server Error:", err.message);
  res.status(500).json({ error: err.message || "Sunucu hatası" });
});

app.use((req, res) => {
  res.status(404).json({ error: "Route not found" });
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
