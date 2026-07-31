const express = require('express');
const cors = require('cors');

const app = express();
const PORT = process.env.PORT || 3000;

const MAX_MESSAGE_CHARS = 20000;
const MAX_IMAGE_BASE64_CHARS = 1500000;

app.use(cors());
app.use(express.json({ limit: '5mb' }));

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

  return data.result[0]?.results || [];
}

// POST /api/send
app.post('/api/send', async (req, res) => {
  try {
    const { name: rawName, message: rawMsg, image: rawImg } = req.body;
    
    const name = (rawName || '').toString().trim().slice(0, 50);
    const message = (rawMsg || '').toString();
    const image = rawImg ? rawImg.toString() : null;

    if (!name) {
      return res.status(400).json({ error: 'İsim gerekli.' });
    }
    if (!message && !image) {
      return res.status(400).json({ error: 'Mesaj veya görsel gerekli.' });
    }
    if (message.length > MAX_MESSAGE_CHARS) {
      return res.status(400).json({ error: 'Mesaj çok uzun.' });
    }
    if (image && image.length > MAX_IMAGE_BASE64_CHARS) {
      return res.status(400).json({ error: 'Görsel çok büyük.' });
    }

    const createdAt = Date.now();
    await queryD1(
      "INSERT INTO messages (name, message, image, created_at) VALUES (?, ?, ?, ?)",
      [name, message, image, createdAt]
    );

    return res.json({ ok: true });
  } catch (err) {
    return res.status(500).json({ error: 'Sunucu hatası: ' + err.message });
  }
});

// GET /api/messages
app.get('/api/messages', async (req, res) => {
  try {
    const after = req.query.after;
    const limit = Math.min(parseInt(req.query.limit || '50', 10) || 50, 200);

    let rows;
    if (after) {
      rows = await queryD1(
        "SELECT id, name, message, image, created_at FROM messages WHERE id > ? ORDER BY id ASC",
        [after]
      );
    } else {
      const results = await queryD1(
        "SELECT id, name, message, image, created_at FROM messages ORDER BY id DESC LIMIT ?",
        [limit]
      );
      rows = results.reverse();
    }

    return res.json({ messages: rows });
  } catch (err) {
    return res.status(500).json({ error: 'Sunucu hatası: ' + err.message });
  }
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
