const express = require('express');
const cors = require('cors');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

const MAX_MESSAGE_CHARS = 20000;
const MAX_IMAGE_BASE64_CHARS = 1500000;

// Enable CORS for all origins (or limit to your github.io domain)
app.use(cors());
app.use(express.json({ limit: '5mb' }));

// Initialize SQLite database
const dbPath = path.resolve(__dirname, 'database.sqlite');
const db = new sqlite3.Database(dbPath, (err) => {
  if (err) {
    console.error('Error opening database:', err);
  } else {
    console.log('Connected to SQLite database.');
    db.run(`
      CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        message TEXT,
        image TEXT,
        created_at INTEGER NOT NULL
      )
    `);
  }
});

// POST /api/send
app.post('/api/send', (req, res) => {
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
      return res.status(400).json({ error: `Mesaj çok uzun. En fazla ${MAX_MESSAGE_CHARS} karakter gönderebilirsiniz.` });
    }
    if (image && image.length > MAX_IMAGE_BASE64_CHARS) {
      return res.status(400).json({ error: 'Görsel çok büyük. Lütfen daha küçük bir görsel gönderin.' });
    }

    const createdAt = Date.now();
    const stmt = db.prepare('INSERT INTO messages (name, message, image, created_at) VALUES (?, ?, ?, ?)');
    
    stmt.run([name, message, image, createdAt], function (err) {
      if (err) {
        return res.status(500).json({ error: 'Sunucu hatası: ' + err.message });
      }
      return res.json({ ok: true, id: this.lastID });
    });
    stmt.finalize();
  } catch (err) {
    return res.status(500).json({ error: 'Sunucu hatası: ' + err.message });
  }
});

// GET /api/messages
app.get('/api/messages', (req, res) => {
  const after = req.query.after;
  const limit = Math.min(parseInt(req.query.limit || '50', 10) || 50, 200);

  if (after) {
    db.all(
      'SELECT id, name, message, image, created_at FROM messages WHERE id > ? ORDER BY id ASC',
      [after],
      (err, rows) => {
        if (err) {
          return res.status(500).json({ error: 'Sunucu hatası: ' + err.message });
        }
        return res.json({ messages: rows || [] });
      }
    );
  } else {
    db.all(
      'SELECT id, name, message, image, created_at FROM messages ORDER BY id DESC LIMIT ?',
      [limit],
      (err, rows) => {
        if (err) {
          return res.status(500).json({ error: 'Sunucu hatası: ' + err.message });
        }
        return res.json({ messages: (rows || []).reverse() });
      }
    );
  }
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
