const express = require("express");
const cors = require("cors");
const fetch = require("node-fetch");

const app = express();

app.use(cors({
  origin: "*",
  methods: ["GET", "POST", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization"]
}));

app.options("*", cors());
app.use(express.json());

const CF_WORKER_URL = "https://winter-king-b73e.totallyrandom000148932804.workers.dev";
const WORKER_SECRET = process.env.WORKER_SECRET;

// --- SSE Real-time Stream Engine (I-Frame / P-Frame Architecture) ---
let sseClients = []; // Stores { username, res }

function broadcastPFrame(eventType, payload, targetUsernames = null) {
  const dataString = `data: ${JSON.stringify({ frameType: "P-FRAME", type: eventType, payload })}\n\n`;
  sseClients.forEach(client => {
    if (!targetUsernames || targetUsernames.includes(client.username)) {
      client.res.write(dataString);
    }
  });
}

// Token Interleaving Helper
function generateToken(username, password) {
  const cleanUser = username.toLowerCase().replace(/[^a-z]/g, "");
  let token = "";
  const maxLen = Math.max(cleanUser.length, password.length);
  for (let i = 0; i < maxLen; i++) {
    if (i < cleanUser.length) token += cleanUser[i];
    if (i < password.length) token += password[i];
  }
  return token;
}

async function queryWorker(sql, params = []) {
  const res = await fetch(`${CF_WORKER_URL}/query`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Worker-Secret": WORKER_SECRET
    },
    body: JSON.stringify({ sql, params })
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Worker Error: ${text}`);
  }
  return await res.json();
}

async function authenticate(req, res, next) {
  const token = req.headers.authorization || req.query.token;
  if (!token) return res.status(401).json({ error: "Missing token" });

  try {
    const userRes = await queryWorker("SELECT * FROM users WHERE token = ?", [token]);
    if (!userRes.results || userRes.results.length === 0) {
      return res.status(401).json({ error: "Invalid token" });
    }
    req.user = userRes.results[0];
    next();
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}

app.get("/", (req, res) => res.send("Discord-like Chat Backend Live!"));

// -------------------------------------------------------------
// SSE Stream Endpoint (I-Frame Snapshot on Connect)
// -------------------------------------------------------------
app.get("/api/stream", authenticate, async (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");

  const client = { username: req.user.username, res };
  sseClients.push(client);

  // Send Initial Snapshot: I-FRAME
  try {
    const groups = req.user.role === "admin" 
      ? (await queryWorker("SELECT * FROM groups")).results
      : (await queryWorker("SELECT g.* FROM groups g JOIN group_members gm ON g.group_token = gm.group_token WHERE gm.username = ?", [req.user.username])).results;

    const friends = (await queryWorker("SELECT * FROM friends WHERE user1 = ? OR user2 = ?", [req.user.username, req.user.username])).results;

    const iFramePayload = {
      frameType: "I-FRAME",
      user: { username: req.user.username, role: req.user.role },
      groups,
      friends
    };

    res.write(`data: ${JSON.stringify(iFramePayload)}\n\n`);
  } catch (err) {
    console.error("I-Frame Error:", err);
  }

  req.on("close", () => {
    sseClients = sseClients.filter(c => c.res !== res);
  });
});

// Auth Routes
app.post("/api/register", async (req, res) => {
  let { username, password } = req.body;
  if (!username || !password) return res.status(400).json({ error: "Missing fields" });
  username = username.toLowerCase().replace(/[^a-z]/g, "");

  try {
    const countRes = await queryWorker("SELECT COUNT(*) as count FROM pending_accounts");
    if (countRes.results[0].count >= 5) return res.status(429).json({ error: "Max pending creation requests reached (5)." });

    const token = generateToken(username, password);
    await queryWorker("INSERT INTO pending_accounts (username, password, token) VALUES (?, ?, ?)", [username, password, token]);
    res.json({ message: "Account creation request submitted for admin approval." });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post("/api/login", async (req, res) => {
  let { username, password } = req.body;
  username = username.toLowerCase().replace(/[^a-z]/g, "");
  const token = generateToken(username, password);

  try {
    const userRes = await queryWorker("SELECT * FROM users WHERE username = ? AND token = ?", [username, token]);
    if (!userRes.results || userRes.results.length === 0) return res.status(401).json({ error: "Invalid credentials" });
    res.json({ token, username: userRes.results[0].username, role: userRes.results[0].role });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Friends Management
app.post("/api/friends/add", authenticate, async (req, res) => {
  let { targetUsername } = req.body;
  targetUsername = targetUsername.toLowerCase().replace(/[^a-z]/g, "");

  if (targetUsername === req.user.username) return res.status(400).json({ error: "Cannot add yourself" });

  try {
    const checkUser = await queryWorker("SELECT username FROM users WHERE username = ?", [targetUsername]);
    if (!checkUser.results.length) return res.status(404).json({ error: "User does not exist" });

    await queryWorker("INSERT OR IGNORE INTO friends (user1, user2) VALUES (?, ?)", [req.user.username, targetUsername]);
    
    // Broadcast P-FRAME to both users
    broadcastPFrame("FRIEND_ADDED", { user1: req.user.username, user2: targetUsername }, [req.user.username, targetUsername]);
    res.json({ message: "Friend added successfully" });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Group Creation & Adding Members
app.post("/api/groups/create", authenticate, async (req, res) => {
  const { groupName, memberToken } = req.body;
  const groupToken = "grp_" + Math.random().toString(36).substring(2, 10);

  try {
    await queryWorker("INSERT INTO groups (group_token, group_name, created_by) VALUES (?, ?, ?)", [groupToken, groupName, req.user.username]);
    await queryWorker("INSERT INTO group_members (group_token, username, custom_member_token) VALUES (?, ?, ?)", [groupToken, req.user.username, memberToken || req.user.token]);

    broadcastPFrame("GROUP_CREATED", { group_token: groupToken, group_name: groupName, created_by: req.user.username });
    res.json({ message: "Group created", groupToken });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post("/api/groups/add-member", authenticate, async (req, res) => {
  let { groupToken, targetUsername } = req.body;
  targetUsername = targetUsername.toLowerCase().replace(/[^a-z]/g, "");

  try {
    await queryWorker("INSERT OR REPLACE INTO group_members (group_token, username, custom_member_token) VALUES (?, ?, 'DEFAULT')", [groupToken, targetUsername]);
    broadcastPFrame("GROUP_MEMBER_ADDED", { groupToken, targetUsername }, [targetUsername]);
    res.json({ message: `${targetUsername} added to group.` });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Direct Messaging Dynamic Group Creator
app.post("/api/dm/open", authenticate, async (req, res) => {
  let { targetUsername } = req.body;
  targetUsername = targetUsername.toLowerCase().replace(/[^a-z]/g, "");

  const dmGroupToken = "dm_" + [req.user.username, targetUsername].sort().join("_");

  try {
    await queryWorker("INSERT OR IGNORE INTO groups (group_token, group_name, created_by) VALUES (?, ?, ?)", [dmGroupToken, `@${targetUsername}`, req.user.username]);
    await queryWorker("INSERT OR IGNORE INTO group_members (group_token, username, custom_member_token) VALUES (?, ?, 'DM')", [dmGroupToken, req.user.username]);
    await queryWorker("INSERT OR IGNORE INTO group_members (group_token, username, custom_member_token) VALUES (?, ?, 'DM')", [dmGroupToken, targetUsername]);

    broadcastPFrame("GROUP_CREATED", { group_token: dmGroupToken, group_name: `@${targetUsername}`, created_by: req.user.username }, [req.user.username, targetUsername]);
    res.json({ groupToken: dmGroupToken });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Messaging Endpoints
app.post("/api/messages/send", authenticate, async (req, res) => {
  const { groupToken, content } = req.body;
  const timestamp = Date.now();

  try {
    const result = await queryWorker("INSERT INTO messages (group_token, sender, content, created_at) VALUES (?, ?, ?, ?) RETURNING id", [groupToken, req.user.username, content, timestamp]);
    
    // Broadcast P-Frame Delta update to all connected clients
    const pFrameData = {
      id: result.meta?.last_row_id || Date.now(),
      group_token: groupToken,
      sender: req.user.username,
      content,
      created_at: timestamp
    };
    broadcastPFrame("NEW_MESSAGE", pFrameData);

    res.json({ message: "Sent" });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get("/api/messages/:groupToken", authenticate, async (req, res) => {
  try {
    const msgs = await queryWorker("SELECT * FROM messages WHERE group_token = ? ORDER BY created_at ASC", [req.params.groupToken]);
    res.json(msgs.results);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Admin Routes
app.get("/api/admin/pending", authenticate, async (req, res) => {
  if (req.user.role !== "admin") return res.status(403).json({ error: "Forbidden" });
  const data = await queryWorker("SELECT * FROM pending_accounts");
  res.json(data.results);
});

app.post("/api/admin/action", authenticate, async (req, res) => {
  if (req.user.role !== "admin") return res.status(403).json({ error: "Forbidden" });
  const { id, action } = req.body;
  const reqRes = await queryWorker("SELECT * FROM pending_accounts WHERE id = ?", [id]);
  if (!reqRes.results.length) return res.status(404).json({ error: "Not found" });

  const item = reqRes.results[0];
  if (action === "approve") {
    await queryWorker("INSERT INTO users (username, token) VALUES (?, ?)", [item.username, item.token]);
  }
  await queryWorker("DELETE FROM pending_accounts WHERE id = ?", [id]);
  broadcastPFrame("ADMIN_ACTION", { id, action });
  res.json({ message: `Account request ${action}d.` });
});

app.post("/api/admin/delete-user", authenticate, async (req, res) => {
  if (req.user.role !== "admin") return res.status(403).json({ error: "Forbidden" });
  const { username } = req.body;
  await queryWorker("DELETE FROM users WHERE username = ?", [username]);
  await queryWorker("DELETE FROM group_members WHERE username = ?", [username]);
  await queryWorker("DELETE FROM friends WHERE user1 = ? OR user2 = ?", [username, username]);
  broadcastPFrame("USER_DELETED", { username });
  res.json({ message: `User ${username} deleted.` });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Discord-like Server listening on port ${PORT}`));
