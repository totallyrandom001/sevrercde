const express = require("express");
const cors = require("cors");
const fetch = require("node-fetch");

const app = express();

app.use(express.json({ limit: "2mb" }));
app.use(cors({
  origin: "*",
  methods: ["GET", "POST", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization"]
}));

app.options("*", cors());

const CF_WORKER_URL = "https://winter-king-b73e.totallyrandom000148932804.workers.dev";
const WORKER_SECRET = process.env.WORKER_SECRET;

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
  if (!WORKER_SECRET) throw new Error("WORKER_SECRET environment variable is missing!");
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

// SSE Broadcast Engine
let sseClients = [];
function broadcastPFrame(eventType, payload, targetUsernames = null) {
  const dataString = `data: ${JSON.stringify({ frameType: "P-FRAME", type: eventType, payload })}\n\n`;
  sseClients.forEach(client => {
    if (!targetUsernames || targetUsernames.includes(client.username)) {
      client.res.write(dataString);
    }
  });
}

app.get("/", (req, res) => res.send("Chat Backend Online"));

// SSE Stream Setup
app.get("/api/stream", authenticate, async (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");

  const client = { username: req.user.username, res };
  sseClients.push(client);

  try {
    const groups = req.user.role === "admin"
      ? (await queryWorker("SELECT * FROM groups")).results
      : (await queryWorker("SELECT g.* FROM groups g JOIN group_members gm ON g.group_token = gm.group_token WHERE gm.username = ?", [req.user.username])).results;

    const friends = (await queryWorker(
      "SELECT f.*, u.pfp FROM friends f JOIN users u ON (u.username = CASE WHEN f.user1 = ? THEN f.user2 ELSE f.user1 END) WHERE f.user1 = ? OR f.user2 = ?",
      [req.user.username, req.user.username, req.user.username]
    )).results;

    res.write(`data: ${JSON.stringify({
      frameType: "I-FRAME",
      user: { username: req.user.username, role: req.user.role, pfp: req.user.pfp || "" },
      groups,
      friends
    })}\n\n`);
  } catch (err) {
    console.error("Stream I-Frame Error:", err);
  }

  req.on("close", () => {
    sseClients = sseClients.filter(c => c.res !== res);
  });
});

// Account Registration with Existing Account Prevention Check
app.post("/api/register", async (req, res) => {
  let { username, password } = req.body;
  if (!username || !password) return res.status(400).json({ error: "Missing required fields" });
  username = username.toLowerCase().replace(/[^a-z]/g, "");

  if (!username) return res.status(400).json({ error: "Username must contain valid letters" });

  try {
    // 1. Check if user already exists in registered accounts
    const existingUser = await queryWorker("SELECT username FROM users WHERE username = ?", [username]);
    if (existingUser.results && existingUser.results.length > 0) {
      return res.status(409).json({ error: "An account with this username already exists." });
    }

    // 2. Check if user already exists in pending approval requests
    const existingPending = await queryWorker("SELECT username FROM pending_accounts WHERE username = ?", [username]);
    if (existingPending.results && existingPending.results.length > 0) {
      return res.status(409).json({ error: "An account creation request for this username is already pending approval." });
    }

    // 3. Check max pending account limit (5)
    const countRes = await queryWorker("SELECT COUNT(*) as count FROM pending_accounts");
    if (countRes.results[0].count >= 5) {
      return res.status(429).json({ error: "Max 5 pending account requests reached. Try again later." });
    }

    const token = generateToken(username, password);
    await queryWorker("INSERT INTO pending_accounts (username, password, token) VALUES (?, ?, ?)", [username, password, token]);
    res.json({ message: "Account creation request submitted for admin approval." });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/login", async (req, res) => {
  let { username, password } = req.body;
  username = username.toLowerCase().replace(/[^a-z]/g, "");
  const token = generateToken(username, password);

  try {
    const userRes = await queryWorker("SELECT * FROM users WHERE username = ? AND token = ?", [username, token]);
    if (!userRes.results || userRes.results.length === 0) return res.status(401).json({ error: "Invalid credentials or pending approval" });
    res.json({ token, username: userRes.results[0].username, role: userRes.results[0].role, pfp: userRes.results[0].pfp || "" });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Profile Picture Upload
app.post("/api/user/pfp", authenticate, async (req, res) => {
  const { pfpBase64 } = req.body;
  if (!pfpBase64) return res.status(400).json({ error: "No image provided" });

  const byteSize = Buffer.byteLength(pfpBase64, "utf8");
  if (byteSize > 512 * 1024) {
    return res.status(413).json({ error: "Image exceeds 512KB limit." });
  }

  try {
    await queryWorker("UPDATE users SET pfp = ? WHERE username = ?", [pfpBase64, req.user.username]);
    broadcastPFrame("PFP_UPDATED", { username: req.user.username, pfp: pfpBase64 });
    res.json({ message: "Profile picture updated." });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Friends Management
app.post("/api/friends/request", authenticate, async (req, res) => {
  let { targetUsername } = req.body;
  targetUsername = targetUsername.toLowerCase().replace(/[^a-z]/g, "");

  if (targetUsername === req.user.username) return res.status(400).json({ error: "Cannot add yourself" });

  try {
    const checkUser = await queryWorker("SELECT username FROM users WHERE username = ?", [targetUsername]);
    if (!checkUser.results.length) return res.status(404).json({ error: "User does not exist" });

    await queryWorker("INSERT OR IGNORE INTO friends (user1, user2, status) VALUES (?, ?, 'pending')", [req.user.username, targetUsername]);
    broadcastPFrame("FRIEND_REQUEST_SENT", { from: req.user.username, to: targetUsername }, [targetUsername]);
    res.json({ message: "Friend request sent!" });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post("/api/friends/accept", authenticate, async (req, res) => {
  let { targetUsername } = req.body;
  targetUsername = targetUsername.toLowerCase().replace(/[^a-z]/g, "");

  try {
    await queryWorker("UPDATE friends SET status = 'accepted' WHERE user1 = ? AND user2 = ?", [targetUsername, req.user.username]);
    broadcastPFrame("FRIEND_ACCEPTED", { user1: targetUsername, user2: req.user.username }, [req.user.username, targetUsername]);
    res.json({ message: "Friend request accepted!" });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Group Creation & In-Group Member Management
app.post("/api/groups/create", authenticate, async (req, res) => {
  const { groupName, memberToken } = req.body;
  if (!groupName) return res.status(400).json({ error: "Group name required" });
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
    const checkUser = await queryWorker("SELECT username FROM users WHERE username = ?", [targetUsername]);
    if (!checkUser.results.length) return res.status(404).json({ error: "User does not exist" });

    await queryWorker("INSERT OR REPLACE INTO group_members (group_token, username, custom_member_token) VALUES (?, ?, 'DEFAULT')", [groupToken, targetUsername]);
    broadcastPFrame("GROUP_MEMBER_ADDED", { groupToken, targetUsername }, [targetUsername]);
    res.json({ message: `${targetUsername} added to group successfully.` });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post("/api/dm/open", authenticate, async (req, res) => {
  let { targetUsername } = req.body;
  targetUsername = targetUsername.toLowerCase().replace(/[^a-z]/g, "");

  try {
    const friendCheck = await queryWorker(
      "SELECT * FROM friends WHERE ((user1 = ? AND user2 = ?) OR (user1 = ? AND user2 = ?)) AND status = 'accepted'",
      [req.user.username, targetUsername, targetUsername, req.user.username]
    );

    if (!friendCheck.results.length && req.user.role !== "admin") {
      return res.status(403).json({ error: "Must be accepted friends to initiate DMs" });
    }

    const dmGroupToken = "dm_" + [req.user.username, targetUsername].sort().join("_");
    await queryWorker("INSERT OR IGNORE INTO groups (group_token, group_name, created_by) VALUES (?, ?, ?)", [dmGroupToken, `@${targetUsername}`, req.user.username]);
    await queryWorker("INSERT OR IGNORE INTO group_members (group_token, username, custom_member_token) VALUES (?, ?, 'DM')", [dmGroupToken, req.user.username]);
    await queryWorker("INSERT OR IGNORE INTO group_members (group_token, username, custom_member_token) VALUES (?, ?, 'DM')", [dmGroupToken, targetUsername]);

    res.json({ groupToken: dmGroupToken });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Messages Engine
app.post("/api/messages/send", authenticate, async (req, res) => {
  const { groupToken, content } = req.body;
  const timestamp = Date.now();

  try {
    await queryWorker("INSERT INTO messages (group_token, sender, content, created_at) VALUES (?, ?, ?, ?)", [groupToken, req.user.username, content, timestamp]);
    const pFrameData = { group_token: groupToken, sender: req.user.username, content, created_at: timestamp, pfp: req.user.pfp || "" };
    broadcastPFrame("NEW_MESSAGE", pFrameData);
    res.json({ message: "Sent" });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get("/api/messages/:groupToken", authenticate, async (req, res) => {
  try {
    const msgs = await queryWorker(
      "SELECT m.*, u.pfp FROM messages m LEFT JOIN users u ON m.sender = u.username WHERE m.group_token = ? ORDER BY m.created_at ASC",
      [req.params.groupToken]
    );
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
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
