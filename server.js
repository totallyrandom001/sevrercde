const express = require("express");
const cors = require("cors");
const fetch = require("node-fetch");

const app = express();

app.use(express.json({ limit: "10mb" }));
app.use(cors({
  origin: "*",
  methods: ["GET", "POST", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization"]
}));

app.options("*", cors());

const CF_WORKER_URL = "https://winter-king-b73e.totallyrandom000148932804.workers.dev";
const WORKER_SECRET = process.env.WORKER_SECRET;

// --- In-Memory Rate Limiting Engine ---
const rateLimitMap = new Map();

/**
 * Checks and updates rate limits for a given identifier key.
 * @param {string} key - Unique key per user or IP action
 * @param {number} windowMs - Time window in milliseconds
 * @param {number} maxHits - Maximum allowed requests within the window
 * @returns {{ limited: boolean, retryAfterSec?: number }}
 */
function checkRateLimit(key, windowMs, maxHits) {
  const now = Date.now();
  if (!rateLimitMap.has(key)) {
    rateLimitMap.set(key, []);
  }

  // Filter out timestamps outside the active window
  const timestamps = rateLimitMap.get(key).filter(ts => now - ts < windowMs);

  if (timestamps.length >= maxHits) {
    const oldestTimestamp = timestamps[0];
    const retryAfterMs = windowMs - (now - oldestTimestamp);
    return { limited: true, retryAfterSec: Math.ceil(retryAfterMs / 1000) };
  }

  timestamps.push(now);
  rateLimitMap.set(key, timestamps);
  return { limited: false };
}

// Memory Cleanup: Sweep expired entries every 10 minutes to prevent memory leaks
setInterval(() => {
  const now = Date.now();
  for (const [key, timestamps] of rateLimitMap.entries()) {
    const valid = timestamps.filter(ts => now - ts < 15 * 60 * 1000);
    if (valid.length === 0) rateLimitMap.delete(key);
    else rateLimitMap.set(key, valid);
  }
}, 10 * 60 * 1000);

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
  if (!token) return res.status(401).json({ error: "Missing authentication token" });

  try {
    const userRes = await queryWorker("SELECT * FROM users WHERE token = ?", [token]);
    if (!userRes.results || userRes.results.length === 0) {
      return res.status(401).json({ error: "Invalid token session" });
    }
    req.user = userRes.results[0];
    next();
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}

// SSE Clients Registry
let sseClients = [];
function broadcastPFrame(eventType, payload, targetUsernames = null) {
  const dataString = `data: ${JSON.stringify({ frameType: "P-FRAME", type: eventType, payload })}\n\n`;
  sseClients.forEach(client => {
    if (!targetUsernames || targetUsernames.includes(client.username)) {
      client.res.write(dataString);
    }
  });
}

async function fetchUserSnapshot(user) {
  const groupsRes = user.role === "admin"
    ? await queryWorker("SELECT * FROM groups")
    : await queryWorker("SELECT g.* FROM groups g JOIN group_members gm ON g.group_token = gm.group_token WHERE gm.username = ?", [user.username]);
  const groups = groupsRes.results || [];

  const friendsRes = await queryWorker("SELECT * FROM friends WHERE user1 = ? OR user2 = ?", [user.username, user.username]);
  const friendRows = friendsRes.results || [];

  const usersRes = await queryWorker("SELECT username, pfp FROM users");
  const userMap = {};
  (usersRes.results || []).forEach(u => userMap[u.username] = u.pfp || "");

  const friends = friendRows.map(f => {
    const otherUser = f.user1 === user.username ? f.user2 : f.user1;
    return {
      id: f.id,
      user1: f.user1,
      user2: f.user2,
      status: f.status,
      pfp: userMap[otherUser] || ""
    };
  });

  return { groups, friends };
}

app.get("/", (req, res) => res.send("Chat Backend Online"));

// Snapshot API
app.get("/api/snapshot", authenticate, async (req, res) => {
  try {
    const snapshot = await fetchUserSnapshot(req.user);
    res.json(snapshot);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// SSE Stream Setup
app.get("/api/stream", authenticate, async (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");

  const client = { username: req.user.username, res };
  sseClients.push(client);

  try {
    const snapshot = await fetchUserSnapshot(req.user);
    res.write(`data: ${JSON.stringify({
      frameType: "I-FRAME",
      user: { username: req.user.username, role: req.user.role, pfp: req.user.pfp || "" },
      ...snapshot
    })}\n\n`);
  } catch (err) {
    console.error("Stream Error:", err);
  }

  req.on("close", () => {
    sseClients = sseClients.filter(c => c.res !== res);
  });
});

// Registration (Rate Limit: 3 attempts per 15 minutes per IP)
app.post("/api/register", async (req, res) => {
  const ip = req.headers["x-forwarded-for"] || req.ip || "unknown";
  const limit = checkRateLimit(`reg_${ip}`, 15 * 60 * 1000, 3);
  if (limit.limited) {
    return res.status(429).json({ error: `Too many registration requests. Please wait ${limit.retryAfterSec}s.` });
  }

  let { username, password } = req.body;
  if (!username || !password) return res.status(400).json({ error: "Missing required fields" });
  username = username.toLowerCase().replace(/[^a-z]/g, "");

  if (!username) return res.status(400).json({ error: "Username must contain valid letters" });

  try {
    const existingUser = await queryWorker("SELECT username FROM users WHERE username = ?", [username]);
    if (existingUser.results && existingUser.results.length > 0) {
      return res.status(409).json({ error: "An account with this username already exists" });
    }

    const existingPending = await queryWorker("SELECT username FROM pending_accounts WHERE username = ?", [username]);
    if (existingPending.results && existingPending.results.length > 0) {
      return res.status(409).json({ error: "Creation request for this username is pending" });
    }

    const countRes = await queryWorker("SELECT COUNT(*) as count FROM pending_accounts");
    if (countRes.results[0].count >= 5) {
      return res.status(429).json({ error: "Max 5 pending requests allowed" });
    }

    const token = generateToken(username, password);
    await queryWorker("INSERT INTO pending_accounts (username, password, token) VALUES (?, ?, ?)", [username, password, token]);
    res.json({ message: "Account creation request submitted successfully" });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Login (Rate Limit: 5 attempts per 1 minute per IP)
app.post("/api/login", async (req, res) => {
  const ip = req.headers["x-forwarded-for"] || req.ip || "unknown";
  const limit = checkRateLimit(`login_${ip}`, 60 * 1000, 5);
  if (limit.limited) {
    return res.status(429).json({ error: `Too many login attempts. Please wait ${limit.retryAfterSec}s.` });
  }

  let { username, password } = req.body;
  if (!username || !password) return res.status(400).json({ error: "Enter username and password" });
  username = username.toLowerCase().replace(/[^a-z]/g, "");
  const token = generateToken(username, password);

  try {
    const userRes = await queryWorker("SELECT * FROM users WHERE username = ? AND token = ?", [username, token]);
    if (!userRes.results || userRes.results.length === 0) return res.status(401).json({ error: "Invalid credentials or pending approval" });
    res.json({ token, username: userRes.results[0].username, role: userRes.results[0].role, pfp: userRes.results[0].pfp || "" });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// PFP Upload (Rate Limit: 1 update every 10 minutes per user)
app.post("/api/user/pfp", authenticate, async (req, res) => {
  const limit = checkRateLimit(`pfp_${req.user.username}`, 10 * 60 * 1000, 1);
  if (limit.limited) {
    const mins = Math.ceil(limit.retryAfterSec / 60);
    return res.status(429).json({ error: `PFP can only be updated once every 10 minutes. Try again in ${mins} minute(s).` });
  }

  const { pfpBase64 } = req.body;
  if (!pfpBase64) return res.status(400).json({ error: "No image provided" });

  const byteSize = Buffer.byteLength(pfpBase64, "utf8");
  if (byteSize > 512 * 1024) {
    return res.status(413).json({ error: "Image exceeds 512KB server limit" });
  }

  try {
    await queryWorker("UPDATE users SET pfp = ? WHERE username = ?", [pfpBase64, req.user.username]);
    broadcastPFrame("PFP_UPDATED", { username: req.user.username, pfp: pfpBase64 });
    res.json({ message: "Profile picture updated successfully" });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Friends Management (Rate Limit: 5 requests per 1 minute per user)
app.post("/api/friends/request", authenticate, async (req, res) => {
  const limit = checkRateLimit(`freq_${req.user.username}`, 60 * 1000, 5);
  if (limit.limited) {
    return res.status(429).json({ error: `Sending requests too quickly. Please wait ${limit.retryAfterSec}s.` });
  }

  let { targetUsername } = req.body;
  if (!targetUsername) return res.status(400).json({ error: "Target username required" });
  targetUsername = targetUsername.toLowerCase().replace(/[^a-z]/g, "");

  if (targetUsername === req.user.username) return res.status(400).json({ error: "Cannot add yourself" });

  try {
    const checkUser = await queryWorker("SELECT username FROM users WHERE username = ?", [targetUsername]);
    if (!checkUser.results || !checkUser.results.length) return res.status(404).json({ error: "user could not be found" });

    const checkExisting = await queryWorker(
      "SELECT * FROM friends WHERE (user1 = ? AND user2 = ?) OR (user1 = ? AND user2 = ?)",
      [req.user.username, targetUsername, targetUsername, req.user.username]
    );

    if (checkExisting.results && checkExisting.results.length > 0) {
      const rel = checkExisting.results[0];
      if (rel.status === "accepted") return res.status(409).json({ error: "You are already friends with this user" });
      return res.status(409).json({ error: "Friend request already pending" });
    }

    await queryWorker("INSERT INTO friends (user1, user2, status) VALUES (?, ?, 'pending')", [req.user.username, targetUsername]);
    broadcastPFrame("FRIEND_REQUEST_SENT", { from: req.user.username, to: targetUsername }, [targetUsername]);
    res.json({ message: "sent request successfully" });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post("/api/friends/accept", authenticate, async (req, res) => {
  let { targetUsername } = req.body;
  targetUsername = targetUsername.toLowerCase().replace(/[^a-z]/g, "");

  try {
    await queryWorker("UPDATE friends SET status = 'accepted' WHERE user1 = ? AND user2 = ?", [targetUsername, req.user.username]);
    broadcastPFrame("FRIEND_ACCEPTED", { user1: targetUsername, user2: req.user.username }, [req.user.username, targetUsername]);
    res.json({ message: "Accepted friend request" });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post("/api/friends/unfriend", authenticate, async (req, res) => {
  let { targetUsername } = req.body;
  targetUsername = targetUsername.toLowerCase().replace(/[^a-z]/g, "");

  try {
    await queryWorker(
      "DELETE FROM friends WHERE (user1 = ? AND user2 = ?) OR (user1 = ? AND user2 = ?)",
      [req.user.username, targetUsername, targetUsername, req.user.username]
    );
    broadcastPFrame("FRIEND_REMOVED", { user1: req.user.username, user2: targetUsername }, [req.user.username, targetUsername]);
    res.json({ message: `Unfriended @${targetUsername}` });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Group & DM Routes (Group Creation Rate Limit: 2 groups per 5 minutes per user)
app.get("/api/groups/:groupToken/members", authenticate, async (req, res) => {
  try {
    const members = await queryWorker(
      "SELECT gm.username, u.pfp FROM group_members gm LEFT JOIN users u ON gm.username = u.username WHERE gm.group_token = ?",
      [req.params.groupToken]
    );
    res.json(members.results || []);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post("/api/groups/create", authenticate, async (req, res) => {
  const limit = checkRateLimit(`grp_${req.user.username}`, 5 * 60 * 1000, 2);
  if (limit.limited) {
    return res.status(429).json({ error: `You can only create 2 groups every 5 minutes. Try again in ${limit.retryAfterSec}s.` });
  }

  const { groupName } = req.body;
  if (!groupName || !groupName.trim()) return res.status(400).json({ error: "Group name is required" });
  const groupToken = "grp_" + Math.random().toString(36).substring(2, 10);

  try {
    await queryWorker("INSERT INTO groups (group_token, group_name, created_by) VALUES (?, ?, ?)", [groupToken, groupName.trim(), req.user.username]);
    await queryWorker("INSERT INTO group_members (group_token, username, custom_member_token) VALUES (?, ?, ?)", [groupToken, req.user.username, req.user.token]);
    broadcastPFrame("GROUP_CREATED", { group_token: groupToken, group_name: groupName, created_by: req.user.username });
    res.json({ message: "Group created successfully", groupToken });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post("/api/groups/add-member", authenticate, async (req, res) => {
  let { groupToken, targetUsername } = req.body;
  if (!groupToken || !targetUsername) return res.status(400).json({ error: "Group token and target user required" });
  targetUsername = targetUsername.toLowerCase().replace(/[^a-z]/g, "");

  try {
    if (req.user.role !== "admin") {
      const isMember = await queryWorker("SELECT * FROM group_members WHERE group_token = ? AND username = ?", [groupToken, req.user.username]);
      if (!isMember.results || isMember.results.length === 0) {
        return res.status(403).json({ error: "You are not a member of this group" });
      }

      const isFriend = await queryWorker(
        "SELECT * FROM friends WHERE ((user1 = ? AND user2 = ?) OR (user1 = ? AND user2 = ?)) AND status = 'accepted'",
        [req.user.username, targetUsername, targetUsername, req.user.username]
      );

      if (!isFriend.results || isFriend.results.length === 0) {
        return res.status(403).json({ error: `Must be accepted friends with @${targetUsername} to add them to a group` });
      }
    }

    await queryWorker("INSERT OR REPLACE INTO group_members (group_token, username, custom_member_token) VALUES (?, ?, 'DEFAULT')", [groupToken, targetUsername]);
    broadcastPFrame("GROUP_MEMBER_ADDED", { groupToken, targetUsername }, [targetUsername]);
    res.json({ message: `Added @${targetUsername} to group` });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post("/api/groups/leave", authenticate, async (req, res) => {
  const { groupToken } = req.body;
  if (!groupToken) return res.status(400).json({ error: "Group token required" });

  try {
    await queryWorker("DELETE FROM group_members WHERE group_token = ? AND username = ?", [groupToken, req.user.username]);
    broadcastPFrame("GROUP_MEMBER_LEFT", { groupToken, username: req.user.username });
    res.json({ message: "Left group successfully" });
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

// Messaging Engine (Rate Limit: Max 5 messages every 3 seconds per user)
app.post("/api/messages/send", authenticate, async (req, res) => {
  const limit = checkRateLimit(`msg_${req.user.username}`, 3 * 1000, 5);
  if (limit.limited) {
    return res.status(429).json({ error: "You are sending messages too quickly. Please slow down." });
  }

  const { groupToken, content } = req.body;
  if (!groupToken || !content) return res.status(400).json({ error: "Missing message content" });
  const timestamp = Date.now();

  if (content.startsWith("data:image/")) {
    const byteSize = Buffer.byteLength(content, "utf8");
    if (byteSize > 1024 * 1024) {
      return res.status(413).json({ error: "Image attachment exceeds 1MB limit" });
    }
  }

  try {
    await queryWorker("INSERT INTO messages (group_token, sender, content, created_at) VALUES (?, ?, ?, ?)", [groupToken, req.user.username, content, timestamp]);
    const pFrameData = { group_token: groupToken, sender: req.user.username, content, created_at: timestamp, pfp: req.user.pfp || "" };
    broadcastPFrame("NEW_MESSAGE", pFrameData);
    res.json({ message: "Message sent" });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get("/api/messages/:groupToken", authenticate, async (req, res) => {
  try {
    const msgs = await queryWorker(
      "SELECT m.*, u.pfp FROM messages m LEFT JOIN users u ON m.sender = u.username WHERE m.group_token = ? ORDER BY m.created_at ASC",
      [req.params.groupToken]
    );
    res.json(msgs.results || []);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Admin Control Routes
app.get("/api/admin/pending", authenticate, async (req, res) => {
  if (req.user.role !== "admin") return res.status(403).json({ error: "Forbidden" });
  const data = await queryWorker("SELECT * FROM pending_accounts");
  res.json(data.results || []);
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
app.listen(PORT, () => console.log(`Server online on port ${PORT}`));
