const express = require("express");
const cors = require("cors");
const fetch = require("node-fetch");

const app = express();
app.use(express.json());

// CORS configuration for GitHub Pages Frontend
app.use(cors({
  origin: ["https://totallyrandom001.github.io", "http://localhost:3000"],
  methods: ["GET", "POST", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization"]
}));

const CF_WORKER_URL = "https://winter-king-b73e.totallyrandom000148932804.workers.dev";
const WORKER_SECRET = "REPLACE_WITH_SHARED_Backend_SECRET_KEY";

// Helper for Token Calculation (1:1 Interleaving of username & password)
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

// Helper to query Cloudflare Worker
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

// Middleware: Verify Auth Token
async function authenticate(req, res, next) {
  const token = req.headers.authorization;
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

// 1. Account Creation Request
app.post("/api/register", async (req, res) => {
  let { username, password } = req.body;
  if (!username || !password) return res.status(400).json({ error: "Missing fields" });
  
  username = username.toLowerCase().replace(/[^a-z]/g, "");
  if (!username) return res.status(400).json({ error: "Username must contain valid letters" });

  try {
    const pendingCountRes = await queryWorker("SELECT COUNT(*) as count FROM pending_accounts");
    const count = pendingCountRes.results[0].count;
    if (count >= 5) {
      return res.status(429).json({ error: "Max pending creation requests reached (5). Try again later." });
    }

    const token = generateToken(username, password);
    await queryWorker(
      "INSERT INTO pending_accounts (username, password, token) VALUES (?, ?, ?)",
      [username, password, token]
    );

    res.json({ message: "Account creation request submitted for admin approval." });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 2. User Login
app.post("/api/login", async (req, res) => {
  let { username, password } = req.body;
  if (!username || !password) return res.status(400).json({ error: "Missing fields" });

  username = username.toLowerCase().replace(/[^a-z]/g, "");
  const token = generateToken(username, password);

  try {
    const userRes = await queryWorker("SELECT * FROM users WHERE username = ? AND token = ?", [username, token]);
    if (!userRes.results || userRes.results.length === 0) {
      return res.status(401).json({ error: "Invalid credentials or account pending approval." });
    }

    res.json({ token, username: userRes.results[0].username, role: userRes.results[0].role });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 3. Admin: Get Pending Accounts
app.get("/api/admin/pending", authenticate, async (req, res) => {
  if (req.user.role !== "admin") return res.status(403).json({ error: "Forbidden" });
  try {
    const data = await queryWorker("SELECT * FROM pending_accounts");
    res.json(data.results);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 4. Admin: Approve / Dismiss Request
app.post("/api/admin/action", authenticate, async (req, res) => {
  if (req.user.role !== "admin") return res.status(403).json({ error: "Forbidden" });
  const { id, action } = req.body; // action: 'approve' or 'deny'

  try {
    const reqRes = await queryWorker("SELECT * FROM pending_accounts WHERE id = ?", [id]);
    if (!reqRes.results || reqRes.results.length === 0) return res.status(404).json({ error: "Request not found" });

    const item = reqRes.results[0];
    if (action === "approve") {
      await queryWorker("INSERT INTO users (username, token) VALUES (?, ?)", [item.username, item.token]);
    }
    await queryWorker("DELETE FROM pending_accounts WHERE id = ?", [id]);
    res.json({ message: `Account request ${action}d.` });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 5. Admin: Delete User Account
app.post("/api/admin/delete-user", authenticate, async (req, res) => {
  if (req.user.role !== "admin") return res.status(403).json({ error: "Forbidden" });
  const { username } = req.body;

  try {
    await queryWorker("DELETE FROM users WHERE username = ?", [username]);
    await queryWorker("DELETE FROM group_members WHERE username = ?", [username]);
    await queryWorker("DELETE FROM friends WHERE user1 = ? OR user2 = ?", [username, username]);
    res.json({ message: `User ${username} deleted.` });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 6. Get Groups
app.get("/api/groups", authenticate, async (req, res) => {
  try {
    if (req.user.role === "admin") {
      const allGroups = await queryWorker("SELECT * FROM groups");
      return res.json(allGroups.results);
    }
    const myGroups = await queryWorker(
      "SELECT g.* FROM groups g JOIN group_members gm ON g.group_token = gm.group_token WHERE gm.username = ?",
      [req.user.username]
    );
    res.json(myGroups.results);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 7. Create Group
app.post("/api/groups/create", authenticate, async (req, res) => {
  const { groupName, memberToken } = req.body;
  const groupToken = "grp_" + Math.random().toString(36).substring(2, 10);

  try {
    await queryWorker("INSERT INTO groups (group_token, group_name, created_by) VALUES (?, ?, ?)", [groupToken, groupName, req.user.username]);
    await queryWorker("INSERT INTO group_members (group_token, username, custom_member_token) VALUES (?, ?, ?)", [groupToken, req.user.username, memberToken || req.user.token]);
    res.json({ message: "Group created", groupToken });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 8. Join Group
app.post("/api/groups/join", authenticate, async (req, res) => {
  const { groupToken, customMemberToken } = req.body;
  try {
    await queryWorker("INSERT OR REPLACE INTO group_members (group_token, username, custom_member_token) VALUES (?, ?, ?)", [groupToken, req.user.username, customMemberToken || req.user.token]);
    res.json({ message: "Joined group" });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 9. Send & Fetch Messages
app.post("/api/messages/send", authenticate, async (req, res) => {
  const { groupToken, content } = req.body;
  try {
    await queryWorker("INSERT INTO messages (group_token, sender, content, created_at) VALUES (?, ?, ?, ?)", [groupToken, req.user.username, content, Date.now()]);
    res.json({ message: "Sent" });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get("/api/messages/:groupToken", authenticate, async (req, res) => {
  const { groupToken } = req.params;
  try {
    const msgs = await queryWorker("SELECT * FROM messages WHERE group_token = ? ORDER BY created_at ASC", [groupToken]);
    res.json(msgs.results);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
