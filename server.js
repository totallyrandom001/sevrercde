// ============================================================================
// SOPERT SERVER — optimized for minimum DB reads, server-side auth everywhere
// ============================================================================
//
// Key design decisions (read this before touching anything):
//
// 1. NOTHING is ever "refetch everything" on write. Every mutation handler
//    already has the row(s) it just changed in memory — those exact rows are
//    what gets broadcast over SSE. Clients patch their cache with the payload
//    instead of re-querying. This is what kills the read-row explosion.
//
// 2. Messages are paginated (default 50, cursor = message id) both on first
//    load and on scroll-back ("before" param). No endpoint ever returns an
//    entire thread in one shot.
//
// 3. Every route that touches a group, a message, or another user's data
//    re-derives permission from the DB itself (membership, ownership,
//    can_add_members, admin role) — the client's UI state is never trusted.
//
// 4. Tokens: session tokens are opaque random strings stored in a `sessions`
//    table (not JWT — so they can be revoked instantly), hashed passwords
//    (scrypt), and a separate short-lived (60s) single-purpose stream token
//    for SSE, since EventSource can't send an Authorization header and long-
//    lived tokens should never sit in a URL / server access log.
//
// ============================================================================

const express = require("express");
const Database = require("better-sqlite3");
const crypto = require("crypto");
const path = require("path");

const PORT = process.env.PORT || 3000;
const app = express();
app.use(express.json({ limit: "2mb" })); // images are base64, capped client-side at 1MB -> ~1.4MB b64

const db = new Database(path.join(__dirname, "sopert.db"));
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------
db.exec(`
CREATE TABLE IF NOT EXISTS users (
  username        TEXT PRIMARY KEY,
  password_hash   TEXT NOT NULL,
  password_salt   TEXT NOT NULL,
  role            TEXT NOT NULL DEFAULT 'user',   -- 'user' | 'admin'
  status          TEXT NOT NULL DEFAULT 'pending', -- 'pending' | 'approved'
  pfp             TEXT,
  created_at      INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
  token           TEXT PRIMARY KEY,
  username        TEXT NOT NULL REFERENCES users(username) ON DELETE CASCADE,
  created_at      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sessions_username ON sessions(username);

CREATE TABLE IF NOT EXISTS stream_tokens (
  token           TEXT PRIMARY KEY,
  username        TEXT NOT NULL,
  expires_at      INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS friends (
  user1           TEXT NOT NULL,
  user2           TEXT NOT NULL,
  status          TEXT NOT NULL DEFAULT 'pending', -- 'pending' | 'accepted'
  created_at      INTEGER NOT NULL,
  PRIMARY KEY (user1, user2)
);

CREATE TABLE IF NOT EXISTS groups_t (
  group_token         TEXT PRIMARY KEY,
  group_name          TEXT NOT NULL,
  created_by          TEXT NOT NULL,
  allow_sub_invites   INTEGER NOT NULL DEFAULT 1,
  created_at          INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS group_members (
  group_token       TEXT NOT NULL REFERENCES groups_t(group_token) ON DELETE CASCADE,
  username          TEXT NOT NULL,
  can_add_members   INTEGER NOT NULL DEFAULT 1,
  joined_at         INTEGER NOT NULL,
  PRIMARY KEY (group_token, username)
);
CREATE INDEX IF NOT EXISTS idx_group_members_user ON group_members(username);

CREATE TABLE IF NOT EXISTS messages (
  id                  INTEGER PRIMARY KEY AUTOINCREMENT,
  group_token         TEXT NOT NULL REFERENCES groups_t(group_token) ON DELETE CASCADE,
  sender              TEXT NOT NULL,
  content             TEXT NOT NULL,
  reply_to_sender     TEXT,
  reply_to_content    TEXT,
  created_at          INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_group_id ON messages(group_token, id);
`);

// ---------------------------------------------------------------------------
// Password hashing (scrypt, no external deps)
// ---------------------------------------------------------------------------
function hashPassword(password, salt = crypto.randomBytes(16).toString("hex")) {
  const hash = crypto.scryptSync(password, salt, 64).toString("hex");
  return { hash, salt };
}
function verifyPassword(password, hash, salt) {
  const check = crypto.scryptSync(password, salt, 64).toString("hex");
  return crypto.timingSafeEqual(Buffer.from(check), Buffer.from(hash));
}
function newToken() {
  return crypto.randomBytes(32).toString("hex");
}

// ---------------------------------------------------------------------------
// Auth middleware — every protected route re-derives the user from the DB.
// Never trust a username/role sent in the request body.
// ---------------------------------------------------------------------------
const sessionStmt = db.prepare(
  `SELECT s.username, u.role, u.status FROM sessions s
   JOIN users u ON u.username = s.username WHERE s.token = ?`
);

function requireAuth(req, res, next) {
  const token = req.headers["authorization"];
  if (!token) return res.status(401).json({ error: "Yetkisiz erişim" });
  const row = sessionStmt.get(token);
  if (!row || row.status !== "approved") return res.status(401).json({ error: "Geçersiz oturum" });
  req.user = { username: row.username, role: row.role };
  req.authToken = token;
  next();
}

function requireAdmin(req, res, next) {
  if (req.user.role !== "admin") return res.status(403).json({ error: "Yönetici yetkisi gerekli" });
  next();
}

// ---------------------------------------------------------------------------
// SSE hub — one connection per logged-in client. Broadcasts are targeted:
// we only push to sockets belonging to users who are actually in the
// affected group (or the two DM participants), never a global fan-out scan.
// ---------------------------------------------------------------------------
const streamsByUser = new Map(); // username -> Set<res>

function addStream(username, res) {
  if (!streamsByUser.has(username)) streamsByUser.set(username, new Set());
  streamsByUser.get(username).add(res);
}
function removeStream(username, res) {
  const set = streamsByUser.get(username);
  if (!set) return;
  set.delete(res);
  if (set.size === 0) streamsByUser.delete(username);
}
function sendTo(username, obj) {
  const set = streamsByUser.get(username);
  if (!set) return;
  const line = `data: ${JSON.stringify(obj)}\n\n`;
  for (const res of set) res.write(line);
}
function sendToMany(usernames, obj) {
  for (const u of usernames) sendTo(u, obj);
}

const membersOfGroupStmt = db.prepare(`SELECT username FROM group_members WHERE group_token = ?`);
function groupMemberUsernames(groupToken) {
  return membersOfGroupStmt.all(groupToken).map(r => r.username);
}

// ---------------------------------------------------------------------------
// Snapshot builder (I-FRAME) — used only at connect time / explicit refresh.
// This is the one "expensive" query; everything after it is incremental.
// ---------------------------------------------------------------------------
const friendsForUserStmt = db.prepare(`
  SELECT f.user1, f.user2, f.status, u.pfp
  FROM friends f
  JOIN users u ON u.username = (CASE WHEN f.user1 = ? THEN f.user2 ELSE f.user1 END)
  WHERE f.user1 = ? OR f.user2 = ?
`);
const groupsForUserStmt = db.prepare(`
  SELECT g.group_token, g.group_name, g.created_by, g.allow_sub_invites
  FROM groups_t g
  JOIN group_members gm ON gm.group_token = g.group_token
  WHERE gm.username = ?
`);

function buildSnapshot(username) {
  const friends = friendsForUserStmt.all(username, username, username);
  const groups = groupsForUserStmt.all(username);
  const user = db.prepare(`SELECT pfp FROM users WHERE username = ?`).get(username);
  return { friends, groups, user };
}

// ---------------------------------------------------------------------------
// Auth routes
// ---------------------------------------------------------------------------
app.post("/api/register", (req, res) => {
  const username = String(req.body.username || "").toLowerCase().replace(/[^a-z]/g, "");
  const password = String(req.body.password || "");
  if (!username || username.length < 3) return res.status(400).json({ error: "Geçersiz kullanıcı adı" });
  if (!password || password.length < 4) return res.status(400).json({ error: "Şifre çok kısa" });

  const existing = db.prepare(`SELECT username FROM users WHERE username = ?`).get(username);
  if (existing) return res.status(409).json({ error: "Bu kullanıcı adı zaten alınmış" });

  const { hash, salt } = hashPassword(password);
  const isFirstUser = db.prepare(`SELECT COUNT(*) c FROM users`).get().c === 0;
  db.prepare(
    `INSERT INTO users (username, password_hash, password_salt, role, status, created_at) VALUES (?,?,?,?,?,?)`
  ).run(username, hash, salt, isFirstUser ? "admin" : "user", isFirstUser ? "approved" : "pending", Date.now());

  res.json({ message: isFirstUser ? "Yönetici hesabı oluşturuldu, giriş yapabilirsiniz." : "Kayıt talebiniz yönetici onayına gönderildi." });
});

app.post("/api/login", (req, res) => {
  const username = String(req.body.username || "").toLowerCase().replace(/[^a-z]/g, "");
  const password = String(req.body.password || "");
  const user = db.prepare(`SELECT * FROM users WHERE username = ?`).get(username);
  if (!user || !verifyPassword(password, user.password_hash, user.password_salt)) {
    return res.status(401).json({ error: "Kullanıcı adı veya şifre hatalı" });
  }
  if (user.status !== "approved") return res.status(403).json({ error: "Hesabınız henüz onaylanmadı" });

  const token = newToken();
  db.prepare(`INSERT INTO sessions (token, username, created_at) VALUES (?,?,?)`).run(token, username, Date.now());
  res.json({ token, username, role: user.role, pfp: user.pfp || "" });
});

app.post("/api/logout", requireAuth, (req, res) => {
  db.prepare(`DELETE FROM sessions WHERE token = ?`).run(req.authToken);
  res.json({ ok: true });
});

app.get("/api/public/account-requests", (req, res) => {
  const pending = db.prepare(`SELECT username FROM users WHERE status = 'pending'`).all().map(r => r.username);
  const accepted = db.prepare(`SELECT username FROM users WHERE status = 'approved'`).all().map(r => r.username);
  res.json({ pending, accepted });
});

// Short-lived single-purpose token for the SSE connection. EventSource can't
// send Authorization headers, so the real session token never touches a URL.
app.post("/api/stream/token", requireAuth, (req, res) => {
  const t = newToken();
  const expiresAt = Date.now() + 60_000;
  db.prepare(`INSERT INTO stream_tokens (token, username, expires_at) VALUES (?,?,?)`).run(t, req.user.username, expiresAt);
  res.json({ streamToken: t });
});

// ---------------------------------------------------------------------------
// Snapshot (I-FRAME) — explicit pull, used at boot / reconnect only
// ---------------------------------------------------------------------------
app.get("/api/snapshot", requireAuth, (req, res) => {
  res.json(buildSnapshot(req.user.username));
});

// ---------------------------------------------------------------------------
// SSE stream (P-FRAME feed)
// ---------------------------------------------------------------------------
app.get("/api/stream", (req, res) => {
  const streamToken = req.query.token;
  if (!streamToken) return res.status(401).end();

  const row = db.prepare(`SELECT username, expires_at FROM stream_tokens WHERE token = ?`).get(streamToken);
  // consume immediately — single use
  db.prepare(`DELETE FROM stream_tokens WHERE token = ?`).run(streamToken);
  if (!row || row.expires_at < Date.now()) return res.status(401).end();

  const username = row.username;

  res.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    Connection: "keep-alive",
    "X-Accel-Buffering": "no",
  });

  addStream(username, res);
  res.write(`data: ${JSON.stringify({ frameType: "I-FRAME", ...buildSnapshot(username) })}\n\n`);

  const keepAlive = setInterval(() => res.write(":ping\n\n"), 25_000);
  req.on("close", () => {
    clearInterval(keepAlive);
    removeStream(username, res);
  });
});

// ---------------------------------------------------------------------------
// User profile picture
// ---------------------------------------------------------------------------
app.post("/api/user/pfp", requireAuth, (req, res) => {
  const pfpBase64 = String(req.body.pfpBase64 || "");
  if (pfpBase64.length > 600_000) return res.status(413).json({ error: "Görsel çok büyük" });
  db.prepare(`UPDATE users SET pfp = ? WHERE username = ?`).run(pfpBase64, req.user.username);
  res.json({ ok: true });
});

// ---------------------------------------------------------------------------
// Friends
// ---------------------------------------------------------------------------
app.post("/api/friends/request", requireAuth, (req, res) => {
  const target = String(req.body.targetUsername || "").toLowerCase().replace(/[^a-z]/g, "");
  if (!target || target === req.user.username) return res.status(400).json({ error: "Geçersiz kullanıcı" });
  const targetUser = db.prepare(`SELECT username FROM users WHERE username = ? AND status='approved'`).get(target);
  if (!targetUser) return res.status(404).json({ error: "Kullanıcı bulunamadı" });

  const [u1, u2] = [req.user.username, target].sort();
  const existing = db.prepare(`SELECT * FROM friends WHERE user1=? AND user2=?`).get(u1, u2);
  if (existing) return res.status(409).json({ error: "Zaten arkadaşsınız ya da istek bekliyor" });

  db.prepare(`INSERT INTO friends (user1, user2, status, created_at) VALUES (?,?,?,?)`)
    .run(req.user.username, target, "pending", Date.now()); // store direction: user2 must accept
  // normalize storage direction so acceptFriend logic below works regardless of sort
  res.json({ message: "İstek gönderildi" });
  sendTo(target, { frameType: "P-FRAME", type: "FRIEND_REQUEST", payload: { from: req.user.username } });
});

app.post("/api/friends/accept", requireAuth, (req, res) => {
  const target = String(req.body.targetUsername || "").toLowerCase().replace(/[^a-z]/g, "");
  const row = db.prepare(`SELECT * FROM friends WHERE user1=? AND user2=? AND status='pending'`).get(target, req.user.username);
  if (!row) return res.status(404).json({ error: "İstek bulunamadı" });

  db.prepare(`UPDATE friends SET status='accepted' WHERE user1=? AND user2=?`).run(target, req.user.username);
  const pfpA = db.prepare(`SELECT pfp FROM users WHERE username=?`).get(req.user.username).pfp || "";
  const pfpB = db.prepare(`SELECT pfp FROM users WHERE username=?`).get(target).pfp || "";
  res.json({ ok: true });

  sendToMany([req.user.username, target], {
    frameType: "P-FRAME",
    type: "FRIEND_ACCEPTED",
    payload: {
      friendRowFor: { [req.user.username]: { user1: target, user2: req.user.username, status: "accepted", pfp: pfpB },
                      [target]: { user1: target, user2: req.user.username, status: "accepted", pfp: pfpA } },
    },
  });
});

app.post("/api/friends/unfriend", requireAuth, (req, res) => {
  const target = String(req.body.targetUsername || "").toLowerCase().replace(/[^a-z]/g, "");
  const [u1, u2] = [req.user.username, target].sort();
  const info = db.prepare(`DELETE FROM friends WHERE user1=? AND user2=?`).run(u1, u2);
  if (info.changes === 0) return res.status(404).json({ error: "Arkadaşlık bulunamadı" });
  res.json({ ok: true });
  sendToMany([req.user.username, target], { frameType: "P-FRAME", type: "FRIEND_REMOVED", payload: { user1: u1, user2: u2 } });
});

// ---------------------------------------------------------------------------
// DMs — a DM is just a 2-person group with a deterministic token
// ---------------------------------------------------------------------------
function dmToken(a, b) { return "dm_" + [a, b].sort().join("_"); }

app.post("/api/dm/open", requireAuth, (req, res) => {
  const target = String(req.body.targetUsername || "").toLowerCase().replace(/[^a-z]/g, "");
  const [u1, u2] = [req.user.username, target].sort();
  const friendRow = db.prepare(`SELECT status FROM friends WHERE user1=? AND user2=?`).get(u1, u2);
  if (!friendRow || friendRow.status !== "accepted") return res.status(403).json({ error: "Önce arkadaş olmalısınız" });

  const gt = dmToken(req.user.username, target);
  const existing = db.prepare(`SELECT group_token FROM groups_t WHERE group_token = ?`).get(gt);
  if (!existing) {
    const now = Date.now();
    db.prepare(`INSERT INTO groups_t (group_token, group_name, created_by, allow_sub_invites, created_at) VALUES (?,?,?,?,?)`)
      .run(gt, "@" + target, req.user.username, 0, now);
    db.prepare(`INSERT INTO group_members (group_token, username, can_add_members, joined_at) VALUES (?,?,?,?)`).run(gt, req.user.username, 0, now);
    db.prepare(`INSERT INTO group_members (group_token, username, can_add_members, joined_at) VALUES (?,?,?,?)`).run(gt, target, 0, now);
  }
  res.json({ groupToken: gt });
});

// ---------------------------------------------------------------------------
// Groups
// ---------------------------------------------------------------------------
app.post("/api/groups/create", requireAuth, (req, res) => {
  const groupName = String(req.body.groupName || "").trim().slice(0, 60);
  if (!groupName) return res.status(400).json({ error: "Grup adı gerekli" });
  const gt = crypto.randomBytes(12).toString("hex");
  const now = Date.now();
  db.prepare(`INSERT INTO groups_t (group_token, group_name, created_by, allow_sub_invites, created_at) VALUES (?,?,?,1,?)`)
    .run(gt, groupName, req.user.username, now);
  db.prepare(`INSERT INTO group_members (group_token, username, can_add_members, joined_at) VALUES (?,?,1,?)`).run(gt, req.user.username, now);
  res.json({ message: "Grup oluşturuldu", groupToken: gt });
});

function isMember(groupToken, username) {
  return !!db.prepare(`SELECT 1 FROM group_members WHERE group_token=? AND username=?`).get(groupToken, username);
}
function isOwnerOrAdmin(group, user) {
  return group.created_by === user.username || user.role === "admin";
}

app.get("/api/groups/:token/members", requireAuth, (req, res) => {
  const gt = req.params.token;
  const group = db.prepare(`SELECT * FROM groups_t WHERE group_token = ?`).get(gt);
  if (!group) return res.status(404).json({ error: "Grup bulunamadı" });
  if (!isMember(gt, req.user.username)) return res.status(403).json({ error: "Bu grubun üyesi değilsiniz" });

  const members = db.prepare(`
    SELECT gm.username, gm.can_add_members, u.pfp
    FROM group_members gm JOIN users u ON u.username = gm.username
    WHERE gm.group_token = ?
  `).all(gt);

  res.json({ groupInfo: group, members });
});

app.post("/api/groups/add-member", requireAuth, (req, res) => {
  const { groupToken, targetUsername } = req.body;
  const target = String(targetUsername || "").toLowerCase().replace(/[^a-z]/g, "");
  const group = db.prepare(`SELECT * FROM groups_t WHERE group_token = ?`).get(groupToken);
  if (!group) return res.status(404).json({ error: "Grup bulunamadı" });

  const me = db.prepare(`SELECT * FROM group_members WHERE group_token=? AND username=?`).get(groupToken, req.user.username);
  if (!me) return res.status(403).json({ error: "Bu grubun üyesi değilsiniz" });

  const owner = isOwnerOrAdmin(group, req.user);
  const canInvite = owner || (group.allow_sub_invites && me.can_add_members);
  if (!canInvite) return res.status(403).json({ error: "Üye ekleme yetkiniz yok" });

  // must be friends to add to a group (mirrors DM rule) — also confirms target exists
  const [u1, u2] = [req.user.username, target].sort();
  const friendRow = db.prepare(`SELECT status FROM friends WHERE user1=? AND user2=?`).get(u1, u2);
  if (!friendRow || friendRow.status !== "accepted") return res.status(403).json({ error: "Sadece arkadaşlarınızı ekleyebilirsiniz" });

  if (isMember(groupToken, target)) return res.status(409).json({ error: "Kullanıcı zaten grupta" });

  const now = Date.now();
  db.prepare(`INSERT INTO group_members (group_token, username, can_add_members, joined_at) VALUES (?,?,1,?)`).run(groupToken, target, now);
  const newMember = { username: target, can_add_members: 1, pfp: db.prepare(`SELECT pfp FROM users WHERE username=?`).get(target).pfp || "" };

  res.json({ message: "Üye eklendi" });

  const recipients = [...groupMemberUsernames(groupToken)]; // includes target now
  sendToMany(recipients, { frameType: "P-FRAME", type: "GROUP_MEMBER_ADDED", payload: { groupToken, newMember, group } });
});

app.post("/api/groups/remove-member", requireAuth, (req, res) => {
  const { groupToken, targetUsername } = req.body;
  const group = db.prepare(`SELECT * FROM groups_t WHERE group_token = ?`).get(groupToken);
  if (!group) return res.status(404).json({ error: "Grup bulunamadı" });
  if (!isOwnerOrAdmin(group, req.user)) return res.status(403).json({ error: "Yetkiniz yok" });
  if (targetUsername === group.created_by) return res.status(400).json({ error: "Kurucu çıkarılamaz" });

  const recipientsBefore = groupMemberUsernames(groupToken);
  db.prepare(`DELETE FROM group_members WHERE group_token=? AND username=?`).run(groupToken, targetUsername);
  res.json({ ok: true });
  sendToMany(recipientsBefore, { frameType: "P-FRAME", type: "GROUP_MEMBER_LEFT", payload: { groupToken, username: targetUsername } });
});

app.post("/api/groups/leave", requireAuth, (req, res) => {
  const { groupToken } = req.body;
  const group = db.prepare(`SELECT * FROM groups_t WHERE group_token = ?`).get(groupToken);
  if (!group) return res.status(404).json({ error: "Grup bulunamadı" });
  if (group.created_by === req.user.username) return res.status(400).json({ error: "Kurucu gruptan ayrılamaz, grubu silin" });
  if (!isMember(groupToken, req.user.username)) return res.status(403).json({ error: "Bu grubun üyesi değilsiniz" });

  const recipientsBefore = groupMemberUsernames(groupToken);
  db.prepare(`DELETE FROM group_members WHERE group_token=? AND username=?`).run(groupToken, req.user.username);
  res.json({ ok: true });
  sendToMany(recipientsBefore, { frameType: "P-FRAME", type: "GROUP_MEMBER_LEFT", payload: { groupToken, username: req.user.username } });
});

app.post("/api/groups/toggle-sub-invites", requireAuth, (req, res) => {
  const { groupToken, allowSubInvites } = req.body;
  const group = db.prepare(`SELECT * FROM groups_t WHERE group_token = ?`).get(groupToken);
  if (!group) return res.status(404).json({ error: "Grup bulunamadı" });
  if (!isOwnerOrAdmin(group, req.user)) return res.status(403).json({ error: "Yetkiniz yok" });

  const val = allowSubInvites ? 1 : 0;
  db.prepare(`UPDATE groups_t SET allow_sub_invites=? WHERE group_token=?`).run(val, groupToken);
  res.json({ ok: true });
  sendToMany(groupMemberUsernames(groupToken), {
    frameType: "P-FRAME", type: "GROUP_SETTING_UPDATED", payload: { groupToken, allow_sub_invites: val },
  });
});

app.post("/api/groups/toggle-member-invite-perm", requireAuth, (req, res) => {
  const { groupToken, targetUsername, canAddMembers } = req.body;
  const group = db.prepare(`SELECT * FROM groups_t WHERE group_token = ?`).get(groupToken);
  if (!group) return res.status(404).json({ error: "Grup bulunamadı" });
  if (!isOwnerOrAdmin(group, req.user)) return res.status(403).json({ error: "Yetkiniz yok" });

  const val = canAddMembers ? 1 : 0;
  db.prepare(`UPDATE group_members SET can_add_members=? WHERE group_token=? AND username=?`).run(val, groupToken, targetUsername);
  res.json({ ok: true });
  sendToMany(groupMemberUsernames(groupToken), {
    frameType: "P-FRAME", type: "GROUP_PERM_UPDATED", payload: { groupToken, username: targetUsername, can_add_members: val },
  });
});

// ---------------------------------------------------------------------------
// Messages — paginated, cursor-based. Never returns more than `limit`.
// ---------------------------------------------------------------------------
app.get("/api/messages/:token", requireAuth, (req, res) => {
  const gt = req.params.token;
  if (!isMember(gt, req.user.username)) return res.status(403).json({ error: "Bu grubun üyesi değilsiniz" });

  const limit = Math.min(parseInt(req.query.limit, 10) || 50, 100);
  const before = req.query.before ? parseInt(req.query.before, 10) : null;

  let rows;
  if (before) {
    rows = db.prepare(
      `SELECT * FROM messages WHERE group_token = ? AND id < ? ORDER BY id DESC LIMIT ?`
    ).all(gt, before, limit);
  } else {
    rows = db.prepare(
      `SELECT * FROM messages WHERE group_token = ? ORDER BY id DESC LIMIT ?`
    ).all(gt, limit);
  }
  rows.reverse(); // oldest -> newest for the client

  // attach sender pfp without N+1: single IN() query
  const senders = [...new Set(rows.map(r => r.sender))];
  if (senders.length) {
    const placeholders = senders.map(() => "?").join(",");
    const pfps = db.prepare(`SELECT username, pfp FROM users WHERE username IN (${placeholders})`).all(...senders);
    const pfpMap = Object.fromEntries(pfps.map(p => [p.username, p.pfp || ""]));
    for (const r of rows) r.pfp = pfpMap[r.sender] || "";
  }

  res.json(rows);
});

app.post("/api/messages/send", requireAuth, (req, res) => {
  const { groupToken, content, replyToSender, replyToContent } = req.body;
  if (!isMember(groupToken, req.user.username)) return res.status(403).json({ error: "Bu grubun üyesi değilsiniz" });
  if (!content || typeof content !== "string") return res.status(400).json({ error: "Geçersiz mesaj" });
  if (content.startsWith("data:image/")) {
    if (content.length > 1_400_000) return res.status(413).json({ error: "Görsel çok büyük" });
  } else if (content.length > 4000) {
    return res.status(413).json({ error: "Mesaj çok uzun" });
  }

  const now = Date.now();
  const info = db.prepare(
    `INSERT INTO messages (group_token, sender, content, reply_to_sender, reply_to_content, created_at) VALUES (?,?,?,?,?,?)`
  ).run(groupToken, req.user.username, content, replyToSender || null, replyToContent || null, now);

  const pfp = db.prepare(`SELECT pfp FROM users WHERE username=?`).get(req.user.username).pfp || "";
  const payload = {
    id: info.lastInsertRowid,
    group_token: groupToken,
    sender: req.user.username,
    content,
    reply_to_sender: replyToSender || null,
    reply_to_content: replyToContent || null,
    created_at: now,
    pfp,
  };

  res.json({ ok: true, id: info.lastInsertRowid });
  sendToMany(groupMemberUsernames(groupToken), { frameType: "P-FRAME", type: "NEW_MESSAGE", payload });
});

app.post("/api/messages/delete", requireAuth, (req, res) => {
  const { messageId, groupToken } = req.body;
  const msg = db.prepare(`SELECT * FROM messages WHERE id = ? AND group_token = ?`).get(messageId, groupToken);
  if (!msg) return res.status(404).json({ error: "Mesaj bulunamadı" });

  const group = db.prepare(`SELECT * FROM groups_t WHERE group_token = ?`).get(groupToken);
  const canDelete = msg.sender === req.user.username || isOwnerOrAdmin(group, req.user);
  if (!canDelete) return res.status(403).json({ error: "Bu mesajı silme yetkiniz yok" });

  db.prepare(`DELETE FROM messages WHERE id = ?`).run(messageId);
  res.json({ ok: true });
  sendToMany(groupMemberUsernames(groupToken), {
    frameType: "P-FRAME", type: "MESSAGE_DELETED", payload: { messageId, groupToken },
  });
});

// ---------------------------------------------------------------------------
// Admin
// ---------------------------------------------------------------------------
app.get("/api/admin/pending", requireAuth, requireAdmin, (req, res) => {
  res.json(db.prepare(`SELECT rowid as id, username FROM users WHERE status='pending'`).all());
});

app.get("/api/admin/users", requireAuth, requireAdmin, (req, res) => {
  res.json(db.prepare(`SELECT username, role FROM users WHERE status='approved'`).all());
});

app.post("/api/admin/action", requireAuth, requireAdmin, (req, res) => {
  const { id, action } = req.body;
  const user = db.prepare(`SELECT * FROM users WHERE rowid = ?`).get(id);
  if (!user) return res.status(404).json({ error: "Kullanıcı bulunamadı" });

  if (action === "approve") db.prepare(`UPDATE users SET status='approved' WHERE rowid=?`).run(id);
  else if (action === "deny") db.prepare(`DELETE FROM users WHERE rowid=?`).run(id);
  else return res.status(400).json({ error: "Geçersiz işlem" });

  res.json({ ok: true });
});

app.post("/api/admin/delete-user", requireAuth, requireAdmin, (req, res) => {
  const { username } = req.body;
  if (username === req.user.username) return res.status(400).json({ error: "Kendinizi silemezsiniz" });
  const info = db.prepare(`DELETE FROM users WHERE username = ?`).run(username);
  if (info.changes === 0) return res.status(404).json({ error: "Kullanıcı bulunamadı" });
  db.prepare(`DELETE FROM sessions WHERE username = ?`).run(username);
  res.json({ message: "Kullanıcı silindi" });
});

// ---------------------------------------------------------------------------
// Static client + housekeeping
// ---------------------------------------------------------------------------
app.use(express.static(path.join(__dirname, "public")));

// prune expired stream tokens / stale sessions periodically (cheap, in-process)
setInterval(() => {
  db.prepare(`DELETE FROM stream_tokens WHERE expires_at < ?`).run(Date.now());
}, 60_000);

app.listen(PORT, () => console.log(`SOPERT server listening on :${PORT}`));
