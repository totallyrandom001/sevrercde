// ============================================================================
// SOPERT SERVER — now a thin HTTP layer in front of the Cloudflare Worker.
//
// IMPORTANT: This server no longer touches a local SQLite file. All reads
// and writes go over HTTPS to your Cloudflare Worker's /query endpoint,
// which itself talks to two D1 databases via the `DB` and `imgdb` bindings.
// That Worker is the single source of truth. If Render restarts / redeploys,
// nothing is lost, because nothing was ever stored on Render's disk.
//
// Env vars required on Render:
//   WORKER_URL     e.g. https://your-worker-subdomain.workers.dev
//   WORKER_SECRET  must match env.WORKER_SECRET set on the Worker
//
// Everything else (sessions, SSE hub, permission checks) behaves the same
// as before, just async now because every query is a network round trip.
// ============================================================================

const express = require("express");
const crypto = require("crypto");
const path = require("path");

const PORT = process.env.PORT || 3000;
const WORKER_URL = (process.env.WORKER_URL || "").replace(/\/+$/, "");
const WORKER_SECRET = process.env.WORKER_SECRET || "";

if (!WORKER_URL || !WORKER_SECRET) {
  console.error("FATAL: WORKER_URL and WORKER_SECRET env vars must be set (Render dashboard -> Environment).");
  process.exit(1);
}

const app = express();

// ---------------------------------------------------------------------------
// CORS
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

app.use(express.json({ limit: "2mb" }));

// ---------------------------------------------------------------------------
// Worker query client — every DB operation in this file goes through here.
// targetDb: "DB" (main) or "imgdb" (images).
// ---------------------------------------------------------------------------
async function workerQuery(sql, params = [], targetDb = "DB") {
  const res = await fetch(`${WORKER_URL}/query`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Worker-Secret": WORKER_SECRET },
    body: JSON.stringify({ sql, params, targetDb }),
  });
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch { throw new Error(`Worker returned non-JSON (${res.status}): ${text.slice(0, 300)}`); }
  if (!res.ok || data.error) throw new Error(data.error || `Worker query failed (${res.status})`);
  return data; // { results, success, meta }
}
async function dbAll(sql, params = [], targetDb = "DB") {
  const r = await workerQuery(sql, params, targetDb);
  return r.results || [];
}
async function dbGet(sql, params = [], targetDb = "DB") {
  const rows = await dbAll(sql, params, targetDb);
  return rows[0] || null;
}
async function dbRun(sql, params = [], targetDb = "DB") {
  const r = await workerQuery(sql, params, targetDb);
  const meta = r.meta || {};
  return { changes: meta.changes ?? 0, lastInsertRowid: meta.last_row_id ?? meta.lastRowId ?? null };
}

// ---------------------------------------------------------------------------
// Schema — D1 doesn't like multi-statement .prepare(), so each statement is
// sent as its own /query call. Safe to run on every boot (IF NOT EXISTS).
// ---------------------------------------------------------------------------
const MAIN_SCHEMA = [
  `CREATE TABLE IF NOT EXISTS users (
    username        TEXT PRIMARY KEY,
    password_hash   TEXT NOT NULL,
    password_salt   TEXT NOT NULL,
    role            TEXT NOT NULL DEFAULT 'user',
    status          TEXT NOT NULL DEFAULT 'pending',
    pfp             TEXT,
    created_at      INTEGER NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS sessions (
    token           TEXT PRIMARY KEY,
    username        TEXT NOT NULL,
    created_at      INTEGER NOT NULL
  )`,
  `CREATE INDEX IF NOT EXISTS idx_sessions_username ON sessions(username)`,
  `CREATE TABLE IF NOT EXISTS stream_tokens (
    token           TEXT PRIMARY KEY,
    username        TEXT NOT NULL,
    expires_at      INTEGER NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS friends (
    user1           TEXT NOT NULL,
    user2           TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    created_at      INTEGER NOT NULL,
    PRIMARY KEY (user1, user2)
  )`,
  `CREATE TABLE IF NOT EXISTS groups_t (
    group_token         TEXT PRIMARY KEY,
    group_name          TEXT NOT NULL,
    created_by          TEXT NOT NULL,
    allow_sub_invites   INTEGER NOT NULL DEFAULT 1,
    created_at          INTEGER NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS group_members (
    group_token       TEXT NOT NULL,
    username          TEXT NOT NULL,
    can_add_members   INTEGER NOT NULL DEFAULT 1,
    joined_at         INTEGER NOT NULL,
    PRIMARY KEY (group_token, username)
  )`,
  `CREATE INDEX IF NOT EXISTS idx_group_members_user ON group_members(username)`,
  `CREATE TABLE IF NOT EXISTS messages (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    group_token         TEXT NOT NULL,
    sender              TEXT NOT NULL,
    content             TEXT NOT NULL,
    reply_to_sender     TEXT,
    reply_to_content    TEXT,
    created_at          INTEGER NOT NULL
  )`,
  `CREATE INDEX IF NOT EXISTS idx_messages_group_id ON messages(group_token, id)`,
];

const IMG_SCHEMA = [
  `CREATE TABLE IF NOT EXISTS images (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    data         TEXT NOT NULL,
    created_at   INTEGER NOT NULL
  )`,
];

async function initSchema() {
  for (const stmt of MAIN_SCHEMA) await workerQuery(stmt, [], "DB");
  for (const stmt of IMG_SCHEMA) await workerQuery(stmt, [], "imgdb");
  console.log("Schema OK on Worker/D1.");
}

// ---------------------------------------------------------------------------
// Password hashing
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

// small helper so every async route reports errors instead of hanging/crashing
function ah(fn) {
  return (req, res) => fn(req, res).catch((err) => {
    console.error(err);
    if (!res.headersSent) res.status(500).json({ error: "Sunucu hatası: " + err.message });
  });
}

// ---------------------------------------------------------------------------
// Auth middleware
// ---------------------------------------------------------------------------
async function requireAuth(req, res, next) {
  try {
    const token = req.headers["authorization"];
    if (!token) return res.status(401).json({ error: "Yetkisiz erişim" });
    const row = await dbGet(
      `SELECT s.username as username, u.role as role, u.status as status
       FROM sessions s JOIN users u ON u.username = s.username WHERE s.token = ?`,
      [token]
    );
    if (!row || row.status !== "approved") return res.status(401).json({ error: "Geçersiz oturum" });
    req.user = { username: row.username, role: row.role };
    req.authToken = token;
    next();
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Sunucu hatası: " + err.message });
  }
}
function requireAdmin(req, res, next) {
  if (req.user.role !== "admin") return res.status(403).json({ error: "Yönetici yetkisi gerekli" });
  next();
}

// ---------------------------------------------------------------------------
// SSE hub — unchanged, purely in-memory per Render instance
// ---------------------------------------------------------------------------
const streamsByUser = new Map();
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
async function groupMemberUsernames(groupToken) {
  const rows = await dbAll(`SELECT username FROM group_members WHERE group_token = ?`, [groupToken]);
  return rows.map((r) => r.username);
}

// ---------------------------------------------------------------------------
// Snapshot builder
// ---------------------------------------------------------------------------
async function buildSnapshot(username) {
  const friends = await dbAll(
    `SELECT f.user1 as user1, f.user2 as user2, f.status as status, u.pfp as pfp
     FROM friends f
     JOIN users u ON u.username = (CASE WHEN f.user1 = ? THEN f.user2 ELSE f.user1 END)
     WHERE f.user1 = ? OR f.user2 = ?`,
    [username, username, username]
  );
  const groups = await dbAll(
    `SELECT g.group_token as group_token, g.group_name as group_name, g.created_by as created_by, g.allow_sub_invites as allow_sub_invites
     FROM groups_t g JOIN group_members gm ON gm.group_token = g.group_token WHERE gm.username = ?`,
    [username]
  );
  const user = await dbGet(`SELECT pfp FROM users WHERE username = ?`, [username]);
  return { friends, groups, user };
}

// ---------------------------------------------------------------------------
// Auth routes
// ---------------------------------------------------------------------------
app.post("/api/register", ah(async (req, res) => {
  const username = String(req.body.username || "").toLowerCase().replace(/[^a-z]/g, "");
  const password = String(req.body.password || "");
  if (!username || username.length < 3) return res.status(400).json({ error: "Geçersiz kullanıcı adı" });
  if (!password || password.length < 4) return res.status(400).json({ error: "Şifre çok kısa" });

  const existing = await dbGet(`SELECT username FROM users WHERE username = ?`, [username]);
  if (existing) return res.status(409).json({ error: "Bu kullanıcı adı zaten alınmış" });

  const { hash, salt } = hashPassword(password);
  const countRow = await dbGet(`SELECT COUNT(*) as c FROM users`);
  const isFirstUser = (countRow?.c || 0) === 0;
  await dbRun(
    `INSERT INTO users (username, password_hash, password_salt, role, status, created_at) VALUES (?,?,?,?,?,?)`,
    [username, hash, salt, isFirstUser ? "admin" : "user", isFirstUser ? "approved" : "pending", Date.now()]
  );

  res.json({ message: isFirstUser ? "Yönetici hesabı oluşturuldu, giriş yapabilirsiniz." : "Kayıt talebiniz yönetici onayına gönderildi." });
}));

app.post("/api/login", ah(async (req, res) => {
  const username = String(req.body.username || "").toLowerCase().replace(/[^a-z]/g, "");
  const password = String(req.body.password || "");
  const user = await dbGet(`SELECT * FROM users WHERE username = ?`, [username]);
  if (!user || !verifyPassword(password, user.password_hash, user.password_salt)) {
    return res.status(401).json({ error: "Kullanıcı adı veya şifre hatalı" });
  }
  if (user.status !== "approved") return res.status(403).json({ error: "Hesabınız henüz onaylanmadı" });

  const token = newToken();
  await dbRun(`INSERT INTO sessions (token, username, created_at) VALUES (?,?,?)`, [token, username, Date.now()]);
  res.json({ token, username, role: user.role, pfp: user.pfp || "" });
}));

app.post("/api/logout", requireAuth, ah(async (req, res) => {
  await dbRun(`DELETE FROM sessions WHERE token = ?`, [req.authToken]);
  res.json({ ok: true });
}));

app.get("/api/public/account-requests", ah(async (req, res) => {
  const pendingRows = await dbAll(`SELECT username FROM users WHERE status = 'pending'`);
  const acceptedRows = await dbAll(`SELECT username FROM users WHERE status = 'approved'`);
  res.json({ pending: pendingRows.map((r) => r.username), accepted: acceptedRows.map((r) => r.username) });
}));

app.post("/api/stream/token", requireAuth, ah(async (req, res) => {
  const t = newToken();
  const expiresAt = Date.now() + 60_000;
  await dbRun(`INSERT INTO stream_tokens (token, username, expires_at) VALUES (?,?,?)`, [t, req.user.username, expiresAt]);
  res.json({ streamToken: t });
}));

// ---------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------
app.get("/api/snapshot", requireAuth, ah(async (req, res) => {
  res.json(await buildSnapshot(req.user.username));
}));

// ---------------------------------------------------------------------------
// SSE stream
// ---------------------------------------------------------------------------
app.get("/api/stream", ah(async (req, res) => {
  const streamToken = req.query.token;
  if (!streamToken) return res.status(401).end();

  const row = await dbGet(`SELECT username, expires_at FROM stream_tokens WHERE token = ?`, [streamToken]);
  await dbRun(`DELETE FROM stream_tokens WHERE token = ?`, [streamToken]);
  if (!row || row.expires_at < Date.now()) return res.status(401).end();

  const username = row.username;

  res.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    Connection: "keep-alive",
    "X-Accel-Buffering": "no",
  });

  addStream(username, res);
  res.write(`data: ${JSON.stringify({ frameType: "I-FRAME", ...(await buildSnapshot(username)) })}\n\n`);

  const keepAlive = setInterval(() => res.write(":ping\n\n"), 25_000);
  req.on("close", () => {
    clearInterval(keepAlive);
    removeStream(username, res);
  });
}));

// ---------------------------------------------------------------------------
// User profile picture
// ---------------------------------------------------------------------------
app.post("/api/user/pfp", requireAuth, ah(async (req, res) => {
  const pfpBase64 = String(req.body.pfpBase64 || "");
  if (pfpBase64.length > 600_000) return res.status(413).json({ error: "Görsel çok büyük" });
  await dbRun(`UPDATE users SET pfp = ? WHERE username = ?`, [pfpBase64, req.user.username]);
  res.json({ ok: true });
}));

// ---------------------------------------------------------------------------
// Images (stored in imgdb, referenced from messages as "img:<id>")
// ---------------------------------------------------------------------------
app.get("/api/images/:id", requireAuth, ah(async (req, res) => {
  const row = await dbGet(`SELECT data FROM images WHERE id = ?`, [req.params.id], "imgdb");
  if (!row) return res.status(404).json({ error: "Görsel bulunamadı" });
  res.json({ content: row.data });
}));

// ---------------------------------------------------------------------------
// Friends
// ---------------------------------------------------------------------------
app.post("/api/friends/request", requireAuth, ah(async (req, res) => {
  const target = String(req.body.targetUsername || "").toLowerCase().replace(/[^a-z]/g, "");
  if (!target || target === req.user.username) return res.status(400).json({ error: "Geçersiz kullanıcı" });
  const targetUser = await dbGet(`SELECT username FROM users WHERE username = ? AND status='approved'`, [target]);
  if (!targetUser) return res.status(404).json({ error: "Kullanıcı bulunamadı" });

  const [u1, u2] = [req.user.username, target].sort();
  const existing = await dbGet(`SELECT * FROM friends WHERE user1=? AND user2=?`, [u1, u2]);
  if (existing) return res.status(409).json({ error: "Zaten arkadaşsınız ya da istek bekliyor" });

  await dbRun(`INSERT INTO friends (user1, user2, status, created_at) VALUES (?,?,?,?)`, [req.user.username, target, "pending", Date.now()]);
  res.json({ message: "İstek gönderildi" });
  sendTo(target, { frameType: "P-FRAME", type: "FRIEND_REQUEST", payload: { from: req.user.username } });
}));

app.post("/api/friends/accept", requireAuth, ah(async (req, res) => {
  const target = String(req.body.targetUsername || "").toLowerCase().replace(/[^a-z]/g, "");
  const row = await dbGet(`SELECT * FROM friends WHERE user1=? AND user2=? AND status='pending'`, [target, req.user.username]);
  if (!row) return res.status(404).json({ error: "İstek bulunamadı" });

  await dbRun(`UPDATE friends SET status='accepted' WHERE user1=? AND user2=?`, [target, req.user.username]);
  const meRow = await dbGet(`SELECT pfp FROM users WHERE username=?`, [req.user.username]);
  const targetRow = await dbGet(`SELECT pfp FROM users WHERE username=?`, [target]);
  const pfpA = meRow?.pfp || "";
  const pfpB = targetRow?.pfp || "";
  res.json({ ok: true });

  sendToMany([req.user.username, target], {
    frameType: "P-FRAME",
    type: "FRIEND_ACCEPTED",
    payload: {
      friendRowFor: {
        [req.user.username]: { user1: target, user2: req.user.username, status: "accepted", pfp: pfpB },
        [target]: { user1: target, user2: req.user.username, status: "accepted", pfp: pfpA },
      },
    },
  });
}));

app.post("/api/friends/unfriend", requireAuth, ah(async (req, res) => {
  const target = String(req.body.targetUsername || "").toLowerCase().replace(/[^a-z]/g, "");
  const [u1, u2] = [req.user.username, target].sort();
  const info = await dbRun(`DELETE FROM friends WHERE user1=? AND user2=?`, [u1, u2]);
  if (info.changes === 0) return res.status(404).json({ error: "Arkadaşlık bulunamadı" });
  res.json({ ok: true });
  sendToMany([req.user.username, target], { frameType: "P-FRAME", type: "FRIEND_REMOVED", payload: { user1: u1, user2: u2 } });
}));

// ---------------------------------------------------------------------------
// DMs
// ---------------------------------------------------------------------------
function dmToken(a, b) { return "dm_" + [a, b].sort().join("_"); }

app.post("/api/dm/open", requireAuth, ah(async (req, res) => {
  const target = String(req.body.targetUsername || "").toLowerCase().replace(/[^a-z]/g, "");
  const [u1, u2] = [req.user.username, target].sort();
  const friendRow = await dbGet(`SELECT status FROM friends WHERE user1=? AND user2=?`, [u1, u2]);
  if (!friendRow || friendRow.status !== "accepted") return res.status(403).json({ error: "Önce arkadaş olmalısınız" });

  const gt = dmToken(req.user.username, target);
  const existing = await dbGet(`SELECT group_token FROM groups_t WHERE group_token = ?`, [gt]);
  if (!existing) {
    const now = Date.now();
    await dbRun(`INSERT INTO groups_t (group_token, group_name, created_by, allow_sub_invites, created_at) VALUES (?,?,?,?,?)`, [gt, "@" + target, req.user.username, 0, now]);
    await dbRun(`INSERT INTO group_members (group_token, username, can_add_members, joined_at) VALUES (?,?,?,?)`, [gt, req.user.username, 0, now]);
    await dbRun(`INSERT INTO group_members (group_token, username, can_add_members, joined_at) VALUES (?,?,?,?)`, [gt, target, 0, now]);
  }
  res.json({ groupToken: gt });
}));

// ---------------------------------------------------------------------------
// Groups
// ---------------------------------------------------------------------------
app.post("/api/groups/create", requireAuth, ah(async (req, res) => {
  const groupName = String(req.body.groupName || "").trim().slice(0, 60);
  if (!groupName) return res.status(400).json({ error: "Grup adı gerekli" });
  const gt = crypto.randomBytes(12).toString("hex");
  const now = Date.now();
  await dbRun(`INSERT INTO groups_t (group_token, group_name, created_by, allow_sub_invites, created_at) VALUES (?,?,?,1,?)`, [gt, groupName, req.user.username, now]);
  await dbRun(`INSERT INTO group_members (group_token, username, can_add_members, joined_at) VALUES (?,?,1,?)`, [gt, req.user.username, now]);
  res.json({ message: "Grup oluşturuldu", groupToken: gt });
}));

async function isMember(groupToken, username) {
  return !!(await dbGet(`SELECT 1 as x FROM group_members WHERE group_token=? AND username=?`, [groupToken, username]));
}
function isOwnerOrAdmin(group, user) {
  return group.created_by === user.username || user.role === "admin";
}

app.get("/api/groups/:token/members", requireAuth, ah(async (req, res) => {
  const gt = req.params.token;
  const group = await dbGet(`SELECT * FROM groups_t WHERE group_token = ?`, [gt]);
  if (!group) return res.status(404).json({ error: "Grup bulunamadı" });
  if (!(await isMember(gt, req.user.username))) return res.status(403).json({ error: "Bu grubun üyesi değilsiniz" });

  const members = await dbAll(
    `SELECT gm.username as username, gm.can_add_members as can_add_members, u.pfp as pfp
     FROM group_members gm JOIN users u ON u.username = gm.username WHERE gm.group_token = ?`,
    [gt]
  );

  res.json({ groupInfo: group, members });
}));

app.post("/api/groups/add-member", requireAuth, ah(async (req, res) => {
  const { groupToken, targetUsername } = req.body;
  const target = String(targetUsername || "").toLowerCase().replace(/[^a-z]/g, "");
  const group = await dbGet(`SELECT * FROM groups_t WHERE group_token = ?`, [groupToken]);
  if (!group) return res.status(404).json({ error: "Grup bulunamadı" });

  const me = await dbGet(`SELECT * FROM group_members WHERE group_token=? AND username=?`, [groupToken, req.user.username]);
  if (!me) return res.status(403).json({ error: "Bu grubun üyesi değilsiniz" });

  const owner = isOwnerOrAdmin(group, req.user);
  const canInvite = owner || (group.allow_sub_invites && me.can_add_members);
  if (!canInvite) return res.status(403).json({ error: "Üye ekleme yetkiniz yok" });

  const [u1, u2] = [req.user.username, target].sort();
  const friendRow = await dbGet(`SELECT status FROM friends WHERE user1=? AND user2=?`, [u1, u2]);
  if (!friendRow || friendRow.status !== "accepted") return res.status(403).json({ error: "Sadece arkadaşlarınızı ekleyebilirsiniz" });

  if (await isMember(groupToken, target)) return res.status(409).json({ error: "Kullanıcı zaten grupta" });

  const now = Date.now();
  await dbRun(`INSERT INTO group_members (group_token, username, can_add_members, joined_at) VALUES (?,?,1,?)`, [groupToken, target, now]);
  const targetUserRow = await dbGet(`SELECT pfp FROM users WHERE username=?`, [target]);
  const newMember = { username: target, can_add_members: 1, pfp: targetUserRow?.pfp || "" };

  res.json({ message: "Üye eklendi" });

  const recipients = await groupMemberUsernames(groupToken);
  sendToMany(recipients, { frameType: "P-FRAME", type: "GROUP_MEMBER_ADDED", payload: { groupToken, newMember, group } });
}));

app.post("/api/groups/remove-member", requireAuth, ah(async (req, res) => {
  const { groupToken, targetUsername } = req.body;
  const group = await dbGet(`SELECT * FROM groups_t WHERE group_token = ?`, [groupToken]);
  if (!group) return res.status(404).json({ error: "Grup bulunamadı" });
  if (!isOwnerOrAdmin(group, req.user)) return res.status(403).json({ error: "Yetkiniz yok" });
  if (targetUsername === group.created_by) return res.status(400).json({ error: "Kurucu çıkarılamaz" });

  const recipientsBefore = await groupMemberUsernames(groupToken);
  await dbRun(`DELETE FROM group_members WHERE group_token=? AND username=?`, [groupToken, targetUsername]);
  res.json({ ok: true });
  sendToMany(recipientsBefore, { frameType: "P-FRAME", type: "GROUP_MEMBER_LEFT", payload: { groupToken, username: targetUsername } });
}));

app.post("/api/groups/leave", requireAuth, ah(async (req, res) => {
  const { groupToken } = req.body;
  const group = await dbGet(`SELECT * FROM groups_t WHERE group_token = ?`, [groupToken]);
  if (!group) return res.status(404).json({ error: "Grup bulunamadı" });
  if (group.created_by === req.user.username) return res.status(400).json({ error: "Kurucu gruptan ayrılamaz, grubu silin" });
  if (!(await isMember(groupToken, req.user.username))) return res.status(403).json({ error: "Bu grubun üyesi değilsiniz" });

  const recipientsBefore = await groupMemberUsernames(groupToken);
  await dbRun(`DELETE FROM group_members WHERE group_token=? AND username=?`, [groupToken, req.user.username]);
  res.json({ ok: true });
  sendToMany(recipientsBefore, { frameType: "P-FRAME", type: "GROUP_MEMBER_LEFT", payload: { groupToken, username: req.user.username } });
}));

app.post("/api/groups/toggle-sub-invites", requireAuth, ah(async (req, res) => {
  const { groupToken, allowSubInvites } = req.body;
  const group = await dbGet(`SELECT * FROM groups_t WHERE group_token = ?`, [groupToken]);
  if (!group) return res.status(404).json({ error: "Grup bulunamadı" });
  if (!isOwnerOrAdmin(group, req.user)) return res.status(403).json({ error: "Yetkiniz yok" });

  const val = allowSubInvites ? 1 : 0;
  await dbRun(`UPDATE groups_t SET allow_sub_invites=? WHERE group_token=?`, [val, groupToken]);
  res.json({ ok: true });
  sendToMany(await groupMemberUsernames(groupToken), {
    frameType: "P-FRAME", type: "GROUP_SETTING_UPDATED", payload: { groupToken, allow_sub_invites: val },
  });
}));

app.post("/api/groups/toggle-member-invite-perm", requireAuth, ah(async (req, res) => {
  const { groupToken, targetUsername, canAddMembers } = req.body;
  const group = await dbGet(`SELECT * FROM groups_t WHERE group_token = ?`, [groupToken]);
  if (!group) return res.status(404).json({ error: "Grup bulunamadı" });
  if (!isOwnerOrAdmin(group, req.user)) return res.status(403).json({ error: "Yetkiniz yok" });

  const val = canAddMembers ? 1 : 0;
  await dbRun(`UPDATE group_members SET can_add_members=? WHERE group_token=? AND username=?`, [val, groupToken, targetUsername]);
  res.json({ ok: true });
  sendToMany(await groupMemberUsernames(groupToken), {
    frameType: "P-FRAME", type: "GROUP_PERM_UPDATED", payload: { groupToken, username: targetUsername, can_add_members: val },
  });
}));

// ---------------------------------------------------------------------------
// Messages — paginated, cursor-based. Images live in imgdb, referenced as
// "img:<id>" inside `content` / `reply_to_content`.
// ---------------------------------------------------------------------------
app.get("/api/messages/:token", requireAuth, ah(async (req, res) => {
  const gt = req.params.token;
  if (!(await isMember(gt, req.user.username))) return res.status(403).json({ error: "Bu grubun üyesi değilsiniz" });

  const limit = Math.min(parseInt(req.query.limit, 10) || 50, 100);
  const before = req.query.before ? parseInt(req.query.before, 10) : null;

  let rows;
  if (before) {
    rows = await dbAll(`SELECT * FROM messages WHERE group_token = ? AND id < ? ORDER BY id DESC LIMIT ?`, [gt, before, limit]);
  } else {
    rows = await dbAll(`SELECT * FROM messages WHERE group_token = ? ORDER BY id DESC LIMIT ?`, [gt, limit]);
  }
  rows.reverse();

  const senders = [...new Set(rows.map((r) => r.sender))];
  if (senders.length) {
    const placeholders = senders.map(() => "?").join(",");
    const pfps = await dbAll(`SELECT username, pfp FROM users WHERE username IN (${placeholders})`, senders);
    const pfpMap = Object.fromEntries(pfps.map((p) => [p.username, p.pfp || ""]));
    for (const r of rows) r.pfp = pfpMap[r.sender] || "";
  }

  res.json(rows);
}));

app.post("/api/messages/send", requireAuth, ah(async (req, res) => {
  const { groupToken, content, replyToSender, replyToContent } = req.body;
  if (!(await isMember(groupToken, req.user.username))) return res.status(403).json({ error: "Bu grubun üyesi değilsiniz" });
  if (!content || typeof content !== "string") return res.status(400).json({ error: "Geçersiz mesaj" });

  let storedContent = content;
  if (content.startsWith("data:image/")) {
    if (content.length > 1_400_000) return res.status(413).json({ error: "Görsel çok büyük" });
    const now0 = Date.now();
    const imgInsert = await dbRun(`INSERT INTO images (data, created_at) VALUES (?,?)`, [content, now0], "imgdb");
    storedContent = `img:${imgInsert.lastInsertRowid}`;
  } else if (content.length > 4000) {
    return res.status(413).json({ error: "Mesaj çok uzun" });
  }

  // reply preview: if replying to an image, store the ref, not the raw base64
  let storedReplyContent = replyToContent || null;
  if (storedReplyContent && storedReplyContent.startsWith("data:image/")) {
    storedReplyContent = "img:0"; // shouldn't normally happen (client sends the already-stored ref); safe fallback
  }

  const now = Date.now();
  const info = await dbRun(
    `INSERT INTO messages (group_token, sender, content, reply_to_sender, reply_to_content, created_at) VALUES (?,?,?,?,?,?)`,
    [groupToken, req.user.username, storedContent, replyToSender || null, storedReplyContent, now]
  );

  const senderRow = await dbGet(`SELECT pfp FROM users WHERE username=?`, [req.user.username]);
  const payload = {
    id: info.lastInsertRowid,
    group_token: groupToken,
    sender: req.user.username,
    content: storedContent,
    reply_to_sender: replyToSender || null,
    reply_to_content: storedReplyContent,
    created_at: now,
    pfp: senderRow?.pfp || "",
  };

  res.json({ ok: true, id: info.lastInsertRowid });
  sendToMany(await groupMemberUsernames(groupToken), { frameType: "P-FRAME", type: "NEW_MESSAGE", payload });
}));

app.post("/api/messages/delete", requireAuth, ah(async (req, res) => {
  const { messageId, groupToken } = req.body;
  const msg = await dbGet(`SELECT * FROM messages WHERE id = ? AND group_token = ?`, [messageId, groupToken]);
  if (!msg) return res.status(404).json({ error: "Mesaj bulunamadı" });

  const group = await dbGet(`SELECT * FROM groups_t WHERE group_token = ?`, [groupToken]);
  const canDelete = msg.sender === req.user.username || isOwnerOrAdmin(group, req.user);
  if (!canDelete) return res.status(403).json({ error: "Bu mesajı silme yetkiniz yok" });

  await dbRun(`DELETE FROM messages WHERE id = ?`, [messageId]);
  if (typeof msg.content === "string" && msg.content.startsWith("img:")) {
    const imgId = msg.content.split(":")[1];
    dbRun(`DELETE FROM images WHERE id = ?`, [imgId], "imgdb").catch(() => {}); // best-effort, don't block response
  }
  res.json({ ok: true });
  sendToMany(await groupMemberUsernames(groupToken), {
    frameType: "P-FRAME", type: "MESSAGE_DELETED", payload: { messageId, groupToken },
  });
}));

// ---------------------------------------------------------------------------
// Admin
// ---------------------------------------------------------------------------
app.get("/api/admin/pending", requireAuth, requireAdmin, ah(async (req, res) => {
  res.json(await dbAll(`SELECT rowid as id, username FROM users WHERE status='pending'`));
}));

app.get("/api/admin/users", requireAuth, requireAdmin, ah(async (req, res) => {
  res.json(await dbAll(`SELECT username, role FROM users WHERE status='approved'`));
}));

app.post("/api/admin/action", requireAuth, requireAdmin, ah(async (req, res) => {
  const { id, action } = req.body;
  const user = await dbGet(`SELECT * FROM users WHERE rowid = ?`, [id]);
  if (!user) return res.status(404).json({ error: "Kullanıcı bulunamadı" });

  if (action === "approve") await dbRun(`UPDATE users SET status='approved' WHERE rowid=?`, [id]);
  else if (action === "deny") await dbRun(`DELETE FROM users WHERE rowid=?`, [id]);
  else return res.status(400).json({ error: "Geçersiz işlem" });

  res.json({ ok: true });
}));

app.post("/api/admin/delete-user", requireAuth, requireAdmin, ah(async (req, res) => {
  const { username } = req.body;
  if (username === req.user.username) return res.status(400).json({ error: "Kendinizi silemezsiniz" });
  const info = await dbRun(`DELETE FROM users WHERE username = ?`, [username]);
  if (info.changes === 0) return res.status(404).json({ error: "Kullanıcı bulunamadı" });
  await dbRun(`DELETE FROM sessions WHERE username = ?`, [username]);
  res.json({ message: "Kullanıcı silindi" });
}));

// ---------------------------------------------------------------------------
// Static client
// ---------------------------------------------------------------------------
app.use(express.static(path.join(__dirname, "public")));

// prune expired stream tokens periodically
setInterval(() => {
  dbRun(`DELETE FROM stream_tokens WHERE expires_at < ?`, [Date.now()]).catch((e) => console.error("prune error:", e.message));
}, 60_000);

initSchema()
  .then(() => {
    app.listen(PORT, () => console.log(`SOPERT server listening on :${PORT} (DB via Worker: ${WORKER_URL})`));
  })
  .catch((err) => {
    console.error("FATAL: could not reach Worker to init schema:", err.message);
    process.exit(1);
  });
