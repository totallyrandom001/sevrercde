// ============================================================================
// SOPERT SERVER — HTTP layer in front of the Cloudflare Worker, now with a
// local SQLite read-cache so that D1 is only hit on cache misses and writes.
//
// ARCHITECTURE:
//   - D1 (via Cloudflare Worker) = source of truth. Every write goes there.
//   - Local SQLite (/tmp/sopert_cache.db) = read cache, rebuilt from D1 on
//     cache miss. Lives on Render's local disk, survives within a single
//     running instance but is wiped on restart (that's fine — it just re-warms).
//   - Images (imgdb) are NOT cached locally because base64 blobs would bloat
//     the disk fast. Image reads still go straight to D1/Worker. New image
//     writes still go to D1; we don't cache them on disk.
//
// CACHE INVALIDATION:
//   - Every write path updates D1 first, then immediately patches the local
//     SQLite cache so subsequent reads still hit locally.
//   - There is no TTL / expiry. The cache is effectively write-through.
//
// Env vars required on Render:
//   WORKER_URL     e.g. https://your-worker.workers.dev
//   WORKER_SECRET  must match env.WORKER_SECRET on the Worker
// ============================================================================

const express = require("express");
const crypto  = require("crypto");
const path    = require("path");
const Database = require("better-sqlite3");

const PORT          = process.env.PORT || 3000;
const WORKER_URL    = (process.env.WORKER_URL || "").replace(/\/+$/, "");
const WORKER_SECRET = process.env.WORKER_SECRET || "";
const CACHE_PATH    = process.env.CACHE_PATH || "/tmp/sopert_cache.db";

if (!WORKER_URL || !WORKER_SECRET) {
  console.error("FATAL: WORKER_URL and WORKER_SECRET env vars must be set.");
  process.exit(1);
}

// ---------------------------------------------------------------------------
// Local SQLite cache (better-sqlite3 = synchronous, fast, no async overhead)
// ---------------------------------------------------------------------------
const cache = new Database(CACHE_PATH);
cache.pragma("journal_mode = WAL");
cache.pragma("synchronous = NORMAL");

cache.exec(`
  CREATE TABLE IF NOT EXISTS users (
    username TEXT PRIMARY KEY, password_hash TEXT, password_salt TEXT,
    role TEXT, status TEXT, pfp TEXT, created_at INTEGER
  );
  CREATE TABLE IF NOT EXISTS sessions (
    token TEXT PRIMARY KEY, username TEXT NOT NULL, created_at INTEGER
  );
  CREATE TABLE IF NOT EXISTS stream_tokens (
    token TEXT PRIMARY KEY, username TEXT NOT NULL, expires_at INTEGER
  );
  CREATE TABLE IF NOT EXISTS friends (
    user1 TEXT NOT NULL, user2 TEXT NOT NULL, status TEXT NOT NULL,
    created_at INTEGER, PRIMARY KEY (user1, user2)
  );
  CREATE TABLE IF NOT EXISTS groups_t (
    group_token TEXT PRIMARY KEY, group_name TEXT, created_by TEXT,
    allow_sub_invites INTEGER DEFAULT 1, created_at INTEGER
  );
  CREATE TABLE IF NOT EXISTS group_members (
    group_token TEXT NOT NULL, username TEXT NOT NULL, can_add_members INTEGER DEFAULT 1,
    joined_at INTEGER, PRIMARY KEY (group_token, username)
  );
  CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY, group_token TEXT, sender TEXT, content TEXT,
    reply_to_sender TEXT, reply_to_content TEXT, created_at INTEGER
  );
  -- Track which groups have had their messages fully loaded into cache
  CREATE TABLE IF NOT EXISTS cache_meta (
    key TEXT PRIMARY KEY, value TEXT
  );
`);

// Prepared statements for hot paths
const cacheStmts = {
  getUser:    cache.prepare("SELECT * FROM users WHERE username = ?"),
  upsertUser: cache.prepare(`INSERT OR REPLACE INTO users
    (username,password_hash,password_salt,role,status,pfp,created_at) VALUES
    (?,?,?,?,?,?,?)`),
  deleteUser: cache.prepare("DELETE FROM users WHERE username = ?"),

  getSession:    cache.prepare("SELECT s.username as username, u.role as role, u.status as status FROM sessions s JOIN users u ON u.username = s.username WHERE s.token = ?"),
  upsertSession: cache.prepare("INSERT OR REPLACE INTO sessions (token,username,created_at) VALUES (?,?,?)"),
  deleteSession: cache.prepare("DELETE FROM sessions WHERE token = ?"),
  deleteSessionsByUser: cache.prepare("DELETE FROM sessions WHERE username = ?"),

  getStreamToken:    cache.prepare("SELECT username, expires_at FROM stream_tokens WHERE token = ?"),
  upsertStreamToken: cache.prepare("INSERT OR REPLACE INTO stream_tokens (token,username,expires_at) VALUES (?,?,?)"),
  deleteStreamToken: cache.prepare("DELETE FROM stream_tokens WHERE token = ?"),
  pruneStreamTokens: cache.prepare("DELETE FROM stream_tokens WHERE expires_at < ?"),

  getFriendPair:    cache.prepare("SELECT * FROM friends WHERE user1=? AND user2=?"),
  getFriendsOf:     cache.prepare(`SELECT f.user1,f.user2,f.status,u.pfp FROM friends f
    JOIN users u ON u.username=(CASE WHEN f.user1=? THEN f.user2 ELSE f.user1 END)
    WHERE f.user1=? OR f.user2=?`),
  upsertFriend:     cache.prepare("INSERT OR REPLACE INTO friends (user1,user2,status,created_at) VALUES (?,?,?,?)"),
  updateFriendStatus: cache.prepare("UPDATE friends SET status=? WHERE user1=? AND user2=?"),
  deleteFriend:     cache.prepare("DELETE FROM friends WHERE user1=? AND user2=?"),
  isFriendCached:   cache.prepare("SELECT 1 FROM cache_meta WHERE key='friends_loaded'"),
  markFriendsLoaded: cache.prepare("INSERT OR REPLACE INTO cache_meta (key,value) VALUES ('friends_loaded','1')"),

  getGroup:    cache.prepare("SELECT * FROM groups_t WHERE group_token=?"),
  upsertGroup: cache.prepare(`INSERT OR REPLACE INTO groups_t
    (group_token,group_name,created_by,allow_sub_invites,created_at) VALUES (?,?,?,?,?)`),
  updateGroupSubInvites: cache.prepare("UPDATE groups_t SET allow_sub_invites=? WHERE group_token=?"),
  getGroupsForUser: cache.prepare(`SELECT g.* FROM groups_t g
    JOIN group_members gm ON gm.group_token=g.group_token WHERE gm.username=?`),

  getMember:    cache.prepare("SELECT * FROM group_members WHERE group_token=? AND username=?"),
  getMembers:   cache.prepare(`SELECT gm.username,gm.can_add_members,u.pfp FROM group_members gm
    JOIN users u ON u.username=gm.username WHERE gm.group_token=?`),
  upsertMember: cache.prepare("INSERT OR REPLACE INTO group_members (group_token,username,can_add_members,joined_at) VALUES (?,?,?,?)"),
  deleteMember: cache.prepare("DELETE FROM group_members WHERE group_token=? AND username=?"),
  updateMemberPerm: cache.prepare("UPDATE group_members SET can_add_members=? WHERE group_token=? AND username=?"),
  isMembersGroupCached: cache.prepare("SELECT 1 FROM cache_meta WHERE key=?"),
  markMembersGroupCached: cache.prepare("INSERT OR REPLACE INTO cache_meta (key,value) VALUES (?,?)"),
  getGroupsForUserAll: cache.prepare("SELECT group_token FROM group_members WHERE username=?"),

  getMessages:       cache.prepare("SELECT * FROM messages WHERE group_token=? ORDER BY id DESC LIMIT ?"),
  getMessagesBefore: cache.prepare("SELECT * FROM messages WHERE group_token=? AND id<? ORDER BY id DESC LIMIT ?"),
  getMessage:        cache.prepare("SELECT * FROM messages WHERE id=? AND group_token=?"),
  insertMessage:     cache.prepare(`INSERT OR REPLACE INTO messages
    (id,group_token,sender,content,reply_to_sender,reply_to_content,created_at) VALUES (?,?,?,?,?,?,?)`),
  deleteMessage:     cache.prepare("DELETE FROM messages WHERE id=?"),
  isMessageGroupCached: cache.prepare("SELECT 1 FROM cache_meta WHERE key=?"),
  markMessageGroupCached: cache.prepare("INSERT OR REPLACE INTO cache_meta (key,value) VALUES (?,?)"),

  getUserCount: cache.prepare("SELECT COUNT(*) as c FROM users"),
  getPendingUsers: cache.prepare("SELECT rowid as id, username FROM users WHERE status='pending'"),
  getApprovedUsers: cache.prepare("SELECT username, role FROM users WHERE status='approved'"),
  getUserByRowid: cache.prepare("SELECT * FROM users WHERE rowid=?"),
  updateUserByRowid: cache.prepare("UPDATE users SET status=? WHERE rowid=?"),
  deleteUserByRowid: cache.prepare("DELETE FROM users WHERE rowid=?"),
  getUserPfp: cache.prepare("SELECT pfp FROM users WHERE username=?"),
  updateUserPfp: cache.prepare("UPDATE users SET pfp=? WHERE username=?"),
  getUsernames: cache.prepare("SELECT username FROM users WHERE status='approved'"),
};

// ---------------------------------------------------------------------------
// D1 via Worker — used only on cache misses and for ALL writes
// ---------------------------------------------------------------------------
async function workerQuery(sql, params = [], targetDb = "DB") {
  const res = await fetch(`${WORKER_URL}/query`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Worker-Secret": WORKER_SECRET },
    body: JSON.stringify({ sql, params, targetDb }),
  });
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); }
  catch { throw new Error(`Worker non-JSON (${res.status}): ${text.slice(0, 300)}`); }
  if (!res.ok || data.error) throw new Error(data.error || `Worker query failed (${res.status})`);
  return data;
}
async function d1All(sql, params = [], targetDb = "DB") {
  const r = await workerQuery(sql, params, targetDb);
  return r.results || [];
}
async function d1Get(sql, params = [], targetDb = "DB") {
  return (await d1All(sql, params, targetDb))[0] || null;
}
async function d1Run(sql, params = [], targetDb = "DB") {
  const r = await workerQuery(sql, params, targetDb);
  const meta = r.meta || {};
  return { changes: meta.changes ?? 0, lastInsertRowid: meta.last_row_id ?? meta.lastRowId ?? null };
}

// ---------------------------------------------------------------------------
// Schema init on D1 (unchanged from original)
// ---------------------------------------------------------------------------
const MAIN_SCHEMA = [
  `CREATE TABLE IF NOT EXISTS users (
    username TEXT PRIMARY KEY, password_hash TEXT NOT NULL, password_salt TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'user', status TEXT NOT NULL DEFAULT 'pending',
    pfp TEXT, created_at INTEGER NOT NULL)`,
  `CREATE TABLE IF NOT EXISTS sessions (
    token TEXT PRIMARY KEY, username TEXT NOT NULL, created_at INTEGER NOT NULL)`,
  `CREATE INDEX IF NOT EXISTS idx_sessions_username ON sessions(username)`,
  `CREATE TABLE IF NOT EXISTS stream_tokens (
    token TEXT PRIMARY KEY, username TEXT NOT NULL, expires_at INTEGER NOT NULL)`,
  `CREATE TABLE IF NOT EXISTS friends (
    user1 TEXT NOT NULL, user2 TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
    created_at INTEGER NOT NULL, PRIMARY KEY (user1, user2))`,
  `CREATE TABLE IF NOT EXISTS groups_t (
    group_token TEXT PRIMARY KEY, group_name TEXT NOT NULL, created_by TEXT NOT NULL,
    allow_sub_invites INTEGER NOT NULL DEFAULT 1, created_at INTEGER NOT NULL)`,
  `CREATE TABLE IF NOT EXISTS group_members (
    group_token TEXT NOT NULL, username TEXT NOT NULL, can_add_members INTEGER NOT NULL DEFAULT 1,
    joined_at INTEGER NOT NULL, PRIMARY KEY (group_token, username))`,
  `CREATE INDEX IF NOT EXISTS idx_group_members_user ON group_members(username)`,
  `CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT, group_token TEXT NOT NULL, sender TEXT NOT NULL,
    content TEXT NOT NULL, reply_to_sender TEXT, reply_to_content TEXT, created_at INTEGER NOT NULL)`,
  `CREATE INDEX IF NOT EXISTS idx_messages_group_id ON messages(group_token, id)`,
];
const IMG_SCHEMA = [
  `CREATE TABLE IF NOT EXISTS images (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT NOT NULL, created_at INTEGER NOT NULL)`,
];
async function initSchema() {
  for (const s of MAIN_SCHEMA) await workerQuery(s, [], "DB");
  for (const s of IMG_SCHEMA)  await workerQuery(s, [], "imgdb");
  console.log("D1 schema OK.");
}

// ---------------------------------------------------------------------------
// Cache-aside helpers — the core of the read-cache pattern
// ---------------------------------------------------------------------------

// Users
async function cacheGetUser(username) {
  const hit = cacheStmts.getUser.get(username);
  if (hit) return hit;
  const row = await d1Get("SELECT * FROM users WHERE username=?", [username]);
  if (row) cacheStmts.upsertUser.run(row.username, row.password_hash, row.password_salt, row.role, row.status, row.pfp, row.created_at);
  return row;
}

// Sessions — join with users is done locally once user is cached
async function cacheGetSession(token) {
  // Try local join first (both session + user must be in cache)
  const hit = cacheStmts.getSession.get(token);
  if (hit) return hit;
  // Cache miss: fetch session from D1, then also warm user into cache
  const row = await d1Get(
    `SELECT s.username as username, u.role as role, u.status as status
     FROM sessions s JOIN users u ON u.username=s.username WHERE s.token=?`, [token]);
  if (row) {
    // Warm the session row
    const sRow = await d1Get("SELECT * FROM sessions WHERE token=?", [token]);
    if (sRow) cacheStmts.upsertSession.run(sRow.token, sRow.username, sRow.created_at);
    // Warm the user row
    const uRow = await d1Get("SELECT * FROM users WHERE username=?", [row.username]);
    if (uRow) cacheStmts.upsertUser.run(uRow.username, uRow.password_hash, uRow.password_salt, uRow.role, uRow.status, uRow.pfp, uRow.created_at);
  }
  return row;
}

// Friends — load all friends for a user on first access, cache the set
async function cacheGetFriendsOf(username) {
  // Check if we've ever loaded this user's friends
  const meta = cache.prepare("SELECT 1 FROM cache_meta WHERE key=?").get(`friends_loaded:${username}`);
  if (meta) {
    return cacheStmts.getFriendsOf.all(username, username, username);
  }
  // Cache miss: load from D1, store all rows
  const rows = await d1All(
    `SELECT f.user1,f.user2,f.status,u.pfp FROM friends f
     JOIN users u ON u.username=(CASE WHEN f.user1=? THEN f.user2 ELSE f.user1 END)
     WHERE f.user1=? OR f.user2=?`, [username, username, username]);
  for (const f of rows) cacheStmts.upsertFriend.run(f.user1, f.user2, f.status, f.created_at ?? Date.now());
  cache.prepare("INSERT OR REPLACE INTO cache_meta (key,value) VALUES (?,?)").run(`friends_loaded:${username}`, "1");
  return cacheStmts.getFriendsOf.all(username, username, username);
}

// Groups for user — load on first access
async function cacheGetGroupsForUser(username) {
  const metaKey = `groups_loaded:${username}`;
  const meta = cache.prepare("SELECT 1 FROM cache_meta WHERE key=?").get(metaKey);
  if (meta) {
    return cacheStmts.getGroupsForUser.all(username);
  }
  const rows = await d1All(
    `SELECT g.* FROM groups_t g JOIN group_members gm ON gm.group_token=g.group_token WHERE gm.username=?`,
    [username]);
  for (const g of rows) cacheStmts.upsertGroup.run(g.group_token, g.group_name, g.created_by, g.allow_sub_invites, g.created_at);
  // Also warm group_members for completeness
  for (const g of rows) {
    const members = await d1All(
      `SELECT gm.username,gm.can_add_members,u.pfp,gm.joined_at FROM group_members gm
       JOIN users u ON u.username=gm.username WHERE gm.group_token=?`, [g.group_token]);
    for (const m of members) {
      cacheStmts.upsertMember.run(g.group_token, m.username, m.can_add_members, m.joined_at ?? Date.now());
      if (m.pfp) cacheStmts.upsertUser.run(m.username, "", "", "user", "approved", m.pfp, 0);
    }
  }
  cache.prepare("INSERT OR REPLACE INTO cache_meta (key,value) VALUES (?,?)").run(metaKey, "1");
  return cacheStmts.getGroupsForUser.all(username);
}

// Group members — load on first access per group
async function cacheGetGroupMembers(groupToken) {
  const metaKey = `members_loaded:${groupToken}`;
  const meta = cacheStmts.isMembersGroupCached.get(metaKey);
  if (meta) {
    const group = cacheStmts.getGroup.get(groupToken);
    const members = cacheStmts.getMembers.all(groupToken);
    return { groupInfo: group, members };
  }
  const res = await d1Get("SELECT * FROM groups_t WHERE group_token=?", [groupToken]);
  if (res) cacheStmts.upsertGroup.run(res.group_token, res.group_name, res.created_by, res.allow_sub_invites, res.created_at);
  const members = await d1All(
    `SELECT gm.username,gm.can_add_members,u.pfp,gm.joined_at FROM group_members gm
     JOIN users u ON u.username=gm.username WHERE gm.group_token=?`, [groupToken]);
  for (const m of members) {
    cacheStmts.upsertMember.run(groupToken, m.username, m.can_add_members, m.joined_at ?? Date.now());
    if (m.pfp) {
      const existing = cacheStmts.getUser.get(m.username);
      if (!existing) cacheStmts.upsertUser.run(m.username, "", "", "user", "approved", m.pfp, 0);
    }
  }
  cacheStmts.markMembersGroupCached.run(metaKey, "1");
  return { groupInfo: res, members: cacheStmts.getMembers.all(groupToken) };
}

// Messages — load on first access per group, paginated
async function cacheGetMessages(groupToken, limit, beforeId = null) {
  const metaKey = `msgs_loaded:${groupToken}`;
  const meta = cacheStmts.isMessageGroupCached.get(metaKey);

  if (meta) {
    // Serve from local cache
    const rows = beforeId
      ? cacheStmts.getMessagesBefore.all(groupToken, beforeId, limit)
      : cacheStmts.getMessages.all(groupToken, limit);
    return enrichWithPfp(rows.reverse());
  }

  // Cache miss: load from D1
  const rows = beforeId
    ? await d1All("SELECT * FROM messages WHERE group_token=? AND id<? ORDER BY id DESC LIMIT ?", [groupToken, beforeId, limit])
    : await d1All("SELECT * FROM messages WHERE group_token=? ORDER BY id DESC LIMIT ?", [groupToken, limit]);

  for (const m of rows) {
    cacheStmts.insertMessage.run(m.id, m.group_token, m.sender, m.content, m.reply_to_sender, m.reply_to_content, m.created_at);
  }
  // Only mark as fully cached if we got a full page (meaning we loaded from the tail)
  if (!beforeId) cacheStmts.markMessageGroupCached.run(metaKey, "1");

  return enrichWithPfp([...rows].reverse());
}

function enrichWithPfp(rows) {
  const senders = [...new Set(rows.map(r => r.sender))];
  const pfpMap = {};
  for (const s of senders) {
    const u = cacheStmts.getUser.get(s);
    pfpMap[s] = u?.pfp || "";
  }
  return rows.map(r => ({ ...r, pfp: pfpMap[r.sender] || "" }));
}

// ---------------------------------------------------------------------------
// Express setup
// ---------------------------------------------------------------------------
const app = express();

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
// Password helpers
// ---------------------------------------------------------------------------
function hashPassword(password, salt = crypto.randomBytes(16).toString("hex")) {
  return { hash: crypto.scryptSync(password, salt, 64).toString("hex"), salt };
}
function verifyPassword(password, hash, salt) {
  const check = crypto.scryptSync(password, salt, 64).toString("hex");
  return crypto.timingSafeEqual(Buffer.from(check), Buffer.from(hash));
}
function newToken() { return crypto.randomBytes(32).toString("hex"); }

function ah(fn) {
  return (req, res) => fn(req, res).catch(err => {
    console.error(err);
    if (!res.headersSent) res.status(500).json({ error: "Sunucu hatası: " + err.message });
  });
}

// ---------------------------------------------------------------------------
// Auth middleware — reads from local cache, falls through to D1 on miss
// ---------------------------------------------------------------------------
async function requireAuth(req, res, next) {
  try {
    const token = req.headers["authorization"];
    if (!token) return res.status(401).json({ error: "Yetkisiz erişim" });
    const row = await cacheGetSession(token);
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
// SSE hub (in-memory, unchanged)
// ---------------------------------------------------------------------------
const streamsByUser = new Map();
function addStream(username, res)    { if (!streamsByUser.has(username)) streamsByUser.set(username, new Set()); streamsByUser.get(username).add(res); }
function removeStream(username, res) { const s = streamsByUser.get(username); if (!s) return; s.delete(res); if (s.size === 0) streamsByUser.delete(username); }
function sendTo(username, obj)       { const s = streamsByUser.get(username); if (!s) return; const line = `data: ${JSON.stringify(obj)}\n\n`; for (const r of s) r.write(line); }
function sendToMany(users, obj)      { for (const u of users) sendTo(u, obj); }

async function groupMemberUsernames(groupToken) {
  // Always serve from cache (group members are warmed by the first /members call)
  const cached = cacheStmts.getMembers.all(groupToken);
  if (cached.length) return cached.map(r => r.username);
  // Fallback to D1 if not yet cached
  const rows = await d1All("SELECT username FROM group_members WHERE group_token=?", [groupToken]);
  return rows.map(r => r.username);
}

// ---------------------------------------------------------------------------
// Snapshot builder — uses cache-aside helpers
// ---------------------------------------------------------------------------
async function buildSnapshot(username) {
  const friends = await cacheGetFriendsOf(username);
  const groups  = await cacheGetGroupsForUser(username);
  const user    = cacheStmts.getUserPfp.get(username);
  return { friends, groups, user };
}

// ---------------------------------------------------------------------------
// Auth routes
// ---------------------------------------------------------------------------
app.post("/api/register", ah(async (req, res) => {
  const username = String(req.body.username || "").toLowerCase().replace(/[^a-z]/g, "");
  const password = String(req.body.password || "");
  if (!username || username.length < 3) return res.status(400).json({ error: "Geçersiz kullanıcı adı" });
  if (!password || password.length < 4)  return res.status(400).json({ error: "Şifre çok kısa" });

  const existing = await cacheGetUser(username);
  if (existing) return res.status(409).json({ error: "Bu kullanıcı adı zaten alınmış" });

  const { hash, salt } = hashPassword(password);
  // Count from cache (good enough — registration race conditions are harmless)
  const countRow = cacheStmts.getUserCount.get();
  const isFirst  = (countRow?.c || 0) === 0;
  const role     = isFirst ? "admin" : "user";
  const status   = isFirst ? "approved" : "pending";

  // Write to D1 first
  await d1Run(
    `INSERT INTO users (username,password_hash,password_salt,role,status,created_at) VALUES (?,?,?,?,?,?)`,
    [username, hash, salt, role, status, Date.now()]
  );
  // Warm into local cache
  cacheStmts.upsertUser.run(username, hash, salt, role, status, null, Date.now());

  res.json({ message: isFirst ? "Yönetici hesabı oluşturuldu, giriş yapabilirsiniz." : "Kayıt talebiniz yönetici onayına gönderildi." });
}));

app.post("/api/login", ah(async (req, res) => {
  const username = String(req.body.username || "").toLowerCase().replace(/[^a-z]/g, "");
  const password = String(req.body.password || "");
  const user = await cacheGetUser(username);
  if (!user || user.password_hash === "" || !verifyPassword(password, user.password_hash, user.password_salt)) {
    // password_hash may be empty if user was only partially cached; re-fetch from D1
    const fullUser = user?.password_hash === "" ? await d1Get("SELECT * FROM users WHERE username=?", [username]) : user;
    if (!fullUser || !verifyPassword(password, fullUser.password_hash, fullUser.password_salt))
      return res.status(401).json({ error: "Kullanıcı adı veya şifre hatalı" });
    // Warm the full user
    cacheStmts.upsertUser.run(fullUser.username, fullUser.password_hash, fullUser.password_salt, fullUser.role, fullUser.status, fullUser.pfp, fullUser.created_at);
    if (fullUser.status !== "approved") return res.status(403).json({ error: "Hesabınız henüz onaylanmadı" });
    const token = newToken();
    await d1Run("INSERT INTO sessions (token,username,created_at) VALUES (?,?,?)", [token, username, Date.now()]);
    cacheStmts.upsertSession.run(token, username, Date.now());
    return res.json({ token, username, role: fullUser.role, pfp: fullUser.pfp || "" });
  }
  if (user.status !== "approved") return res.status(403).json({ error: "Hesabınız henüz onaylanmadı" });
  const token = newToken();
  await d1Run("INSERT INTO sessions (token,username,created_at) VALUES (?,?,?)", [token, username, Date.now()]);
  cacheStmts.upsertSession.run(token, username, Date.now());
  res.json({ token, username, role: user.role, pfp: user.pfp || "" });
}));

app.post("/api/logout", requireAuth, ah(async (req, res) => {
  await d1Run("DELETE FROM sessions WHERE token=?", [req.authToken]);
  cacheStmts.deleteSession.run(req.authToken);
  res.json({ ok: true });
}));

app.get("/api/public/account-requests", ah(async (req, res) => {
  // Small table, acceptable to always read from cache (warmed by admin panel or first user)
  // Fallback to D1 if cache is empty
  let pending  = cacheStmts.getPendingUsers.all();
  let accepted = cacheStmts.getApprovedUsers.all();
  if (!pending.length && !accepted.length) {
    const allUsers = await d1All("SELECT * FROM users");
    for (const u of allUsers) cacheStmts.upsertUser.run(u.username, u.password_hash, u.password_salt, u.role, u.status, u.pfp, u.created_at);
    pending  = cacheStmts.getPendingUsers.all();
    accepted = cacheStmts.getApprovedUsers.all();
  }
  res.json({ pending: pending.map(r => r.username), accepted: accepted.map(r => r.username) });
}));

app.post("/api/stream/token", requireAuth, ah(async (req, res) => {
  const t = newToken();
  const expiresAt = Date.now() + 60_000;
  await d1Run("INSERT INTO stream_tokens (token,username,expires_at) VALUES (?,?,?)", [t, req.user.username, expiresAt]);
  cacheStmts.upsertStreamToken.run(t, req.user.username, expiresAt);
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

  // Check local cache first, then D1
  let row = cacheStmts.getStreamToken.get(streamToken);
  if (!row) row = await d1Get("SELECT username,expires_at FROM stream_tokens WHERE token=?", [streamToken]);

  await d1Run("DELETE FROM stream_tokens WHERE token=?", [streamToken]);
  cacheStmts.deleteStreamToken.run(streamToken);

  if (!row || row.expires_at < Date.now()) return res.status(401).end();

  const username = row.username;
  res.writeHead(200, {
    "Content-Type": "text/event-stream", "Cache-Control": "no-cache",
    Connection: "keep-alive", "X-Accel-Buffering": "no",
  });
  addStream(username, res);
  res.write(`data: ${JSON.stringify({ frameType: "I-FRAME", ...(await buildSnapshot(username)) })}\n\n`);
  const keepAlive = setInterval(() => res.write(":ping\n\n"), 25_000);
  req.on("close", () => { clearInterval(keepAlive); removeStream(username, res); });
}));

// ---------------------------------------------------------------------------
// User profile picture
// ---------------------------------------------------------------------------
app.post("/api/user/pfp", requireAuth, ah(async (req, res) => {
  const pfpBase64 = String(req.body.pfpBase64 || "");
  if (pfpBase64.length > 600_000) return res.status(413).json({ error: "Görsel çok büyük" });
  await d1Run("UPDATE users SET pfp=? WHERE username=?", [pfpBase64, req.user.username]);
  cacheStmts.updateUserPfp.run(pfpBase64, req.user.username);
  res.json({ ok: true });
}));

// ---------------------------------------------------------------------------
// Images — NOT cached locally (blobs too large). Still proxied via D1.
// ---------------------------------------------------------------------------
app.get("/api/images/:id", requireAuth, ah(async (req, res) => {
  const row = await d1Get("SELECT data FROM images WHERE id=?", [req.params.id], "imgdb");
  if (!row) return res.status(404).json({ error: "Görsel bulunamadı" });
  res.json({ content: row.data });
}));

// ---------------------------------------------------------------------------
// Friends
// ---------------------------------------------------------------------------
app.post("/api/friends/request", requireAuth, ah(async (req, res) => {
  const target = String(req.body.targetUsername || "").toLowerCase().replace(/[^a-z]/g, "");
  if (!target || target === req.user.username) return res.status(400).json({ error: "Geçersiz kullanıcı" });

  const targetUser = await cacheGetUser(target);
  if (!targetUser || targetUser.status !== "approved") return res.status(404).json({ error: "Kullanıcı bulunamadı" });

  const [u1, u2] = [req.user.username, target].sort();
  let existing = cacheStmts.getFriendPair.get(u1, u2);
  if (!existing) existing = await d1Get("SELECT * FROM friends WHERE user1=? AND user2=?", [u1, u2]);
  if (existing) return res.status(409).json({ error: "Zaten arkadaşsınız ya da istek bekliyor" });

  await d1Run("INSERT INTO friends (user1,user2,status,created_at) VALUES (?,?,?,?)", [req.user.username, target, "pending", Date.now()]);
  cacheStmts.upsertFriend.run(req.user.username, target, "pending", Date.now());
  // Invalidate friend cache for both users so next snapshot re-queries
  cache.prepare("DELETE FROM cache_meta WHERE key=?").run(`friends_loaded:${req.user.username}`);
  cache.prepare("DELETE FROM cache_meta WHERE key=?").run(`friends_loaded:${target}`);

  res.json({ message: "İstek gönderildi" });
  sendTo(target, { frameType: "P-FRAME", type: "FRIEND_REQUEST", payload: { from: req.user.username } });
}));

app.post("/api/friends/accept", requireAuth, ah(async (req, res) => {
  const target = String(req.body.targetUsername || "").toLowerCase().replace(/[^a-z]/g, "");
  let row = cacheStmts.getFriendPair.get(target, req.user.username);
  if (!row) row = await d1Get("SELECT * FROM friends WHERE user1=? AND user2=? AND status='pending'", [target, req.user.username]);
  if (!row || row.status !== "pending") return res.status(404).json({ error: "İstek bulunamadı" });

  await d1Run("UPDATE friends SET status='accepted' WHERE user1=? AND user2=?", [target, req.user.username]);
  cacheStmts.updateFriendStatus.run("accepted", target, req.user.username);

  const meRow     = cacheStmts.getUserPfp.get(req.user.username) || await d1Get("SELECT pfp FROM users WHERE username=?", [req.user.username]);
  const targetRow = cacheStmts.getUserPfp.get(target)             || await d1Get("SELECT pfp FROM users WHERE username=?", [target]);
  const pfpA = meRow?.pfp || "", pfpB = targetRow?.pfp || "";

  res.json({ ok: true });
  sendToMany([req.user.username, target], {
    frameType: "P-FRAME", type: "FRIEND_ACCEPTED",
    payload: {
      friendRowFor: {
        [req.user.username]: { user1: target, user2: req.user.username, status: "accepted", pfp: pfpB },
        [target]:             { user1: target, user2: req.user.username, status: "accepted", pfp: pfpA },
      },
    },
  });
}));

app.post("/api/friends/unfriend", requireAuth, ah(async (req, res) => {
  const target = String(req.body.targetUsername || "").toLowerCase().replace(/[^a-z]/g, "");
  const [u1, u2] = [req.user.username, target].sort();
  const info = await d1Run("DELETE FROM friends WHERE user1=? AND user2=?", [u1, u2]);
  if (info.changes === 0) return res.status(404).json({ error: "Arkadaşlık bulunamadı" });
  cacheStmts.deleteFriend.run(u1, u2);
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

  let friendRow = cacheStmts.getFriendPair.get(u1, u2);
  if (!friendRow) friendRow = await d1Get("SELECT status FROM friends WHERE user1=? AND user2=?", [u1, u2]);
  if (!friendRow || friendRow.status !== "accepted") return res.status(403).json({ error: "Önce arkadaş olmalısınız" });

  const gt = dmToken(req.user.username, target);
  let existing = cacheStmts.getGroup.get(gt);
  if (!existing) existing = await d1Get("SELECT group_token FROM groups_t WHERE group_token=?", [gt]);

  if (!existing) {
    const now = Date.now();
    await d1Run("INSERT INTO groups_t (group_token,group_name,created_by,allow_sub_invites,created_at) VALUES (?,?,?,0,?)", [gt, "@" + target, req.user.username, now]);
    await d1Run("INSERT INTO group_members (group_token,username,can_add_members,joined_at) VALUES (?,?,0,?)", [gt, req.user.username, now]);
    await d1Run("INSERT INTO group_members (group_token,username,can_add_members,joined_at) VALUES (?,?,0,?)", [gt, target, now]);
    cacheStmts.upsertGroup.run(gt, "@" + target, req.user.username, 0, now);
    cacheStmts.upsertMember.run(gt, req.user.username, 0, now);
    cacheStmts.upsertMember.run(gt, target, 0, now);
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
  await d1Run("INSERT INTO groups_t (group_token,group_name,created_by,allow_sub_invites,created_at) VALUES (?,?,?,1,?)", [gt, groupName, req.user.username, now]);
  await d1Run("INSERT INTO group_members (group_token,username,can_add_members,joined_at) VALUES (?,?,1,?)", [gt, req.user.username, now]);
  cacheStmts.upsertGroup.run(gt, groupName, req.user.username, 1, now);
  cacheStmts.upsertMember.run(gt, req.user.username, 1, now);
  // Mark as cached so subsequent member loads don't go to D1
  cacheStmts.markMembersGroupCached.run(`members_loaded:${gt}`, "1");
  res.json({ message: "Grup oluşturuldu", groupToken: gt });
}));

async function isMember(groupToken, username) {
  const hit = cacheStmts.getMember.get(groupToken, username);
  if (hit) return true;
  const row = await d1Get("SELECT 1 as x FROM group_members WHERE group_token=? AND username=?", [groupToken, username]);
  if (row) cacheStmts.upsertMember.run(groupToken, username, 1, Date.now());
  return !!row;
}
function isOwnerOrAdmin(group, user) {
  return group.created_by === user.username || user.role === "admin";
}

app.get("/api/groups/:token/members", requireAuth, ah(async (req, res) => {
  const gt = req.params.token;
  if (!(await isMember(gt, req.user.username))) return res.status(403).json({ error: "Bu grubun üyesi değilsiniz" });
  const data = await cacheGetGroupMembers(gt);
  if (!data.groupInfo) return res.status(404).json({ error: "Grup bulunamadı" });
  res.json(data);
}));

app.post("/api/groups/add-member", requireAuth, ah(async (req, res) => {
  const { groupToken, targetUsername } = req.body;
  const target = String(targetUsername || "").toLowerCase().replace(/[^a-z]/g, "");

  let group = cacheStmts.getGroup.get(groupToken);
  if (!group) group = await d1Get("SELECT * FROM groups_t WHERE group_token=?", [groupToken]);
  if (!group) return res.status(404).json({ error: "Grup bulunamadı" });

  const me = cacheStmts.getMember.get(groupToken, req.user.username)
          || await d1Get("SELECT * FROM group_members WHERE group_token=? AND username=?", [groupToken, req.user.username]);
  if (!me) return res.status(403).json({ error: "Bu grubun üyesi değilsiniz" });

  const owner = isOwnerOrAdmin(group, req.user);
  if (!owner && !(group.allow_sub_invites && me.can_add_members)) return res.status(403).json({ error: "Üye ekleme yetkiniz yok" });

  const [u1, u2] = [req.user.username, target].sort();
  let friendRow = cacheStmts.getFriendPair.get(u1, u2);
  if (!friendRow) friendRow = await d1Get("SELECT status FROM friends WHERE user1=? AND user2=?", [u1, u2]);
  if (!friendRow || friendRow.status !== "accepted") return res.status(403).json({ error: "Sadece arkadaşlarınızı ekleyebilirsiniz" });

  if (await isMember(groupToken, target)) return res.status(409).json({ error: "Kullanıcı zaten grupta" });

  const now = Date.now();
  await d1Run("INSERT INTO group_members (group_token,username,can_add_members,joined_at) VALUES (?,?,1,?)", [groupToken, target, now]);
  cacheStmts.upsertMember.run(groupToken, target, 1, now);

  const targetUserRow = cacheStmts.getUserPfp.get(target) || await d1Get("SELECT pfp FROM users WHERE username=?", [target]);
  const newMember = { username: target, can_add_members: 1, pfp: targetUserRow?.pfp || "" };

  res.json({ message: "Üye eklendi" });
  const recipients = await groupMemberUsernames(groupToken);
  sendToMany(recipients, { frameType: "P-FRAME", type: "GROUP_MEMBER_ADDED", payload: { groupToken, newMember, group } });
}));

app.post("/api/groups/remove-member", requireAuth, ah(async (req, res) => {
  const { groupToken, targetUsername } = req.body;
  let group = cacheStmts.getGroup.get(groupToken);
  if (!group) group = await d1Get("SELECT * FROM groups_t WHERE group_token=?", [groupToken]);
  if (!group) return res.status(404).json({ error: "Grup bulunamadı" });
  if (!isOwnerOrAdmin(group, req.user)) return res.status(403).json({ error: "Yetkiniz yok" });
  if (targetUsername === group.created_by) return res.status(400).json({ error: "Kurucu çıkarılamaz" });

  const recipientsBefore = await groupMemberUsernames(groupToken);
  await d1Run("DELETE FROM group_members WHERE group_token=? AND username=?", [groupToken, targetUsername]);
  cacheStmts.deleteMember.run(groupToken, targetUsername);
  res.json({ ok: true });
  sendToMany(recipientsBefore, { frameType: "P-FRAME", type: "GROUP_MEMBER_LEFT", payload: { groupToken, username: targetUsername } });
}));

app.post("/api/groups/leave", requireAuth, ah(async (req, res) => {
  const { groupToken } = req.body;
  let group = cacheStmts.getGroup.get(groupToken);
  if (!group) group = await d1Get("SELECT * FROM groups_t WHERE group_token=?", [groupToken]);
  if (!group) return res.status(404).json({ error: "Grup bulunamadı" });
  if (group.created_by === req.user.username) return res.status(400).json({ error: "Kurucu gruptan ayrılamaz, grubu silin" });
  if (!(await isMember(groupToken, req.user.username))) return res.status(403).json({ error: "Bu grubun üyesi değilsiniz" });

  const recipientsBefore = await groupMemberUsernames(groupToken);
  await d1Run("DELETE FROM group_members WHERE group_token=? AND username=?", [groupToken, req.user.username]);
  cacheStmts.deleteMember.run(groupToken, req.user.username);
  res.json({ ok: true });
  sendToMany(recipientsBefore, { frameType: "P-FRAME", type: "GROUP_MEMBER_LEFT", payload: { groupToken, username: req.user.username } });
}));

app.post("/api/groups/toggle-sub-invites", requireAuth, ah(async (req, res) => {
  const { groupToken, allowSubInvites } = req.body;
  let group = cacheStmts.getGroup.get(groupToken);
  if (!group) group = await d1Get("SELECT * FROM groups_t WHERE group_token=?", [groupToken]);
  if (!group) return res.status(404).json({ error: "Grup bulunamadı" });
  if (!isOwnerOrAdmin(group, req.user)) return res.status(403).json({ error: "Yetkiniz yok" });

  const val = allowSubInvites ? 1 : 0;
  await d1Run("UPDATE groups_t SET allow_sub_invites=? WHERE group_token=?", [val, groupToken]);
  cacheStmts.updateGroupSubInvites.run(val, groupToken);
  res.json({ ok: true });
  sendToMany(await groupMemberUsernames(groupToken), { frameType: "P-FRAME", type: "GROUP_SETTING_UPDATED", payload: { groupToken, allow_sub_invites: val } });
}));

app.post("/api/groups/toggle-member-invite-perm", requireAuth, ah(async (req, res) => {
  const { groupToken, targetUsername, canAddMembers } = req.body;
  let group = cacheStmts.getGroup.get(groupToken);
  if (!group) group = await d1Get("SELECT * FROM groups_t WHERE group_token=?", [groupToken]);
  if (!group) return res.status(404).json({ error: "Grup bulunamadı" });
  if (!isOwnerOrAdmin(group, req.user)) return res.status(403).json({ error: "Yetkiniz yok" });

  const val = canAddMembers ? 1 : 0;
  await d1Run("UPDATE group_members SET can_add_members=? WHERE group_token=? AND username=?", [val, groupToken, targetUsername]);
  cacheStmts.updateMemberPerm.run(val, groupToken, targetUsername);
  res.json({ ok: true });
  sendToMany(await groupMemberUsernames(groupToken), { frameType: "P-FRAME", type: "GROUP_PERM_UPDATED", payload: { groupToken, username: targetUsername, can_add_members: val } });
}));

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------
app.get("/api/messages/:token", requireAuth, ah(async (req, res) => {
  const gt = req.params.token;
  if (!(await isMember(gt, req.user.username))) return res.status(403).json({ error: "Bu grubun üyesi değilsiniz" });

  const limit  = Math.min(parseInt(req.query.limit, 10) || 50, 100);
  const before = req.query.before ? parseInt(req.query.before, 10) : null;
  const rows   = await cacheGetMessages(gt, limit, before);
  res.json(rows);
}));

app.post("/api/messages/send", requireAuth, ah(async (req, res) => {
  const { groupToken, content, replyToSender, replyToContent } = req.body;
  if (!(await isMember(groupToken, req.user.username))) return res.status(403).json({ error: "Bu grubun üyesi değilsiniz" });
  if (!content || typeof content !== "string") return res.status(400).json({ error: "Geçersiz mesaj" });

  let storedContent = content;
  let newId;

  if (content.startsWith("data:image/")) {
    if (content.length > 1_400_000) return res.status(413).json({ error: "Görsel çok büyük" });
    const imgInsert = await d1Run("INSERT INTO images (data,created_at) VALUES (?,?)", [content, Date.now()], "imgdb");
    storedContent = `img:${imgInsert.lastInsertRowid}`;
  } else if (content.length > 4000) {
    return res.status(413).json({ error: "Mesaj çok uzun" });
  }

  let storedReplyContent = replyToContent || null;
  if (storedReplyContent && storedReplyContent.startsWith("data:image/")) storedReplyContent = "img:0";

  const now  = Date.now();
  const info = await d1Run(
    "INSERT INTO messages (group_token,sender,content,reply_to_sender,reply_to_content,created_at) VALUES (?,?,?,?,?,?)",
    [groupToken, req.user.username, storedContent, replyToSender || null, storedReplyContent, now]
  );
  newId = info.lastInsertRowid;

  // Warm into local message cache
  cacheStmts.insertMessage.run(newId, groupToken, req.user.username, storedContent, replyToSender || null, storedReplyContent, now);

  const senderRow = cacheStmts.getUserPfp.get(req.user.username);
  const payload = {
    id: newId, group_token: groupToken, sender: req.user.username, content: storedContent,
    reply_to_sender: replyToSender || null, reply_to_content: storedReplyContent,
    created_at: now, pfp: senderRow?.pfp || "",
  };
  res.json({ ok: true, id: newId });
  sendToMany(await groupMemberUsernames(groupToken), { frameType: "P-FRAME", type: "NEW_MESSAGE", payload });
}));

app.post("/api/messages/delete", requireAuth, ah(async (req, res) => {
  const { messageId, groupToken } = req.body;
  let msg = cacheStmts.getMessage.get(messageId, groupToken);
  if (!msg) msg = await d1Get("SELECT * FROM messages WHERE id=? AND group_token=?", [messageId, groupToken]);
  if (!msg) return res.status(404).json({ error: "Mesaj bulunamadı" });

  let group = cacheStmts.getGroup.get(groupToken);
  if (!group) group = await d1Get("SELECT * FROM groups_t WHERE group_token=?", [groupToken]);
  const canDelete = msg.sender === req.user.username || isOwnerOrAdmin(group, req.user);
  if (!canDelete) return res.status(403).json({ error: "Bu mesajı silme yetkiniz yok" });

  await d1Run("DELETE FROM messages WHERE id=?", [messageId]);
  cacheStmts.deleteMessage.run(messageId);
  if (typeof msg.content === "string" && msg.content.startsWith("img:")) {
    const imgId = msg.content.split(":")[1];
    d1Run("DELETE FROM images WHERE id=?", [imgId], "imgdb").catch(() => {});
  }
  res.json({ ok: true });
  sendToMany(await groupMemberUsernames(groupToken), { frameType: "P-FRAME", type: "MESSAGE_DELETED", payload: { messageId, groupToken } });
}));

// ---------------------------------------------------------------------------
// Admin
// ---------------------------------------------------------------------------
app.get("/api/admin/pending", requireAuth, requireAdmin, ah(async (req, res) => {
  // Ensure all users are loaded
  const allUsers = await d1All("SELECT * FROM users");
  for (const u of allUsers) {
    const ex = cacheStmts.getUser.get(u.username);
    if (!ex || ex.password_hash === "") cacheStmts.upsertUser.run(u.username, u.password_hash, u.password_salt, u.role, u.status, u.pfp, u.created_at);
  }
  res.json(cacheStmts.getPendingUsers.all());
}));

app.get("/api/admin/users", requireAuth, requireAdmin, ah(async (req, res) => {
  const allUsers = await d1All("SELECT * FROM users");
  for (const u of allUsers) {
    const ex = cacheStmts.getUser.get(u.username);
    if (!ex || ex.password_hash === "") cacheStmts.upsertUser.run(u.username, u.password_hash, u.password_salt, u.role, u.status, u.pfp, u.created_at);
  }
  res.json(cacheStmts.getApprovedUsers.all());
}));

app.post("/api/admin/action", requireAuth, requireAdmin, ah(async (req, res) => {
  const { id, action } = req.body;
  const user = cacheStmts.getUserByRowid.get(id) || await d1Get("SELECT * FROM users WHERE rowid=?", [id]);
  if (!user) return res.status(404).json({ error: "Kullanıcı bulunamadı" });
  if (action === "approve") {
    await d1Run("UPDATE users SET status='approved' WHERE rowid=?", [id]);
    cacheStmts.updateUserByRowid.run("approved", id);
  } else if (action === "deny") {
    await d1Run("DELETE FROM users WHERE rowid=?", [id]);
    cacheStmts.deleteUserByRowid.run(id);
  } else return res.status(400).json({ error: "Geçersiz işlem" });
  res.json({ ok: true });
}));

app.post("/api/admin/delete-user", requireAuth, requireAdmin, ah(async (req, res) => {
  const { username } = req.body;
  if (username === req.user.username) return res.status(400).json({ error: "Kendinizi silemezsiniz" });
  const info = await d1Run("DELETE FROM users WHERE username=?", [username]);
  if (info.changes === 0) return res.status(404).json({ error: "Kullanıcı bulunamadı" });
  await d1Run("DELETE FROM sessions WHERE username=?", [username]);
  cacheStmts.deleteUser.run(username);
  cacheStmts.deleteSessionsByUser.run(username);
  res.json({ message: "Kullanıcı silindi" });
}));

// ---------------------------------------------------------------------------
// Static client
// ---------------------------------------------------------------------------
app.use(express.static(path.join(__dirname, "public")));

// Prune expired stream tokens from both stores
setInterval(() => {
  cacheStmts.pruneStreamTokens.run(Date.now());
  d1Run("DELETE FROM stream_tokens WHERE expires_at<?", [Date.now()]).catch(e => console.error("prune:", e.message));
}, 60_000);

// ---------------------------------------------------------------------------
// Boot
// ---------------------------------------------------------------------------
initSchema()
  .then(() => app.listen(PORT, () => console.log(`SOPERT listening on :${PORT} (cache: ${CACHE_PATH}, D1: ${WORKER_URL})`)))
  .catch(err => { console.error("FATAL:", err.message); process.exit(1); });
