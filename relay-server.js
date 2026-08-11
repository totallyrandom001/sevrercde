// ============================================================================
// SOPERT — Render + Turso
// ============================================================================

const express             = require("express");
const crypto              = require("crypto");
const { WebSocketServer } = require("ws");
const { createClient }    = require("@libsql/client");

const PORT = process.env.PORT || 3000;

// ============================================================================
// Logger (console only — no file I/O on Render free tier)
// ============================================================================
const LOG_LEVEL_PRIORITY = { DEBUG: 0, INFO: 1, WARN: 2, ERROR: 3 };
const MIN_LEVEL = (process.env.LOG_LEVEL || "INFO").toUpperCase();
const COLOURS = {
  reset: "\x1b[0m", dim: "\x1b[2m",
  DEBUG: "\x1b[36m", INFO: "\x1b[32m", WARN: "\x1b[33m", ERROR: "\x1b[31m",
  field: "\x1b[90m", val: "\x1b[97m",
};

function log(level, category, message, ctx = {}) {
  if (LOG_LEVEL_PRIORITY[level] < LOG_LEVEL_PRIORITY[MIN_LEVEL]) return;
  const ts = new Date().toISOString();
  const C = COLOURS, cc = C[level] || C.reset;
  const ctxParts = Object.entries(ctx).map(([k, v]) => {
    let sv = typeof v === "object" ? JSON.stringify(v) : String(v);
    if (sv.length > 120) sv = sv.slice(0, 117) + "…";
    return `${C.field}${k}${C.reset}=${C.val}${sv}${C.reset}`;
  });
  const line = [
    `${C.dim}${ts}${C.reset}`,
    `${cc}[${level.padEnd(5)}]${C.reset}`,
    `${cc}[${category.padEnd(8)}]${C.reset}`,
    message,
    ctxParts.length ? "  " + ctxParts.join("  ") : "",
  ].join(" ");
  (level === "ERROR" ? process.stderr : process.stdout).write(line + "\n");
}

const logger = {
  debug: (c, m, x) => log("DEBUG", c, m, x),
  info:  (c, m, x) => log("INFO",  c, m, x),
  warn:  (c, m, x) => log("WARN",  c, m, x),
  error: (c, m, x) => log("ERROR", c, m, x),
};

// ============================================================================
// Turso DB
// ============================================================================
const db = createClient({
  url:       process.env.TURSO_URL,
  authToken: process.env.TURSO_TOKEN,
});

async function initSchema() {
  const stmts = [
    `CREATE TABLE IF NOT EXISTS users (
      username TEXT PRIMARY KEY, password TEXT NOT NULL,
      role TEXT NOT NULL DEFAULT 'user', status TEXT NOT NULL DEFAULT 'pending',
      pfp TEXT, created_at INTEGER NOT NULL)`,
    `CREATE TABLE IF NOT EXISTS sessions (
      token TEXT PRIMARY KEY, username TEXT NOT NULL, created_at INTEGER NOT NULL)`,
    `CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(username)`,
    `CREATE TABLE IF NOT EXISTS stream_tokens (
      token TEXT PRIMARY KEY, username TEXT NOT NULL, expires_at INTEGER NOT NULL)`,
    `CREATE TABLE IF NOT EXISTS friends (
      user1 TEXT NOT NULL, user2 TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending', created_at INTEGER NOT NULL,
      PRIMARY KEY (user1, user2))`,
    `CREATE TABLE IF NOT EXISTS groups_t (
      group_token TEXT PRIMARY KEY, group_name TEXT NOT NULL,
      created_by TEXT NOT NULL, allow_sub_invites INTEGER NOT NULL DEFAULT 1,
      created_at INTEGER NOT NULL)`,
    `CREATE TABLE IF NOT EXISTS group_members (
      group_token TEXT NOT NULL, username TEXT NOT NULL,
      can_add_members INTEGER NOT NULL DEFAULT 1, joined_at INTEGER NOT NULL,
      PRIMARY KEY (group_token, username))`,
    `CREATE INDEX IF NOT EXISTS idx_gm_user ON group_members(username)`,
    `CREATE TABLE IF NOT EXISTS messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT, group_token TEXT NOT NULL,
      sender TEXT NOT NULL, content TEXT NOT NULL,
      reply_to_sender TEXT, reply_to_content TEXT, created_at INTEGER NOT NULL)`,
    `CREATE INDEX IF NOT EXISTS idx_msg_group ON messages(group_token, id)`,
    `CREATE TABLE IF NOT EXISTS images (
      id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT NOT NULL,
      group_token TEXT, created_at INTEGER NOT NULL)`,
  ];
  for (const sql of stmts) await db.execute(sql);
  logger.info("DB", "Schema ready");
}

async function initDefaultGroups() {
  const now = Date.now();
  // Check and create "everyone"
  const everyoneGroup = await q.getGroup(GRP_EVERYONE);
  if (!everyoneGroup) {
    await q.insertGroup(GRP_EVERYONE, "everyone", SYSTEM_FRIEND, 0, now);
    logger.info("DB", "Created default group: everyone");
  }
  
  // Check and create "anonslar"
  const anonslarGroup = await q.getGroup(GRP_ANONSLAR);
  if (!anonslarGroup) {
    await q.insertGroup(GRP_ANONSLAR, "anonslar", SYSTEM_FRIEND, 0, now);
    logger.info("DB", "Created default group: anonslar");
  }
}

// ============================================================================
// Query helpers (all async)
// ============================================================================
const q = {
  // Users
  getUser:           async (u)             => (await db.execute({ sql: "SELECT * FROM users WHERE username=?", args: [u] })).rows[0] || null,
  getUserPfp:        async (u)             => (await db.execute({ sql: "SELECT pfp FROM users WHERE username=?", args: [u] })).rows[0] || null,
  insertUser:        async (u,p,r,s,t)     => db.execute({ sql: "INSERT INTO users(username,password,role,status,created_at)VALUES(?,?,?,?,?)", args: [u,p,r,s,t] }),
  updateUserPfp:     async (pfp,u)         => db.execute({ sql: "UPDATE users SET pfp=? WHERE username=?", args: [pfp,u] }),
  updateUserStatus:  async (s,id)          => db.execute({ sql: "UPDATE users SET status=? WHERE rowid=?", args: [s,id] }),
  updateUserPassword:async (p,u)           => db.execute({ sql: "UPDATE users SET password=? WHERE username=?", args: [p,u] }),
  deleteUser:        async (u)             => db.execute({ sql: "DELETE FROM users WHERE username=?", args: [u] }),
  deleteUserById:    async (id)            => db.execute({ sql: "DELETE FROM users WHERE rowid=?", args: [id] }),
  getUserById:       async (id)            => (await db.execute({ sql: "SELECT *,rowid FROM users WHERE rowid=?", args: [id] })).rows[0] || null,
  countUsers:        async ()              => (await db.execute("SELECT COUNT(*) as c FROM users")).rows[0],
  getPending:        async ()              => (await db.execute("SELECT rowid as id, username FROM users WHERE status='pending' AND username NOT IN (SELECT DISTINCT sender FROM messages)")).rows,
  getApproved:       async ()              => (await db.execute("SELECT username,role FROM users WHERE status='approved' AND username!='totally' AND username NOT IN (SELECT DISTINCT sender FROM messages)")).rows,
  getAdminUserList:  async ()              => (await db.execute("SELECT rowid as id,username,role,status FROM users WHERE status!='pending' AND username!='totally'")).rows,

  // Sessions
  getSessionRaw:     async (t)             => (await db.execute({ sql: "SELECT s.token,s.username,s.created_at,u.role,u.status FROM sessions s JOIN users u ON u.username=s.username WHERE s.token=?", args: [t] })).rows[0] || null,
  insertSession:     async (t,u,ts)        => db.execute({ sql: "INSERT INTO sessions(token,username,created_at)VALUES(?,?,?)", args: [t,u,ts] }),
  deleteSession:     async (t)             => db.execute({ sql: "DELETE FROM sessions WHERE token=?", args: [t] }),
  deleteUserSessions:async (u)             => db.execute({ sql: "DELETE FROM sessions WHERE username=?", args: [u] }),
  pruneSessions:     async (before)        => db.execute({ sql: "DELETE FROM sessions WHERE created_at<?", args: [before] }),
  countUserSessions: async (u)             => (await db.execute({ sql: "SELECT COUNT(*) as c FROM sessions WHERE username=?", args: [u] })).rows[0],

  // Stream tokens
  pruneStreamTokens: async (before)        => db.execute({ sql: "DELETE FROM stream_tokens WHERE expires_at<?", args: [before] }),

  // Friends
  getFriend:    async (u1,u2)              => (await db.execute({ sql: "SELECT * FROM friends WHERE user1=? AND user2=?", args: [u1,u2] })).rows[0] || null,
  getFriendsOf: async (u)                  => (await db.execute({ sql: "SELECT f.user1,f.user2,f.status,u2.pfp FROM friends f JOIN users u2 ON u2.username=(CASE WHEN f.user1=? THEN f.user2 ELSE f.user1 END) WHERE f.user1=? OR f.user2=?", args: [u,u,u] })).rows,
  insertFriend: async (u1,u2,s,t)          => db.execute({ sql: "INSERT INTO friends(user1,user2,status,created_at)VALUES(?,?,?,?)", args: [u1,u2,s,t] }),
  updateFriend: async (s,u1,u2)            => db.execute({ sql: "UPDATE friends SET status=? WHERE user1=? AND user2=?", args: [s,u1,u2] }),
  deleteFriend: async (u1,u2)              => db.execute({ sql: "DELETE FROM friends WHERE user1=? AND user2=?", args: [u1,u2] }),

  // Groups
  getGroup:          async (gt)            => (await db.execute({ sql: "SELECT * FROM groups_t WHERE group_token=?", args: [gt] })).rows[0] || null,
  getGroupsForUser:  async (u)             => (await db.execute({ sql: "SELECT g.* FROM groups_t g JOIN group_members gm ON gm.group_token=g.group_token WHERE gm.username=?", args: [u] })).rows,
  insertGroup:       async (gt,gn,cb,asi,t)=> db.execute({ sql: "INSERT INTO groups_t(group_token,group_name,created_by,allow_sub_invites,created_at)VALUES(?,?,?,?,?)", args: [gt,gn,cb,asi,t] }),
  updateSubInvites:  async (v,gt)          => db.execute({ sql: "UPDATE groups_t SET allow_sub_invites=? WHERE group_token=?", args: [v,gt] }),
  updateGroupCreator:async (u,gt)          => db.execute({ sql: "UPDATE groups_t SET created_by=? WHERE group_token=?", args: [u,gt] }),
  deleteGroup:       async (gt)            => db.execute({ sql: "DELETE FROM groups_t WHERE group_token=?", args: [gt] }),

  // Group members
  getMember:          async (gt,u)         => (await db.execute({ sql: "SELECT * FROM group_members WHERE group_token=? AND username=?", args: [gt,u] })).rows[0] || null,
  getMembers:         async (gt)           => (await db.execute({ sql: "SELECT gm.username,gm.can_add_members,u.pfp FROM group_members gm JOIN users u ON u.username=gm.username WHERE gm.group_token=?", args: [gt] })).rows,
  getMemberNames:     async (gt)           => (await db.execute({ sql: "SELECT username FROM group_members WHERE group_token=?", args: [gt] })).rows,
  insertMember:       async (gt,u,cam,t)   => db.execute({ sql: "INSERT INTO group_members(group_token,username,can_add_members,joined_at)VALUES(?,?,?,?)", args: [gt,u,cam,t] }),
  deleteMember:       async (gt,u)         => db.execute({ sql: "DELETE FROM group_members WHERE group_token=? AND username=?", args: [gt,u] }),
  updateMemberPerm:   async (v,gt,u)       => db.execute({ sql: "UPDATE group_members SET can_add_members=? WHERE group_token=? AND username=?", args: [v,gt,u] }),
  deleteGroupMembers: async (gt)           => db.execute({ sql: "DELETE FROM group_members WHERE group_token=?", args: [gt] }),
  getRandomOtherMember:async(gt,u)         => (await db.execute({ sql: "SELECT username FROM group_members WHERE group_token=? AND username!=? ORDER BY RANDOM() LIMIT 1", args: [gt,u] })).rows[0] || null,

  // Messages
  getMessages:        async (gt,lim)       => (await db.execute({ sql: "SELECT * FROM messages WHERE group_token=? ORDER BY id DESC LIMIT ?", args: [gt,lim] })).rows,
  getMessagesBefore:  async (gt,b,lim)     => (await db.execute({ sql: "SELECT * FROM messages WHERE group_token=? AND id<? ORDER BY id DESC LIMIT ?", args: [gt,b,lim] })).rows,
  getMessage:         async (id,gt)        => (await db.execute({ sql: "SELECT * FROM messages WHERE id=? AND group_token=?", args: [id,gt] })).rows[0] || null,
  insertMessage:      async (gt,s,c,rs,rc,t) => db.execute({ sql: "INSERT INTO messages(group_token,sender,content,reply_to_sender,reply_to_content,created_at)VALUES(?,?,?,?,?,?)", args: [gt,s,c,rs,rc,t] }),
  deleteMessage:      async (id)           => db.execute({ sql: "DELETE FROM messages WHERE id=?", args: [id] }),
  deleteGroupMessages:async (gt)           => db.execute({ sql: "DELETE FROM messages WHERE group_token=?", args: [gt] }),
  getAllGroups:        async ()             => (await db.execute("SELECT group_token,group_name,created_by FROM groups_t")).rows,
  getAllMessages:      async (gt)           => (await db.execute({ sql: "SELECT * FROM messages WHERE group_token=? ORDER BY id ASC", args: [gt] })).rows,

  // Images
  insertImage: async (data,gt,t)           => db.execute({ sql: "INSERT INTO images(data,group_token,created_at)VALUES(?,?,?)", args: [data,gt,t] }),
  getImage:    async (id)                  => (await db.execute({ sql: "SELECT data FROM images WHERE id=?", args: [id] })).rows[0] || null,
  getImageMeta:async (id)                  => (await db.execute({ sql: "SELECT group_token FROM images WHERE id=?", args: [id] })).rows[0] || null,
  deleteImage: async (id)                  => db.execute({ sql: "DELETE FROM images WHERE id=?", args: [id] }),
};

// ============================================================================
// Password handling
// ============================================================================
const SCRYPT_KEYLEN = 64;

function hashPassword(password) {
  const salt = crypto.randomBytes(16).toString("hex");
  const hash = crypto.scryptSync(String(password), salt, SCRYPT_KEYLEN).toString("hex");
  return { hash: `scrypt:${salt}:${hash}` };
}

function verifyPassword(password, stored) {
  if (typeof stored !== "string") return false;
  if (stored.startsWith("scrypt:")) {
    const parts = stored.split(":");
    if (parts.length !== 3) return false;
    const [, salt, hashHex] = parts;
    let candidate;
    try { candidate = crypto.scryptSync(String(password), salt, SCRYPT_KEYLEN); } catch { return false; }
    let stored_;
    try { stored_ = Buffer.from(hashHex, "hex"); } catch { return false; }
    if (candidate.length !== stored_.length) return false;
    return crypto.timingSafeEqual(candidate, stored_);
  }
  const a = Buffer.from(String(password));
  const b = Buffer.from(String(stored));
  if (a.length !== b.length) { crypto.timingSafeEqual(Buffer.alloc(a.length), Buffer.alloc(a.length)); return false; }
  return crypto.timingSafeEqual(a, b);
}

function isLegacyPlaintext(stored) { return typeof stored === "string" && !stored.startsWith("scrypt:"); }

// ============================================================================
// Superadmin (no terminal prompt on Render — password check is sufficient)
// ============================================================================
const SUPERADMIN_USERNAME = "admin";
const SUPERADMIN_PASSWORD = "sopertyonetimhesap2023";
const SUPERADMIN_SENTINEL = "__admin__";
const SUPERADMIN_ROLE     = "superadmin";
const superadminSessions  = new Set();

function safeEqual(a, b) {
  const ba = Buffer.from(String(a));
  const bb = Buffer.from(String(b));
  if (ba.length !== bb.length) { crypto.timingSafeEqual(Buffer.alloc(ba.length), Buffer.alloc(ba.length)); return false; }
  return crypto.timingSafeEqual(ba, bb);
}

function isSuperadminSession(token) { return typeof token === "string" && superadminSessions.has(token); }

const SUPERADMIN_LOGIN_STAGES = [{ max: 3, cooldownMs: 5 * 60_000 }, { max: 1, cooldownMs: 30 * 60_000 }];

// ============================================================================
// Helpers
// ============================================================================
function newToken() { return crypto.randomBytes(32).toString("hex"); }

function escapeHtml(str) {
  return String(str)
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}

const VALID_IMAGE_DATA_URI = /^data:image\/(png|jpe?g|gif|webp);base64,[A-Za-z0-9+/]+=*$/;
function isValidImageDataUri(str) {
  if (typeof str !== "string") return false;
  if (str.length > 1_400_000) return false;
  return VALID_IMAGE_DATA_URI.test(str);
}

async function enrichWithPfp(rows) {
  const senders = [...new Set(rows.map(r => r.sender))];
  const pfpMap  = {};
  for (const s of senders) pfpMap[s] = (await q.getUserPfp(s))?.pfp || "";
  return rows.map(r => ({ ...r, pfp: pfpMap[r.sender] || "" }));
}

async function groupMemberUsernames(groupToken) {
  return (await q.getMemberNames(groupToken)).map(r => r.username);
}

const SYSTEM_FRIEND = "totally";
const GRP_EVERYONE = "grp_everyone";
const GRP_ANONSLAR = "grp_anonslar";
const ALLOWED_ANONS_SENDERS = ["totally", "mehmetfezup"];
async function autoFriendWithTotally(newUsername) {
  if (newUsername === SYSTEM_FRIEND) return;
  const systemUser = await q.getUser(SYSTEM_FRIEND);
  if (!systemUser) return;
  const [u1, u2] = [newUsername, SYSTEM_FRIEND].sort();
  try {
    await q.insertFriend(u1, u2, "accepted", Date.now());
    logger.info("FRIEND", `Auto-friended new user with ${SYSTEM_FRIEND}`, { username: newUsername });
  } catch (e) {
    if (!e.message?.includes("UNIQUE")) throw e;
  }
}

async function autoJoinDefaultGroups(username) {
  const now = Date.now();
  // Use try/catch to ignore unique constraint errors if they are already in the group
  try { await q.insertMember(GRP_EVERYONE, username, 0, now); } catch (e) {}
  try { await q.insertMember(GRP_ANONSLAR, username, 0, now); } catch (e) {}
  logger.info("GROUP", "Added user to default groups", { username });
}

// ah — wraps both sync and async handlers, catches errors
function ah(fn) {
  return (req, res) => {
    Promise.resolve().then(() => fn(req, res)).catch(err => {
      logger.error("HANDLER", "Unhandled exception in route handler", {
        method: req.method, path: req.path, error: err.message,
        user: req.user?.username || "unauthenticated",
      });
      if (!res.headersSent) res.status(999).json({ error: "Sunucu hatası: " + err.message });
    });
  };
}

// asyncMw — wraps async middleware so errors reach Express error handler
function asyncMw(fn) {
  return (req, res, next) => {
    Promise.resolve(fn(req, res, next)).catch(next);
  };
}

// ============================================================================
// Express
// ============================================================================
const app = express();
app.set("trust proxy", 1);

app.use((req, res, next) => {
  const allowedOrigins = [
    "https://totallyrandom001.github.io",
    "https://sopertchat.pages.dev",
  ];
  const origin = req.headers.origin;
  if (allowedOrigins.includes(origin)) res.setHeader("Access-Control-Allow-Origin", origin);
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, x-original-ip");
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

app.use(express.json({ limit: "2mb" }));
app.use((err, req, res, next) => {
  if (err.type === "entity.parse.failed" || err instanceof SyntaxError)
    return res.status(400).json({ error: "Invalid JSON body" });
  next(err);
});

// ============================================================================
// Request logging
// ============================================================================
app.use((req, res, next) => {
  const start = Date.now();
  const reqId = crypto.randomBytes(6).toString("hex");
  req._reqId  = reqId;
  const isStream = req.path === "/api/stream";
  logger[isStream ? "debug" : "info"]("HTTP", "→ Incoming request", {
    reqId, method: req.method, path: req.path, ip: req.ip,
  });
  const _json = res.json.bind(res);
  res.json = function(body) { res._logBody = body; return _json(body); };
  res.on("finish", () => {
    const ms = Date.now() - start, status = res.statusCode;
    const level = status >= 500 ? "error" : status >= 400 ? "warn" : isStream ? "debug" : "info";
    logger[level]("HTTP", "← Response sent", { reqId, method: req.method, path: req.path, status, ms, user: req.user?.username || "—" });
  });
  next();
});

// ============================================================================
// Attack detection
// ============================================================================
const ATTACK_TIMEOUT_MS = 10 * 60_000;
const blockedIps = new Map();

function isIpBlocked(ip) {
  const until = blockedIps.get(ip);
  if (!until) return false;
  if (Date.now() >= until) { blockedIps.delete(ip); return false; }
  return true;
}
function blockIp(ip) { blockedIps.set(ip, Date.now() + ATTACK_TIMEOUT_MS); }
setInterval(() => { const now = Date.now(); for (const [ip, until] of blockedIps) { if (now >= until) blockedIps.delete(ip); } }, 60_000);

const ATTACK_PATTERNS = [
  /(\bUNION\b.{0,20}\bSELECT\b)/i, /(\bOR\b\s+['"]?\d\s*=\s*\d)/i,
  /(\bDROP\b\s+\bTABLE\b)/i, /(;\s*(DROP|DELETE|INSERT|UPDATE)\s)/i,
  /(<script[\s>])/i, /(javascript:)/i, /(\.\.\/){2,}/, /(\$\{.*\})/, /(\bexec\b.{0,10}\()/i,
];

function detectAttack(value) {
  if (typeof value !== "string") return null;
  for (const pattern of ATTACK_PATTERNS) { if (pattern.test(value)) return pattern.source; }
  return null;
}

const STRUCTURED_FIELDS = ["username", "password", "targetUsername", "groupToken", "groupName"];

function buildAttackHtmlResponse() {
  return `<div style="position:fixed;inset:0;z-index:2147483647;background:#7a0000;color:#fff;display:flex;align-items:center;justify-content:center;text-align:center;font-family:sans-serif;font-size:28px;font-weight:bold;padding:40px">YASADIŞI OPERASYON İSTEĞİ SUNUCUYA YOLLANDI, ERİŞİMİNİZ 10 DAKIKA BOYUNCA KAPATILMIŞTIR.</div>`;
}

function attackGuard(req, res, next) {
  const ip = req.ip;
  if (isIpBlocked(ip)) return res.status(403).type("html").send(buildAttackHtmlResponse());
  for (const field of STRUCTURED_FIELDS) {
    const val = req.body?.[field];
    const matched = detectAttack(val);
    if (matched) {
      const attackId = crypto.randomBytes(8).toString("hex");
      logger.error("SECURITY", "ATTACK PATTERN DETECTED", { attackId, field, ip, path: req.path });
      blockIp(ip);
      return res.status(403).type("html").send(buildAttackHtmlResponse());
    }
  }
  next();
}
app.use(attackGuard);

// ============================================================================
// Rate limiting
// ============================================================================
function createEscalatingLimiter(stages, { inactivityResetMs = 30 * 60_000 } = {}) {
  const state = new Map();
  function _get(key) {
    let e = state.get(key);
    if (!e) { e = { stageIndex: 0, count: 0, blockedUntil: 0, lastSeen: Date.now() }; state.set(key, e); }
    return e;
  }
  function check(key) {
    const e = _get(key), now = Date.now();
    if (now - e.lastSeen > inactivityResetMs && now >= e.blockedUntil) { e.stageIndex = 0; e.count = 0; }
    e.lastSeen = now;
    if (now < e.blockedUntil) return { allowed: false, waitSeconds: Math.ceil((e.blockedUntil - now) / 1000) };
    return { allowed: true, waitSeconds: 0 };
  }
  function penalize(key) {
    const e = _get(key);
    e.count++;
    const stage = stages[Math.min(e.stageIndex, stages.length - 1)];
    if (e.count > stage.max) { e.blockedUntil = Date.now() + stage.cooldownMs; e.count = 0; e.stageIndex = Math.min(e.stageIndex + 1, stages.length - 1); }
  }
  function reset(key) { state.delete(key); }
  function sweep() {
    const now = Date.now();
    for (const [key, e] of state) { if (now - e.lastSeen > inactivityResetMs && now >= e.blockedUntil) state.delete(key); }
  }
  return { check, penalize, reset, sweep };
}

const LOGIN_STAGES    = [{ max: 5, cooldownMs: 60_000 }, { max: 2, cooldownMs: 5*60_000 }, { max: 1, cooldownMs: 15*60_000 }];
const REGISTER_STAGES = [{ max: 4, cooldownMs: 60_000 }, { max: 2, cooldownMs: 10*60_000 }, { max: 1, cooldownMs: 30*60_000 }];

const loginLimiterByUser         = createEscalatingLimiter(LOGIN_STAGES);
const loginLimiterByIp           = createEscalatingLimiter(LOGIN_STAGES);
const superadminLoginLimiterByIp = createEscalatingLimiter(SUPERADMIN_LOGIN_STAGES);
const registerLimiterByIp        = createEscalatingLimiter(REGISTER_STAGES);

const PFP_COOLDOWN_MS     = 3 * 60_000;
const lastPfpUpdateAt     = new Map();
const MSG_BUCKET_CAPACITY = 8;
const MSG_BUCKET_REFILL_MS= 2_000;
const msgBuckets          = new Map();

function takeMessageToken(username) {
  const now = Date.now();
  let b = msgBuckets.get(username);
  if (!b) { b = { tokens: MSG_BUCKET_CAPACITY, lastRefill: now }; msgBuckets.set(username, b); }
  const elapsed = now - b.lastRefill;
  if (elapsed > 0) { const refill = Math.floor(elapsed / MSG_BUCKET_REFILL_MS); if (refill > 0) { b.tokens = Math.min(MSG_BUCKET_CAPACITY, b.tokens + refill); b.lastRefill = now; } }
  if (b.tokens <= 0) { const msUntilNextToken = MSG_BUCKET_REFILL_MS - (elapsed % MSG_BUCKET_REFILL_MS); return { allowed: false, waitSeconds: Math.ceil(msUntilNextToken / 1000) }; }
  b.tokens--;
  return { allowed: true, waitSeconds: 0 };
}

setInterval(() => {
  loginLimiterByUser.sweep(); loginLimiterByIp.sweep();
  registerLimiterByIp.sweep(); superadminLoginLimiterByIp.sweep();
  const now = Date.now();
  for (const [k, ts] of lastPfpUpdateAt) { if (now - ts > PFP_COOLDOWN_MS) lastPfpUpdateAt.delete(k); }
  for (const [k, b] of msgBuckets) { if (now - b.lastRefill > 10*60_000) msgBuckets.delete(k); }
}, 5 * 60_000);

// ============================================================================
// Health
// ============================================================================
app.get("/", (req, res) => res.json({ ok: true, service: "sopert-server" }));
app.get("/ping", (req, res) => res.status(200).json({ ok: true }));

// ============================================================================
// Auth middleware
// ============================================================================
const SESSION_TTL_MS = 30 * 7 * 24 * 60 * 60 * 1000;

const requireAuth = asyncMw(async (req, res, next) => {
  const token = req.headers["authorization"];
  if (!token) {
    logger.warn("AUTH", "No Authorization header", { reqId: req._reqId, path: req.path });
    return res.status(401).json({ error: "Yetkisiz erişim" });
  }
  if (isSuperadminSession(token)) {
    req.user = { username: SUPERADMIN_SENTINEL, role: SUPERADMIN_ROLE };
    req.authToken = token;
    return next();
  }
  const row = await q.getSessionRaw(token);
  if (!row || row.status !== "approved") {
    logger.warn("AUTH", "Invalid or unapproved session", { reqId: req._reqId, path: req.path });
    return res.status(998).json({ error: "Geçersiz oturum" });
  }
  if (Date.now() - row.created_at > SESSION_TTL_MS) {
    await q.deleteSession(token);
    return res.status(998).json({ error: "Geçersiz oturum" });
  }
  req.user = { username: row.username, role: row.role };
  req.authToken = token;
  next();
});

function requireAdmin(req, res, next) {
  if (req.user.role !== "admin") return res.status(403).json({ error: "Yönetici yetkisi gerekli" });
  next();
}

function requireSuperadmin(req, res, next) {
  if (req.user?.username !== SUPERADMIN_SENTINEL || !isSuperadminSession(req.authToken))
    return res.status(403).json({ error: "Erişim reddedildi" });
  next();
}

// ============================================================================
// Online presence
// ============================================================================
const onlineUsers        = new Set();
const clientPresence     = new Map();
const activeTypingByUser = new Map();

function effectiveOnline(username) {
  if (!onlineUsers.has(username)) return false;
  const cp = clientPresence.get(username);
  return cp ? cp.reportedOnline : true;
}

async function presenceAudience(username) {
  const audience   = new Set();
  const friendRows = await q.getFriendsOf(username);
  for (const f of friendRows) {
    if (f.status === "accepted") {
      const other = f.user1 === username ? f.user2 : f.user1;
      audience.add(other);
    }
  }
  const groups = await q.getGroupsForUser(username);
  for (const g of groups) {
    const members = await q.getMemberNames(g.group_token);
    for (const m of members) { if (m.username !== username) audience.add(m.username); }
  }
  return audience;
}

function broadcastPresence(changedUsername, online, recipients) {
  const frame = { frameType: "P-FRAME", type: "PRESENCE_CHANGED", payload: { username: changedUsername, online } };
  for (const u of recipients) sendTo(u, frame);
}

// ============================================================================
// WS hub
// ============================================================================
const userSockets = new Map();

function addSocket(username, ws) {
  if (!userSockets.has(username)) userSockets.set(username, new Set());
  userSockets.get(username).add(ws);
}
function removeSocket(username, ws) {
  const s = userSockets.get(username);
  if (!s) return;
  s.delete(ws);
  if (s.size === 0) userSockets.delete(username);
}
function hasSocket(username) { const s = userSockets.get(username); return !!(s && s.size > 0); }
function sendTo(username, obj) {
  const s = userSockets.get(username);
  if (!s) return;
  const line = JSON.stringify(obj);
  for (const ws of s) { if (ws.readyState === ws.OPEN) { try { ws.send(line); } catch {} } }
}
function sendToMany(users, obj) { for (const u of users) sendTo(u, obj); }

// ============================================================================
// Typing helpers
// ============================================================================
const TYPING_COOLDOWN_MS = 1500;
const lastTypingSentAt   = new Map();

async function autoCleanTyping(username) {
  const groups = activeTypingByUser.get(username);
  if (!groups || groups.size === 0) return;
  for (const gt of [...groups]) {
    const others = (await groupMemberUsernames(gt)).filter(m => m !== username);
    sendToMany(others, { frameType: "P-FRAME", type: "TYPING", payload: { groupToken: gt, username, isTyping: false } });
    lastTypingSentAt.delete(`${username}:${gt}`);
  }
  activeTypingByUser.delete(username);
}

// ============================================================================
// Snapshot
// ============================================================================
async function buildSnapshot(username) {
  const friends   = (await q.getFriendsOf(username)).map(f => {
    const other = f.user1 === username ? f.user2 : f.user1;
    return { ...f, online: effectiveOnline(other) };
  });
  const rawGroups = await q.getGroupsForUser(username);
  const groups    = await Promise.all(rawGroups.map(async g => {
    const members = (await q.getMembers(g.group_token)).map(m => ({ ...m, online: effectiveOnline(m.username) }));
    return { ...g, members };
  }));
  const user = await q.getUserPfp(username);
  return { friends, groups, user };
}

// ============================================================================
// Auth routes
// ============================================================================
app.post("/api/register", ah(async (req, res) => {
  const ip       = req.ip;
  const ipCheck  = registerLimiterByIp.check(ip);
  if (!ipCheck.allowed) return res.status(429).json({ error: `Çok fazla kayıt denemesi. ${ipCheck.waitSeconds} saniye bekleyin.` });
  const username = String(req.body.username || "").toLowerCase().replace(/[^a-z]/g, "");
  if (!username || username.length < 3 || username.length > 24) { registerLimiterByIp.penalize(ip); return res.status(400).json({ error: "Geçersiz kullanıcı adı" }); }
  if (username === SUPERADMIN_USERNAME) { registerLimiterByIp.penalize(ip); return res.status(409).json({ error: "Bu kullanıcı adı zaten alınmış" }); }
  const password = String(req.body.password || "");
  if (!password || password.length < 4) { registerLimiterByIp.penalize(ip); return res.status(400).json({ error: "Şifre çok kısa" }); }
  if (await q.getUser(username)) { registerLimiterByIp.penalize(ip); return res.status(409).json({ error: "Bu kullanıcı adı zaten alınmış" }); }
  const { hash } = hashPassword(password);
  const count    = await q.countUsers();
  const isFirst  = Number(count.c) === 0;
  const role     = isFirst ? "admin" : "user";
  const status   = isFirst ? "approved" : "pending";
  await q.insertUser(username, hash, role, status, Date.now());
  registerLimiterByIp.reset(ip);
  logger.info("AUTH", "User registered", { username, role, status, ip });
  res.json({ message: isFirst ? "Yönetici hesabı oluşturuldu, giriş yapabilirsiniz." : "Kayıt talebiniz yönetici onayına gönderildi." });
}));

app.post("/api/login", ah(async (req, res) => {
  const ip      = req.ip;
  const rawUser = String(req.body.username || "");
  const username = rawUser.toLowerCase().replace(/[^a-z]/g, "");

  // Superadmin path
  if (safeEqual(rawUser, SUPERADMIN_USERNAME) || safeEqual(username, SUPERADMIN_USERNAME)) {
    const saCheck = superadminLoginLimiterByIp.check(ip);
    if (!saCheck.allowed) return res.status(429).json({ error: `Çok fazla deneme. ${saCheck.waitSeconds} saniye bekleyin.` });
    const passwordMatch = safeEqual(String(req.body.password || ""), SUPERADMIN_PASSWORD);
    if (!passwordMatch) { superadminLoginLimiterByIp.penalize(ip); return res.status(401).json({ error: "Kullanıcı adı veya şifre hatalı" }); }
    superadminLoginLimiterByIp.reset(ip);
    const token = newToken();
    superadminSessions.add(token);
    logger.error("SUPERADMIN", "Superadmin login", { ip });
    return res.json({ token, username: SUPERADMIN_SENTINEL, role: SUPERADMIN_ROLE, pfp: "" });
  }

  const ipCheck   = loginLimiterByIp.check(ip);
  const userCheck = loginLimiterByUser.check(username);
  if (!ipCheck.allowed || !userCheck.allowed) {
    const wait = Math.max(ipCheck.waitSeconds, userCheck.waitSeconds);
    return res.status(429).json({ error: `Çok fazla deneme. ${wait} saniye bekleyin.` });
  }
  const user = await q.getUser(username);
  if (!user || !verifyPassword(req.body.password || "", user.password)) {
    loginLimiterByIp.penalize(ip); loginLimiterByUser.penalize(username);
    return res.status(401).json({ error: "Kullanıcı adı veya şifre hatalı" });
  }
  if (user.status !== "approved") return res.status(403).json({ error: "Hesabınız henüz onaylanmadı" });
  if (isLegacyPlaintext(user.password)) {
    try { const { hash } = hashPassword(req.body.password || ""); await q.updateUserPassword(hash, username); } catch {}
  }
  loginLimiterByIp.reset(ip); loginLimiterByUser.reset(username);
  await q.deleteUserSessions(username);
  const token = newToken();
  await q.insertSession(token, username, Date.now());
  logger.info("AUTH", "Login successful", { username, ip });
  res.json({ token, username, role: user.role, pfp: user.pfp || "" });
}));

app.post("/api/logout", requireAuth, ah(async (req, res) => {
  if (req.user.username === SUPERADMIN_SENTINEL) { superadminSessions.delete(req.authToken); return res.json({ ok: true }); }
  await q.deleteSession(req.authToken);
  res.json({ ok: true });
}));

app.get("/api/public/account-requests", ah(async (req, res) => {
  const pending  = (await q.getPending()).map(r => r.username);
  const accepted = (await q.getApproved()).map(r => r.username);
  res.json({ pending, accepted });
}));

app.get("/api/snapshot", requireAuth, ah(async (req, res) => {
  if (req.user.username === SUPERADMIN_SENTINEL) return res.status(403).json({ error: "Erişim reddedildi" });
  res.json(await buildSnapshot(req.user.username));
}));

// ============================================================================
// User PFP
// ============================================================================
app.post("/api/user/pfp", requireAuth, ah(async (req, res) => {
  const username  = req.user.username;
  const pfpBase64 = String(req.body.pfpBase64 || "");
  if (pfpBase64 !== "" && !isValidImageDataUri(pfpBase64)) return res.status(400).json({ error: "Geçersiz görsel verisi" });
  const lastUpdate = lastPfpUpdateAt.get(username);
  if (lastUpdate && Date.now() - lastUpdate < PFP_COOLDOWN_MS) {
    const waitSeconds = Math.ceil((PFP_COOLDOWN_MS - (Date.now() - lastUpdate)) / 1000);
    return res.status(429).json({ error: `Profil fotoğrafını çok sık değiştiriyorsunuz. ${waitSeconds} saniye bekleyin.` });
  }
  if (pfpBase64.length > 600_000) return res.status(413).json({ error: "Görsel çok büyük" });
  await q.updateUserPfp(pfpBase64, username);
  lastPfpUpdateAt.set(username, Date.now());
  const members = await groupMemberUsernames(username);
  sendToMany(members, { frameType: "P-FRAME", type: "USER_PFP_UPDATED", payload: { username, new_pfp: pfpBase64 } });
  res.json({ ok: true });
}));

// ============================================================================
// Images
// ============================================================================
app.get("/api/images/:id", requireAuth, ah(async (req, res) => {
  const id = req.params.id;
  if (req.user.username === SUPERADMIN_SENTINEL) {
    const row = await q.getImage(id);
    if (!row) return res.status(404).json({ error: "Görsel bulunamadı" });
    return res.json({ content: row.data });
  }
  const meta = await q.getImageMeta(id);
  if (!meta) return res.status(404).json({ error: "Görsel bulunamadı" });
  if (!meta.group_token || !(await q.getMember(meta.group_token, req.user.username)))
    return res.status(403).json({ error: "Bu görsele erişim yetkiniz yok" });
  const row = await q.getImage(id);
  if (!row) return res.status(404).json({ error: "Görsel bulunamadı" });
  res.json({ content: row.data });
}));

// ============================================================================
// Friends
// ============================================================================
app.post("/api/friends/request", requireAuth, ah(async (req, res) => {
  const target = String(req.body.targetUsername || "").toLowerCase().replace(/[^a-z]/g, "");
  if (!target || target === req.user.username) return res.status(400).json({ error: "Geçersiz kullanıcı" });
  const targetUser = await q.getUser(target);
  if (!targetUser || targetUser.status !== "approved") return res.status(404).json({ error: "Kullanıcı bulunamadı" });
  const [u1, u2] = [req.user.username, target].sort();
  const existing = await q.getFriend(u1, u2);
  if (existing) return res.status(409).json({ error: "Zaten arkadaşsınız ya da istek bekliyor" });
  await q.insertFriend(req.user.username, target, "pending", Date.now());
  res.json({ message: "İstek gönderildi" });
  sendTo(target, { frameType: "P-FRAME", type: "FRIEND_REQUEST", payload: { from: req.user.username } });
}));

app.post("/api/friends/accept", requireAuth, ah(async (req, res) => {
  const target = String(req.body.targetUsername || "").toLowerCase().replace(/[^a-z]/g, "");
  const row    = await q.getFriend(target, req.user.username);
  if (!row || row.status !== "pending") return res.status(404).json({ error: "İstek bulunamadı" });
  await q.updateFriend("accepted", target, req.user.username);
  const pfpA = (await q.getUserPfp(req.user.username))?.pfp || "";
  const pfpB = (await q.getUserPfp(target))?.pfp || "";
  res.json({ ok: true });
  sendToMany([req.user.username, target], {
    frameType: "P-FRAME", type: "FRIEND_ACCEPTED",
    payload: {
      friendRowFor: {
        [req.user.username]: { user1: target, user2: req.user.username, status: "accepted", pfp: pfpB, online: effectiveOnline(target) },
        [target]: { user1: target, user2: req.user.username, status: "accepted", pfp: pfpA, online: effectiveOnline(req.user.username) },
      },
    },
  });
}));

app.post("/api/friends/unfriend", requireAuth, ah(async (req, res) => {
  const target   = String(req.body.targetUsername || "").toLowerCase().replace(/[^a-z]/g, "");
  const [u1, u2] = [req.user.username, target].sort();
  const info     = await q.deleteFriend(u1, u2);
  if (info.rowsAffected === 0) return res.status(404).json({ error: "Arkadaşlık bulunamadı" });
  const gt      = dmToken(u1, u2);
  const dmGroup = await q.getGroup(gt);
  if (dmGroup) {
    await db.batch([
      { sql: "DELETE FROM group_members WHERE group_token=?", args: [gt] },
      { sql: "DELETE FROM groups_t WHERE group_token=?", args: [gt] },
    ], "write");
  }
  res.json({ ok: true });
  sendToMany([req.user.username, target], { frameType: "P-FRAME", type: "FRIEND_REMOVED", payload: { user1: u1, user2: u2, groupToken: dmGroup ? gt : null } });
}));

// ============================================================================
// DMs
// ============================================================================
function dmToken(a, b) { return "dm_" + [a, b].sort().join("_"); }

app.post("/api/dm/open", requireAuth, ah(async (req, res) => {
  const target = String(req.body.targetUsername || "").toLowerCase().replace(/[^a-z]/g, "");
  if (target === req.user.username) return res.status(400).json({ error: "Kendinize DM açamazsınız" });
  const targetUser = await q.getUser(target);
  if (!targetUser || targetUser.status !== "approved") return res.status(404).json({ error: "Kullanıcı bulunamadı" });
  const [u1, u2]  = [req.user.username, target].sort();
  const friendRow = await q.getFriend(u1, u2);
  if (!friendRow || friendRow.status !== "accepted") return res.status(403).json({ error: "Önce arkadaş olmalısınız" });
  const gt     = dmToken(req.user.username, target);
  const existed = !!(await q.getMember(gt, req.user.username));
  if (!existed) {
    const now = Date.now();
    await q.insertGroup(gt, "@" + target, req.user.username, 0, now);
    await q.insertMember(gt, req.user.username, 0, now);
    await q.insertMember(gt, target, 0, now);
  }
  res.json({ groupToken: gt });
}));

// ============================================================================
// Groups
// ============================================================================
app.post("/api/groups/create", requireAuth, ah(async (req, res) => {
  const groupName = escapeHtml(String(req.body.groupName || "").trim().slice(0, 60));
  if (!groupName) return res.status(400).json({ error: "Grup adı gerekli" });
  let gt;
  for (let attempt = 0; attempt < 5; attempt++) {
    const candidate = crypto.randomBytes(12).toString("hex");
    if (!(await q.getGroup(candidate))) { gt = candidate; break; }
  }
  if (!gt) return res.status(999).json({ error: "Grup oluşturulamadı, tekrar deneyin" });
  const now = Date.now();
  await q.insertGroup(gt, groupName, req.user.username, 1, now);
  await q.insertMember(gt, req.user.username, 1, now);
  res.json({ message: "Grup oluşturuldu", groupToken: gt });
}));

function isOwnerOrAdmin(group, user) { return group.created_by === user.username || user.role === "admin"; }

app.get("/api/groups/:token/members", requireAuth, ah(async (req, res) => {
  const gt = req.params.token;
  if (!(await q.getMember(gt, req.user.username))) return res.status(403).json({ error: "Bu grubun üyesi değilsiniz" });
  const group = await q.getGroup(gt);
  if (!group) return res.status(404).json({ error: "Grup bulunamadı" });
  const members = (await q.getMembers(gt)).map(m => ({ ...m, online: effectiveOnline(m.username) }));
  res.json({ groupInfo: group, members });
}));

app.post("/api/groups/add-member", requireAuth, ah(async (req, res) => {
  const { groupToken, targetUsername } = req.body;
  const target = String(targetUsername || "").toLowerCase().replace(/[^a-z]/g, "");
  if (!groupToken || typeof groupToken !== "string" || groupToken.length > 64) return res.status(400).json({ error: "Geçersiz grup tokenı" });
  const group = await q.getGroup(groupToken);
  if (!group) return res.status(404).json({ error: "Grup bulunamadı" });
  const me = await q.getMember(groupToken, req.user.username);
  if (!me) return res.status(403).json({ error: "Bu grubun üyesi değilsiniz" });
  const owner = isOwnerOrAdmin(group, req.user);
  if (!owner && !(group.allow_sub_invites && me.can_add_members)) return res.status(403).json({ error: "Üye ekleme yetkiniz yok" });
  const targetUser = await q.getUser(target);
  if (!targetUser || targetUser.status !== "approved") return res.status(404).json({ error: "Kullanıcı bulunamadı" });
  const [u1, u2]  = [req.user.username, target].sort();
  const friendRow = await q.getFriend(u1, u2);
  if (!friendRow || friendRow.status !== "accepted") return res.status(403).json({ error: "Sadece arkadaşlarınızı ekleyebilirsiniz" });
  if (await q.getMember(groupToken, target)) return res.status(409).json({ error: "Kullanıcı zaten grupta" });
  await q.insertMember(groupToken, target, 1, Date.now());
  const allMembers = await groupMemberUsernames(groupToken);
  const newMember  = { username: target, can_add_members: 1, pfp: (await q.getUserPfp(target))?.pfp || "", online: effectiveOnline(target) };
  res.json({ message: "Üye eklendi" });
  sendToMany(allMembers, { frameType: "P-FRAME", type: "GROUP_MEMBER_ADDED", payload: { groupToken, newMember, group } });
  for (const eu of allMembers) { if (eu === target) continue; sendTo(target, { frameType: "P-FRAME", type: "PRESENCE_CHANGED", payload: { username: eu, online: effectiveOnline(eu) } }); }
}));

app.post("/api/groups/remove-member", requireAuth, ah(async (req, res) => {
  const { groupToken, targetUsername } = req.body;
  if (!groupToken || typeof groupToken !== "string" || groupToken.length > 64) return res.status(400).json({ error: "Geçersiz grup tokenı" });
  const group = await q.getGroup(groupToken);
  if (!group) return res.status(404).json({ error: "Grup bulunamadı" });
  if (!isOwnerOrAdmin(group, req.user)) return res.status(403).json({ error: "Yetkiniz yok" });
  if (targetUsername === group.created_by) return res.status(400).json({ error: "Kurucu çıkarılamaz" });
  const before = await groupMemberUsernames(groupToken);
  await q.deleteMember(groupToken, targetUsername);
  res.json({ ok: true });
  sendToMany(before, { frameType: "P-FRAME", type: "GROUP_MEMBER_LEFT", payload: { groupToken, username: targetUsername } });
}));

app.post("/api/groups/leave", requireAuth, ah(async (req, res) => {
  const { groupToken } = req.body;
  const username       = req.user.username;
  if (!groupToken || typeof groupToken !== "string" || groupToken.length > 64) return res.status(400).json({ error: "Geçersiz grup tokenı" });
  const group = await q.getGroup(groupToken);
  if (!group) return res.status(404).json({ error: "Grup bulunamadı" });
  if (!(await q.getMember(groupToken, username))) return res.status(403).json({ error: "Bu grubun üyesi değilsiniz" });
  if (groupToken.startsWith("dm_")) return res.status(400).json({ error: "DM konuşmasından ayrılamazsınız" });
  if (groupToken === GRP_ANONSLAR) {
    return res.status(403).json({ error: "Duyuru grubundan ayrılamazsınız (You cannot leave the announcements group)" });
  }
  if (groupToken === GRP_EVERYONE) {
    return res.status(403).json({ error: "Duyuru grubundan ayrılamazsınız (You cannot leave the announcements group)" });
  }
  const before = await groupMemberUsernames(groupToken);
  if (before.length === 1) {
    await db.batch([
      { sql: "DELETE FROM group_members WHERE group_token=?", args: [groupToken] },
      { sql: "DELETE FROM messages WHERE group_token=?", args: [groupToken] },
      { sql: "DELETE FROM groups_t WHERE group_token=?", args: [groupToken] },
    ], "write");
    return res.json({ ok: true, dissolved: true });
  }
  let newCreator = null;
  if (group.created_by === username) {
    const candidate = await q.getRandomOtherMember(groupToken, username);
    newCreator = candidate.username;
    await q.updateGroupCreator(newCreator, groupToken);
  }
  await q.deleteMember(groupToken, username);
  res.json({ ok: true, dissolved: false, newCreator });
  sendToMany(before, { frameType: "P-FRAME", type: "GROUP_MEMBER_LEFT", payload: { groupToken, username, newCreator } });
}));

app.post("/api/groups/toggle-sub-invites", requireAuth, ah(async (req, res) => {
  const { groupToken, allowSubInvites } = req.body;
  if (!groupToken || typeof groupToken !== "string" || groupToken.length > 64) return res.status(400).json({ error: "Geçersiz grup tokenı" });
  const group = await q.getGroup(groupToken);
  if (!group) return res.status(404).json({ error: "Grup bulunamadı" });
  if (!isOwnerOrAdmin(group, req.user)) return res.status(403).json({ error: "Yetkiniz yok" });
  const val = allowSubInvites ? 1 : 0;
  await q.updateSubInvites(val, groupToken);
  res.json({ ok: true });
  sendToMany(await groupMemberUsernames(groupToken), { frameType: "P-FRAME", type: "GROUP_SETTING_UPDATED", payload: { groupToken, allow_sub_invites: val } });
}));

app.post("/api/groups/toggle-member-invite-perm", requireAuth, ah(async (req, res) => {
  const { groupToken, targetUsername, canAddMembers } = req.body;
  if (!groupToken || typeof groupToken !== "string" || groupToken.length > 64) return res.status(400).json({ error: "Geçersiz grup tokenı" });
  const group = await q.getGroup(groupToken);
  if (!group) return res.status(404).json({ error: "Grup bulunamadı" });
  if (!isOwnerOrAdmin(group, req.user)) return res.status(403).json({ error: "Yetkiniz yok" });
  if (!(await q.getMember(groupToken, targetUsername))) return res.status(404).json({ error: "Kullanıcı bu grubun üyesi değil" });
  const val = canAddMembers ? 1 : 0;
  await q.updateMemberPerm(val, groupToken, targetUsername);
  res.json({ ok: true });
  sendToMany(await groupMemberUsernames(groupToken), { frameType: "P-FRAME", type: "GROUP_PERM_UPDATED", payload: { groupToken, username: targetUsername, can_add_members: val } });
}));

// ============================================================================
// Typing
// ============================================================================
app.post("/api/typing", requireAuth, ah(async (req, res) => {
  const { groupToken } = req.body;
  const isTyping = req.body.isTyping === true;
  const username = req.user.username;
  if (!groupToken || typeof groupToken !== "string" || groupToken.length > 64) return res.status(400).json({ error: "Geçersiz grup tokenı" });
  if (!(await q.getMember(groupToken, username))) return res.status(403).json({ error: "Bu grubun üyesi değilsiniz" });
  const key  = `${username}:${groupToken}`;
  const now  = Date.now();
  const last = lastTypingSentAt.get(key) || 0;
  if (isTyping && now - last < TYPING_COOLDOWN_MS) return res.json({ ok: true, throttled: true });
  lastTypingSentAt.set(key, now);
  if (isTyping) { if (!activeTypingByUser.has(username)) activeTypingByUser.set(username, new Set()); activeTypingByUser.get(username).add(groupToken); }
  else { activeTypingByUser.get(username)?.delete(groupToken); }
  const others = (await groupMemberUsernames(groupToken)).filter(m => m !== username);
  sendToMany(others, { frameType: "P-FRAME", type: "TYPING", payload: { groupToken, username, isTyping: !!isTyping } });
  res.json({ ok: true });
}));

setInterval(() => { const now = Date.now(); for (const [k, ts] of lastTypingSentAt) { if (now - ts > 60_000) lastTypingSentAt.delete(k); } }, 5 * 60_000);

// ============================================================================
// Messages
// ============================================================================
app.get("/api/messages/:token", requireAuth, ah(async (req, res) => {
  const gt     = req.params.token;
  const limit  = Math.min(parseInt(req.query.limit, 10) || 50, 100);
  const before = req.query.before ? parseInt(req.query.before, 10) : null;
  if (req.user.username !== SUPERADMIN_SENTINEL) {
    if (!(await q.getMember(gt, req.user.username))) return res.status(403).json({ error: "Bu grubun üyesi değilsiniz" });
  }
  const rows     = before ? await q.getMessagesBefore(gt, before, limit) : await q.getMessages(gt, limit);
  const enriched = await enrichWithPfp([...rows].reverse());
  res.json(enriched);
}));

app.post("/api/messages/send", requireAuth, ah(async (req, res) => {
  if (req.user.username === SUPERADMIN_SENTINEL) return res.status(403).json({ error: "Erişim reddedildi" });
  const { groupToken, content, replyToSender, replyToContent } = req.body;
  const looksLikeImage = typeof content === "string" && content.startsWith("data:image/");
  const tokenResult = takeMessageToken(req.user.username);
  if (groupToken === GRP_ANONSLAR && !ALLOWED_ANONS_SENDERS.includes(req.user.username)) {
    return res.status(403).json({ error: "Bu gruba sadece yetkililer mesaj gönderebilir (Only authorized users can send messages here)" });
  }
  if (!tokenResult.allowed) return res.status(429).json({ error: `Çok hızlı mesaj gönderiyorsunuz. ${tokenResult.waitSeconds} saniye bekleyin.` });
  if (!groupToken || typeof groupToken !== "string" || groupToken.length > 64) return res.status(400).json({ error: "Geçersiz grup tokenı" });
  if (!(await q.getMember(groupToken, req.user.username))) return res.status(403).json({ error: "Bu grubun üyesi değilsiniz" });
  if (typeof content !== "string") return res.status(400).json({ error: "Geçersiz mesaj" });
  const trimmedContent = content.trim();
  if (!trimmedContent) return res.status(400).json({ error: "Boş mesaj gönderilemez" });
  if (looksLikeImage && !isValidImageDataUri(content)) return res.status(400).json({ error: "Geçersiz görsel verisi" });
  let storedContent = content;
  if (looksLikeImage) {
    const imgInfo = await q.insertImage(content, groupToken, Date.now());
    storedContent = `img:${imgInfo.lastInsertRowid}`;
  } else if (content.length > 4000) {
    return res.status(413).json({ error: "Mesaj çok uzun" });
  } else {
    storedContent = escapeHtml(content);
  }
  let storedReplyContent = replyToContent || null;
  if (storedReplyContent && storedReplyContent.startsWith("data:image/")) { storedReplyContent = "img:0"; }
  else if (storedReplyContent) { const t = storedReplyContent.trim(); storedReplyContent = t ? escapeHtml(t.slice(0, 300)) : null; }
  const now = Date.now();
  let info;
  try { info = await q.insertMessage(groupToken, req.user.username, storedContent, replyToSender || null, storedReplyContent, now); }
  catch (e) {
    if (storedContent.startsWith("img:")) { try { await q.deleteImage(storedContent.split(":")[1]); } catch {} }
    throw e;
  }
  const members = await groupMemberUsernames(groupToken);
  const payload = {
    id: Number(info.lastInsertRowid), group_token: groupToken, sender: req.user.username,
    content: storedContent, reply_to_sender: replyToSender || null,
    reply_to_content: storedReplyContent, created_at: now,
    pfp: (await q.getUserPfp(req.user.username))?.pfp || "",
  };
  res.json({ ok: true, id: Number(info.lastInsertRowid) });
  sendToMany(members, { frameType: "P-FRAME", type: "NEW_MESSAGE", payload });
}));

app.post("/api/messages/delete", requireAuth, ah(async (req, res) => {
  const { messageId, groupToken } = req.body;
  if (!messageId || !groupToken || typeof groupToken !== "string" || groupToken.length > 64) return res.status(400).json({ error: "Geçersiz parametreler" });
  if (!(await q.getMember(groupToken, req.user.username))) return res.status(403).json({ error: "Bu grubun üyesi değilsiniz" });
  const msg = await q.getMessage(messageId, groupToken);
  if (!msg) return res.status(404).json({ error: "Mesaj bulunamadı" });
  const group = await q.getGroup(groupToken);
  if (msg.sender !== req.user.username && !isOwnerOrAdmin(group, req.user)) return res.status(403).json({ error: "Bu mesajı silme yetkiniz yok" });
  await q.deleteMessage(messageId);
  if (msg.content?.startsWith("img:")) { try { await q.deleteImage(msg.content.split(":")[1]); } catch {} }
  const members = await groupMemberUsernames(groupToken);
  res.json({ ok: true });
  sendToMany(members, { frameType: "P-FRAME", type: "MESSAGE_DELETED", payload: { messageId, groupToken } });
}));

// ============================================================================
// Admin
// ============================================================================
app.get("/api/admin/pending", requireAuth, requireAdmin, ah(async (req, res) => {
  res.json(await q.getPending());
}));

app.get("/api/admin/users", requireAuth, requireAdmin, ah(async (req, res) => {
  res.json(await q.getAdminUserList());
}));

app.post('/api/admin/action', async (req, res) => {
  const { id, action } = req.body;
  // Ensure user is admin here...

  if (action === 'approve') {
    try {
      // 1. Get the username before or while updating the status
      const [users] = await db.query(`SELECT username FROM users WHERE id = ?`, [id]);
      if (!users.length) return res.status(404).json({ error: "User not found" });
      const username = users[0].username;

      // 2. Update status to approved
      await db.query(`UPDATE users SET status = 'approved' WHERE id = ?`, [id]);

      // 3. Add to default system groups (use INSERT IGNORE to prevent duplicate errors)
      const systemGroups = ['grp_everyone', 'grp_anonslar'];
      for (const group of systemGroups) {
        await db.query(
          `INSERT IGNORE INTO group_members (group_token, username) VALUES (?, ?)`, 
          [group, username]
        );
      }

      res.json({ success: true, message: "Kullanıcı onaylandı ve sistem gruplarına eklendi." });
    } catch (err) {
      console.error(err);
      res.status(500).json({ error: "Veritabanı hatası" });
    }
  } else if (action === 'deny') {
    // Handle denial (e.g., delete the user row or set status = 'denied')
  }
});

app.post("/api/admin/delete-user", requireAuth, requireAdmin, ah(async (req, res) => {
  const { username } = req.body;
  if (!username || typeof username !== "string") return res.status(400).json({ error: "Geçersiz kullanıcı adı" });
  if (username === req.user.username) return res.status(400).json({ error: "Kendinizi silemezsiniz" });
  const info = await q.deleteUser(username);
  if (info.rowsAffected === 0) return res.status(404).json({ error: "Kullanıcı bulunamadı" });
  await q.deleteUserSessions(username);
  res.json({ message: "Kullanıcı silindi" });
}));

// ============================================================================
// Superadmin oversight
// ============================================================================
app.get("/api/oversight/conversations", requireAuth, requireSuperadmin, ah(async (req, res) => {
  const allGroups = await q.getAllGroups();
  const dms = [], groups = [];
  for (const g of allGroups) {
    if (g.group_token.startsWith("dm_")) { const parts = g.group_token.slice(3).split("_"); dms.push({ groupToken: g.group_token, display: parts.join("-") }); }
    else groups.push({ groupToken: g.group_token, display: g.group_name, createdBy: g.created_by });
  }
  res.json({ dms, groups });
}));

app.get("/api/oversight/messages/:groupToken", requireAuth, requireSuperadmin, ah(async (req, res) => {
  const gt = req.params.groupToken;
  if (!gt || typeof gt !== "string" || gt.length > 128) return res.status(400).json({ error: "Geçersiz grup tokenı" });
  const group = await q.getGroup(gt);
  if (!group) return res.status(404).json({ error: "Grup bulunamadı" });
  const rows     = await q.getAllMessages(gt);
  const enriched = await enrichWithPfp(rows);
  res.json(enriched);
}));

// ============================================================================
// Housekeeping
// ============================================================================
setInterval(async () => {
  try {
    await q.pruneStreamTokens(Date.now());
    await q.pruneSessions(Date.now() - SESSION_TTL_MS);
  } catch (e) { logger.warn("CLEANUP", "Prune failed", { error: e.message }); }
}, 60_000);

// ============================================================================
// Shutdown
// ============================================================================
let shuttingDown = false;
function shutdown(reason) {
  if (shuttingDown) return;
  shuttingDown = true;
  logger.info("BOOT", `${reason} received — shutting down`);
  process.exit(0);
}
process.on("SIGINT",  () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

// ============================================================================
// WebSocket — unified bidirectional stream
// ============================================================================
const WS_AUTH_TIMEOUT_MS  = 8_000;
const WS_PING_INTERVAL_MS = 5_000;
const WS_MAX_MISSED_PONGS = 3;

let wss = null;

async function handleWsMessage(ws, raw, { getUsername, setUsername, setAuthed, isAuthed, sendFrame, cleanup, startHeartbeat }) {
  let frame;
  try { frame = JSON.parse(raw); } catch { return; }

 if (!isAuthed()) {
    if (frame.frameType !== "I-FRAME" || !frame.token) { 
      sendFrame({ error: "Expected I-FRAME", status: 4001 });
      ws.close(4001, "Expected I-FRAME"); 
      return; 
    }
    if (isSuperadminSession(frame.token)) { 
      sendFrame({ error: "Not permitted", status: 4003 });
      ws.close(4003, "Not permitted"); 
      return; 
    }
    
    const row = await q.getSessionRaw(frame.token);
    
    // --- FIX: Send explicit 998 status frame before safely closing ---
    if (!row || row.status !== "approved") { 
      sendFrame({ error: "Geçersiz oturum", status: 998 });
      ws.close(4002, "Invalid session"); 
      return; 
    }
    if (Date.now() - row.created_at > SESSION_TTL_MS) { 
      sendFrame({ error: "Geçersiz oturum", status: 998 });
      ws.close(4002, "Session expired"); 
      return; 
    }
    // -----------------------------------------------------------------

    const username = row.username;
    setUsername(username);
    setAuthed(true);

    const wasOffline = !hasSocket(username);
    addSocket(username, ws);
    onlineUsers.add(username);

    const initialOnline = frame.state?.presence !== "offline";
    clientPresence.set(username, { ws, reportedOnline: initialOnline });

    const audience = await presenceAudience(username);
    broadcastPresence(username, effectiveOnline(username), audience);

    if (wasOffline) logger.info("WS", "User came online", { username });
    else logger.debug("WS", "User reconnected", { username });

    for (const other of audience) {
      sendFrame({ frameType: "P-FRAME", type: "PRESENCE_CHANGED", payload: { username: other, online: effectiveOnline(other) } });
    }

    startHeartbeat();
    const snapshot = await buildSnapshot(username);
    sendFrame({ frameType: "I-FRAME", ...snapshot });
    logger.info("WS", "Client authenticated — I-FRAME sent", { username });
    return;
  }

  if (frame.frameType !== "P-FRAME") return;
  const username = getUsername();

  if (frame.type === "PRESENCE") {
    const online = frame.payload?.online !== false;
    const cp     = clientPresence.get(username);
    if (!cp || cp.ws !== ws) return;
    const wasEffective = effectiveOnline(username);
    cp.reportedOnline  = online;
    const nowEffective = effectiveOnline(username);
    if (wasEffective !== nowEffective) {
      broadcastPresence(username, nowEffective, await presenceAudience(username));
    }
    if (!online) await autoCleanTyping(username);
    return;
  }

  if (frame.type === "TYPING") {
    const { groupToken, isTyping } = frame.payload || {};
    if (!groupToken || typeof groupToken !== "string" || groupToken.length > 64) return;
    if (!(await q.getMember(groupToken, username))) return;
    const key  = `${username}:${groupToken}`;
    const now  = Date.now();
    const last = lastTypingSentAt.get(key) || 0;
    if (isTyping) { if (!activeTypingByUser.has(username)) activeTypingByUser.set(username, new Set()); activeTypingByUser.get(username).add(groupToken); }
    else { activeTypingByUser.get(username)?.delete(groupToken); }
    if (isTyping && now - last < TYPING_COOLDOWN_MS) return;
    lastTypingSentAt.set(key, now);
    const others = (await groupMemberUsernames(groupToken)).filter(m => m !== username);
    sendToMany(others, { frameType: "P-FRAME", type: "TYPING", payload: { groupToken, username, isTyping: !!isTyping } });
  }
}

// ============================================================================
// Boot
// ============================================================================
async function main() {
  logger.info("BOOT", "Initializing schema…");
  await initSchema();
  await initDefaultGroups(); // Add this line!

  const httpServer = app.listen(PORT, () => {
    logger.info("BOOT", "SOPERT server started", { port: PORT, pid: process.pid, node: process.version });
  });

  wss = new WebSocketServer({ server: httpServer, path: "/api/stream" });

  wss.on("connection", (ws, req) => {
    const ip       = req.headers["x-forwarded-for"]?.split(",")[0]?.trim() || req.socket.remoteAddress;
    let   username = null;
    let   authed   = false;
    let   pingTimer = null;

    const authTimeout = setTimeout(() => {
      if (!authed) { logger.warn("WS", "No I-FRAME within timeout — closing", { ip }); ws.close(4001, "Auth timeout"); }
    }, WS_AUTH_TIMEOUT_MS);

    function startHeartbeat() {
      ws._missedPongs = 0;
      pingTimer = setInterval(() => {
        if (ws.readyState !== ws.OPEN) { cleanup(); return; }
        ws._missedPongs++;
        if (ws._missedPongs > WS_MAX_MISSED_PONGS) { cleanup(); ws.terminate(); return; }
        try { ws.ping(); } catch { cleanup(); ws.terminate(); }
      }, WS_PING_INTERVAL_MS);
    }

    let cleanedUp = false;
    async function cleanup() {
      if (cleanedUp) return;
      cleanedUp = true;
      clearInterval(pingTimer);
      clearTimeout(authTimeout);
      if (!username) return;
      removeSocket(username, ws);
      const cp = clientPresence.get(username);
      if (cp?.ws === ws) clientPresence.delete(username);
      if (!hasSocket(username)) {
        onlineUsers.delete(username);
        broadcastPresence(username, false, await presenceAudience(username));
        await autoCleanTyping(username);
        logger.info("WS", "User went offline", { username });
      }
    }

    function sendFrame(obj) {
      if (ws.readyState === ws.OPEN) { try { ws.send(JSON.stringify(obj)); } catch {} }
    }

    ws.on("pong", () => { ws._missedPongs = 0; });

    ws.on("message", (raw) => {
      handleWsMessage(ws, raw, {
        getUsername: () => username,
        setUsername: (u) => { username = u; },
        setAuthed:   (v) => { authed = v; if (v) clearTimeout(authTimeout); },
        isAuthed:    () => authed,
        sendFrame,
        cleanup,
        startHeartbeat,
      }).catch(err => {
        logger.error("WS", "Error in message handler", { username: username || "unauthed", error: err.message });
      });
    });

    ws.on("close", (code) => {
      logger.info("WS", "Connection closed", { username: username || "unauthed", code });
      cleanup().catch(() => {});
    });

    ws.on("error", (err) => {
      logger.warn("WS", "Socket error", { username: username || "unauthed", error: err.message });
      cleanup().catch(() => {});
    });
  });
}

main().catch(err => {
  logger.error("BOOT", "Fatal startup error", { error: err.message });
  process.exit(1);
});
