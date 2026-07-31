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

// In-Memory Rate Limiter
const rateLimitMap = new Map();

function checkRateLimit(key, windowMs, maxHits) {
  const now = Date.now();
  if (!rateLimitMap.has(key)) {
    rateLimitMap.set(key, []);
  }

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

setInterval(() => {
  const now = Date.now();
  for (const [key, timestamps] of rateLimitMap.entries()) {
    const valid = timestamps.filter(ts => now - ts < 15 * 60 * 1000);
    if (valid.length === 0) rateLimitMap.delete(key);
    else rateLimitMap.set(key, valid);
  }
}, 10 * 60 * 1000);

// Otomatik Mesaj Temizliği: 30 günden eski mesajları temizler
async function cleanupOldMessages() {
  try {
    const thirtyDaysAgo = Date.now() - (30 * 24 * 60 * 60 * 1000);
    await queryWorker("DELETE FROM messages WHERE created_at < ?", [thirtyDaysAgo]);
  } catch (err) {
    console.error("Eski mesaj temizleme hatası:", err.message);
  }
}

setInterval(cleanupOldMessages, 60 * 60 * 1000);
cleanupOldMessages();

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

app.get("/", (req, res) => res.send("SOPERT Backend Online"));

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

// PUBLIC: View Account Requests
app.get("/api/public/account-requests", async (req, res) => {
  const ip = req.headers["x-forwarded-for"] || req.ip || "unknown";
  const limit = checkRateLimit(`pub_req_${ip}`, 60 * 1000, 15);
  if (limit.limited) {
    return res.status(429).json({ error: `Çok fazla istek. Lütfen ${limit.retryAfterSec}s bekleyin.` });
  }

  try {
    const pendingRes = await queryWorker("SELECT username FROM pending_accounts");
    const acceptedRes = await queryWorker("SELECT username FROM users");

    res.json({
      pending: (pendingRes.results || []).map(r => r.username),
      accepted: (acceptedRes.results || []).map(r => r.username)
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Registration
app.post("/api/register", async (req, res) => {
  const ip = req.headers["x-forwarded-for"] || req.ip || "unknown";
  const limit = checkRateLimit(`reg_${ip}`, 15 * 60 * 1000, 3);
  if (limit.limited) {
    return res.status(429).json({ error: `Çok fazla kayıt isteği. Lütfen ${limit.retryAfterSec}s bekleyin.` });
  }

  let { username, password } = req.body;
  if (!username || !password) return res.status(400).json({ error: "Gerekli alanlar eksik" });
  username = username.toLowerCase().replace(/[^a-z]/g, "");

  if (!username) return res.status(400).json({ error: "Kullanıcı adı geçerli harfler içermelidir" });

  try {
    const existingUser = await queryWorker("SELECT username FROM users WHERE username = ?", [username]);
    if (existingUser.results && existingUser.results.length > 0) {
      return res.status(409).json({ error: "Bu kullanıcı adına sahip bir hesap zaten var" });
    }

    const existingPending = await queryWorker("SELECT username FROM pending_accounts WHERE username = ?", [username]);
    if (existingPending.results && existingPending.results.length > 0) {
      return res.status(409).json({ error: "Bu kullanıcı adı için hesap oluşturma talebi zaten bekliyor" });
    }

    const countRes = await queryWorker("SELECT COUNT(*) as count FROM pending_accounts");
    if (countRes.results[0].count >= 5) {
      return res.status(429).json({ error: "Maksimum 5 bekleyen isteğe izin verilir" });
    }

    const token = generateToken(username, password);
    await queryWorker("INSERT INTO pending_accounts (username, password, token) VALUES (?, ?, ?)", [username, password, token]);
    res.json({ message: "Hesap oluşturma talebi onay için yöneticiye gönderildi" });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Login
app.post("/api/login", async (req, res) => {
  const ip = req.headers["x-forwarded-for"] || req.ip || "unknown";
  const limit = checkRateLimit(`login_${ip}`, 60 * 1000, 5);
  if (limit.limited) {
    return res.status(429).json({ error: `Çok fazla giriş denemesi. Lütfen ${limit.retryAfterSec}s bekleyin.` });
  }

  let { username, password } = req.body;
  if (!username || !password) return res.status(400).json({ error: "Kullanıcı adı ve şifre girin" });
  username = username.toLowerCase().replace(/[^a-z]/g, "");
  const token = generateToken(username, password);

  try {
    const userRes = await queryWorker("SELECT * FROM users WHERE username = ? AND token = ?", [username, token]);
    if (!userRes.results || userRes.results.length === 0) return res.status(401).json({ error: "Geçersiz kimlik bilgileri veya bekleyen onay" });
    res.json({ token, username: userRes.results[0].username, role: userRes.results[0].role, pfp: userRes.results[0].pfp || "" });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// PFP Upload
app.post("/api/user/pfp", authenticate, async (req, res) => {
  const limit = checkRateLimit(`pfp_${req.user.username}`, 10 * 60 * 1000, 1);
  if (limit.limited) {
    const mins = Math.ceil(limit.retryAfterSec / 60);
    return res.status(429).json({ error: `Profil resmi 10 dakikada bir güncellenebilir. ${mins} dakika sonra tekrar deneyin.` });
  }

  const { pfpBase64 } = req.body;
  if (!pfpBase64) return res.status(400).json({ error: "Görsel sağlanmadı" });

  const byteSize = Buffer.byteLength(pfpBase64, "utf8");
  if (byteSize > 512 * 1024) {
    return res.status(413).json({ error: "Görsel 512KB sunucu sınırını aşıyor" });
  }

  try {
    await queryWorker("UPDATE users SET pfp = ? WHERE username = ?", [pfpBase64, req.user.username]);
    broadcastPFrame("PFP_UPDATED", { username: req.user.username, pfp: pfpBase64 });
    res.json({ message: "Profil resmi başarıyla güncellendi" });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Friends Management
app.post("/api/friends/request", authenticate, async (req, res) => {
  const limit = checkRateLimit(`freq_${req.user.username}`, 60 * 1000, 5);
  if (limit.limited) {
    return res.status(429).json({ error: `Çok hızlı istek gönderiyorsunuz. Lütfen ${limit.retryAfterSec}s bekleyin.` });
  }

  let { targetUsername } = req.body;
  if (!targetUsername) return res.status(400).json({ error: "Hedef kullanıcı adı gerekli" });
  targetUsername = targetUsername.toLowerCase().replace(/[^a-z]/g, "");

  if (targetUsername === req.user.username) return res.status(400).json({ error: "Kendinizi ekleyemezsiniz" });

  try {
    const checkUser = await queryWorker("SELECT username FROM users WHERE username = ?", [targetUsername]);
    if (!checkUser.results || !checkUser.results.length) return res.status(404).json({ error: "Kullanıcı bulunamadı" });

    const checkExisting = await queryWorker(
      "SELECT * FROM friends WHERE (user1 = ? AND user2 = ?) OR (user1 = ? AND user2 = ?)",
      [req.user.username, targetUsername, targetUsername, req.user.username]
    );

    if (checkExisting.results && checkExisting.results.length > 0) {
      const rel = checkExisting.results[0];
      if (rel.status === "accepted") return res.status(409).json({ error: "Bu kullanıcı ile zaten arkadaşsınız" });
      return res.status(409).json({ error: "Arkadaşlık isteği zaten beklemede" });
    }

    await queryWorker("INSERT INTO friends (user1, user2, status) VALUES (?, ?, 'pending')", [req.user.username, targetUsername]);
    broadcastPFrame("FRIEND_REQUEST_SENT", { from: req.user.username, to: targetUsername }, [targetUsername, req.user.username]);
    res.json({ message: "İstek başarıyla gönderildi" });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post("/api/friends/accept", authenticate, async (req, res) => {
  const limit = checkRateLimit(`faccept_${req.user.username}`, 60 * 1000, 10);
  if (limit.limited) return res.status(429).json({ error: "İşlem sınırı aşıldı." });

  let { targetUsername } = req.body;
  targetUsername = targetUsername.toLowerCase().replace(/[^a-z]/g, "");

  try {
    await queryWorker("UPDATE friends SET status = 'accepted' WHERE user1 = ? AND user2 = ?", [targetUsername, req.user.username]);
    broadcastPFrame("FRIEND_ACCEPTED", { user1: targetUsername, user2: req.user.username }, [req.user.username, targetUsername]);
    res.json({ message: "Arkadaşlık isteği kabul edildi" });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post("/api/friends/unfriend", authenticate, async (req, res) => {
  const limit = checkRateLimit(`funfriend_${req.user.username}`, 60 * 1000, 10);
  if (limit.limited) return res.status(429).json({ error: "İşlem sınırı aşıldı." });

  let { targetUsername } = req.body;
  targetUsername = targetUsername.toLowerCase().replace(/[^a-z]/g, "");

  try {
    await queryWorker(
      "DELETE FROM friends WHERE (user1 = ? AND user2 = ?) OR (user1 = ? AND user2 = ?)",
      [req.user.username, targetUsername, targetUsername, req.user.username]
    );
    broadcastPFrame("FRIEND_REMOVED", { user1: req.user.username, user2: targetUsername }, [req.user.username, targetUsername]);
    res.json({ message: `@${targetUsername} arkadaşlıktan çıkarıldı` });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Group & DM Routes
app.get("/api/groups/:groupToken/members", authenticate, async (req, res) => {
  const limit = checkRateLimit(`gmembers_${req.user.username}`, 60 * 1000, 30);
  if (limit.limited) return res.status(429).json({ error: "İşlem sınırı aşıldı." });

  try {
    const groupRes = await queryWorker("SELECT created_by, allow_sub_invites FROM groups WHERE group_token = ?", [req.params.groupToken]);
    if (!groupRes.results || groupRes.results.length === 0) {
      return res.status(404).json({ error: "Grup bulunamadı" });
    }
    const groupInfo = groupRes.results[0];

    if (req.user.role !== "admin") {
      const isMember = await queryWorker("SELECT * FROM group_members WHERE group_token = ? AND username = ?", [req.params.groupToken, req.user.username]);
      if (!isMember.results || isMember.results.length === 0) {
        return res.status(403).json({ error: "Erişim reddedildi" });
      }
    }

    const members = await queryWorker(
      "SELECT gm.username, gm.can_add_members, gm.invited_by, u.pfp FROM group_members gm LEFT JOIN users u ON gm.username = u.username WHERE gm.group_token = ?",
      [req.params.groupToken]
    );

    res.json({
      groupInfo,
      members: members.results || []
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post("/api/groups/create", authenticate, async (req, res) => {
  const limit = checkRateLimit(`grp_${req.user.username}`, 5 * 60 * 1000, 2);
  if (limit.limited) {
    return res.status(429).json({ error: `5 dakikada yalnızca 2 grup oluşturabilirsiniz. Lütfen ${limit.retryAfterSec}s bekleyin.` });
  }

  const { groupName } = req.body;
  if (!groupName || !groupName.trim()) return res.status(400).json({ error: "Grup adı gereklidir" });
  const groupToken = "grp_" + Math.random().toString(36).substring(2, 10);

  try {
    await queryWorker("INSERT INTO groups (group_token, group_name, created_by, allow_sub_invites) VALUES (?, ?, ?, 1)", [groupToken, groupName.trim(), req.user.username]);
    await queryWorker("INSERT INTO group_members (group_token, username, custom_member_token, can_add_members, invited_by) VALUES (?, ?, ?, 1, ?)", [groupToken, req.user.username, req.user.token, req.user.username]);
    broadcastPFrame("GROUP_CREATED", { group_token: groupToken, group_name: groupName, created_by: req.user.username });
    res.json({ message: "Grup başarıyla oluşturuldu", groupToken });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post("/api/groups/add-member", authenticate, async (req, res) => {
  const limit = checkRateLimit(`gadd_${req.user.username}`, 60 * 1000, 10);
  if (limit.limited) return res.status(429).json({ error: "İşlem sınırı aşıldı." });

  let { groupToken, targetUsername } = req.body;
  if (!groupToken || !targetUsername) return res.status(400).json({ error: "Grup jetonu ve hedef kullanıcı gerekli" });
  targetUsername = targetUsername.toLowerCase().replace(/[^a-z]/g, "");

  try {
    const groupRes = await queryWorker("SELECT * FROM groups WHERE group_token = ?", [groupToken]);
    if (!groupRes.results || groupRes.results.length === 0) return res.status(404).json({ error: "Grup bulunamadı" });
    const group = groupRes.results[0];

    if (req.user.role !== "admin") {
      const isMemberRes = await queryWorker("SELECT * FROM group_members WHERE group_token = ? AND username = ?", [groupToken, req.user.username]);
      if (!isMemberRes.results || isMemberRes.results.length === 0) {
        return res.status(403).json({ error: "Bu grubun üyesi değilsiniz" });
      }
      const requesterMember = isMemberRes.results[0];

      const isOwner = group.created_by === req.user.username;
      if (!isOwner) {
        if (requesterMember.can_add_members === 0) {
          return res.status(403).json({ error: "Gruba üye ekleme yetkiniz kapatılmış" });
        }

        const allowSubInvites = group.allow_sub_invites !== 0;
        if (!allowSubInvites) {
          const wasInvitedByOwner = requesterMember.invited_by === group.created_by || requesterMember.username === group.created_by;
          if (!wasInvitedByOwner) {
            return res.status(403).json({ error: "Bu grupta yalnızca kurucu tarafından eklenen üyeler başkalarını davet edebilir" });
          }
        }
      }

      const isFriend = await queryWorker(
        "SELECT * FROM friends WHERE ((user1 = ? AND user2 = ?) OR (user1 = ? AND user2 = ?)) AND status = 'accepted'",
        [req.user.username, targetUsername, targetUsername, req.user.username]
      );

      if (!isFriend.results || isFriend.results.length === 0) {
        return res.status(403).json({ error: `@${targetUsername} kişisini gruba eklemek için kabul edilmiş arkadaş olmalısınız` });
      }
    }

    await queryWorker("INSERT OR REPLACE INTO group_members (group_token, username, custom_member_token, can_add_members, invited_by) VALUES (?, ?, 'DEFAULT', 1, ?)", [groupToken, targetUsername, req.user.username]);
    broadcastPFrame("GROUP_MEMBER_ADDED", { groupToken, targetUsername }, [targetUsername]);
    res.json({ message: `@${targetUsername} gruba eklendi` });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post("/api/groups/remove-member", authenticate, async (req, res) => {
  const limit = checkRateLimit(`grem_${req.user.username}`, 60 * 1000, 10);
  if (limit.limited) return res.status(429).json({ error: "İşlem sınırı aşıldı." });

  let { groupToken, targetUsername } = req.body;
  if (!groupToken || !targetUsername) return res.status(400).json({ error: "Eksik parametreler" });
  targetUsername = targetUsername.toLowerCase().replace(/[^a-z]/g, "");

  try {
    const groupRes = await queryWorker("SELECT * FROM groups WHERE group_token = ?", [groupToken]);
    if (!groupRes.results || groupRes.results.length === 0) return res.status(404).json({ error: "Grup bulunamadı" });
    const group = groupRes.results[0];

    const isOwner = group.created_by === req.user.username;
    if (!isOwner && req.user.role !== "admin") {
      return res.status(403).json({ error: "Yalnızca grup kurucusu üyeleri gruptan çıkarabilir" });
    }

    if (targetUsername === group.created_by) {
      return res.status(400).json({ error: "Grup kurucusu gruptan çıkarılamaz" });
    }

    await queryWorker("DELETE FROM group_members WHERE group_token = ? AND username = ?", [groupToken, targetUsername]);
    broadcastPFrame("GROUP_MEMBER_LEFT", { groupToken, username: targetUsername });
    res.json({ message: `@${targetUsername} gruptan çıkarıldı` });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post("/api/groups/toggle-member-invite-perm", authenticate, async (req, res) => {
  const limit = checkRateLimit(`gtoggle_${req.user.username}`, 60 * 1000, 20);
  if (limit.limited) return res.status(429).json({ error: "İşlem sınırı aşıldı." });

  let { groupToken, targetUsername, canAddMembers } = req.body;
  if (!groupToken || !targetUsername) return res.status(400).json({ error: "Eksik parametreler" });

  try {
    const groupRes = await queryWorker("SELECT * FROM groups WHERE group_token = ?", [groupToken]);
    if (!groupRes.results || groupRes.results.length === 0) return res.status(404).json({ error: "Grup bulunamadı" });
    const group = groupRes.results[0];

    if (group.created_by !== req.user.username && req.user.role !== "admin") {
      return res.status(403).json({ error: "Yalnızca grup kurucusu izinleri değiştirebilir" });
    }

    const newPerm = canAddMembers ? 1 : 0;
    await queryWorker("UPDATE group_members SET can_add_members = ? WHERE group_token = ? AND username = ?", [newPerm, groupToken, targetUsername]);

    broadcastPFrame("GROUP_PERM_UPDATED", { groupToken, targetUsername, canAddMembers: newPerm });
    res.json({ message: "Üye izni güncellendi" });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post("/api/groups/toggle-sub-invites", authenticate, async (req, res) => {
  const limit = checkRateLimit(`gsub_${req.user.username}`, 60 * 1000, 10);
  if (limit.limited) return res.status(429).json({ error: "İşlem sınırı aşıldı." });

  let { groupToken, allowSubInvites } = req.body;
  if (!groupToken) return res.status(400).json({ error: "Grup jetonu gerekli" });

  try {
    const groupRes = await queryWorker("SELECT * FROM groups WHERE group_token = ?", [groupToken]);
    if (!groupRes.results || groupRes.results.length === 0) return res.status(404).json({ error: "Grup bulunamadı" });
    const group = groupRes.results[0];

    if (group.created_by !== req.user.username && req.user.role !== "admin") {
      return res.status(403).json({ error: "Yalnızca grup kurucusu bu ayarı değiştirebilir" });
    }

    const newVal = allowSubInvites ? 1 : 0;
    await queryWorker("UPDATE groups SET allow_sub_invites = ? WHERE group_token = ?", [newVal, groupToken]);

    broadcastPFrame("GROUP_SETTING_UPDATED", { groupToken, allowSubInvites: newVal });
    res.json({ message: "Grup davet ilkesi güncellendi" });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post("/api/groups/leave", authenticate, async (req, res) => {
  const limit = checkRateLimit(`gleave_${req.user.username}`, 60 * 1000, 5);
  if (limit.limited) return res.status(429).json({ error: "İşlem sınırı aşıldı." });

  const { groupToken } = req.body;
  if (!groupToken) return res.status(400).json({ error: "Grup jetonu gerekli" });

  try {
    await queryWorker("DELETE FROM group_members WHERE group_token = ? AND username = ?", [groupToken, req.user.username]);

    const remainingMembers = await queryWorker("SELECT username FROM group_members WHERE group_token = ?", [groupToken]);
    const groupRes = await queryWorker("SELECT created_by FROM groups WHERE group_token = ?", [groupToken]);

    if (groupRes.results && groupRes.results.length > 0) {
      const group = groupRes.results[0];
      if (!remainingMembers.results || remainingMembers.results.length === 0) {
        await queryWorker("DELETE FROM groups WHERE group_token = ?", [groupToken]);
        await queryWorker("DELETE FROM messages WHERE group_token = ?", [groupToken]);
      } else if (group.created_by === req.user.username) {
        const nextOwner = remainingMembers.results[0].username;
        await queryWorker("UPDATE groups SET created_by = ? WHERE group_token = ?", [nextOwner, groupToken]);
      }
    }

    broadcastPFrame("GROUP_MEMBER_LEFT", { groupToken, username: req.user.username });
    res.json({ message: "Gruptan başarıyla ayrılındı" });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post("/api/dm/open", authenticate, async (req, res) => {
  const limit = checkRateLimit(`dm_${req.user.username}`, 60 * 1000, 10);
  if (limit.limited) return res.status(429).json({ error: "İşlem sınırı aşıldı." });

  let { targetUsername } = req.body;
  targetUsername = targetUsername.toLowerCase().replace(/[^a-z]/g, "");

  try {
    const friendCheck = await queryWorker(
      "SELECT * FROM friends WHERE ((user1 = ? AND user2 = ?) OR (user1 = ? AND user2 = ?)) AND status = 'accepted'",
      [req.user.username, targetUsername, targetUsername, req.user.username]
    );

    if (!friendCheck.results.length && req.user.role !== "admin") {
      return res.status(403).json({ error: "Direkt mesaj başlatmak için kabul edilmiş arkadaş olmalısınız" });
    }

    const dmGroupToken = "dm_" + [req.user.username, targetUsername].sort().join("_");
    await queryWorker("INSERT OR IGNORE INTO groups (group_token, group_name, created_by, allow_sub_invites) VALUES (?, ?, ?, 1)", [dmGroupToken, `@${targetUsername}`, req.user.username]);
    await queryWorker("INSERT OR IGNORE INTO group_members (group_token, username, custom_member_token, can_add_members) VALUES (?, ?, 'DM', 1)", [dmGroupToken, req.user.username]);
    await queryWorker("INSERT OR IGNORE INTO group_members (group_token, username, custom_member_token, can_add_members) VALUES (?, ?, 'DM', 1)", [dmGroupToken, targetUsername]);

    res.json({ groupToken: dmGroupToken });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Messaging Engine (Gönderme)
app.post("/api/messages/send", authenticate, async (req, res) => {
  const limit = checkRateLimit(`msg_${req.user.username}`, 3 * 1000, 5);
  if (limit.limited) {
    return res.status(429).json({ error: "Çok hızlı mesaj gönderiyorsunuz. Lütfen yavaşlayın." });
  }

  const { groupToken, content, replyToSender, replyToContent } = req.body;
  if (!groupToken || !content) return res.status(400).json({ error: "Mesaj içeriği eksik" });
  const timestamp = Date.now();

  try {
    if (req.user.role !== "admin") {
      const isMember = await queryWorker("SELECT * FROM group_members WHERE group_token = ? AND username = ?", [groupToken, req.user.username]);
      if (!isMember.results || isMember.results.length === 0) {
        return res.status(403).json({ error: "Mesaj göndermek için bu grubun üyesi olmalısınız" });
      }
    }

    if (content.startsWith("data:image/")) {
      const byteSize = Buffer.byteLength(content, "utf8");
      if (byteSize > 1024 * 1024) {
        return res.status(413).json({ error: "Görsel eki 1MB sınırını aşıyor" });
      }
    }

    await queryWorker(
      "INSERT INTO messages (group_token, sender, content, created_at, reply_to_sender, reply_to_content) VALUES (?, ?, ?, ?, ?, ?)",
      [groupToken, req.user.username, content, timestamp, replyToSender || null, replyToContent || null]
    );

    const pFrameData = {
      group_token: groupToken,
      sender: req.user.username,
      content,
      created_at: timestamp,
      reply_to_sender: replyToSender || null,
      reply_to_content: replyToContent || null,
      pfp: req.user.pfp || ""
    };

    broadcastPFrame("NEW_MESSAGE", pFrameData);
    res.json({ message: "Mesaj gönderildi" });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Kendi Mesajını Silme Endpoint'i (Sunucu Tarafı Yetki Kontrollü)
app.post("/api/messages/delete", authenticate, async (req, res) => {
  const limit = checkRateLimit(`msgdel_${req.user.username}`, 3 * 1000, 10);
  if (limit.limited) return res.status(429).json({ error: "İşlem sınırı aşıldı." });

  const { messageId, groupToken, createdAt } = req.body;

  try {
    let msgRes;
    if (messageId) {
      msgRes = await queryWorker("SELECT * FROM messages WHERE id = ?", [messageId]);
    } else {
      msgRes = await queryWorker("SELECT * FROM messages WHERE group_token = ? AND sender = ? AND created_at = ?", [groupToken, req.user.username, createdAt]);
    }

    if (!msgRes.results || msgRes.results.length === 0) {
      return res.status(404).json({ error: "Silinecek mesaj bulunamadı" });
    }

    const msg = msgRes.results[0];

    // Sunucu Tarafı Güvenlik Kontrolü: Mesajı sadece gönderen veya yönetici silebilir
    if (msg.sender !== req.user.username && req.user.role !== "admin") {
      return res.status(403).json({ error: "Bu mesajı silme yetkiniz yok!" });
    }

    if (messageId) {
      await queryWorker("DELETE FROM messages WHERE id = ?", [messageId]);
    } else {
      await queryWorker("DELETE FROM messages WHERE group_token = ? AND sender = ? AND created_at = ?", [groupToken, req.user.username, createdAt]);
    }

    broadcastPFrame("MESSAGE_DELETED", { groupToken: msg.group_token, messageId: msg.id, createdAt: msg.created_at });
    res.json({ message: "Mesaj başarıyla silindi" });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get("/api/messages/:groupToken", authenticate, async (req, res) => {
  const limit = checkRateLimit(`getmsg_${req.user.username}`, 60 * 1000, 60);
  if (limit.limited) return res.status(429).json({ error: "Çok fazla istek." });

  try {
    if (req.user.role !== "admin") {
      const isMember = await queryWorker("SELECT * FROM group_members WHERE group_token = ? AND username = ?", [req.params.groupToken, req.user.username]);
      if (!isMember.results || isMember.results.length === 0) {
        return res.status(403).json({ error: "Grup mesajlarına erişim reddedildi" });
      }
    }

    const msgs = await queryWorker(
      "SELECT m.*, u.pfp FROM messages m LEFT JOIN users u ON m.sender = u.username WHERE m.group_token = ? ORDER BY m.created_at ASC",
      [req.params.groupToken]
    );
    res.json(msgs.results || []);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Admin Control Routes
app.get("/api/admin/pending", authenticate, async (req, res) => {
  if (req.user.role !== "admin") return res.status(403).json({ error: "Yasaklandı" });
  const data = await queryWorker("SELECT * FROM pending_accounts");
  res.json(data.results || []);
});

app.get("/api/admin/users", authenticate, async (req, res) => {
  if (req.user.role !== "admin") return res.status(403).json({ error: "Yasaklandı" });
  try {
    const data = await queryWorker("SELECT username, role FROM users");
    res.json(data.results || []);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post("/api/admin/action", authenticate, async (req, res) => {
  if (req.user.role !== "admin") return res.status(403).json({ error: "Yasaklandı" });
  const { id, action } = req.body;
  const reqRes = await queryWorker("SELECT * FROM pending_accounts WHERE id = ?", [id]);
  if (!reqRes.results.length) return res.status(404).json({ error: "Bulunamadı" });

  const item = reqRes.results[0];
  if (action === "approve") {
    await queryWorker("INSERT INTO users (username, token) VALUES (?, ?)", [item.username, item.token]);
  }
  await queryWorker("DELETE FROM pending_accounts WHERE id = ?", [id]);
  broadcastPFrame("ADMIN_ACTION", { id, action });
  res.json({ message: `Hesap talebi ${action} edildi.` });
});

app.post("/api/admin/delete-user", authenticate, async (req, res) => {
  if (req.user.role !== "admin") return res.status(403).json({ error: "Yasaklandı" });
  const { username } = req.body;
  await queryWorker("DELETE FROM users WHERE username = ?", [username]);
  await queryWorker("DELETE FROM group_members WHERE username = ?", [username]);
  await queryWorker("DELETE FROM friends WHERE user1 = ? OR user2 = ?", [username, username]);
  broadcastPFrame("USER_DELETED", { username });
  res.json({ message: `${username} kullanıcısı silindi.` });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`SOPERT Sunucusu ${PORT} portunda çevrimiçi`));
