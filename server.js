/**
 * Сервер случайного чата с друзьями, жалобами и банами.
 *
 * Установка:
 *   npm init -y
 *   npm install ws
 *
 * Запуск:
 *   node server.js
 *
 * ВАЖНО (безопасность): пароль администратора берётся из переменной
 * окружения ADMIN_PASSWORD. Задай её в панели хостинга (Render:
 * Environment -> Add Environment Variable), НЕ пиши пароль прямо
 * в этом файле — репозиторий публичный, его увидит кто угодно.
 * Если переменная не задана, админ-панель отключена.
 *
 * ВАЖНО (хранилище): если задана переменная окружения MONGODB_URI —
 * все данные (пользователи, друзья, жалобы, баны) хранятся в MongoDB
 * Atlas и переживают любой передеплой кода. Если MONGODB_URI не задана,
 * сервер работает по-старому — хранит всё в файле data.json рядом
 * с собой (годится для локальных тестов, но на бесплатном хостинге
 * такой файл может обнуляться при каждом обновлении кода).
 *
 * Протокол (WebSocket, JSON-сообщения):
 *
 * Клиент -> Сервер:
 *   { type: "register", username, token, nick, avatar, banner, bio, deviceId }
 *   { type: "join" }
 *   { type: "message", text }
 *   { type: "skip" }
 *   { type: "friend_request" }
 *   { type: "friend_response", from_username, accept }
 *   { type: "get_friends" }
 *   { type: "direct_connect", username }
 *   { type: "report", reason, username }     — жалоба; username опционален (по умолчанию — текущий партнёр)
 *   { type: "get_profile", username }        — открыть чей-то профиль
 *   { type: "block_user", username }         — заблокировать пользователя (не будет попадаться в чате/заявках)
 *
 * Сервер -> Клиент:
 *   { type: "registered", username, token }
 *   { type: "username_taken" }
 *   { type: "banned", reason }
 *   { type: "waiting" }
 *   { type: "matched", partner_nick, partner_avatar, partner_banner, partner_username, already_friends }
 *   { type: "message", text }
 *   { type: "partner_left" }
 *   { type: "friend_request_received", from_username, from_nick, from_avatar }
 *   { type: "friend_added", username, nick, avatar }
 *   { type: "friend_declined" }
 *   { type: "friends_list", friends: [...] }
 *   { type: "friend_offline", username }
 *   { type: "friend_busy", username }
 *   { type: "report_sent" }
 *   { type: "profile", username, nick, avatar, banner, bio, online }
 *   { type: "profile_not_found", username }
 *   { type: "blocked_user", username }
 *   { type: "error", message }
 *
 * Админ-панель (обычный HTTP, отдельно от WebSocket, тот же адрес и порт):
 *   GET  /admin/reports?password=...                       -> список жалоб
 *   POST /admin/ban      { password, username, alsoBanIp }  -> забанить
 *   POST /admin/resolve  { password, reportId }             -> отклонить жалобу без бана
 *   GET  /admin/banned?password=...                         -> список забаненных
 *   POST /admin/unban    { password, username }             -> разбанить
 */

const WebSocket = require("ws");
const http = require("http");
const fs = require("fs");
const path = require("path");
const { MongoClient } = require("mongodb");

const PORT = process.env.PORT || 8080;
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || null;
const MONGODB_URI = process.env.MONGODB_URI || null;

const MAX_AVATAR_LENGTH = 300000;
const MAX_BANNER_LENGTH = 14000000; // ~10 МБ файла раздувается в base64 примерно на треть
const MAX_BIO_LENGTH = 300;
const MAX_REPORT_REASON_LENGTH = 500;
const DATA_FILE = path.join(__dirname, "data.json");

const EMPTY_DB = { users: {}, reports: [], bannedUsernames: {}, bannedIps: {}, bannedDeviceIds: {} };

// ---------- постоянное хранилище ----------
let db = Object.assign({}, EMPTY_DB);

let mongoCollection = null; // задаётся в initStorage(), если есть MONGODB_URI

async function initStorage() {
  if (MONGODB_URI) {
    const client = new MongoClient(MONGODB_URI);
    await client.connect();
    mongoCollection = client.db("randomchat").collection("state");

    const doc = await mongoCollection.findOne({ _id: "main" });
    if (doc) {
      delete doc._id;
      db = Object.assign({}, EMPTY_DB, doc);
    } else {
      await mongoCollection.insertOne(Object.assign({ _id: "main" }, db));
    }
    console.log("Хранилище: MongoDB (данные переживут передеплой).");
  } else {
    loadDataFromFile();
    console.log("Хранилище: локальный файл data.json (MONGODB_URI не задана).");
  }
}

function loadDataFromFile() {
  try {
    const raw = JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
    db = Object.assign({}, EMPTY_DB, raw);
  } catch (e) {
    // файла ещё нет или он битый — начинаем с чистого листа
  }
}

let saveScheduled = false;
function saveData() {
  if (saveScheduled) return;
  saveScheduled = true;
  setTimeout(async () => {
    saveScheduled = false;
    try {
      if (mongoCollection) {
        await mongoCollection.updateOne({ _id: "main" }, { $set: db }, { upsert: true });
      } else {
        fs.writeFileSync(DATA_FILE, JSON.stringify(db));
      }
    } catch (e) {
      console.error("Не удалось сохранить данные:", e.message);
    }
  }, 300);
}

function randomToken() {
  return Math.random().toString(36).slice(2) + Math.random().toString(36).slice(2);
}
function randomId() {
  return Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
}

function getClientIp(req) {
  const fwd = req.headers["x-forwarded-for"];
  if (fwd) return fwd.split(",")[0].trim();
  return req.socket.remoteAddress || "";
}

// ---------- HTTP-сервер (обычные запросы + админ-панель) ----------
function readJsonBody(req) {
  return new Promise((resolve) => {
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", () => {
      try { resolve(JSON.parse(body || "{}")); } catch (e) { resolve({}); }
    });
  });
}

function sendJson(res, status, obj) {
  res.writeHead(status, {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
  });
  res.end(JSON.stringify(obj));
}

const httpServer = http.createServer(async (req, res) => {
  const url = new URL(req.url, "http://localhost");

  if (req.method === "OPTIONS") {
    return sendJson(res, 200, {});
  }

  if (!ADMIN_PASSWORD) {
    return sendJson(res, 503, { error: "Админ-панель отключена: не задан ADMIN_PASSWORD" });
  }

  if (url.pathname === "/admin/reports" && req.method === "GET") {
    if (url.searchParams.get("password") !== ADMIN_PASSWORD) {
      return sendJson(res, 401, { error: "Неверный пароль" });
    }
    const reports = db.reports
      .slice()
      .sort((a, b) => b.createdAt - a.createdAt);
    return sendJson(res, 200, { reports });
  }

  if (url.pathname === "/admin/ban" && req.method === "POST") {
    const body = await readJsonBody(req);
    if (body.password !== ADMIN_PASSWORD) return sendJson(res, 401, { error: "Неверный пароль" });

    const username = (body.username || "").toString();
    if (!username || !db.users[username]) return sendJson(res, 404, { error: "Пользователь не найден" });

    db.bannedUsernames[username] = true;

    if (body.alsoBanIp && db.users[username].lastIp) {
      db.bannedIps[db.users[username].lastIp] = true;
    }
    if (body.alsoBanDevice && db.users[username].lastDeviceId) {
      db.bannedDeviceIds[db.users[username].lastDeviceId] = true;
    }
    saveData();

    const targetWs = onlineSockets.get(username);
    if (targetWs) {
      send(targetWs, { type: "banned", reason: "Тебя забанили за нарушение правил" });
      targetWs.close();
    }
    return sendJson(res, 200, { ok: true });
  }

  if (url.pathname === "/admin/resolve" && req.method === "POST") {
    const body = await readJsonBody(req);
    if (body.password !== ADMIN_PASSWORD) return sendJson(res, 401, { error: "Неверный пароль" });

    const reportId = (body.reportId || "").toString();
    const report = db.reports.find((r) => r.id === reportId);
    if (report) {
      report.resolved = true;
      saveData();
    }
    return sendJson(res, 200, { ok: true });
  }

  if (url.pathname === "/admin/banned" && req.method === "GET") {
    if (url.searchParams.get("password") !== ADMIN_PASSWORD) {
      return sendJson(res, 401, { error: "Неверный пароль" });
    }
    const banned = Object.keys(db.bannedUsernames).map((username) => {
      const profile = db.users[username];
      return {
        username,
        nick: profile ? profile.nick : username,
        avatar: profile ? profile.avatar : null,
      };
    });
    return sendJson(res, 200, { banned });
  }

  if (url.pathname === "/admin/unban" && req.method === "POST") {
    const body = await readJsonBody(req);
    if (body.password !== ADMIN_PASSWORD) return sendJson(res, 401, { error: "Неверный пароль" });

    const username = (body.username || "").toString();
    if (!username) return sendJson(res, 404, { error: "Пользователь не найден" });

    delete db.bannedUsernames[username];

    const profile = db.users[username];
    if (profile) {
      if (profile.lastIp) delete db.bannedIps[profile.lastIp];
      if (profile.lastDeviceId) delete db.bannedDeviceIds[profile.lastDeviceId];
    }
    saveData();
    return sendJson(res, 200, { ok: true });
  }

  if (url.pathname === "/admin/accounts" && req.method === "GET") {
    if (url.searchParams.get("password") !== ADMIN_PASSWORD) {
      return sendJson(res, 401, { error: "Неверный пароль" });
    }
    const accounts = Object.keys(db.users)
      .sort()
      .map((username) => {
        const u = db.users[username];
        return {
          username,
          nick: u.nick,
          avatar: u.avatar,
          online: onlineSockets.has(username),
          banned: !!db.bannedUsernames[username],
          friendsCount: (u.friends || []).length,
        };
      });
    return sendJson(res, 200, { accounts });
  }

  if (url.pathname === "/admin/delete_account" && req.method === "POST") {
    const body = await readJsonBody(req);
    if (body.password !== ADMIN_PASSWORD) return sendJson(res, 401, { error: "Неверный пароль" });

    const username = (body.username || "").toString();
    if (!username || !db.users[username]) return sendJson(res, 404, { error: "Пользователь не найден" });

    // выгоняем, если сейчас онлайн
    const targetWs = onlineSockets.get(username);
    if (targetWs) {
      send(targetWs, { type: "account_deleted" });
      targetWs.close();
      onlineSockets.delete(username);
    }

    // убираем из чужих списков друзей
    Object.values(db.users).forEach((u) => {
      if (Array.isArray(u.friends)) {
        u.friends = u.friends.filter((f) => f !== username);
      }
    });

    delete db.users[username];
    saveData();
    return sendJson(res, 200, { ok: true });
  }

  sendJson(res, 404, { error: "Не найдено" });
});

const wss = new WebSocket.Server({ server: httpServer });

// ---------- рантайм-состояние (не сохраняется на диск) ----------
let queue = [];
const clients = new Map();       // ws -> { username, nick, avatar, banner, bio, partner, ip, deviceId }
const onlineSockets = new Map(); // username -> ws

function send(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}
function removeFromQueue(ws) {
  queue = queue.filter((c) => c !== ws);
}
function publicProfile(username) {
  const u = db.users[username];
  if (!u) return null;
  return { username, nick: u.nick, avatar: u.avatar, banner: u.banner || null, bio: u.bio || "" };
}

function isBlocked(usernameA, usernameB) {
  if (!usernameA || !usernameB) return false;
  const a = db.users[usernameA];
  const b = db.users[usernameB];
  return !!((a && a.blocked && a.blocked.includes(usernameB)) || (b && b.blocked && b.blocked.includes(usernameA)));
}

function pairUp(wsA, wsB) {
  const a = clients.get(wsA);
  const b = clients.get(wsB);
  if (!a || !b) return;

  a.partner = wsB;
  b.partner = wsA;

  const areFriends =
    a.username && b.username && db.users[a.username] && db.users[a.username].friends.includes(b.username);

  send(wsA, { type: "matched", partner_nick: b.nick, partner_avatar: b.avatar, partner_banner: b.banner || null, partner_bio: b.bio || "", partner_username: b.username, already_friends: !!areFriends });
  send(wsB, { type: "matched", partner_nick: a.nick, partner_avatar: a.avatar, partner_banner: a.banner || null, partner_bio: a.bio || "", partner_username: a.username, already_friends: !!areFriends });
}

function findPartner(ws) {
  removeFromQueue(ws);
  const me = clients.get(ws);

  for (let i = 0; i < queue.length; i++) {
    const candidate = queue[i];
    if (candidate.readyState !== WebSocket.OPEN) {
      queue.splice(i, 1);
      i--;
      continue;
    }
    const candidateMeta = clients.get(candidate);
    if (candidateMeta && me && isBlocked(me.username, candidateMeta.username)) continue;

    queue.splice(i, 1);
    pairUp(ws, candidate);
    return;
  }

  queue.push(ws);
  send(ws, { type: "waiting" });
}

function breakPair(ws, { requeueSelf = false } = {}) {
  const me = clients.get(ws);
  if (!me) return;

  const partnerWs = me.partner;
  me.partner = null;

  if (partnerWs && clients.has(partnerWs)) {
    const partner = clients.get(partnerWs);
    partner.partner = null;
    send(partnerWs, { type: "partner_left" });
    findPartner(partnerWs);
  }
  if (requeueSelf) findPartner(ws);
}

wss.on("connection", (ws, req) => {
  const ip = getClientIp(req);

  if (db.bannedIps[ip]) {
    send(ws, { type: "banned", reason: "Доступ заблокирован" });
    ws.close();
    return;
  }

  clients.set(ws, { username: null, nick: "Аноним", avatar: null, banner: null, bio: "", partner: null, ip, deviceId: null });

  ws.on("message", (raw) => {
    let data;
    try { data = JSON.parse(raw); } catch (e) { return send(ws, { type: "error", message: "Некорректный JSON" }); }

    const me = clients.get(ws);
    if (!me) return;

    switch (data.type) {
      case "register": {
        const username = (data.username || "").toString().trim().slice(0, 24);
        const token = (data.token || "").toString();
        const nick = (data.nick || "Аноним").toString().slice(0, 24);
        const deviceId = (data.deviceId || "").toString().slice(0, 100);
        const bio = (data.bio || "").toString().slice(0, MAX_BIO_LENGTH);

        let avatar = null;
        if (typeof data.avatar === "string" && data.avatar.length > 0 && data.avatar.length <= MAX_AVATAR_LENGTH) {
          avatar = data.avatar;
        }
        let banner = null;
        if (typeof data.banner === "string" && data.banner.length > 0 && data.banner.length <= MAX_BANNER_LENGTH) {
          banner = data.banner;
        }

        if (!username) return send(ws, { type: "error", message: "Укажи юзернейм" });

        if (db.bannedUsernames[username]) {
          send(ws, { type: "banned", reason: "Этот аккаунт заблокирован за нарушение правил" });
          return ws.close();
        }
        if (deviceId && db.bannedDeviceIds[deviceId]) {
          send(ws, { type: "banned", reason: "Это устройство заблокировано за нарушение правил" });
          return ws.close();
        }

        const existing = db.users[username];
        if (existing) {
          if (!token || existing.token !== token) {
            return send(ws, { type: "username_taken" });
          }
          existing.nick = nick;
          existing.avatar = avatar;
          existing.banner = banner;
          existing.bio = bio;
          existing.lastIp = ip;
          if (deviceId) existing.lastDeviceId = deviceId;
          if (!existing.blocked) existing.blocked = [];
        } else {
          db.users[username] = {
            token: token || randomToken(),
            nick,
            avatar,
            banner,
            bio,
            friends: [],
            blocked: [],
            lastIp: ip,
            lastDeviceId: deviceId || null,
          };
        }
        saveData();

        me.username = username;
        me.nick = nick;
        me.avatar = avatar;
        me.banner = banner;
        me.bio = bio;
        me.deviceId = deviceId;
        onlineSockets.set(username, ws);

        send(ws, { type: "registered", username, token: db.users[username].token });
        break;
      }

      case "join":
        findPartner(ws);
        break;

      case "message":
        if (me.partner && clients.has(me.partner)) {
          send(me.partner, { type: "message", text: String(data.text || "") });
        }
        break;

      case "skip":
        breakPair(ws, { requeueSelf: true });
        break;

      case "friend_request": {
        if (!me.username || !me.partner) break;
        const partner = clients.get(me.partner);
        if (!partner || !partner.username) break;
        send(me.partner, { type: "friend_request_received", from_username: me.username, from_nick: me.nick, from_avatar: me.avatar });
        break;
      }

      case "friend_response": {
        const fromUsername = (data.from_username || "").toString();
        const accept = !!data.accept;
        const fromWs = onlineSockets.get(fromUsername);

        if (accept && me.username && fromUsername && db.users[me.username] && db.users[fromUsername]) {
          if (!db.users[me.username].friends.includes(fromUsername)) db.users[me.username].friends.push(fromUsername);
          if (!db.users[fromUsername].friends.includes(me.username)) db.users[fromUsername].friends.push(me.username);
          saveData();

          send(ws, { type: "friend_added", username: fromUsername, nick: db.users[fromUsername].nick, avatar: db.users[fromUsername].avatar });
          send(fromWs, { type: "friend_added", username: me.username, nick: db.users[me.username].nick, avatar: db.users[me.username].avatar });
        } else {
          send(fromWs, { type: "friend_declined" });
        }
        break;
      }

      case "get_friends": {
        if (!me.username || !db.users[me.username]) return send(ws, { type: "friends_list", friends: [] });
        const list = db.users[me.username].friends.map((u) => {
          const profile = publicProfile(u);
          return {
            username: u,
            nick: profile ? profile.nick : u,
            avatar: profile ? profile.avatar : null,
            banner: profile ? profile.banner : null,
            online: onlineSockets.has(u),
          };
        });
        send(ws, { type: "friends_list", friends: list });
        break;
      }

      case "direct_connect": {
        const targetUsername = (data.username || "").toString();
        const targetWs = onlineSockets.get(targetUsername);
        if (!targetWs || targetWs.readyState !== WebSocket.OPEN) return send(ws, { type: "friend_offline", username: targetUsername });

        const targetMeta = clients.get(targetWs);
        if (targetMeta.partner) return send(ws, { type: "friend_busy", username: targetUsername });

        breakPair(ws, { requeueSelf: false });
        removeFromQueue(ws);
        removeFromQueue(targetWs);
        pairUp(ws, targetWs);
        break;
      }

      case "report": {
        const reason = (data.reason || "").toString().trim().slice(0, MAX_REPORT_REASON_LENGTH);
        if (!reason) break;

        let reportedUsername = (data.username || "").toString();
        let reportedNick = null;
        let reportedIp = null;
        let reportedDeviceId = null;

        if (reportedUsername && db.users[reportedUsername]) {
          reportedNick = db.users[reportedUsername].nick;
          reportedIp = db.users[reportedUsername].lastIp;
          reportedDeviceId = db.users[reportedUsername].lastDeviceId;
        } else if (me.partner && clients.has(me.partner)) {
          // на случай старого клиента: жалоба без явного username — на текущего партнёра
          const partner = clients.get(me.partner);
          reportedUsername = partner.username || "(без юзернейма)";
          reportedNick = partner.nick;
          reportedIp = partner.ip;
          reportedDeviceId = partner.deviceId;
        } else {
          break;
        }

        db.reports.push({
          id: randomId(),
          reporterUsername: me.username || "(без юзернейма)",
          reporterNick: me.nick,
          reportedUsername,
          reportedNick,
          reportedIp,
          reportedDeviceId,
          reason,
          createdAt: Date.now(),
          resolved: false,
        });
        saveData();
        send(ws, { type: "report_sent" });
        break;
      }

      case "get_profile": {
        const targetUsername = (data.username || "").toString();
        const target = db.users[targetUsername];

        if (!target) {
          send(ws, { type: "profile_not_found", username: targetUsername });
          break;
        }

        const already_friends = !!(me.username && db.users[me.username] && db.users[me.username].friends.includes(targetUsername));

        send(ws, {
          type: "profile",
          username: targetUsername,
          nick: target.nick,
          avatar: target.avatar,
          banner: target.banner || null,
          bio: target.bio || "",
          online: onlineSockets.has(targetUsername),
          already_friends,
        });
        break;
      }

      case "block_user": {
        const targetUsername = (data.username || "").toString();
        if (!me.username || !targetUsername || targetUsername === me.username || !db.users[me.username]) break;

        if (!db.users[me.username].blocked) db.users[me.username].blocked = [];
        if (!db.users[me.username].blocked.includes(targetUsername)) {
          db.users[me.username].blocked.push(targetUsername);
        }

        // блокировка автоматически разрывает дружбу в обе стороны
        db.users[me.username].friends = db.users[me.username].friends.filter((u) => u !== targetUsername);
        if (db.users[targetUsername]) {
          db.users[targetUsername].friends = db.users[targetUsername].friends.filter((u) => u !== me.username);
        }
        saveData();

        // если это текущий собеседник — разрываем пару и ищем нового
        if (me.partner) {
          const partnerMeta = clients.get(me.partner);
          if (partnerMeta && partnerMeta.username === targetUsername) {
            breakPair(ws, { requeueSelf: true });
          }
        }

        send(ws, { type: "blocked_user", username: targetUsername });
        break;
      }

      default:
        send(ws, { type: "error", message: "Неизвестный тип сообщения" });
    }
  });

  ws.on("close", () => {
    const me = clients.get(ws);
    if (me && me.username && onlineSockets.get(me.username) === ws) {
      onlineSockets.delete(me.username);
    }
    breakPair(ws, { requeueSelf: false });
    removeFromQueue(ws);
    clients.delete(ws);
  });
});

async function start() {
  await initStorage();
  httpServer.listen(PORT, () => {
    console.log(`Сервер случайного чата с друзьями/жалобами запущен на порту ${PORT}`);
    console.log(ADMIN_PASSWORD ? "Админ-панель включена." : "Админ-панель ОТКЛЮЧЕНА (нет ADMIN_PASSWORD).");
  });
}

start().catch((err) => {
  console.error("Не удалось запустить сервер:", err.message);
  process.exit(1);
});
