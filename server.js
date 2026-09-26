/**
 * Сервер случайного чата с системой друзей.
 *
 * Установка:
 *   npm init -y
 *   npm install ws
 *
 * Запуск:
 *   node server.js
 *
 * ВАЖНО: список пользователей/друзей хранится в файле data.json рядом
 * с сервером. На бесплатных хостингах диск может очищаться при
 * каждом новом деплое (обновлении кода) — тогда список друзей сбросится.
 * Это не баг, а ограничение бесплатного тарифа.
 *
 * Протокол (JSON-сообщения):
 *
 * Клиент -> Сервер:
 *   { type: "register", username, token, nick, avatar }
 *     — войти под юзернеймом. token пустой при первой регистрации,
 *       дальше сервер выдаёт постоянный token, который клиент обязан
 *       сохранить и присылать при каждом следующем входе под тем же
 *       username (это доказывает, что аккаунт "твой").
 *   { type: "join" }                     — встать в очередь случайного чата
 *   { type: "message", text }            — отправить сообщение партнёру
 *   { type: "skip" }                     — пропустить текущего собеседника
 *   { type: "friend_request" }           — отправить заявку в друзья текущему партнёру
 *   { type: "friend_response", from_username, accept }
 *                                         — ответ на входящую заявку
 *   { type: "get_friends" }              — запросить список друзей
 *   { type: "direct_connect", username } — написать конкретному другу напрямую
 *
 * Сервер -> Клиент:
 *   { type: "registered", username, token }
 *   { type: "username_taken" }
 *   { type: "waiting" }
 *   { type: "matched", partner_nick, partner_avatar, partner_username, already_friends }
 *   { type: "message", text }
 *   { type: "partner_left" }
 *   { type: "friend_request_received", from_username, from_nick, from_avatar }
 *   { type: "friend_added", username, nick, avatar }
 *   { type: "friend_declined" }
 *   { type: "friends_list", friends: [{username, nick, avatar, online}] }
 *   { type: "friend_offline", username }
 *   { type: "friend_busy", username }
 *   { type: "error", message }
 */

const WebSocket = require("ws");
const fs = require("fs");
const path = require("path");

const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });

const MAX_AVATAR_LENGTH = 300000; // ~300 KB строки base64
const DATA_FILE = path.join(__dirname, "data.json");

// Постоянные данные: { [username]: { token, nick, avatar, friends: [username...] } }
let usersData = {};

function loadData() {
  try {
    usersData = JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
  } catch (e) {
    usersData = {};
  }
}
loadData();

let saveScheduled = false;
function saveData() {
  if (saveScheduled) return;
  saveScheduled = true;
  setTimeout(() => {
    saveScheduled = false;
    try {
      fs.writeFileSync(DATA_FILE, JSON.stringify(usersData));
    } catch (e) {
      console.error("Не удалось сохранить data.json:", e.message);
    }
  }, 300);
}

function randomToken() {
  return Math.random().toString(36).slice(2) + Math.random().toString(36).slice(2);
}

// Очередь клиентов, ожидающих случайного собеседника
let queue = [];

// ws -> { username, nick, avatar, partner (ws|null) }
const clients = new Map();

// username -> ws (кто сейчас онлайн)
const onlineSockets = new Map();

function send(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function removeFromQueue(ws) {
  queue = queue.filter((c) => c !== ws);
}

function publicProfile(username) {
  const u = usersData[username];
  if (!u) return null;
  return { username, nick: u.nick, avatar: u.avatar };
}

function pairUp(wsA, wsB) {
  const a = clients.get(wsA);
  const b = clients.get(wsB);
  if (!a || !b) return;

  a.partner = wsB;
  b.partner = wsA;

  const areFriends =
    a.username &&
    b.username &&
    usersData[a.username] &&
    usersData[a.username].friends.includes(b.username);

  send(wsA, {
    type: "matched",
    partner_nick: b.nick,
    partner_avatar: b.avatar,
    partner_username: b.username,
    already_friends: !!areFriends,
  });
  send(wsB, {
    type: "matched",
    partner_nick: a.nick,
    partner_avatar: a.avatar,
    partner_username: a.username,
    already_friends: !!areFriends,
  });
}

function findPartner(ws) {
  removeFromQueue(ws);

  if (queue.length > 0) {
    const partner = queue.shift();
    if (partner.readyState !== WebSocket.OPEN) {
      return findPartner(ws);
    }
    pairUp(ws, partner);
  } else {
    queue.push(ws);
    send(ws, { type: "waiting" });
  }
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

  if (requeueSelf) {
    findPartner(ws);
  }
}

wss.on("connection", (ws) => {
  clients.set(ws, { username: null, nick: "Аноним", avatar: null, partner: null });

  ws.on("message", (raw) => {
    let data;
    try {
      data = JSON.parse(raw);
    } catch (e) {
      return send(ws, { type: "error", message: "Некорректный JSON" });
    }

    const me = clients.get(ws);
    if (!me) return;

    switch (data.type) {
      case "register": {
        const username = (data.username || "").toString().trim().slice(0, 24);
        const token = (data.token || "").toString();
        const nick = (data.nick || "Аноним").toString().slice(0, 24);
        let avatar = null;
        if (
          typeof data.avatar === "string" &&
          data.avatar.length > 0 &&
          data.avatar.length <= MAX_AVATAR_LENGTH
        ) {
          avatar = data.avatar;
        }

        if (!username) {
          return send(ws, { type: "error", message: "Укажи юзернейм" });
        }

        const existing = usersData[username];

        if (existing) {
          if (!token || existing.token !== token) {
            return send(ws, { type: "username_taken" });
          }
          existing.nick = nick;
          existing.avatar = avatar;
        } else {
          usersData[username] = {
            token: token || randomToken(),
            nick,
            avatar,
            friends: [],
          };
        }
        saveData();

        me.username = username;
        me.nick = nick;
        me.avatar = avatar;
        onlineSockets.set(username, ws);

        send(ws, { type: "registered", username, token: usersData[username].token });
        break;
      }

      case "join": {
        findPartner(ws);
        break;
      }

      case "message": {
        if (me.partner && clients.has(me.partner)) {
          send(me.partner, { type: "message", text: String(data.text || "") });
        }
        break;
      }

      case "skip": {
        breakPair(ws, { requeueSelf: true });
        break;
      }

      case "friend_request": {
        if (!me.username || !me.partner) break;
        const partner = clients.get(me.partner);
        if (!partner || !partner.username) break;
        send(me.partner, {
          type: "friend_request_received",
          from_username: me.username,
          from_nick: me.nick,
          from_avatar: me.avatar,
        });
        break;
      }

      case "friend_response": {
        const fromUsername = (data.from_username || "").toString();
        const accept = !!data.accept;
        const fromWs = onlineSockets.get(fromUsername);

        if (
          accept &&
          me.username &&
          fromUsername &&
          usersData[me.username] &&
          usersData[fromUsername]
        ) {
          if (!usersData[me.username].friends.includes(fromUsername)) {
            usersData[me.username].friends.push(fromUsername);
          }
          if (!usersData[fromUsername].friends.includes(me.username)) {
            usersData[fromUsername].friends.push(me.username);
          }
          saveData();

          send(ws, {
            type: "friend_added",
            username: fromUsername,
            nick: usersData[fromUsername].nick,
            avatar: usersData[fromUsername].avatar,
          });
          send(fromWs, {
            type: "friend_added",
            username: me.username,
            nick: usersData[me.username].nick,
            avatar: usersData[me.username].avatar,
          });
        } else {
          send(fromWs, { type: "friend_declined" });
        }
        break;
      }

      case "get_friends": {
        if (!me.username || !usersData[me.username]) {
          return send(ws, { type: "friends_list", friends: [] });
        }
        const list = usersData[me.username].friends.map((u) => {
          const profile = publicProfile(u);
          return {
            username: u,
            nick: profile ? profile.nick : u,
            avatar: profile ? profile.avatar : null,
            online: onlineSockets.has(u),
          };
        });
        send(ws, { type: "friends_list", friends: list });
        break;
      }

      case "direct_connect": {
        const targetUsername = (data.username || "").toString();
        const targetWs = onlineSockets.get(targetUsername);

        if (!targetWs || targetWs.readyState !== WebSocket.OPEN) {
          return send(ws, { type: "friend_offline", username: targetUsername });
        }

        const targetMeta = clients.get(targetWs);
        if (targetMeta.partner) {
          return send(ws, { type: "friend_busy", username: targetUsername });
        }

        breakPair(ws, { requeueSelf: false });
        removeFromQueue(ws);
        removeFromQueue(targetWs);

        pairUp(ws, targetWs);
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

console.log(`Сервер случайного чата с друзьями запущен на порту ${PORT}`);
