/**
 * Сервер подбора случайного собеседника для текстового чата.
 *
 * Установка:
 *   npm init -y
 *   npm install ws
 *
 * Запуск:
 *   node server.js
 *
 * Протокол (JSON-сообщения):
 *
 * Клиент -> Сервер:
 *   { type: "join", nick: "Имя", avatar: "data:image/..." }  — войти и встать в поиск (avatar опционален)
 *   { type: "message", text: "..." }                          — отправить сообщение партнёру
 *   { type: "skip" }                                          — пропустить текущего собеседника
 *
 * Сервер -> Клиент:
 *   { type: "waiting" }                                                          — ищем тебе собеседника
 *   { type: "matched", partner_nick: "...", partner_avatar: "..." | null }        — собеседник найден
 *   { type: "message", text: "..." }                                             — сообщение от партнёра
 *   { type: "partner_left" }                                                     — партнёр отключился/скипнул
 *   { type: "error", message: "..." }
 */

// Ограничение на размер аватарки (data URL), чтобы не гонять огромные файлы
const MAX_AVATAR_LENGTH = 300000; // ~300 KB строки base64

const WebSocket = require("ws");

const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });

// Очередь клиентов, ожидающих собеседника
let queue = [];

// Все подключённые клиенты: ws -> { nick, partner (ws|null) }
const clients = new Map();

function send(ws, obj) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function removeFromQueue(ws) {
  queue = queue.filter((c) => c !== ws);
}

// Пытается найти партнёра для ws. Если не нашёл — ставит в очередь.
function findPartner(ws) {
  removeFromQueue(ws);

  if (queue.length > 0) {
    const partner = queue.shift();

    // partner мог отключиться, пока стоял в очереди
    if (partner.readyState !== WebSocket.OPEN) {
      return findPartner(ws); // пробуем следующего
    }

    const me = clients.get(ws);
    const other = clients.get(partner);

    me.partner = partner;
    other.partner = ws;

    send(ws, { type: "matched", partner_nick: other.nick, partner_avatar: other.avatar });
    send(partner, { type: "matched", partner_nick: me.nick, partner_avatar: me.avatar });
  } else {
    queue.push(ws);
    send(ws, { type: "waiting" });
  }
}

// Разрывает текущую пару (если есть) и уведомляет партнёра
function breakPair(ws, { requeueSelf = false } = {}) {
  const me = clients.get(ws);
  if (!me) return;

  const partnerWs = me.partner;
  me.partner = null;

  if (partnerWs && clients.has(partnerWs)) {
    const partner = clients.get(partnerWs);
    partner.partner = null;
    send(partnerWs, { type: "partner_left" });
    // партнёра тоже сразу возвращаем в поиск
    findPartner(partnerWs);
  }

  if (requeueSelf) {
    findPartner(ws);
  }
}

wss.on("connection", (ws) => {
  clients.set(ws, { nick: "Аноним", avatar: null, partner: null });

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
      case "join": {
        me.nick = (data.nick || "Аноним").toString().slice(0, 24);

        if (typeof data.avatar === "string" && data.avatar.length > 0) {
          me.avatar = data.avatar.length <= MAX_AVATAR_LENGTH ? data.avatar : null;
        } else {
          me.avatar = null;
        }

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

      default:
        send(ws, { type: "error", message: "Неизвестный тип сообщения" });
    }
  });

  ws.on("close", () => {
    breakPair(ws, { requeueSelf: false });
    removeFromQueue(ws);
    clients.delete(ws);
  });
});

console.log(`Сервер подбора собеседников запущен на порту ${PORT}`);
