// Link in Bio — Live Chat Relay
// WebSocket <-> Telegram bridge.
//
// Flow:
//   Browser  --WS-->  Node relay  --HTTPS-->  Telegram Bot API
//                          ^                       |
//                          +-----long-poll---------+
//
// Replies are routed back to the correct visitor by Telegram's
// native reply-to-message feature: every outbound message we send
// is stored in a map { telegram_message_id -> session_id }, so when
// the operator replies in Telegram, we know which WebSocket to push to.

import http from 'node:http';
import { randomUUID } from 'node:crypto';
import express from 'express';
import { WebSocketServer } from 'ws';
import TelegramBot from 'node-telegram-bot-api';

const PORT       = parseInt(process.env.PORT || '3001', 10);
const TG_TOKEN   = process.env.TELEGRAM_BOT_TOKEN;
const TG_CHAT_ID = process.env.TELEGRAM_CHAT_ID;
const SESSION_TTL_MS = 1000 * 60 * 60 * 6; // 6h — keeps reply routing alive after disconnects
const MAX_MSG_LEN = 4000;
const MAX_NAME_LEN = 40;
const VISIT_DEDUP_MS = 1000 * 60 * 60; // 1h dedup window per IP
const EVENT_DEDUP_MS = 1000 * 60 * 15;  // 15m dedup per (ip, event)
const SITE_NAME = process.env.SITE_NAME || 'arunjitk.info';

if (!TG_TOKEN || !TG_CHAT_ID) {
  console.error('FATAL: TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set.');
  process.exit(1);
}

const bot = new TelegramBot(TG_TOKEN, { polling: true });

bot.on('polling_error', (err) => console.error('[tg-poll]', err.code || err.message));

// ---- state ----------------------------------------------------------------
// session_id -> { name, ws, lastSeen, createdAt }
const sessions = new Map();
// telegram_message_id -> session_id
const tgMsgToSession = new Map();
// ip -> lastNotifiedTs (visit notification dedup)
const visitDedup = new Map();
// `${event}|${ip}` -> lastNotifiedTs (event dedup)
const eventDedup = new Map();

// periodic cleanup of stale sessions / map entries
setInterval(() => {
  const now = Date.now();
  for (const [sid, s] of sessions) {
    const dead = !s.ws || s.ws.readyState > 1; // CLOSING/CLOSED
    if (dead && now - s.lastSeen > SESSION_TTL_MS) sessions.delete(sid);
  }
  // tgMsgToSession entries linger so operator can still reply after a
  // brief disconnect, but we prune anything whose session is fully gone
  // and stale.
  if (tgMsgToSession.size > 5000) {
    // hard cap — drop oldest half
    const entries = [...tgMsgToSession.entries()];
    entries.slice(0, entries.length / 2).forEach(([k]) => tgMsgToSession.delete(k));
  }
  // expire visit-dedup entries past the window
  for (const [ip, ts] of visitDedup) {
    if (now - ts > VISIT_DEDUP_MS) visitDedup.delete(ip);
  }
  for (const [k, ts] of eventDedup) {
    if (now - ts > EVENT_DEDUP_MS) eventDedup.delete(k);
  }
}, 60_000);

// ---- helpers --------------------------------------------------------------
const esc = (s) =>
  String(s).replace(/[<>&]/g, (c) => ({ '<': '&lt;', '>': '&gt;', '&': '&amp;' }[c]));

const safe = (s, n) => String(s ?? '').slice(0, n).trim();

async function tgSend(html, replyMarkup) {
  return bot.sendMessage(TG_CHAT_ID, html, {
    parse_mode: 'HTML',
    disable_web_page_preview: true,
    ...(replyMarkup ? { reply_markup: replyMarkup } : {}),
  });
}

function wsSend(ws, payload) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(payload));
}

// Extract client IP from request, honoring X-Forwarded-For from nginx.
function clientIp(req) {
  const xff = req.headers['x-forwarded-for'];
  if (xff) return String(xff).split(',')[0].trim();
  return (req.socket?.remoteAddress || '?').replace(/^::ffff:/, '');
}

// Free, no-auth geo lookup (ip-api.com — 45 req/min limit).
async function geoLookup(ip) {
  if (!ip || ip === '?' || ip.startsWith('127.') || ip.startsWith('10.') ||
      ip.startsWith('192.168.') || ip === '::1') {
    return { status: 'private' };
  }
  try {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), 4000);
    const r = await fetch(
      `http://ip-api.com/json/${encodeURIComponent(ip)}?fields=status,country,regionName,city,isp,org,query`,
      { signal: ctrl.signal }
    );
    clearTimeout(t);
    if (!r.ok) return { status: 'fail' };
    return await r.json();
  } catch (e) {
    return { status: 'fail', error: e.message };
  }
}

// ---- express + ws ---------------------------------------------------------
const app = express();
app.set('trust proxy', true);
app.use(express.json({ limit: '32kb' }));

app.get('/api/health', (_req, res) => {
  res.json({
    ok: true,
    sessions: sessions.size,
    routedMessages: tgMsgToSession.size,
    uptime: process.uptime(),
  });
});

// Visitor notification — called by the frontend once per browser session.
// We dedup by IP for 1h server-side to avoid spam from reloads or shared NAT.
app.post('/api/visit', async (req, res) => {
  const ip = clientIp(req);
  const ua = safe(req.headers['user-agent'], 400) || 'unknown';
  const ref = safe(req.headers['referer'] || req.body?.ref, 200) || '';
  const path = safe(req.body?.path, 200) || '/';
  const tz   = safe(req.body?.tz, 60) || '';
  const lang = safe(req.body?.lang, 60) || '';
  const screen = safe(req.body?.screen, 30) || '';

  // dedup
  const now = Date.now();
  const last = visitDedup.get(ip) || 0;
  if (now - last < VISIT_DEDUP_MS) {
    return res.json({ ok: true, deduped: true });
  }
  visitDedup.set(ip, now);

  // fire-and-forget — don't block the response
  res.json({ ok: true });

  const geo = await geoLookup(ip);
  const location =
    geo.status === 'success'
      ? `${geo.city || '?'}, ${geo.regionName || '?'}, ${geo.country || '?'}`
      : (geo.status === 'private' ? 'private network' : 'unknown');
  const isp = geo.status === 'success' ? (geo.isp || geo.org || '') : '';

  const html =
    `👀 <b>A User Accessed ${esc(SITE_NAME)}</b>\n\n` +
    `<b>IP:</b> <code>${esc(ip)}</code>\n` +
    `<b>Location:</b> ${esc(location)}\n` +
    (isp ? `<b>ISP:</b> ${esc(isp)}\n` : '') +
    `<b>Path:</b> <code>${esc(path)}</code>\n` +
    (ref ? `<b>Referrer:</b> ${esc(ref)}\n` : '') +
    (tz ? `<b>Timezone:</b> ${esc(tz)}\n` : '') +
    (lang ? `<b>Language:</b> ${esc(lang)}\n` : '') +
    (screen ? `<b>Screen:</b> ${esc(screen)}\n` : '') +
    `\n<b>User-Agent:</b>\n<code>${esc(ua)}</code>`;

  try {
    await tgSend(html);
  } catch (e) {
    console.error('[tg-visit]', e.message);
  }
});

// Generic event notification — e.g. visitor clicked a section/card.
// Body: { event: 'portfolio_click', label?: 'Portfolio', target?: 'https://...' }
const EVENT_MESSAGES = {
  portfolio_click: 'Visitor accessed the portfolio section',
  app_click:       'Visitor accessed an app card',
  social_click:    'Visitor accessed a social link',
};

app.post('/api/event', async (req, res) => {
  const ip    = clientIp(req);
  const ua    = safe(req.headers['user-agent'], 400) || 'unknown';
  const event = safe(req.body?.event, 40);
  const label = safe(req.body?.label, 80);
  const target = safe(req.body?.target, 300);

  if (!event) return res.status(400).json({ ok: false, error: 'missing event' });

  // dedup per (event, ip)
  const key = `${event}|${ip}`;
  const now = Date.now();
  const last = eventDedup.get(key) || 0;
  if (now - last < EVENT_DEDUP_MS) {
    return res.json({ ok: true, deduped: true });
  }
  eventDedup.set(key, now);

  res.json({ ok: true });

  const headline = EVENT_MESSAGES[event] || `Visitor triggered "${event}"`;
  const geo = await geoLookup(ip);
  const location =
    geo.status === 'success'
      ? `${geo.city || '?'}, ${geo.regionName || '?'}, ${geo.country || '?'}`
      : (geo.status === 'private' ? 'private network' : 'unknown');

  const html =
    `📂 <b>${esc(headline)} from ${esc(SITE_NAME)}</b>\n\n` +
    (label  ? `<b>Item:</b> ${esc(label)}\n` : '') +
    (target ? `<b>Target:</b> ${esc(target)}\n` : '') +
    `<b>IP:</b> <code>${esc(ip)}</code>\n` +
    `<b>Location:</b> ${esc(location)}\n` +
    `\n<b>User-Agent:</b>\n<code>${esc(ua)}</code>`;

  try {
    await tgSend(html);
  } catch (e) {
    console.error('[tg-event]', e.message);
  }
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws, req) => {
  const ip = (req.headers['x-forwarded-for']?.split(',')[0] || req.socket.remoteAddress || '?').trim();
  let sessionId = null;

  const heartbeat = setInterval(() => {
    if (ws.readyState === 1) ws.ping();
  }, 25_000);

  ws.on('pong', () => {
    if (sessionId) {
      const s = sessions.get(sessionId);
      if (s) s.lastSeen = Date.now();
    }
  });

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    if (msg.type === 'init') {
      sessionId = randomUUID();
      const name = safe(msg.name, MAX_NAME_LEN) || 'Anonymous';
      sessions.set(sessionId, { name, ws, lastSeen: Date.now(), createdAt: Date.now() });

      wsSend(ws, { type: 'ready', sessionId });

      const html =
        `🟢 <b>New chat</b>\n` +
        `<b>From:</b> ${esc(name)}\n` +
        `<b>IP:</b> <code>${esc(ip)}</code>\n` +
        `<b>ID:</b> <code>${sessionId.slice(0, 8)}</code>\n` +
        `\n<i>Reply to this message to respond.</i>`;
      try {
        const sent = await tgSend(html);
        tgMsgToSession.set(sent.message_id, sessionId);
      } catch (e) {
        console.error('[tg-init]', e.message);
      }
      return;
    }

    if (msg.type === 'msg' && sessionId) {
      const s = sessions.get(sessionId);
      if (!s) return;
      const text = safe(msg.text, MAX_MSG_LEN);
      if (!text) return;
      s.lastSeen = Date.now();

      const html =
        `💬 <b>${esc(s.name)}</b>  <code>${sessionId.slice(0, 8)}</code>\n\n${esc(text)}`;
      try {
        const sent = await tgSend(html);
        tgMsgToSession.set(sent.message_id, sessionId);
        wsSend(ws, { type: 'ack', id: msg.id ?? null });
      } catch (e) {
        console.error('[tg-msg]', e.message);
        wsSend(ws, { type: 'error', message: 'Could not deliver message.' });
      }
      return;
    }
  });

  ws.on('close', () => {
    clearInterval(heartbeat);
    if (sessionId) {
      const s = sessions.get(sessionId);
      if (s) {
        s.ws = null; // keep session alive for reply routing window
        s.lastSeen = Date.now();
      }
    }
  });
});

// ---- Telegram -> Browser --------------------------------------------------
bot.on('message', (msg) => {
  if (String(msg.chat.id) !== String(TG_CHAT_ID)) return;
  if (!msg.reply_to_message) return;
  if (!msg.text) return;

  const sid = tgMsgToSession.get(msg.reply_to_message.message_id);
  if (!sid) return;
  const s = sessions.get(sid);
  if (!s) return;

  // Reply back to operator with a ✓/✗ status by editing nothing — instead
  // we tag the operator's own message indirectly through a brief reaction.
  const ok = s.ws && s.ws.readyState === 1;
  wsSend(s.ws, { type: 'reply', text: msg.text, ts: Date.now() });

  if (!ok) {
    bot.sendMessage(TG_CHAT_ID, '⚠️ Visitor is offline — message not delivered.', {
      reply_to_message_id: msg.message_id,
    }).catch(() => {});
  }
});

// ---- start ----------------------------------------------------------------
server.listen(PORT, () => {
  console.log(`[relay] listening on :${PORT}`);
});

process.on('SIGTERM', () => {
  console.log('[relay] SIGTERM, shutting down...');
  bot.stopPolling().finally(() => server.close(() => process.exit(0)));
});
