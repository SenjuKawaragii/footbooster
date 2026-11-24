'use strict';
/*
  Foot Boosters — route /api pour Vercel.
  Les comptes et les sauvegardes sont stockés dans la base Redis liée au projet Vercel.

  Variable d'environnement lue (ajoutée par Vercel quand tu relies la base) :
    - FOOTBOOSTER_REDIS_URL  (redis:// ou rediss://) — c'est celle de ta base "footbooster"
  À défaut : REDIS_URL / KV_URL, ou l'API REST (KV_REST_API_URL + KV_REST_API_TOKEN).
  Aucun paquet npm n'est nécessaire.
*/
const crypto = require('crypto');
const net = require('net');
const tls = require('tls');

/* ------------------------------------------------- Configuration Redis */
const isTcp = u => /^rediss?:\/\//i.test(u || '');
const REST_URL = (process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL || '').replace(/\/+$/, '');
const REST_TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN || '';
const HAS_REST = !!(REST_URL && REST_TOKEN);
// Priorité : FOOTBOOSTER_REDIS_URL (ta base liée sur Vercel), puis l'API REST, puis REDIS_URL / KV_URL
let TCP_URL = isTcp(process.env.FOOTBOOSTER_REDIS_URL) ? process.env.FOOTBOOSTER_REDIS_URL : '';
if (!TCP_URL && !HAS_REST) {
  TCP_URL = [process.env.REDIS_URL, process.env.KV_URL].find(isTcp)
    || process.env[Object.keys(process.env).find(k => /REDIS_URL$/.test(k) && isTcp(process.env[k])) || ''] || '';
}

/* ---------------------------------------- Mini client Redis (sans dépendance) */
function enc(args) {
  const parts = [Buffer.from('*' + args.length + '\r\n')];
  for (const a of args) {
    const s = Buffer.from(String(a), 'utf8');
    parts.push(Buffer.from('$' + s.length + '\r\n'), s, Buffer.from('\r\n'));
  }
  return Buffer.concat(parts);
}
/* Lit une réponse RESP dans b à partir de o. Renvoie [valeur, suite] ou null si la réponse est incomplète. */
function parseReply(b, o) {
  if (o >= b.length) return null;
  const t = b[o], e = b.indexOf('\r\n', o, 'latin1');
  if (e < 0) return null;
  const line = b.toString('utf8', o + 1, e);
  switch (t) {
    case 43: return [line, e + 2];                 // +OK
    case 45: return [new Error(line), e + 2];      // -ERR ...
    case 58: return [Number(line), e + 2];         // :123
    case 36: {                                     // $bulk
      const n = Number(line);
      if (n < 0) return [null, e + 2];
      const end = e + 2 + n;
      if (b.length < end + 2) return null;
      return [b.toString('utf8', e + 2, end), end + 2];
    }
    case 42: {                                     // *tableau
      const n = Number(line);
      if (n < 0) return [null, e + 2];
      const arr = []; let p = e + 2;
      for (let i = 0; i < n; i++) { const r = parseReply(b, p); if (!r) return null; arr.push(r[0]); p = r[1]; }
      return [arr, p];
    }
    default: throw new Error('RESP invalide');
  }
}
function drain(c) {
  try {
    while (c.buf.length) {
      if (!c.q.length) { c.buf = Buffer.alloc(0); return; }
      const r = parseReply(c.buf, 0);
      if (!r) return;
      c.buf = c.buf.subarray(r[1]);
      const w = c.q.shift();
      if (r[0] instanceof Error) w.reject(r[0]); else w.resolve(r[0]);
    }
  } catch (e) { c.kill(e); }
}
function execMany(c, cmds) {
  const ps = cmds.map(() => new Promise((resolve, reject) => c.q.push({ resolve, reject })));
  c.sock.write(Buffer.concat(cmds.map(enc)));
  return ps;
}
let conn = null;
function openConn() {
  const u = new URL(TCP_URL), secure = u.protocol === 'rediss:';
  const c = { sock: null, buf: Buffer.alloc(0), q: [], dead: false };
  const opts = { host: u.hostname, port: Number(u.port) || 6379 };
  if (secure) opts.servername = u.hostname;
  c.kill = err => {
    if (c.dead) return;
    c.dead = true; if (conn === c) conn = null;
    const e = err || new Error('connexion Redis fermée'); e.conn = true;
    c.q.splice(0).forEach(w => w.reject(e));
    try { c.sock.destroy(); } catch (_) { /* déjà fermé */ }
  };
  c.sock = (secure ? tls : net).connect(opts);
  c.sock.setNoDelay(true);
  c.sock.setKeepAlive(true, 30000);
  c.sock.unref();
  c.sock.on('data', d => { c.buf = c.buf.length ? Buffer.concat([c.buf, d]) : d; drain(c); });
  c.sock.on('error', e => c.kill(e));
  c.sock.on('close', () => c.kill());
  c.init = (async () => {
    await new Promise((res, rej) => { c.sock.once(secure ? 'secureConnect' : 'connect', res); c.sock.once('error', rej); });
    const user = decodeURIComponent(u.username || ''), pass = decodeURIComponent(u.password || '');
    const db = parseInt((u.pathname || '').slice(1), 10);
    if (pass) await Promise.all(execMany(c, [user && user !== 'default' ? ['AUTH', user, pass] : ['AUTH', pass]]));
    if (db > 0) await Promise.all(execMany(c, [['SELECT', db]]));
  })();
  c.init.catch(e => c.kill(e));
  return c;
}
async function tcpRun(cmds, retry) {
  let c;
  try {
    if (!conn || conn.dead) conn = openConn();
    c = conn;
    await c.init;
    const timer = setTimeout(() => c.kill(new Error('Redis ne répond pas')), 10000);
    try { return await Promise.all(execMany(c, cmds)); } finally { clearTimeout(timer); }
  } catch (e) {
    if (e && e.conn && !retry) return tcpRun(cmds, true); // connexion périmée (instance endormie) : on réessaie une fois
    throw e;
  }
}

/* Exécute une liste de commandes (tableaux) et renvoie la liste des résultats. */
async function run(cmds) {
  if (!cmds.length) return [];
  if (TCP_URL) return tcpRun(cmds);
  if (HAS_REST) {
    const out = [];
    let batch = [], size = 0;
    const flush = async () => {
      if (!batch.length) return;
      const one = batch.length === 1;
      const r = await fetch(REST_URL + (one ? '' : '/pipeline'), {
        method: 'POST',
        headers: { Authorization: 'Bearer ' + REST_TOKEN, 'Content-Type': 'application/json' },
        body: JSON.stringify(one ? batch[0] : batch)
      });
      const j = await r.json().catch(() => null);
      if (!r.ok) throw new Error((j && j.error) || 'redis ' + r.status);
      if (one) { if (j.error) throw new Error(j.error); out.push(j.result); }
      else j.forEach(x => { if (x.error) throw new Error(x.error); out.push(x.result); });
      batch = []; size = 0;
    };
    for (const c of cmds) {
      const s = JSON.stringify(c).length;
      if (batch.length && size + s > 800000) await flush(); // limite de taille d'une requête REST
      batch.push(c); size += s;
    }
    await flush();
    return out;
  }
  const e = new Error('no_redis'); e.code = 'no_redis'; throw e;
}
const cmd = async (...a) => (await run([a]))[0];
const flatToObj = f => { const o = {}; for (let i = 0; i + 1 < (f || []).length; i += 2) o[f[i]] = f[i + 1]; return o; };

/* ---------------------------------------------------------------- Helpers */
const SEG = /^[A-Za-z0-9_.:~@+-]{1,200}$/;
const SHARED = new Set(['cards', 'clubs', 'tomb']);                 // catalogue partagé : tout joueur connecté peut écrire
const PERUSER = new Set(['pub', 'pcol', 'fr', 'mk', 'out', 'rp']); // un document par joueur : lui seul écrit
const isObj = v => v && typeof v === 'object' && !Array.isArray(v);

function splitPath(path) {
  if (typeof path !== 'string' || path.length > 500) return null;
  const p = path.split('/');
  if (!p.every(s => SEG.test(s))) return null;
  const id = p.pop(), coll = p.join('/');
  return coll && id ? { coll, id } : null;
}
function ownColl(coll, uid) { const m = coll.split('/'); return m.length === 3 && m[0] === 'data' && m[1] === 'users' && m[2] === uid; }
const canRead = (coll, uid) => SHARED.has(coll) || PERUSER.has(coll) || ownColl(coll, uid);
const canWrite = (coll, id, uid) => SHARED.has(coll) || (PERUSER.has(coll) && id === uid) || ownColl(coll, uid);

function deepMerge(a, b) {
  const o = Object.assign({}, a);
  for (const k of Object.keys(b)) {
    if (k === '__proto__' || k === 'constructor' || k === 'prototype') continue;
    if (b[k] === null) delete o[k];
    else o[k] = isObj(o[k]) && isObj(b[k]) ? deepMerge(o[k], b[k]) : b[k];
  }
  return o;
}
const accKey = n => String(n || '').normalize('NFKC').toLowerCase().replace(/\s+/g, ' ').trim();
const SECRET = process.env.FOOTBOOSTER_SECRET || TCP_URL || REST_TOKEN || 'foot-boosters';
const sign = uid => crypto.createHmac('sha256', SECRET).update('tok:' + uid).digest('hex').slice(0, 32);
const newEtag = len => crypto.randomBytes(4).toString('hex') + '.' + len;

function fail(status, code, message) { const e = new Error(message || code); e.status = status; e.code = code; return e; }

function scrypt(pass, salt) {
  return new Promise((res, rej) => crypto.scrypt(pass, salt, 32, (e, k) => (e ? rej(e) : res(k))));
}

/* Écritures : les documents d'abord, puis (en dernier) le compteur de version. */
async function writeDocs(writes) {
  const cmds = [], etags = [];
  for (const w of writes) {
    if (w.data === null || w.data === undefined) {
      cmds.push(['HDEL', 'fb:d:' + w.coll, w.id], ['HDEL', 'fb:e:' + w.coll, w.id]);
      etags.push(null);
    } else {
      const json = JSON.stringify(w.data), et = newEtag(json.length);
      cmds.push(['HSET', 'fb:d:' + w.coll, w.id, json], ['HSET', 'fb:e:' + w.coll, w.id, et]);
      etags.push(et);
    }
  }
  cmds.push(['INCR', 'fb:v']);
  await run(cmds);
  return etags; // renvoyés au site pour qu'il ne retélécharge pas ses propres écritures
}

/* ------------------------------------------------------------------- Ops */
async function opEnter(body, ip) {
  const key = accKey(body.name), pass = typeof body.pass === 'string' ? body.pass : '';
  if (key.length < 2 || key.length > 20) throw fail(400, 'bad_request', 'Le pseudo doit faire entre 2 et 20 caractères.');
  if (pass.length < 4 || pass.length > 200) throw fail(400, 'bad_request', 'Le mot de passe doit faire au moins 4 caractères.');
  const rl = 'fb:rl:' + key;
  const tries = Number(await cmd('GET', rl)) || 0;
  if (tries >= 12) throw fail(429, 'rate_limited', 'Trop d’essais : réessaie dans quelques minutes.');
  let acc = null;
  const raw = await cmd('HGET', 'fb:acc', key);
  if (raw) {
    acc = JSON.parse(raw);
    const h = await scrypt(pass, Buffer.from(acc.salt, 'hex'));
    const ok = crypto.timingSafeEqual(h, Buffer.from(acc.hash, 'hex'));
    if (!ok) {
      const n = await cmd('INCR', rl);
      if (n === 1) await cmd('EXPIRE', rl, 900);
      throw fail(401, 'wrong_password', 'Mot de passe incorrect.');
    }
    try { await cmd('DEL', rl); } catch (e) { /* sans importance */ }
  } else {
    const salt = crypto.randomBytes(16);
    const hash = await scrypt(pass, salt);
    const name = String(body.name).normalize('NFKC').replace(/\s+/g, ' ').trim().slice(0, 20);
    acc = { uid: 'u' + crypto.randomBytes(6).toString('hex'), name, salt: salt.toString('hex'), hash: hash.toString('hex'), t: Date.now() };
    const made = await cmd('HSETNX', 'fb:acc', key, JSON.stringify(acc));
    if (!made) return opEnter(body, ip); // quelqu'un l'a créé juste avant : on se reconnecte
    await run([['HSET', 'fb:uname', acc.uid, acc.name], ['HSET', 'fb:ulogin', acc.uid, acc.name], ['HSETNX', 'fb:dn', key, acc.uid], ['SETNX', 'fb:owner', acc.uid]]);
  }
  // écritures annexes « au mieux » : une base pleine ne doit pas empêcher de se connecter
  try { await run([['HSETNX', 'fb:ulogin', acc.uid, acc.name], ['HSETNX', 'fb:dn', key, acc.uid], ['HSETNX', 'fb:uname', acc.uid, acc.name]]); } catch (e) { /* sans importance */ }
  return { tok: 'fbt_' + acc.uid + '_' + sign(acc.uid), uid: acc.uid, name: acc.name };
}

async function auth(token) {
  if (!token || typeof token !== 'string' || token.length > 120) throw fail(401, 'unauthorized', 'Connexion requise.');
  const m = /^fbt_(u[0-9a-f]{12})_([0-9a-f]{32})$/.exec(token);
  if (m) { // jeton signé : aucune lecture ni écriture dans Redis
    const a = Buffer.from(sign(m[1])), b = Buffer.from(m[2]);
    if (a.length === b.length && crypto.timingSafeEqual(a, b)) return m[1];
    throw fail(401, 'unauthorized', 'Session invalide : reconnecte-toi.');
  }
  if (!/^fbt_[0-9a-f]+$/.test(token)) throw fail(401, 'unauthorized', 'Connexion requise.');
  const uid = await cmd('HGET', 'fb:tok', token); // anciens jetons (avant la mise à jour)
  if (!uid) throw fail(401, 'unauthorized', 'Session expirée : reconnecte-toi.');
  return uid;
}

async function opPoll(body, uid) {
  const subs = isObj(body.subs) ? body.subs : {};
  const paths = Object.keys(subs).slice(0, 12);
  const ver = Number(await cmd('GET', 'fb:v')) || 0;
  const allStarted = paths.every(p => isObj(subs[p]));
  if (!body.force && allStarted && body.ver === ver) return { same: true, ver };

  // les petites collections (joueurs, amis…) d'abord : elles arrivent tout de suite, les lourdes ensuite
  const heavy = p => (p === 'cards' || p.startsWith('data/users/') ? 1 : 0);
  const ok = paths.filter(p => canRead(p, uid)).sort((a, b) => heavy(a) - heavy(b));
  const etagRes = await run(ok.map(p => ['HGETALL', 'fb:e:' + p]));
  const BUDGET = 1800000;           // taille max d'une réponse (limite Vercel : 4,5 Mo ; plus petit = plus rapide)
  let used = 0, more = false;
  const plan = {}, getCmds = [], getPaths = [];
  ok.forEach((p, i) => {
    const et = flatToObj(etagRes[i]);
    const want = isObj(subs[p]) ? subs[p] : {};
    const need = [], del = [];
    Object.keys(et).forEach(id => { if (want[id] !== et[id]) need.push(id); });
    Object.keys(want).forEach(id => { if (!(id in et)) del.push(id); });
    const take = [];
    let truncated = false;
    for (const id of need) {
      const sz = parseInt(String(et[id]).split('.')[1], 10) || 1000;
      if (used > 0 && used + sz > BUDGET) { more = true; truncated = true; break; }
      used += sz; take.push(id);
    }
    plan[p] = { et, take, del, more: truncated };
    if (take.length) { getCmds.push(['HMGET', 'fb:d:' + p].concat(take)); getPaths.push(p); }
  });
  const got = await run(getCmds);
  const cols = {}, orphans = [];
  paths.forEach(p => { cols[p] = { docs: {}, etags: {}, del: [] }; });
  ok.forEach(p => { cols[p].del = plan[p].del; cols[p].more = plan[p].more; });
  getPaths.forEach((p, i) => {
    plan[p].take.forEach((id, k) => {
      const v = got[i] && got[i][k];
      if (v) { try { cols[p].docs[id] = JSON.parse(v); cols[p].etags[id] = plan[p].et[id]; } catch (e) { /* document illisible : ignoré */ } }
      else orphans.push(['HDEL', 'fb:e:' + p, id]); // empreinte sans document : on la supprime
    });
  });
  if (orphans.length) { try { await run(orphans.concat([['INCR', 'fb:v']])); } catch (e) { /* sans importance */ } }
  return { cols, more, ver };
}

async function opWrite(op, body, uid) {
  if (op === 'batch') {
    const ws = Array.isArray(body.writes) ? body.writes.slice(0, 60) : null;
    if (!ws) throw fail(400, 'bad_request', 'Écritures manquantes.');
    const list = ws.map(w => {
      const sp = splitPath(w && w.path);
      if (!sp) throw fail(400, 'invalid_argument', 'Chemin invalide.');
      if (!canWrite(sp.coll, sp.id, uid)) throw fail(403, 'forbidden', 'Accès refusé.');
      if (w.data !== null && w.data !== undefined && !isObj(w.data)) throw fail(400, 'invalid_argument', 'Données invalides.');
      return { coll: sp.coll, id: sp.id, data: w.data === undefined ? null : w.data };
    });
    const etags = await writeDocs(list);
    return { ok: true, etags };
  }
  const sp = splitPath(body.path);
  if (!sp) throw fail(400, 'invalid_argument', 'Chemin invalide.');
  if (op === 'get') {
    if (!canRead(sp.coll, uid)) throw fail(403, 'forbidden', 'Accès refusé.');
    const v = await cmd('HGET', 'fb:d:' + sp.coll, sp.id);
    return { data: v ? JSON.parse(v) : null };
  }
  if (!canWrite(sp.coll, sp.id, uid)) throw fail(403, 'forbidden', 'Accès refusé.');
  if (op === 'del') { await writeDocs([{ coll: sp.coll, id: sp.id, data: null }]); return { ok: true }; }
  if (!isObj(body.data)) throw fail(400, 'invalid_argument', 'Données invalides.');
  if (op === 'set') { const [etag] = await writeDocs([{ coll: sp.coll, id: sp.id, data: body.data }]); return { ok: true, etag }; }
  if (op === 'update') {
    const v = await cmd('HGET', 'fb:d:' + sp.coll, sp.id);
    if (!v) throw fail(400, 'invalid_argument', 'Le document n’existe pas encore.');
    const merged = deepMerge(JSON.parse(v), body.data);
    const [etag] = await writeDocs([{ coll: sp.coll, id: sp.id, data: merged }]);
    return { data: merged, etag };
  }
  throw fail(400, 'bad_request', 'Opération inconnue.');
}

async function handle(op, body, token, ip) {
  if (op === 'enter') return opEnter(body, ip);
  const uid = await auth(token);
  switch (op) {
    case 'me': {
      const [name, login, owner] = await run([['HGET', 'fb:uname', uid], ['HGET', 'fb:ulogin', uid], ['GET', 'fb:owner']]);
      return { uid, name: String(name || login || 'Joueur'), login: String(login || name || 'Joueur'), owner: owner === uid };
    }
    case 'rename': {
      const nm = String(body.name || '').normalize('NFKC').replace(/\s+/g, ' ').trim();
      if (nm.length < 2 || nm.length > 20) throw fail(400, 'bad_request', 'Le pseudo doit faire entre 2 et 20 caractères.');
      const key = accKey(nm);
      if (key === 'legit') throw fail(400, 'bad_request', 'Ce pseudo est réservé.');
      const acc = await cmd('HGET', 'fb:acc', key);
      if (acc && JSON.parse(acc).uid !== uid) throw fail(409, 'name_taken', 'Ce pseudo est déjà pris.');
      const claimed = await cmd('HSETNX', 'fb:dn', key, uid);
      if (!claimed && (await cmd('HGET', 'fb:dn', key)) !== uid) throw fail(409, 'name_taken', 'Ce pseudo est déjà pris.');
      const old = await cmd('HGET', 'fb:uname', uid);
      await cmd('HSET', 'fb:uname', uid, nm);
      if (old && accKey(old) !== key && (await cmd('HGET', 'fb:dn', accKey(old))) === uid) await cmd('HDEL', 'fb:dn', accKey(old));
      return { name: nm };
    }
    case 'names': {
      const ids = (Array.isArray(body.ids) ? body.ids : []).filter(i => typeof i === 'string' && SEG.test(i)).slice(0, 100);
      const names = {};
      if (ids.length) { const r = await cmd('HMGET', 'fb:uname', ...ids); ids.forEach((id, i) => { if (r && r[i]) names[id] = r[i]; }); }
      return { names };
    }
    case 'poll': return opPoll(body, uid);
    case 'purge': { // supprime les anciennes copies des cartes du site (identiques à celles du fichier du site)
      const items = isObj(body.items) ? body.items : {};
      const ids = Object.keys(items).filter(i => SEG.test(i)).slice(0, 150);
      if (!ids.length) return { deleted: 0 };
      const docs = await cmd('HMGET', 'fb:d:cards', ...ids);
      const del = [];
      ids.forEach((id, i) => {
        if (!docs || !docs[i]) return;
        try { const d = JSON.parse(docs[i]); if ([d.name, d.team, d.rating].join('|') === String(items[id])) del.push(id); } catch (e) { /* illisible : on garde */ }
      });
      if (del.length) await writeDocs(del.map(id => ({ coll: 'cards', id, data: null })));
      return { deleted: del.length };
    }
    default: return opWrite(op, body, uid);
  }
}

/* --------------------------------------------------------------- Handler */
function send(res, status, obj) {
  obj.now = Date.now();
  res.statusCode = status;
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store');
  res.end(JSON.stringify(obj));
}
async function readBody(req) {
  if (req.body !== undefined && req.body !== null) {
    if (typeof req.body === 'string') return JSON.parse(req.body || '{}');
    if (Buffer.isBuffer(req.body)) return JSON.parse(req.body.toString('utf8') || '{}');
    return req.body;
  }
  const chunks = [];
  for await (const c of req) chunks.push(c);
  const s = Buffer.concat(chunks).toString('utf8');
  return s ? JSON.parse(s) : {};
}
function mapError(e) {
  if (e && e.status) return { status: e.status, code: e.code, message: e.message };
  if (e && e.code === 'no_redis') return { status: 503, code: 'no_redis', message: 'Base Redis non reliée au projet Vercel.' };
  const m = String((e && e.message) || '');
  if (/size/i.test(m)) return { status: 413, code: 'resource_exhausted', message: 'Données trop volumineuses.' };
  if (/limit|quota|exceed|OOM|maxmemory/i.test(m)) return { status: 507, code: 'quota_exceeded', message: 'Limite de la base atteinte.' };
  if (e instanceof SyntaxError) return { status: 400, code: 'bad_request', message: 'Requête illisible.' };
  console.error('[api] erreur Redis :', m);
  return { status: 503, code: 'unavailable', message: 'Base de données injoignable pour le moment.' };
}

module.exports = async function handler(req, res) {
  try {
    if (req.method === 'OPTIONS') { res.statusCode = 204; return res.end(); }
    const url = new URL(req.url, 'http://x');
    if (req.method === 'GET') {
      if (url.searchParams.get('ping')) {
        if (!HAS_REST && !TCP_URL) return send(res, 200, { ok: false, code: 'no_redis' });
        try {
          await cmd('PING');
          const info = { ok: true };
          try { // diagnostic : nombre de cartes sur le serveur et mémoire utilisée
            const [n, mem] = await run([['HLEN', 'fb:d:cards'], ['INFO', 'memory']]);
            info.cards = Number(n) || 0;
            const g = k => { const m = String(mem || '').match(new RegExp(k + ':(\\d+)')); return m ? Number(m[1]) : 0; };
            info.usedMB = +(g('used_memory') / 1048576).toFixed(1);
            if (g('maxmemory')) info.maxMB = +(g('maxmemory') / 1048576).toFixed(1);
          } catch (e) { /* INFO peut être désactivé : sans importance */ }
          return send(res, 200, info);
        }
        catch (e) { console.error('[api] ping Redis :', e && e.message); return send(res, 200, { ok: false, code: 'unavailable' }); }
      }
      return send(res, 405, { code: 'method_not_allowed' });
    }
    if (req.method !== 'POST') return send(res, 405, { code: 'method_not_allowed' });
    const body = await readBody(req);
    const op = body && body.op;
    if (typeof op !== 'string') return send(res, 400, { code: 'bad_request', message: 'Opération manquante.' });
    const ip = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim();
    const out = await handle(op, body, req.headers['x-token'], ip);
    return send(res, 200, out);
  } catch (e) {
    const m = mapError(e);
    return send(res, m.status, { code: m.code, message: m.message });
  }
};
