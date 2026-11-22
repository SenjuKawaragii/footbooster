import { createClient } from "redis";

const REDIS_URL =
  process.env.FOOTBOOSTER_REDIS_URL ||
  process.env.REDIS_URL ||
  process.env.KV_URL;

let clientPromise = null;

async function getRedisClient() {
  if (!REDIS_URL) {
    throw new Error("Variable d'environnement Redis introuvable.");
  }
  if (!clientPromise) {
    const client = createClient({ url: REDIS_URL });
    client.on("error", (err) => console.error("Redis Client Error:", err));
    clientPromise = client.connect().then(() => client);
  }
  return clientPromise;
}

export default async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") {
    return res.status(200).end();
  }

  if (req.query && req.query.ping) {
    return res.status(200).json({ ok: true, service: "footbooster-vercel-redis" });
  }

  try {
    const client = await getRedisClient();
    const key =
      (req.query && req.query.key) ||
      (req.body && req.body.key) ||
      "footbooster:global_state";

    if (req.method === "GET") {
      const raw = await client.get(key);
      return res.status(200).json({
        ok: true,
        data: raw ? JSON.parse(raw) : null,
      });
    }

    if (req.method === "POST" || req.method === "PUT") {
      const body =
        typeof req.body === "string" ? JSON.parse(req.body || "{}") : req.body || {};
      const payload = body.data !== undefined ? body.data : body;
      await client.set(key, JSON.stringify(payload));
      return res.status(200).json({ ok: true });
    }

    return res.status(405).json({ ok: false, error: "Method not allowed" });
  } catch (err) {
    console.error("API Error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
}
