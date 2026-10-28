import http from "node:http";
import crypto from "node:crypto";
import { EventStore, MAX_EVENT_BYTES, eventError } from "./event-store.mjs";

function respond(res, status, value) {
  res.writeHead(status, { "content-type": "application/json", "cache-control": "no-store", "x-content-type-options": "nosniff" }); res.end(JSON.stringify(value));
}
const statusFor = (code) => ({ invalid_event: 400, unauthorized: 401, subscription_not_found: 404, trigger_not_found: 404, subscription_paused: 409, event_conflict: 409, event_queue_full: 429, event_store_busy: 503, event_too_large: 413 })[code] || (code ? 400 : 500);

export async function startEventReceiver({ directory, port = 7341, host = "127.0.0.1", store: suppliedStore, control, backend = "legacy", version }) {
  if (!["127.0.0.1", "::1"].includes(host)) throw new Error("receiver binds loopback only; use an authenticated HTTPS reverse proxy for remote sources");
  const store = suppliedStore || new EventStore(directory); const instanceId = crypto.randomUUID();
  const server = http.createServer(async (req, res) => {
    try {
      if (req.method === "GET" && req.url === "/health") return respond(res, 200, { service: "omp-event-receiver", backend, version, instanceId, pid: process.pid });
      if (control && req.method === "POST" && req.url === "/v1/control") {
        if (!/^application\/json(?:;|$)/i.test(req.headers["content-type"] || "")) return respond(res, 415, { error: "content_type" });
        const credential = /^Bearer ([A-Za-z0-9_-]{43})$/.exec(req.headers.authorization || "")?.[1];
        if (!credential) return respond(res, 401, { error: "unauthorized" });
        let bytes = 0; const chunks = [];
        for await (const chunk of req) { bytes += chunk.length; if (bytes > 65_536) throw eventError("event_too_large", "control request too large"); chunks.push(chunk); }
        let input; try { input = JSON.parse(Buffer.concat(chunks).toString("utf8")); } catch { throw eventError("invalid_event", "invalid JSON"); }
        return respond(res, 200, await control(credential, input));
      }
      const match = /^\/v1\/events\/([a-f0-9-]{36})$/.exec(req.url || "");
      if (!match) return respond(res, 404, { error: "not_found" });
      if (req.method !== "POST") return respond(res, 405, { error: "method_not_allowed" });
      if (!/^application\/json(?:;|$)/i.test(req.headers["content-type"] || "")) return respond(res, 415, { error: "content_type", message: "send application/json" });
      // Authenticate before reading a body; do not disclose receipt contents to callers.
      const route = suppliedStore ? await store.get(match[1]) : await store.load(match[1]);
      const token = /^Bearer ([A-Za-z0-9_-]{43})$/.exec(req.headers.authorization || "")?.[1];
      const suppliedHash = crypto.createHash("sha256").update(token || "").digest("hex");
      if (!route.tokenHash || !crypto.timingSafeEqual(Buffer.from(suppliedHash), Buffer.from(route.tokenHash))) return respond(res, 401, { error: "unauthorized" });
      let bytes = 0; const chunks = [];
      for await (const chunk of req) { bytes += chunk.length; if (bytes > MAX_EVENT_BYTES) throw eventError("event_too_large", `event exceeds ${MAX_EVENT_BYTES} bytes`); chunks.push(chunk); }
      let body; try { body = JSON.parse(Buffer.concat(chunks).toString("utf8")); } catch { throw eventError("invalid_event", "body must be valid JSON"); }
      const receipt = await store.accept(match[1], token, body);
      respond(res, 202, receipt);
    } catch (error) {
      if (!res.headersSent && !res.destroyed) respond(res, statusFor(error.code), { error: error.code || "receiver_error", message: error.code ? error.message : "event receiver could not accept the request" });
    }
  });
  server.requestTimeout = 10_000; server.headersTimeout = 10_000; server.maxHeadersCount = 32;
  await new Promise((resolve, reject) => { server.once("error", reject); server.listen(port, host, resolve); });
  const address = server.address(); const baseUrl = `http://${host === "::1" ? "[::1]" : host}:${address.port}`;
  return { server, instanceId, baseUrl, close: () => new Promise((resolve) => { server.close(resolve); server.closeIdleConnections(); }) };
}
