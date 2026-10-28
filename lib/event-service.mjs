import fs from "node:fs/promises";
import path from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { eventDirectory, readPrivateJson, withEventLock } from "./event-store.mjs";

export async function receiverStatus(directory = eventDirectory()) {
  try {
    const record = await readPrivateJson(path.join(directory, "receiver.json"));
    const url = new URL(record.baseUrl);
    if (!["127.0.0.1", "[::1]"].includes(url.hostname) || url.protocol !== "http:" || url.username || url.password) throw new Error("invalid receiver address");
    const response = await fetch(new URL("/health", url), { signal: AbortSignal.timeout(500), redirect: "error" });
    const live = await response.json();
    if (!response.ok || live.service !== "omp-event-receiver" || live.instanceId !== record.instanceId || live.pid !== record.pid) throw new Error("receiver identity mismatch");
    return { available: true, ...record, backend: live.backend || "legacy", version: live.version || null };
  } catch { return { available: false }; }
}

export async function ensureReceiver(directory = eventDirectory()) {
  return withEventLock(directory, "receiver-start", async () => {
    const previous = await receiverStatus(directory); if (previous.available) return previous;
    if (process.env.OMP_SESSION_BRIDGE_EVENT_AUTOSTART === "0") return { available: false, startCommand: "node bin/omp-events.mjs serve" };
    const log = await fs.open(path.join(directory, "receiver.log"), "a", 0o600);
    let spawnError;
    const child = spawn(process.env.OMP_SESSION_BRIDGE_NODE || "node", [fileURLToPath(new URL("../bin/omp-events.mjs", import.meta.url)), "serve"], {
      env: { ...process.env, OMP_SESSION_BRIDGE_EVENT_DIR: directory }, detached: true, stdio: ["ignore", log.fd, log.fd],
    });
    child.once("error", (error) => { spawnError = error; }); child.unref(); await log.close();
    for (let i = 0; i < 600; i++) {
      if (spawnError) throw spawnError;
      const current = await receiverStatus(directory); if (current.available) return current;
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    throw new Error(`event receiver failed to start; inspect ${path.join(directory, "receiver.log")}`);
  });
}
