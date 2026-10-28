import fs from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";
import { TriggerStore } from "./trigger-store.mjs";
import { migrateLegacy } from "./trigger-migration.mjs";
import { startEventReceiver } from "./event-receiver.mjs";
import { reconcileRuns } from "./trigger-reconcile.mjs";
import { TriggerIntake } from "./trigger-intake.mjs";

export async function startTriggerDaemon({ directory, port = 7341 }) {
  const version = JSON.parse(await fs.readFile(new URL("../package.json", import.meta.url), "utf8")).version;
  const store = new TriggerStore(directory);
  const migration = await migrateLegacy(store);
  const credentialFile = path.join(directory, "control.token");
  let token;
  try {
    const stat = await fs.lstat(credentialFile);
    if (!stat.isFile() || stat.isSymbolicLink() || (stat.mode & 0o077) || (process.getuid && stat.uid !== process.getuid())) throw new Error("unsafe control credential");
    token = (await fs.readFile(credentialFile, "utf8")).trim();
  } catch (e) {
    if (e.code !== "ENOENT") { store.close(); throw e; }
    token = crypto.randomBytes(32).toString("base64url");
    await fs.writeFile(credentialFile, token + "\n", { flag: "wx", mode: 0o600 });
  }
  let lastError = null;
  const receiver = await startEventReceiver({ directory: path.join(directory, "events"), port, store: new TriggerIntake(store), backend: "triggers-v1", version,
    control: async (credential, input) => {
      if (typeof credential !== "string" || !crypto.timingSafeEqual(Buffer.from(crypto.createHash("sha256").update(credential).digest("hex")), Buffer.from(crypto.createHash("sha256").update(token).digest("hex")))) {
        const e = new Error("invalid service credential"); e.code = "unauthorized"; throw e;
      }
      const { action, binding, owner, ...params } = input;
      switch (action) {
        case "create": return store.create(binding, params);
        case "list": return { triggers: store.list(binding).map((t) => store.view(t)) };
        case "get": return { trigger: store.view(store.get(params.id, binding), true) };
        case "update": return store.update(binding, params);
        case "pause": case "resume": case "delete": return store.setState(binding, params.id, action);
        case "run": return { run: store.runNow(binding, params.id, params.idempotencyKey) };
        case "history": return { runs: store.history(binding, params.id, params.limit) };
        case "revisions": return { revisions: store.versions(binding, params.id) };
        case "reconcile": return reconcileRuns(store, binding, params.id, params.runId);
        case "register": await migrateLegacy(store); return store.register(binding, owner, params.pid);
        case "heartbeat": store.requireOwner(binding, owner); return { ok: true };
        case "claim": return { run: store.claim(binding, owner, params) };
        case "ack": return { run: store.acknowledge(binding, owner, params.id, params.state, params.metadata) };
        case "release": return store.release(binding, owner, params.reason);
        case "status": return { version, backend: "sqlite-wal", schemaVersion: 1, migration, lastError, capabilities: ["webhook", "schedule", "manual", "prompt_revisions", "offline_queue", "per_chat_serialization"] };
        default: { const e = new Error("unknown trigger action"); e.code = "invalid_action"; throw e; }
      }
    } });
  let ticks = 0, migrating = false;
  const timer = setInterval(() => {
    try { store.tick(); lastError = null; } catch (e) { lastError = { code: e.code || "tick_failed", message: e.message }; }
    if (++ticks % 120 === 0 && !migrating) {
      migrating = true; migrateLegacy(store).catch((e) => { lastError = { code: "migration_failed", message: e.message }; }).finally(() => { migrating = false; });
    }
  }, 250);
  return { ...receiver, store, close: async () => { clearInterval(timer); await receiver.close(); store.close(); } };
}
