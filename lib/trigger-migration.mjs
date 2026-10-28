import fs from "node:fs/promises";
import path from "node:path";
import { readPrivateJson, hash, withEventLock } from "./event-store.mjs";
import { processAlive, ensureSecureDirectory } from "./registry.mjs";
import { bindingKey } from "./schedule-store.mjs";

/** Import immutable snapshots once. A live v0.4 scheduler keeps its ownership. */
export async function migrateLegacy(store) {
  const root = store.directory;
  const eventDir = path.join(root, "events");
  const files = [];
  for (const dir of [root, eventDir]) {
    for (const name of await fs.readdir(dir).catch((e) => { if (e.code === "ENOENT") return []; throw e; })) {
      if ((dir === root && /^[a-f0-9]{64}\.json$/.test(name)) || (dir === eventDir && /^[a-f0-9-]{36}\.json$/.test(name))) files.push(path.join(dir, name));
    }
  }
  const result = { imported: 0, deferred: 0, empty: 0 };
  const processFile = async (file) => {
    const marker = `legacy:${file}`;
    const previous = store.db.prepare("SELECT value FROM metadata WHERE key=?").get(marker);
    const record = previous ? JSON.parse(previous.value) : null;
    if (record && !record.empty) return;
    const stat = await fs.stat(file);
    if (record?.empty && record.mtimeMs === stat.mtimeMs && record.size === stat.size) return;
    try {
      const lock = await readPrivateJson(`${file}.lock`);
      if (processAlive(lock.pid)) { result.deferred++; return; }
    } catch (e) { if (e.code !== "ENOENT") throw e; }
    const old = await readPrivateJson(file);
    if (old.schemaVersion !== 1 || !old.binding) throw new Error(`unsupported legacy schema: ${path.basename(file)}`);
    if (Array.isArray(old.events)) {
      try {
        const owner = await readPrivateJson(path.join(root, `${bindingKey(old.binding)}.json.lock`));
        if (processAlive(owner.pid)) { result.deferred++; return; }
      } catch (e) { if (e.code !== "ENOENT") throw e; }
    } else if (!(old.tasks || []).length && !(old.history || []).length) {
      store.db.prepare("INSERT INTO metadata VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value").run(marker, JSON.stringify({ empty: true, mtimeMs: stat.mtimeMs, size: stat.size }));
      result.empty++; return;
    }
    const backupDir = path.join(root, "migration-v1-backup"); await ensureSecureDirectory(backupDir);
    const bytes = await fs.readFile(file);
    const backup = path.join(backupDir, `${hash(file)}.json`);
    try { await fs.writeFile(backup, bytes, { mode: 0o600, flag: "wx" }); } catch (e) { if (e.code !== "EEXIST") throw e; }
    store.transaction(() => {
      const current = store.db.prepare("SELECT value FROM metadata WHERE key=?").get(marker);
      if (current && !JSON.parse(current.value).empty) return;
      const isEvent = Array.isArray(old.events);
      const definitions = isEvent ? [old] : old.tasks || [];
      for (const task of definitions) {
        if (store.db.prepare("SELECT 1 FROM triggers WHERE id=?").get(task.id)) continue;
        const schedule = isEvent ? null : structuredClone(task);
        const source = isEvent ? { type: "webhook" } : { type: "schedule", ...(task.kind === "at" ? { at: new Date(task.at).toISOString() } : task.kind === "every" ? { every: `${task.everyMs}ms` } : { cron: task.cron }) };
        const t = { id: task.id, name: task.name || `schedule-${task.id.slice(0, 8)}`, binding: old.binding, prompt: isEvent ? task.instruction : task.prompt, revision: 1,
          source, schedule, enabled: Boolean(task.enabled && !task.paused && !task.deletedAt), createdAt: task.createdAt, updatedAt: task.updatedAt || task.createdAt,
          policy: isEvent ? "queue" : "coalesce", maxQueueWaitMs: task.maxQueueWaitMs || 86_400_000, runCount: task.runCount || 0,
          ...(task.deletedAt ? { deletedAt: task.deletedAt } : {}), ...(isEvent ? { tokenHash: task.tokenHash, tokenFile: path.join(eventDir, `${task.id}.token`) } : { nextRunAt: task.nextRunAt }) };
        t.fingerprint = hash(JSON.stringify({ name: t.name, prompt: t.prompt, source: t.source, policy: t.policy }));
        store.saveTrigger(t); store.saveRevision(t);
        const history = isEvent ? task.events : (old.history || []).filter((r) => r.taskId === task.id);
        for (const e of history) {
          let state = e.state;
          if (["dispatching", "delivered"].includes(state)) state = "uncertain";
          if (["queued_waiting_for_idle", "queued"].includes(state)) state = "queued";
          const r = { id: e.receiptId || e.runId, triggerId: t.id, binding: t.binding,
            eventId: e.eventId || `legacy:${e.runId}`, type: e.type || "trigger.schedule", data: e.data || { scheduledAt: e.scheduledAt },
            state, acceptedAt: e.acceptedAt || e.queuedAt || e.scheduledAt || task.createdAt,
            ...(e.deliveredAt ? { deliveredAt: e.deliveredAt } : {}), ...(e.settledAt ? { settledAt: e.settledAt } : {}),
            ...(e.error ? { error: e.error } : {}), migratedFrom: "0.4" };
          if (state === "uncertain" && !r.error) r.error = { code: "migration_inflight_unconfirmed" };
          store.saveRun(r);
        }
        if (isEvent) for (const [eventId, seen] of Object.entries(task.seen || {})) {
          const r = store.db.prepare("SELECT document FROM runs WHERE id=?").get(seen.receipt?.receiptId);
          const receipt = r ? store.receipt(JSON.parse(r.document)) : seen.receipt;
          if (!receipt) continue;
          store.db.prepare("INSERT OR IGNORE INTO dedup VALUES(?,?,?,?,?,?)").run(t.id, eventId, seen.contentHash, receipt.receiptId, Date.parse(seen.acceptedAt), JSON.stringify(receipt));
        }
      }
      store.db.prepare("INSERT INTO metadata VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value").run(marker, JSON.stringify({ importedAt: new Date().toISOString(), backup, sha256: hash(bytes) }));
      result.imported++;
    });
  };
  for (const file of files) {
    if (path.dirname(file) === eventDir) await withEventLock(eventDir, path.basename(file, ".json"), () => processFile(file));
    else await processFile(file);
  }
  return result;
}
