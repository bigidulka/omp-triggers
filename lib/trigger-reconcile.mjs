import fs from "node:fs";
import readline from "node:readline";
import crypto from "node:crypto";

const messageText = (m) => typeof m?.content === "string" ? m.content : Array.isArray(m?.content) ? m.content.filter((x) => x.type === "text").map((x) => x.text).join("") : "";

/** Read-only evidence reconciliation; never resumes a session or replays work. */
export async function reconcileRuns(store, binding, triggerId, runId) {
  const trigger = store.get(triggerId, binding);
  const rows = store.db.prepare("SELECT document FROM runs WHERE trigger_id=? AND state IN ('uncertain','responded')").all(trigger.id);
  const targets = rows.map((r) => JSON.parse(r.document)).filter((r) => !runId || r.id === runId);
  if (!targets.length) return { reconciled: [], unresolved: [] };
  const stat = fs.lstatSync(binding.sessionFile);
  if (!stat.isFile() || stat.isSymbolicLink() || (process.getuid && stat.uid !== process.getuid()) || stat.size > 128 * 1024 * 1024) throw new Error("saved session unavailable or exceeds reconciliation limit (128 MiB)");
  const entries = new Map(), anchors = new Map(), candidates = new Map(targets.map((r) => [r.id, []]));
  const lines = readline.createInterface({ input: fs.createReadStream(binding.sessionFile), crlfDelay: Infinity });
  for await (const line of lines) {
    let e; try { e = JSON.parse(line); } catch { continue; }
    if (!e.id) continue;
    const m = e.message;
    entries.set(e.id, { id: e.id, parentId: e.parentId, role: m?.role });
    if (m?.role === "user") {
      const text = messageText(m);
      for (const r of targets) if (text.includes(`"runId":"${r.id}"`) || text.includes(`"receiptId":"${r.id}"`)) anchors.set(e.id, r.id);
    }
    if (m?.role !== "assistant" || m.stopReason !== "stop" || !messageText(m)) continue;
    let cursor = entries.get(e.parentId);
    for (let depth = 0; cursor && depth < 1000; depth++, cursor = entries.get(cursor.parentId)) {
      if (cursor.role !== "user") continue;
      const target = anchors.get(cursor.id);
      if (target) candidates.get(target).push({ messageId: e.id, promptMessageId: cursor.id, timestamp: m.timestamp, stopReason: m.stopReason, textHash: crypto.createHash("sha256").update(messageText(m)).digest("hex") });
      break; // A later interactive prompt must never be attributed to this run.
    }
  }
  const reconciled = [], unresolved = [];
  store.transaction(() => {
    for (const target of targets) {
      const matches = candidates.get(target.id);
      if (matches.length !== 1) { unresolved.push({ runId: target.id, reason: matches.length ? "ambiguous_responses" : "no_saved_final_response" }); continue; }
      const row = store.db.prepare("SELECT document FROM runs WHERE id=?").get(target.id);
      if (!row) continue;
      const r = JSON.parse(row.document);
      if (!["uncertain", "responded"].includes(r.state)) continue;
      r.reconciledFrom = r.state; r.state = "responded"; r.response = matches[0]; r.evidence = "saved_omp_final_response";
      r.respondedAt ||= typeof r.response.timestamp === "number" ? new Date(r.response.timestamp).toISOString() : r.response.timestamp;
      if (r.error) { r.lifecycleError = r.error; delete r.error; }
      r.reconciledAt = new Date().toISOString(); store.saveRun(r); reconciled.push(store.receipt(r));
    }
  });
  return { reconciled, unresolved };
}
