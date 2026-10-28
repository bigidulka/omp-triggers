import fs from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";
import { ensureReceiver } from "./event-service.mjs";
import { eventDirectory } from "./event-store.mjs";
import { stateDirectory } from "./schedule-store.mjs";
import { BridgeError } from "./protocol.mjs";
import { humanTrigger } from "./tool-contract.mjs";

/** Transport only: no SQLite dependency in the OMP extension host. */
export class TriggerClient {
  constructor({ binding, directory = stateDirectory(), owner = crypto.randomUUID() } = {}) {
    this.binding = binding; this.directory = directory; this.owner = owner; this.receiver = null;
  }
  async connect() {
    this.receiver = await ensureReceiver(eventDirectory(this.directory));
    if (!this.receiver.available) throw new BridgeError("service_unavailable", "trigger service is offline; run omp-events start");
    if (this.receiver.backend !== "triggers-v1") throw new BridgeError("migration_required", "v0.4 event receiver is still running; stop it with omp-events stop, then start the new service (existing URL and credentials are preserved)");
    const file = path.join(this.directory, "control.token"); const stat = await fs.lstat(file);
    if (!stat.isFile() || stat.isSymbolicLink() || (stat.mode & 0o077) || (process.getuid && stat.uid !== process.getuid())) throw new Error("unsafe trigger control credential");
    this.token = (await fs.readFile(file, "utf8")).trim(); return this;
  }
  async call(action, params = {}) {
    if (!this.receiver) await this.connect();
    let response;
    try {
      response = await fetch(`${this.receiver.baseUrl}/v1/control`, { method: "POST", redirect: "error", signal: AbortSignal.timeout(5000),
        headers: { authorization: `Bearer ${this.token}`, "content-type": "application/json" }, body: JSON.stringify({ ...params, action, binding: this.binding, owner: this.owner }) });
    } catch (e) { this.receiver = null; throw new BridgeError("service_unavailable", `trigger service request failed: ${e.message}`); }
    const value = await response.json();
    if (!response.ok) throw new BridgeError(value.error || "service_error", value.message || "trigger service request failed");
    if (value.trigger) value.trigger = humanTrigger(value.trigger);
    if (value.triggers) value.triggers = value.triggers.map(humanTrigger);
    if (value.trigger?.source.type === "webhook") value.trigger.webhookUrl = `${this.receiver.baseUrl}/v1/events/${value.trigger.id}`;
    if (value.triggers) for (const t of value.triggers) if (t.source.type === "webhook") t.webhookUrl = `${this.receiver.baseUrl}/v1/events/${t.id}`;
    return value;
  }
}
