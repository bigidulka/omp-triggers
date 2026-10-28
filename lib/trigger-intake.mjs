import path from "node:path";
import { EventStore } from "./event-store.mjs";

/** Preserve HTTP intake for v0.4 runtimes until they release their binding. */
export class TriggerIntake {
  constructor(store) { this.store = store; this.legacy = new EventStore(path.join(store.directory, "events")); }
  migrated(id) {
    try { return this.store.get(id); }
    catch (e) { if (e.code === "trigger_not_found") return null; throw e; }
  }
  async get(id) { return this.migrated(id) || this.legacy.load(id); }
  async accept(id, token, input) {
    if (this.migrated(id)) return this.store.accept(id, token, input);
    return this.legacy.accept(id, token, input, () => this.migrated(id) ? this.store.accept(id, token, input) : undefined);
  }
}
