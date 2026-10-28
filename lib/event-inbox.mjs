import crypto from "node:crypto";
import { EventStore, eventPrompt, publicReceipt } from "./event-store.mjs";
import { globalTimers } from "./host-runtime.mjs";

export class EventInbox {
  constructor({ binding, directory, dispatch, canDispatch, timers = globalTimers() }) {
    this.binding = binding; this.store = new EventStore(directory); this.dispatch = dispatch; this.canDispatch = canDispatch; this.timers = timers;
    this.owner = crypto.randomUUID(); this.closed = true; this.pending = new Set(); this.lastError = null;
  }
  track(promise) {
    const tracked = promise.catch((error) => { this.lastError = error.code || error.message; }).finally(() => this.pending.delete(tracked));
    this.pending.add(tracked); return tracked;
  }
  async start() {
    // An interrupted delivery is ambiguous. Preserve it for reconciliation; never replay it.
    for (const route of await this.store.list(this.binding)) {
      if (!route.events.some((event) => ["dispatching", "delivered"].includes(event.state))) continue;
      await this.store.mutate(route.id, this.binding, (current) => {
        for (const event of current.events) if (["dispatching", "delivered"].includes(event.state)) {
          event.state = "uncertain"; event.error = "runtime_restarted_before_terminal"; event.settledAt = new Date().toISOString();
          current.seen[event.eventId].receipt = publicReceipt(event);
        }
      });
    }
    this.closed = false;
    this.timer = this.timers.setInterval(() => this.track(this.tick()), 250);
  }
  async tick() {
    if (this.closed || this.polling || !this.canDispatch()) return;
    this.polling = true;
    try {
      const routes = await this.store.list(this.binding);
      const candidates = routes.filter((route) => route.enabled && !route.deletedAt).flatMap((route) => route.events.filter((event) => event.state === "queued").map((event) => ({ route, event })));
      candidates.sort((a, b) => a.event.acceptedAt.localeCompare(b.event.acceptedAt));
      const next = candidates[0];
      if (!next || this.closed || !this.canDispatch()) return;
      const claimed = await this.store.mutate(next.route.id, this.binding, (route) => {
        const event = route.events.find((e) => e.receiptId === next.event.receiptId);
        if (!route.enabled || route.deletedAt || event?.state !== "queued") return null;
        event.state = "dispatching"; event.owner = this.owner; event.ownerPid = process.pid;
        route.seen[event.eventId].receipt = publicReceipt(event);
        return { route, event };
      });
      if (!claimed) return;
      if (this.closed) { await this.store.transition(claimed.route.id, claimed.event.receiptId, "queued"); return; }
      const update = (state, extra) => this.track(this.store.transition(claimed.route.id, claimed.event.receiptId, state, extra));
      try {
        const prompt = eventPrompt(claimed.route, claimed.event);
        this.dispatch(prompt, {
          onDelivered: () => update("delivered", { deliveredAt: new Date().toISOString() }),
          onSettled: () => update("settled"),
          onFailed: (error) => update(["delivery_failed", "queue_full", "queue_timeout"].includes(error.code) ? "failed" : "uncertain", { error: error.code || "delivery_error" }),
        });
      } catch (error) { await update("failed", { error: error.code || "delivery_error" }); }
    } finally { this.polling = false; }
  }
  async close() {
    this.closed = true; this.timers.clear(this.timer);
    // The bridge closes queued/active jobs first, so their callbacks are included.
    while (this.pending.size) await Promise.all([...this.pending]);
    while (this.polling) await new Promise((resolve) => setTimeout(resolve, 5));
    while (this.pending.size) await Promise.all([...this.pending]);
  }
}
