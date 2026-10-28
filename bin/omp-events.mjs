#!/usr/bin/env node
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { eventDirectory, EventStore, readPrivateJson, withEventLock } from "../lib/event-store.mjs";
import { startTriggerDaemon } from "../lib/trigger-daemon.mjs";
import { receiverStatus, ensureReceiver } from "../lib/event-service.mjs";
import { writeRegistryAtomic } from "../lib/registry.mjs";

export async function main(args = process.argv.slice(2)) {
  const directory = process.env.OMP_SESSION_BRIDGE_EVENT_DIR || eventDirectory();
  const command = args.shift();
  if (command === "serve") {
    // Hold a distinct lifetime lock. Subscription autostart uses receiver-start.
    await withEventLock(directory, "receiver-server", async () => {
      const receiver = await startTriggerDaemon({ directory: path.dirname(directory), port: Number(process.env.OMP_SESSION_BRIDGE_EVENT_PORT || 7341) });
      const descriptor = path.join(directory, "receiver.json");
      await writeRegistryAtomic(descriptor, { baseUrl: receiver.baseUrl, instanceId: receiver.instanceId, pid: process.pid }, directory);
      await new Promise((resolve) => { process.once("SIGTERM", resolve); process.once("SIGINT", resolve); });
      await receiver.close();
      if ((await readPrivateJson(descriptor)).instanceId === receiver.instanceId) await fs.unlink(descriptor);
    });
  } else if (command === "start") {
    process.stdout.write(JSON.stringify(await ensureReceiver(directory)) + "\n");
  } else if (command === "status") {
    process.stdout.write(JSON.stringify(await receiverStatus(directory)) + "\n");
  } else if (command === "stop") {
    const live = await receiverStatus(directory);
    if (live.available) process.kill(live.pid, "SIGTERM");
    process.stdout.write(JSON.stringify({ stopRequested: live.available }) + "\n");
  } else if (command === "emit") {
    // Raw event data comes through stdin; no shell interpolation or printed credentials.
    const id = args.shift(); if (!id || args.length) throw new Error("usage: omp-events emit SUBSCRIPTION_ID < event.json");
    if (!/^[a-f0-9-]{36}$/.test(id)) throw new Error("invalid subscription ID");
    const tokenFile = path.join(directory, `${id}.token`); const st = await fs.lstat(tokenFile);
    if (!st.isFile() || st.isSymbolicLink() || (st.mode & 0o077) || (process.getuid && st.uid !== process.getuid())) throw new Error("unsafe subscription credential file");
    const token = (await fs.readFile(tokenFile, "utf8")).trim(); const live = await receiverStatus(directory);
    if (!live.available) throw new Error("receiver is offline; run omp-events start");
    const body = await fs.readFile(0, "utf8");
    const response = await fetch(`${live.baseUrl}/v1/events/${id}`, { method: "POST", headers: { authorization: `Bearer ${token}`, "content-type": "application/json" }, body, signal: AbortSignal.timeout(10_000) });
    const receipt = await response.json(); if (!response.ok) throw new Error(`${receipt.error}: ${receipt.message || "event not accepted"}`);
    process.stdout.write(JSON.stringify(receipt) + "\n");
  } else {
    process.stdout.write("Usage: omp-events start|serve|status|stop\n       omp-events emit SUBSCRIPTION_ID < event.json\n");
    if (command && !["--help", "-h"].includes(command)) process.exitCode = 2;
  }
}
if (process.argv[1] && (await fs.realpath(process.argv[1]).catch(() => process.argv[1])) === fileURLToPath(import.meta.url)) main().catch((error) => { process.stderr.write(`omp-events: ${error.message}\n`); process.exitCode = 1; });
