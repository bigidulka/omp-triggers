import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { artifactPaths, ensureSecureDirectory, readSecureRegistry, resolveTarget, safeSessionKey, writeRegistryAtomic } from "../lib/registry.mjs";

async function temp() { return fs.mkdtemp(path.join(os.tmpdir(), "omp-session-bridge-test-")); }
test("artifact names are safe, deterministic, and registry writes are private", async (t) => {
  const directory = await temp(); t.after(() => fs.rm(directory, { recursive: true, force: true }));
  const paths = artifactPaths("../../unsafe session:🙂", directory);
  assert.match(path.basename(paths.socket), /^[A-Za-z0-9_-]+\.sock$/);
  assert.equal(safeSessionKey("a/b"), safeSessionKey("a/b"));
  await ensureSecureDirectory(directory);
  await writeRegistryAtomic(paths.registry, { protocolVersion: 1, sessionId: "id", socketPath: paths.socket, pid: process.pid }, directory);
  assert.equal((await fs.stat(paths.registry)).mode & 0o077, 0);
  assert.equal((await readSecureRegistry(paths.registry, directory)).sessionId, "id");
});

test("explicit target selectors only match their requested record field", async (t) => {
  const directory = await temp(); t.after(() => fs.rm(directory, { recursive: true, force: true }));
  const socket = path.join(directory, "one.sock"); await fs.writeFile(socket, "");
  const registry = path.join(directory, "one.json");
  await writeRegistryAtomic(registry, { protocolVersion: 1, sessionId: "actual-id", sessionFile: "/tmp/value", name: "actual-name", socketPath: socket, pid: process.pid }, directory);
  await assert.rejects(resolveTarget(undefined, { directory, sessionId: "actual-name" }), /no live OMP bridge/);
  await assert.rejects(resolveTarget(undefined, { directory, name: "actual-id" }), /no live OMP bridge/);
});

test("symlink registry is rejected", async (t) => {
  const directory = await temp(); t.after(() => fs.rm(directory, { recursive: true, force: true }));
  const target = path.join(directory, "target"); const link = path.join(directory, "x.json");
  await fs.writeFile(target, "{}", { mode: 0o600 }); await fs.symlink(target, link);
  await assert.rejects(readSecureRegistry(link, directory), /unsafe registry path/);
});
