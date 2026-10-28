import crypto from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

const uid = typeof process.getuid === "function" ? process.getuid() : undefined;
const MAX_SOCKET_PATH = 100;

export function runtimeDirectory(env = process.env) {
  if (env.OMP_SESSION_BRIDGE_DIR) return path.resolve(env.OMP_SESSION_BRIDGE_DIR);
  if (env.XDG_RUNTIME_DIR) return path.join(path.resolve(env.XDG_RUNTIME_DIR), "omp-session-bridge");
  return path.join(os.tmpdir(), `omp-session-bridge-${uid ?? process.pid}`);
}

export function safeSessionKey(sessionId) {
  if (typeof sessionId !== "string" || !sessionId) throw new Error("session id is required");
  const visible = sessionId.replace(/[^A-Za-z0-9_-]/g, "_").slice(0, 32) || "session";
  return `${visible}-${crypto.createHash("sha256").update(sessionId).digest("hex").slice(0, 16)}`;
}

export function artifactPaths(sessionId, directory = runtimeDirectory()) {
  const key = safeSessionKey(sessionId);
  const registry = path.join(directory, `${key}.json`);
  let socket = path.join(directory, `${key}.sock`);
  if (Buffer.byteLength(socket) > MAX_SOCKET_PATH) socket = path.join(directory, `${crypto.createHash("sha256").update(sessionId).digest("hex").slice(0, 28)}.sock`);
  return { key, registry, socket };
}

export async function ensureSecureDirectory(directory) {
  await fs.mkdir(directory, { recursive: true, mode: 0o700 });
  const stat = await fs.lstat(directory);
  if (!stat.isDirectory() || stat.isSymbolicLink()) throw new Error(`unsafe bridge directory: ${directory}`);
  if (uid !== undefined && stat.uid !== uid) throw new Error(`bridge directory is not owned by current user: ${directory}`);
  await fs.chmod(directory, 0o700);
  return directory;
}

export async function safeUnlink(file, expectedDirectory) {
  try {
    const resolvedDirectory = path.resolve(expectedDirectory);
    if (path.dirname(path.resolve(file)) !== resolvedDirectory) throw new Error("artifact escapes bridge directory");
    const stat = await fs.lstat(file);
    if (stat.isSymbolicLink()) throw new Error(`refusing to unlink symlink: ${file}`);
    if (uid !== undefined && stat.uid !== uid) throw new Error(`refusing to unlink artifact not owned by current user: ${file}`);
    await fs.unlink(file);
    return true;
  } catch (error) {
    if (error?.code === "ENOENT") return false;
    throw error;
  }
}

export async function writeRegistryAtomic(file, record, directory = path.dirname(file)) {
  await ensureSecureDirectory(directory);
  const name = path.basename(file);
  const temporary = path.join(directory, `.${name}.${process.pid}.${crypto.randomBytes(6).toString("hex")}.tmp`);
  try {
    await fs.writeFile(temporary, `${JSON.stringify(record)}\n`, { encoding: "utf8", mode: 0o600, flag: "wx" });
    await fs.rename(temporary, file);
    await fs.chmod(file, 0o600);
  } finally {
    await fs.unlink(temporary).catch(() => {});
  }
}

export async function readSecureRegistry(file, directory = path.dirname(file)) {
  const resolvedDirectory = path.resolve(directory);
  if (path.dirname(path.resolve(file)) !== resolvedDirectory) throw new Error("registry escapes bridge directory");
  const dirStat = await fs.lstat(resolvedDirectory);
  const stat = await fs.lstat(file);
  if (!dirStat.isDirectory() || dirStat.isSymbolicLink() || stat.isSymbolicLink() || !stat.isFile()) throw new Error("unsafe registry path");
  if (uid !== undefined && (dirStat.uid !== uid || stat.uid !== uid)) throw new Error("registry is not owned by current user");
  if ((dirStat.mode & 0o077) !== 0) throw new Error("bridge directory is accessible by group or other users");
  if ((stat.mode & 0o077) !== 0) throw new Error("registry is accessible by group or other users");
  const record = JSON.parse(await fs.readFile(file, "utf8"));
  if (!record || record.protocolVersion !== 1 || typeof record.sessionId !== "string" || typeof record.socketPath !== "string") throw new Error("invalid bridge registry");
  return record;
}

export async function listRegistries(directory = runtimeDirectory()) {
  try {
    await ensureSecureDirectory(directory);
    const names = await fs.readdir(directory);
    const records = await Promise.all(names.filter((n) => n.endsWith(".json")).map(async (name) => {
      try { return await readSecureRegistry(path.join(directory, name), directory); } catch { return null; }
    }));
    return records.filter(Boolean).sort((a, b) => String(b.startedAt).localeCompare(String(a.startedAt)));
  } catch (error) {
    if (error?.code === "ENOENT") return [];
    throw error;
  }
}

export function processAlive(pid) {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try { process.kill(pid, 0); return true; } catch (error) { return error.code === "EPERM"; }
}

export async function removeStaleArtifacts(directory = runtimeDirectory()) {
  await ensureSecureDirectory(directory);
  for (const name of await fs.readdir(directory)) {
    if (!/^[A-Za-z0-9_-]+\.(json|sock)$/.test(name)) continue;
    const file = path.join(directory, name);
    if (!name.endsWith(".json")) continue;
    try {
      const record = await readSecureRegistry(file, directory);
      if (processAlive(record.pid)) continue;
      await safeUnlink(file, directory);
      await safeUnlink(record.socketPath, directory).catch(() => {});
    } catch { /* untrusted artifacts are deliberately left untouched */ }
  }
}

export async function resolveTarget(target, options = {}) {
  if (options.socket) return { socketPath: path.resolve(options.socket) };
  const directory = options.directory || runtimeDirectory(options.env);
  const records = await listRegistries(directory);
  const selector = options.sessionId !== undefined ? ["sessionId", options.sessionId]
    : options.sessionFile !== undefined ? ["sessionFile", options.sessionFile]
      : options.name !== undefined ? ["name", options.name] : [null, target];
  const [field, value] = selector;
  const matches = records.filter((record) => {
    if (value === undefined || value === null || value === "") return !field;
    if (field) return record[field] === value;
    return record.sessionId === value || record.sessionFile === value || record.name === value || record.sessionId.startsWith(value);
  });
  if (!matches.length) throw new Error(value ? `no live OMP bridge matches ${value}` : "no live OMP session bridges found");
  if (matches.length > 1) throw new Error(`target is ambiguous; use --session-id or --socket (${matches.map((r) => r.sessionId).join(", ")})`);
  const record = matches[0];
  if (!processAlive(record.pid)) throw new Error("matched bridge is stale");
  const socketStat = await fs.lstat(record.socketPath);
  if (!socketStat.isSocket() || socketStat.isSymbolicLink() || (uid !== undefined && socketStat.uid !== uid)) throw new Error("unsafe bridge socket");
  return record;
}
