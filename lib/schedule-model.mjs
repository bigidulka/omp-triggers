import crypto from "node:crypto";
import { BridgeError, MAX_PROMPT_BYTES, byteLength } from "./protocol.mjs";

export const SCHEDULE_SCHEMA_VERSION = 1;
export const MAX_ACTIVE_TASKS = 50;
export const MAX_HISTORY_PER_TASK = 20;
export const MAX_HISTORY_PER_SESSION = 200;
export const MIN_INTERVAL_MS = 60_000;
export const MAX_EXPIRY_MS = 365 * 24 * 60 * 60 * 1000;
export const DEFAULT_RECURRING_EXPIRY_MS = 7 * 24 * 60 * 60 * 1000;
export const DEFAULT_ONE_SHOT_GRACE_MS = 60 * 60 * 1000;

export function scheduleError(code, message) { return new BridgeError(code, message); }
export function iso(value) { return new Date(value).toISOString(); }
export function parseDuration(value, label = "duration") {
  if (typeof value === "number" && Number.isSafeInteger(value) && value > 0) return value;
  if (typeof value !== "string") throw scheduleError("invalid_schedule", `${label} must be a duration such as 5m`);
  const isoDuration = /^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$/i.exec(value.trim());
  if (isoDuration && isoDuration.slice(1).some(Boolean)) {
    const result = Number(isoDuration[1] || 0) * 86_400_000 + Number(isoDuration[2] || 0) * 3_600_000 + Number(isoDuration[3] || 0) * 60_000 + Number(isoDuration[4] || 0) * 1000;
    if (Number.isSafeInteger(result) && result > 0) return result;
  }
  const match = /^(\d+)(ms|s|m|h|d)$/.exec(value.trim());
  if (!match) throw scheduleError("invalid_schedule", `${label} must use an integer and ms, s, m, h, or d`);
  const factor = { ms: 1, s: 1000, m: 60_000, h: 3_600_000, d: 86_400_000 }[match[2]];
  const result = Number(match[1]) * factor;
  if (!Number.isSafeInteger(result) || result <= 0) throw scheduleError("invalid_schedule", `${label} is out of range`);
  return result;
}

function decimal(raw) { return typeof raw === "string" && /^\d+$/.test(raw) ? Number(raw) : NaN; }
function parseField(raw, min, max, field) {
  if (typeof raw !== "string" || !raw) throw scheduleError("invalid_cron", `${field} field is required`);
  const values = new Set();
  for (const item of raw.split(",")) {
    const [base, stepRaw] = item.split("/");
    if (item.split("/").length > 2 || !base) throw scheduleError("invalid_cron", `invalid ${field} field`);
    const step = stepRaw === undefined ? 1 : decimal(stepRaw);
    if (!Number.isInteger(step) || step < 1 || step > max - min + 1) throw scheduleError("invalid_cron", `invalid ${field} step`);
    let start = min; let end = max;
    if (base !== "*") {
      const range = base.split("-");
      if (range.length === 1) start = end = decimal(range[0]);
      else if (range.length === 2) { start = decimal(range[0]); end = decimal(range[1]); }
      else throw scheduleError("invalid_cron", `invalid ${field} range`);
    }
    if (!Number.isInteger(start) || !Number.isInteger(end) || start < min || end > max || start > end) throw scheduleError("invalid_cron", `${field} is out of range`);
    for (let value = start; value <= end; value += step) values.add(value);
  }
  return values;
}

/** Strict five-field numeric Vixie cron. Calendar evaluation uses local Date time. */
export function parseCron(expression) {
  if (typeof expression !== "string") throw scheduleError("invalid_cron", "cron must be a five-field expression");
  const fields = expression.trim().split(/\s+/);
  if (fields.length !== 5) throw scheduleError("invalid_cron", "cron must have exactly five fields");
  const [minuteRaw, hourRaw, domRaw, monthRaw, dowRaw] = fields;
  const dow = parseField(dowRaw, 0, 7, "day-of-week");
  if (dow.has(7)) { dow.delete(7); dow.add(0); }
  return {
    expression: fields.join(" "), minute: parseField(minuteRaw, 0, 59, "minute"), hour: parseField(hourRaw, 0, 23, "hour"),
    dom: parseField(domRaw, 1, 31, "day-of-month"), month: parseField(monthRaw, 1, 12, "month"), dow,
    domWildcard: domRaw === "*", dowWildcard: dowRaw === "*",
  };
}

export function cronMatches(cron, date) {
  if (!cron.minute.has(date.getMinutes()) || !cron.hour.has(date.getHours()) || !cron.month.has(date.getMonth() + 1)) return false;
  const dom = cron.dom.has(date.getDate()); const dow = cron.dow.has(date.getDay());
  return cron.domWildcard ? dow : cron.dowWildcard ? dom : dom || dow;
}

/** Finds the next local-calendar minute. DST gaps naturally skip; repeated local minutes fire once per distinct Date instant. */
export function nextCronAt(cronOrExpression, afterMs) {
  const cron = typeof cronOrExpression === "string" ? parseCron(cronOrExpression) : cronOrExpression;
  const candidate = new Date(Math.floor(afterMs / 60_000) * 60_000 + 60_000);
  // Eight Gregorian years cover the longest leap-day gap across a non-leap
  // century. Advance the instant, rather than local wall time, so both sides
  // of a DST fallback remain observable.
  for (let i = 0; i <= 8 * 366 * 24 * 60; i++) {
    if (cronMatches(cron, candidate)) return candidate.getTime();
    candidate.setTime(candidate.getTime() + 60_000);
  }
  throw scheduleError("invalid_cron", "cron has no occurrence in the next eight years");
}

function stableJitter(taskId, occurrence, cap) {
  if (!cap) return 0;
  const n = crypto.createHash("sha256").update(`${taskId}:${occurrence}`).digest().readUInt32BE(0);
  return n % (cap + 1);
}

export function taskNextAt(task, afterMs = Date.now()) {
  if (task.kind === "at") return task.at > afterMs ? task.at : null;
  let scheduled;
  if (task.kind === "every") {
    const base = task.createdAtMs;
    const steps = Math.max(1, Math.floor((afterMs - base) / task.everyMs) + 1);
    scheduled = base + steps * task.everyMs;
  } else scheduled = nextCronAt(task.cron, afterMs);
  return scheduled + stableJitter(task.id, scheduled, task.jitterMs || 0);
}

export function validatePrompt(prompt) {
  if (typeof prompt !== "string" || !prompt.trim()) throw scheduleError("invalid_schedule", "prompt must be a non-empty string");
  if (byteLength(prompt) > MAX_PROMPT_BYTES) throw scheduleError("prompt_too_large", `prompt exceeds ${MAX_PROMPT_BYTES} bytes`);
  return prompt;
}

function isLeapYear(year) { return year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0); }
function daysInMonth(year, month) { return [31, isLeapYear(year) ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1]; }
export function parseRfc3339(value) {
  const match = typeof value === "string" && /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,3}))?)?(Z|[+-]\d{2}:\d{2})$/i.exec(value);
  if (!match) throw scheduleError("invalid_schedule", "at must be an RFC3339 timestamp with timezone");
  const [, yearRaw, monthRaw, dayRaw, hourRaw, minuteRaw, secondRaw = "0", fractionRaw = "", zone] = match;
  const year = Number(yearRaw); const month = Number(monthRaw); const day = Number(dayRaw); const hour = Number(hourRaw); const minute = Number(minuteRaw); const second = Number(secondRaw); const millisecond = Number(fractionRaw.padEnd(3, "0"));
  if (month < 1 || month > 12 || day < 1 || day > daysInMonth(year, month) || hour > 23 || minute > 59 || second > 59) throw scheduleError("invalid_schedule", "at has an invalid RFC3339 date or time");
  let offsetMinutes = 0;
  if (zone.toUpperCase() !== "Z") {
    const sign = zone[0] === "+" ? 1 : -1; const offsetHour = Number(zone.slice(1, 3)); const offsetMinute = Number(zone.slice(4, 6));
    if (offsetHour > 23 || offsetMinute > 59) throw scheduleError("invalid_schedule", "at has an invalid RFC3339 timezone offset");
    offsetMinutes = sign * (offsetHour * 60 + offsetMinute);
  }
  const at = Date.parse(value);
  const local = new Date(at + offsetMinutes * 60_000);
  if (!Number.isFinite(at) || local.getUTCFullYear() !== year || local.getUTCMonth() + 1 !== month || local.getUTCDate() !== day || local.getUTCHours() !== hour || local.getUTCMinutes() !== minute || local.getUTCSeconds() !== second || local.getUTCMilliseconds() !== millisecond) throw scheduleError("invalid_schedule", "at must round-trip as RFC3339");
  return at;
}

export function normalizeCreate(input, binding, nowMs = Date.now()) {
  if (!input || typeof input !== "object") throw scheduleError("invalid_schedule", "schedule parameters must be an object");
  input = Object.fromEntries(Object.entries(input).filter(([, value]) => value !== "" && value !== undefined));
  const prompt = validatePrompt(input.prompt);
  const defined = [input.at !== undefined, input.every !== undefined, input.cron !== undefined].filter(Boolean).length;
  if (defined !== 1) throw scheduleError("invalid_schedule", "provide exactly one of at, every, or cron");
  const id = crypto.randomUUID();
  const createdAt = iso(nowMs);
  if (input.coalesceKey !== undefined && (typeof input.coalesceKey !== "string" || !/^[a-zA-Z0-9._:-]{1,128}$/.test(input.coalesceKey))) throw scheduleError("invalid_schedule", "coalesceKey must be 1..128 safe characters");
  const task = { id, prompt, binding, createdAt, createdAtMs: nowMs, updatedAt: createdAt, enabled: true, paused: false, runCount: 0, lastRunAt: null, nextRunAt: null, expiresAt: null, timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || "local", timezoneOffsetMinutes: -new Date(nowMs).getTimezoneOffset(), jitterMs: 0, oneShotGraceMs: DEFAULT_ONE_SHOT_GRACE_MS, deliveryPolicy: "idle_only", maxQueueWaitMs: input.maxQueueWait === undefined ? 24 * 60 * 60 * 1000 : parseDuration(input.maxQueueWait, "maxQueueWait"), coalesceKey: input.coalesceKey || null };
  if (input.at !== undefined) {
    const at = parseRfc3339(input.at);
    task.kind = "at"; task.at = at;
    task.oneShotGraceMs = input.grace === undefined ? DEFAULT_ONE_SHOT_GRACE_MS : parseDuration(input.grace, "grace");
    task.nextRunAt = iso(at);
  } else if (input.every !== undefined) {
    task.kind = "every"; task.everyMs = parseDuration(input.every, "every");
    if (task.everyMs < MIN_INTERVAL_MS) throw scheduleError("interval_too_short", "every must be at least 1m");
  } else {
    task.kind = "cron"; task.cron = parseCron(input.cron).expression;
  }
  if (task.kind !== "at") {
    const expiryMs = input.expires === undefined ? DEFAULT_RECURRING_EXPIRY_MS : expiryDuration(input.expires, nowMs);
    if (expiryMs > MAX_EXPIRY_MS) throw scheduleError("invalid_schedule", "expires exceeds one year");
    task.expiresAt = iso(nowMs + expiryMs);
    if (input.jitter !== undefined) {
      task.jitterMs = parseDuration(input.jitter, "jitter");
      const cap = task.kind === "every" ? Math.min(Math.floor(task.everyMs / 2), 30 * 60_000) : 30 * 60_000;
      if (task.jitterMs > cap) throw scheduleError("invalid_schedule", `jitter exceeds ${Math.floor(cap / 1000)}s cap`);
    }
    if (task.coalesceKey && (typeof task.coalesceKey !== "string" || !/^[a-zA-Z0-9._:-]{1,128}$/.test(task.coalesceKey))) throw scheduleError("invalid_schedule", "coalesceKey must be 1..128 safe characters");
    task.nextRunAt = iso(taskNextAt(task, nowMs));
  }
  return task;
}

export function expiryDuration(value, nowMs = Date.now()) {
  const duration = typeof value === "string" && /^\d{4}-/.test(value) ? parseRfc3339(value) - nowMs : parseDuration(value, "expires");
  if (duration <= 0) throw scheduleError("invalid_schedule", "expires must be in the future");
  if (duration > MAX_EXPIRY_MS) throw scheduleError("invalid_schedule", "expires exceeds one year");
  return duration;
}

export function publicTask(task) {
  const { prompt, binding, createdAtMs, everyMs, at, ...rest } = task;
  return { ...rest, ...(task.kind === "at" ? { at: iso(at) } : task.kind === "every" ? { every: `${everyMs}ms` } : { cron: task.cron }), prompt };
}

export function promptHash(prompt) { return crypto.createHash("sha256").update(prompt).digest("hex"); }
