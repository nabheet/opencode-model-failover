// Tests run against a fake v2 ctx; no OpenCode process is needed.

import assert from "node:assert/strict";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import plugin from "../index.js";

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function waitFor(predicate, { timeout = 3000, interval = 5 } = {}) {
  const deadline = Date.now() + timeout;
  for (;;) {
    if (predicate()) return;
    if (Date.now() > deadline) throw new Error("waitFor timed out");
    await delay(interval);
  }
}

function mkLog() {
  return join(mkdtempSync(join(tmpdir(), "model-failover-")), "failover.log");
}

function readLog(path) {
  try {
    return readFileSync(path, "utf8");
  } catch {
    return "";
  }
}

function createEventStream() {
  const queue = [];
  let waiter = null;
  let closed = false;
  return {
    push(value) {
      if (closed) return;
      if (waiter) {
        const resolve = waiter;
        waiter = null;
        resolve({ value, done: false });
      } else {
        queue.push(value);
      }
    },
    close() {
      closed = true;
      if (waiter) {
        const resolve = waiter;
        waiter = null;
        resolve({ value: undefined, done: true });
      }
    },
    [Symbol.asyncIterator]() {
      return {
        next() {
          if (queue.length > 0) return Promise.resolve({ value: queue.shift(), done: false });
          if (closed) return Promise.resolve({ value: undefined, done: true });
          return new Promise((resolve) => {
            waiter = resolve;
          });
        },
      };
    },
  };
}

function toRef(ref) {
  const at = ref.indexOf("/");
  return { providerID: ref.slice(0, at), modelID: ref.slice(at + 1) };
}

function failedEvent(sessionID, error) {
  return { type: "session.execution.failed", data: { sessionID, error } };
}

function succeededEvent(sessionID) {
  return { type: "session.execution.succeeded", data: { sessionID } };
}

function modelSelectedEvent(sessionID, model) {
  return { type: "session.model.selected", data: { sessionID, model } };
}

async function start(t, options, cfg = {}) {
  const stream = createEventStream();
  const calls = { switchModel: [], prompt: [], context: 0, list: 0 };
  const ctx = {
    options,
    model: {
      list: async () => {
        calls.list += 1;
        return { data: (cfg.models || []).map(toRef) };
      },
    },
    session: {
      switchModel: async (args) => {
        calls.switchModel.push(args);
        if (cfg.onSwitch) await cfg.onSwitch(args);
      },
      context: async () => {
        calls.context += 1;
        return { data: cfg.transcript || [] };
      },
      prompt: async (args) => {
        calls.prompt.push(args);
      },
    },
    event: { subscribe: () => stream },
  };

  const cleanup = await plugin.setup(ctx);
  t.after(() => {
    cleanup();
    stream.close();
  });

  return { calls, push: (event) => stream.push(event) };
}

test("exports a v2 plugin with a setup function", () => {
  assert.equal(plugin.id, "model-failover");
  assert.equal(typeof plugin.setup, "function");
});

test("ignores a non-retryable error", async (t) => {
  const logFile = mkLog();
  const { calls, push } = await start(
    t,
    { fallbackModels: ["a/one"], notify: false, logFile },
    { models: ["a/one"] },
  );

  push(failedEvent("s1", { type: "validation", message: "invalid request body" }));
  await waitFor(() => readLog(logFile).includes("not retryable, ignoring"));

  assert.equal(calls.switchModel.length, 0);
});

test("does nothing when no model in the chain is available", async (t) => {
  const logFile = mkLog();
  const { calls, push } = await start(
    t,
    { fallbackModels: ["a/one"], notify: false, logFile },
    { models: [] },
  );

  push(failedEvent("s1", { type: "provider.rateLimit", message: "429 Too Many Requests" }));
  await waitFor(() => readLog(logFile).includes("no available model left in chain"));

  assert.equal(calls.switchModel.length, 0);
});

test("cools a model down after switchModel throws", async (t) => {
  const logFile = mkLog();
  const { calls, push } = await start(
    t,
    { fallbackModels: ["a/one"], notify: false, logFile },
    {
      models: ["a/one"],
      onSwitch: () => {
        throw new Error("switch rejected");
      },
    },
  );

  push(failedEvent("s1", { type: "provider.timeout", message: "request timed out" }));
  await waitFor(() => readLog(logFile).includes("switchModel to a/one failed"));

  push(failedEvent("s1", { type: "provider.timeout", message: "request timed out" }));
  await waitFor(() => readLog(logFile).includes("no available model left in chain"));

  assert.equal(calls.switchModel.length, 1);
});

test("stops after maxAttempts failovers for a session", async (t) => {
  const logFile = mkLog();
  const { calls, push } = await start(
    t,
    { fallbackModels: ["a/one"], maxAttempts: 1, notify: false, logFile },
    { models: ["a/one"] },
  );

  push(failedEvent("s1", { type: "provider.overloaded", message: "503 Service Unavailable" }));
  await waitFor(() => readLog(logFile).includes("moved to a/one"));

  push(failedEvent("s1", { type: "provider.overloaded", message: "503 Service Unavailable" }));
  await waitFor(() => readLog(logFile).includes("max attempts reached"));

  assert.equal(calls.switchModel.length, 1);
});

test("does not replay the prompt when autoRetry is unset", async (t) => {
  const logFile = mkLog();
  const { calls, push } = await start(
    t,
    { fallbackModels: ["a/one"], notify: false, logFile },
    {
      models: ["a/one"],
      transcript: [{ role: "user", parts: [{ type: "text", text: "hello" }] }],
    },
  );

  push(failedEvent("s1", { type: "provider.rateLimit", message: "429" }));
  await waitFor(() => readLog(logFile).includes("moved to a/one"));
  await delay(20);

  assert.equal(calls.context, 0);
  assert.equal(calls.prompt.length, 0);
});

test("replays the last user message when autoRetry is on", async (t) => {
  const logFile = mkLog();
  const { calls, push } = await start(
    t,
    { fallbackModels: ["a/one"], autoRetry: true, notify: false, logFile },
    {
      models: ["a/one"],
      transcript: [
        { role: "user", parts: [{ type: "text", text: "first prompt" }] },
        { role: "assistant", parts: [{ type: "text", text: "a reply" }] },
        { role: "user", parts: [{ type: "text", text: "second prompt" }] },
      ],
    },
  );

  push(failedEvent("s1", { type: "provider.rateLimit", message: "429" }));
  await waitFor(() => calls.prompt.length === 1);

  assert.deepEqual(calls.prompt[0], { sessionID: "s1", text: "second prompt" });
});

test("advances to the next chain entry after each inference failure", async (t) => {
  const logFile = mkLog();
  const { calls, push } = await start(
    t,
    { fallbackModels: ["a/one", "a/two", "b/three"], maxAttempts: 5, notify: false, logFile },
    { models: ["a/one", "a/two", "b/three"] },
  );

  push(failedEvent("s1", { type: "provider.rateLimit", message: "429" }));
  await waitFor(() => calls.switchModel.length === 1);
  push(failedEvent("s1", { type: "provider.overloaded", message: "503" }));
  await waitFor(() => calls.switchModel.length === 2);
  push(failedEvent("s1", { type: "provider.timeout", message: "timed out" }));
  await waitFor(() => calls.switchModel.length === 3);

  assert.deepEqual(
    calls.switchModel.map((c) => `${c.model.providerID}/${c.model.id}`),
    ["a/one", "a/two", "b/three"],
  );
});

test("starts a fresh sequence after a successful execution", async (t) => {
  const logFile = mkLog();
  const { calls, push } = await start(
    t,
    { fallbackModels: ["a/one", "a/two"], maxAttempts: 1, notify: false, logFile },
    { models: ["a/one", "a/two"] },
  );

  push(modelSelectedEvent("s1", { providerID: "a", id: "one" }));
  await delay(20);

  push(failedEvent("s1", { type: "provider.rateLimit", message: "429" }));
  await waitFor(() => calls.switchModel.length === 1);
  assert.equal(calls.switchModel[0].model.id, "two");

  push(succeededEvent("s1"));
  await delay(30);

  // New sequence, fresh budget: the chain restarts, so the entry it is not
  // currently on (`a/one`) is eligible again.
  push(failedEvent("s1", { type: "provider.timeout", message: "timed out" }));
  await waitFor(() => calls.switchModel.length === 2);

  assert.equal(calls.switchModel[1].model.id, "one");
  assert.ok(!readLog(logFile).includes("max attempts reached"));
});

test("keeps attempt state per session", async (t) => {
  const logFile = mkLog();
  const { calls, push } = await start(
    t,
    { fallbackModels: ["a/one"], maxAttempts: 1, notify: false, logFile },
    { models: ["a/one"] },
  );

  push(failedEvent("s1", { type: "provider.rateLimit", message: "429" }));
  await waitFor(() => calls.switchModel.length === 1);
  push(failedEvent("s2", { type: "provider.rateLimit", message: "429" }));
  await waitFor(() => calls.switchModel.length === 2);

  assert.deepEqual(
    calls.switchModel.map((c) => c.sessionID),
    ["s1", "s2"],
  );
});

test("skips the model already selected for the session", async (t) => {
  const logFile = mkLog();
  const { calls, push } = await start(
    t,
    { fallbackModels: ["a/one", "a/two"], notify: false, logFile },
    { models: ["a/one", "a/two"] },
  );

  push(modelSelectedEvent("s1", { providerID: "a", id: "one" }));
  await delay(20);

  push(failedEvent("s1", { type: "provider.rateLimit", message: "429" }));
  await waitFor(() => calls.switchModel.length === 1);

  assert.equal(calls.switchModel[0].model.id, "two");
});

test("a failed switch cools the model down without using an attempt", async (t) => {
  const logFile = mkLog();
  const { calls, push } = await start(
    t,
    { fallbackModels: ["a/one", "a/two"], maxAttempts: 2, notify: false, logFile },
    {
      models: ["a/one", "a/two"],
      onSwitch: (args) => {
        if (args.model.id === "one") throw new Error("switch rejected");
      },
    },
  );

  push(failedEvent("s1", { type: "provider.timeout", message: "timed out" }));
  await waitFor(() => readLog(logFile).includes("switchModel to a/one failed"));

  push(failedEvent("s1", { type: "provider.timeout", message: "timed out" }));
  await waitFor(() => calls.switchModel.length === 2);

  assert.equal(calls.switchModel[1].model.id, "two");
  assert.ok(readLog(logFile).includes("(attempt 1/2)"));
});

test("never returns to the model that just failed", async (t) => {
  const logFile = mkLog();
  const { calls, push } = await start(
    t,
    { fallbackModels: ["a/one", "a/two", "b/three"], maxAttempts: 5, notify: false, logFile },
    { models: ["a/one", "a/two", "b/three"] },
  );

  push(modelSelectedEvent("s1", { providerID: "a", id: "one" }));
  await delay(20);
  push(failedEvent("s1", { type: "provider.rateLimit", message: "429" }));
  await waitFor(() => calls.switchModel.length === 1);

  push(modelSelectedEvent("s1", { providerID: "a", id: "two" }));
  await delay(20);
  push(failedEvent("s1", { type: "provider.rateLimit", message: "429" }));
  await waitFor(() => calls.switchModel.length === 2);

  assert.deepEqual(
    calls.switchModel.map((c) => c.model.id),
    ["two", "three"],
  );
});
