/**
 * U007 / w101 — hook `systemMessage` is a USER-facing warning channel.
 *
 * Claude Code's `systemMessage` field is documented as "Warning message shown to
 * the user" and is yielded as a dedicated `hook_system_message` attachment,
 * DISTINCT from the model-facing `additionalContext`. AMI historically collapsed
 * `systemMessage` into model context (never shown to the human). This locks in the
 * dedicated user-facing extractor + the UserPromptSubmit aggregation that carries
 * `systemMessage` separately so the engine can surface it to the human.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { HookManager, extractSystemMessages } from '../src/hooks';

describe('extractSystemMessages (U007/w101)', () => {
  it('extracts the JSON systemMessage field (user-facing warning)', () => {
    assert.deepEqual(extractSystemMessages('{"systemMessage":"touches a protected file"}'), ['touches a protected file']);
  });

  it('trims the systemMessage and drops a whitespace-only one', () => {
    assert.deepEqual(extractSystemMessages('{"systemMessage":"  warn  "}'), ['warn']);
    assert.deepEqual(extractSystemMessages('{"systemMessage":"   "}'), []);
  });

  it('extracts the flattened SYSTEM: sentinel the command path emits', () => {
    assert.deepEqual(extractSystemMessages('SYSTEM: heads up'), ['heads up']);
    assert.deepEqual(extractSystemMessages('SYSTEM:   '), []);
  });

  it('returns [] for additionalContext / plain / empty output (NOT a warning)', () => {
    assert.deepEqual(extractSystemMessages('{"additionalContext":"z"}'), []);
    assert.deepEqual(extractSystemMessages('nothing'), []);
    assert.deepEqual(extractSystemMessages(''), []);
  });
});

describe('HookManager.executeUserPromptSubmit — systemMessage aggregation (U007/w101)', () => {
  it('carries a single hook systemMessage on the result (user-facing)', async () => {
    const hm = new HookManager();
    hm.onUserPromptSubmit(async () => ({ systemMessage: 'protected path touched' }));
    const r = await hm.executeUserPromptSubmit({ prompt: 'edit /etc/hosts', turnCount: 0 });
    assert.ok(r, 'a systemMessage-only result must still be returned (not swallowed)');
    assert.equal(r.systemMessage, 'protected path touched');
    assert.equal(r.additionalContext, undefined, 'systemMessage must NOT leak into model additionalContext');
  });

  it('aggregates systemMessage across multiple hooks (join, not last-wins)', async () => {
    const hm = new HookManager();
    hm.onUserPromptSubmit(async () => ({ systemMessage: 'warn A' }));
    hm.onUserPromptSubmit(async () => ({ systemMessage: 'warn B' }));
    const r = await hm.executeUserPromptSubmit({ prompt: 'x', turnCount: 0 });
    assert.ok(r);
    assert.equal(r.systemMessage, 'warn A\n\nwarn B');
  });

  it('keeps additionalContext and systemMessage on separate fields', async () => {
    const hm = new HookManager();
    hm.onUserPromptSubmit(async () => ({ additionalContext: 'model ctx', systemMessage: 'user warn' }));
    const r = await hm.executeUserPromptSubmit({ prompt: 'x', turnCount: 0 });
    assert.ok(r);
    assert.equal(r.additionalContext, 'model ctx');
    assert.equal(r.systemMessage, 'user warn');
  });

  it('returns void when no hook contributes anything', async () => {
    const hm = new HookManager();
    hm.onUserPromptSubmit(async () => {});
    const r = await hm.executeUserPromptSubmit({ prompt: 'x', turnCount: 0 });
    assert.equal(r, undefined);
  });
});
