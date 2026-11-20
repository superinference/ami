// U007 (w74) — Sampling-origin discriminator for pre/postSampling hooks.
//
// Parity with the reference's MagicDocs querySource boundary (magicDocs.ts
// L222-224: `if (querySource !== 'repl_main_thread') return`): a pre/postSampling
// hook can be scoped by the ORIGIN of the sampling call ('main' = top-level loop,
// 'subagent' = a forked/nested agent) so a hook that itself samples the model can
// exclude background/forked work and avoid re-entrant recursion. The origin is
// carried on HookContext.samplingOrigin, usable as the matcher field and
// serialized into the hook stdin as `sampling_origin`.
import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  HookManager,
  SAMPLING_ORIGINS,
  isSamplingOrigin,
  serializeHookEvent,
  matcherAppliesToCtx,
  HOOK_MATCHER_FIELD,
  eventSupportsMatcher,
  getHookEventMetadata,
  type PreSamplingContext,
  type HookContext,
} from '../src/hooks';

function writeTmpHooks(entries: any[]): string {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hooks-u007-so-'));
  const dir = path.join(tmpDir, '.ami');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'hooks.json'), JSON.stringify({ hooks: entries }));
  return tmpDir;
}

function preCtx(overrides: Partial<PreSamplingContext> = {}): PreSamplingContext {
  return { messages: [], turnCount: 1, apiMessages: [], ...overrides };
}

let hm: HookManager;
beforeEach(() => { hm = new HookManager(); });

describe('sampling-origin — constant + guard', () => {
  it('SAMPLING_ORIGINS enumerates main + subagent', () => {
    assert.deepEqual([...SAMPLING_ORIGINS], ['main', 'subagent']);
  });
  it('isSamplingOrigin narrows only the valid values', () => {
    assert.equal(isSamplingOrigin('main'), true);
    assert.equal(isSamplingOrigin('subagent'), true);
    assert.equal(isSamplingOrigin('repl_main_thread'), false);
    assert.equal(isSamplingOrigin(undefined), false);
    assert.equal(isSamplingOrigin(7), false);
  });
});

describe('sampling-origin — matcher wiring', () => {
  it('pre/postSampling match on the samplingOrigin field', () => {
    assert.equal(HOOK_MATCHER_FIELD.preSampling, 'samplingOrigin');
    assert.equal(HOOK_MATCHER_FIELD.postSampling, 'samplingOrigin');
    assert.equal(eventSupportsMatcher('preSampling'), true);
    assert.equal(eventSupportsMatcher('postSampling'), true);
  });

  it('matcherAppliesToCtx gates sampling hooks by origin', () => {
    // matcher 'main' only fires for a main-thread sampling
    assert.equal(matcherAppliesToCtx('postSampling', 'main', { samplingOrigin: 'main' }), true);
    assert.equal(matcherAppliesToCtx('postSampling', 'main', { samplingOrigin: 'subagent' }), false);
    // no matcher = always applies
    assert.equal(matcherAppliesToCtx('postSampling', undefined, { samplingOrigin: 'subagent' }), true);
  });

  it('metadata enumerates the matcher values for introspection', () => {
    const meta = getHookEventMetadata('postSampling');
    assert.equal(meta?.matcherField, 'samplingOrigin');
    assert.deepEqual(meta?.matcherValues, ['main', 'subagent']);
  });
});

describe('sampling-origin — serialization', () => {
  it('serializes samplingOrigin as the Claude Code sampling_origin alias', () => {
    const line = serializeHookEvent({ event: 'postSampling', samplingOrigin: 'subagent' });
    const parsed = JSON.parse(line);
    assert.equal(parsed.samplingOrigin, 'subagent');
    assert.equal(parsed.sampling_origin, 'subagent');
    assert.equal(parsed.hook_event_name, 'PostSampling');
  });
});

describe('sampling-origin — end-to-end hook gating', () => {
  it('postSampling command hook with matcher "main" runs only for main-thread sampling', async () => {
    // Build the config AFTER the temp dir exists so the hook's `touch` command can
    // reference a sentinel path inside it.
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hooks-u007-so-e2e-'));
    const sentinel = path.join(tmpDir, 'ran');
    const dir = path.join(tmpDir, '.ami');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'hooks.json'), JSON.stringify({
      hooks: [{ event: 'postSampling', matcher: 'main', hook: { type: 'command', command: `touch '${sentinel}'` } }],
    }));
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    // Subagent-origin sampling: matcher 'main' does not apply → hook skipped.
    await hm.executePostSampling({ messages: [], turnCount: 1, samplingOrigin: 'subagent' } as HookContext);
    assert.equal(fs.existsSync(sentinel), false, 'hook must not run for subagent origin');

    // Main-origin sampling: matcher applies → hook runs.
    await hm.executePostSampling({ messages: [], turnCount: 1, samplingOrigin: 'main' } as HookContext);
    assert.equal(fs.existsSync(sentinel), true, 'hook must run for main origin');

    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('preSampling command hook with matcher "subagent" only denies subagent-origin sampling', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preSampling', matcher: 'subagent', hook: { type: 'command', command: 'echo "DENY:no background sampling"' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    // Main origin → matcher 'subagent' does not apply → allow.
    const mainDecision = await hm.executePreSampling(preCtx({ samplingOrigin: 'main' }));
    assert.equal(mainDecision.action, 'allow');

    // Subagent origin → matcher applies → deny.
    const subDecision = await hm.executePreSampling(preCtx({ samplingOrigin: 'subagent' }));
    assert.equal(subDecision.action, 'deny');

    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});
