/**
 * U007 — SessionStart / Setup / SessionEnd lifecycle hook input enrichment
 * (the reference → AMI parity). Covers:
 *  - G1 SessionStart hook result as an ARRAY of injectable messages
 *       (`initialUserMessages[]`) + aggregation of ALL hooks (not last-wins)
 *       — refs useDeferredHookMessages.ts:L12-L45.
 *  - G2 Setup hook `trigger` (init|maintenance): SETUP_TRIGGERS const +
 *       isSetupTrigger validator + matcher scoping + stdin serialization
 *       — refs coreSchemas.ts:L504-511, sessionStart.ts:L177-184.
 *  - G3 Session-exit-reason classification (SESSION_END_REASONS) + isSessionEndReason +
 *       enumerated in sessionEnd introspection metadata
 *       — refs coreTypes.ts:L55-L62.
 *  - G4 SessionStart `source` enum (SESSION_START_SOURCES) + `model` / `agent_type`
 *       in the hook stdin payload — refs coreSchemas.ts:L493-502,
 *       sessionStart.ts:L130-140.
 */

import { describe, it, beforeEach, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  HookManager,
  serializeHookEvent,
  getHookEventMetadata,
  SESSION_START_SOURCES,
  isSessionStartSource,
  SESSION_END_REASONS,
  isSessionEndReason,
  SETUP_TRIGGERS,
  isSetupTrigger,
} from '../src/hooks';

// ---------------------------------------------------------------------------
// G1 — SessionStart result: array of injectable messages + aggregate-all
// ---------------------------------------------------------------------------

describe('U007 G1 — executeSessionStart aggregates ALL hook messages', () => {
  let hm: HookManager;
  beforeEach(() => { hm = new HookManager(); });

  it('joins initialUserMessage from EVERY hook (not last-wins)', async () => {
    hm.onSessionStart(async () => ({ initialUserMessage: 'from-hook-A' }));
    hm.onSessionStart(async () => ({ initialUserMessage: 'from-hook-B' }));
    const res = await hm.executeSessionStart({ sessionId: 's', cwd: process.cwd() });
    assert.ok(res, 'returns a result');
    // Last-wins (old behavior) would drop from-hook-A.
    assert.match(res!.initialUserMessage!, /from-hook-A/);
    assert.match(res!.initialUserMessage!, /from-hook-B/);
    assert.equal(res!.initialUserMessage, 'from-hook-A\n\nfrom-hook-B');
  });

  it('accepts a single hook returning an initialUserMessages[] array', async () => {
    hm.onSessionStart(async () => ({ initialUserMessages: ['m1', 'm2', 'm3'] }));
    const res = await hm.executeSessionStart({ sessionId: 's', cwd: process.cwd() });
    assert.equal(res!.initialUserMessage, 'm1\n\nm2\n\nm3');
  });

  it('mixes single + array messages across hooks', async () => {
    hm.onSessionStart(async () => ({ initialUserMessage: 'solo' }));
    hm.onSessionStart(async () => ({ initialUserMessages: ['arr1', 'arr2'] }));
    const res = await hm.executeSessionStart({ sessionId: 's', cwd: process.cwd() });
    assert.equal(res!.initialUserMessage, 'solo\n\narr1\n\narr2');
  });

  it('parses initialUserMessages[] from a command-hook JSON result', async () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-g1-'));
    try {
      const amiDir = path.join(tmp, '.ami');
      fs.mkdirSync(amiDir, { recursive: true });
      fs.writeFileSync(path.join(amiDir, 'hooks.json'), JSON.stringify({
        hooks: [{
          event: 'sessionStart',
          hook: { type: 'command', command: `printf '{"initialUserMessages":["cmd-x","cmd-y"]}'` },
        }],
      }));
      hm.setWorkspaceTrusted(true);
      hm.setCwd(tmp);
      hm.loadFromFile(tmp);
      const res = await hm.executeSessionStart({ sessionId: 's', cwd: tmp });
      assert.equal(res!.initialUserMessage, 'cmd-x\n\ncmd-y');
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });

  it('ignores non-string / empty entries in initialUserMessages[]', async () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-g1b-'));
    try {
      const amiDir = path.join(tmp, '.ami');
      fs.mkdirSync(amiDir, { recursive: true });
      fs.writeFileSync(path.join(amiDir, 'hooks.json'), JSON.stringify({
        hooks: [{
          event: 'sessionStart',
          hook: { type: 'command', command: `printf '{"initialUserMessages":["keep", "", 5, null]}'` },
        }],
      }));
      hm.setWorkspaceTrusted(true);
      hm.setCwd(tmp);
      hm.loadFromFile(tmp);
      const res = await hm.executeSessionStart({ sessionId: 's', cwd: tmp });
      assert.equal(res!.initialUserMessage, 'keep');
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });

  it('falls through to systemMessage when initialUserMessages[] is all-empty', async () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-g1c-'));
    try {
      const amiDir = path.join(tmp, '.ami');
      fs.mkdirSync(amiDir, { recursive: true });
      fs.writeFileSync(path.join(amiDir, 'hooks.json'), JSON.stringify({
        hooks: [{
          event: 'sessionStart',
          hook: { type: 'command', command: `printf '{"initialUserMessages":["",null],"systemMessage":"fallback"}'` },
        }],
      }));
      const hm = new HookManager();
      hm.setWorkspaceTrusted(true);
      hm.setCwd(tmp);
      hm.loadFromFile(tmp);
      const res = await hm.executeSessionStart({ sessionId: 's', cwd: tmp });
      assert.equal(res!.initialUserMessage, 'fallback');
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// G2 — Setup hook `trigger` (init | maintenance)
// ---------------------------------------------------------------------------

describe('U007 G2 — Setup hook trigger field', () => {
  it('SETUP_TRIGGERS + isSetupTrigger validator', () => {
    assert.deepEqual([...SETUP_TRIGGERS], ['init', 'maintenance']);
    assert.ok(isSetupTrigger('init'));
    assert.ok(isSetupTrigger('maintenance'));
    assert.ok(!isSetupTrigger('startup'));
    assert.ok(!isSetupTrigger(42));
    assert.ok(!isSetupTrigger(undefined));
  });

  it('setup metadata enumerates the allowed triggers + uses the matcher field', () => {
    const md = getHookEventMetadata('setup');
    assert.ok(md);
    assert.equal(md!.matcherField, 'trigger');
    assert.match(md!.description, /init\|maintenance/);
  });

  it('a matcher on a setup hook scopes on the trigger (runs vs skips)', async () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-g2-'));
    try {
      const amiDir = path.join(tmp, '.ami');
      fs.mkdirSync(amiDir, { recursive: true });
      const marker = path.join(tmp, 'ran.txt');
      fs.writeFileSync(path.join(amiDir, 'hooks.json'), JSON.stringify({
        hooks: [{
          event: 'setup',
          matcher: 'init',
          hook: { type: 'command', command: `printf init > ${marker}` },
        }],
      }));
      const hm = new HookManager();
      hm.setWorkspaceTrusted(true);
      hm.setCwd(tmp);
      hm.loadFromFile(tmp);

      // Wrong trigger → matcher does not apply → hook skipped.
      await hm.executeSetup({ sessionId: 's', cwd: tmp, trigger: 'maintenance' });
      assert.ok(!fs.existsSync(marker), 'maintenance must NOT run an init-scoped setup hook');

      // Matching trigger → hook runs.
      await hm.executeSetup({ sessionId: 's', cwd: tmp, trigger: 'init' });
      assert.ok(fs.existsSync(marker), 'init trigger must run an init-scoped setup hook');
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });

  it('serializes the trigger into the setup stdin payload', async () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-g2b-'));
    try {
      const amiDir = path.join(tmp, '.ami');
      fs.mkdirSync(amiDir, { recursive: true });
      const capture = path.join(tmp, 'stdin.json');
      fs.writeFileSync(path.join(amiDir, 'hooks.json'), JSON.stringify({
        hooks: [{ event: 'setup', hook: { type: 'command', command: `cat > ${capture}` } }],
      }));
      const hm = new HookManager();
      hm.setWorkspaceTrusted(true);
      hm.setCwd(tmp);
      hm.loadFromFile(tmp);
      await hm.executeSetup({ sessionId: 's', cwd: tmp, trigger: 'maintenance' });
      const payload = JSON.parse(fs.readFileSync(capture, 'utf-8'));
      assert.equal(payload.trigger, 'maintenance');
      assert.equal(payload.hook_event_name, 'Setup');
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// G3 — Session-exit-reason classification (SESSION_END_REASONS)
// ---------------------------------------------------------------------------

describe('U007 G3 — SESSION_END_REASONS classification', () => {
  it('SESSION_END_REASONS matches the reference enum + isSessionEndReason validator', () => {
    assert.deepEqual([...SESSION_END_REASONS], [
      'clear', 'resume', 'logout', 'prompt_input_exit', 'other', 'bypass_permissions_disabled',
    ]);
    assert.ok(isSessionEndReason('logout'));
    assert.ok(isSessionEndReason('bypass_permissions_disabled'));
    assert.ok(!isSessionEndReason('startup'));
    assert.ok(!isSessionEndReason(null));
  });

  it('sessionEnd metadata enumerates the exit reasons', () => {
    const md = getHookEventMetadata('sessionEnd');
    assert.ok(md);
    assert.equal(md!.matcherField, 'reason');
    for (const r of SESSION_END_REASONS) assert.match(md!.description, new RegExp(r));
  });
});

// ---------------------------------------------------------------------------
// G4 — SessionStart source enum + model / agent_type in the payload
// ---------------------------------------------------------------------------

describe('U007 G4 — SessionStart source enum + model/agent_type payload', () => {
  it('SESSION_START_SOURCES + isSessionStartSource validator', () => {
    assert.deepEqual([...SESSION_START_SOURCES], ['startup', 'resume', 'clear', 'compact']);
    assert.ok(isSessionStartSource('resume'));
    assert.ok(!isSessionStartSource('logout'));
    assert.ok(!isSessionStartSource(0));
  });

  it('sessionStart metadata enumerates the sources + mentions model/agent_type', () => {
    const md = getHookEventMetadata('sessionStart');
    assert.ok(md);
    assert.match(md!.description, /startup\|resume\|clear\|compact/);
    assert.match(md!.description, /agent_type/);
    assert.match(md!.description, /model/);
  });

  it('serializeHookEvent maps agentType → agent_type', () => {
    const s = serializeHookEvent({ event: 'sessionStart', agentType: 'reviewer' });
    const parsed = JSON.parse(s);
    assert.equal(parsed.agent_type, 'reviewer');
    assert.equal(parsed.hook_event_name, 'SessionStart');
  });

  it('serializes source, model (from provider config fallback) and agent_type into the stdin payload', async () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-g4-'));
    try {
      const amiDir = path.join(tmp, '.ami');
      fs.mkdirSync(amiDir, { recursive: true });
      const capture = path.join(tmp, 'stdin.json');
      fs.writeFileSync(path.join(amiDir, 'hooks.json'), JSON.stringify({
        hooks: [{ event: 'sessionStart', hook: { type: 'command', command: `cat > ${capture}` } }],
      }));
      const hm = new HookManager();
      hm.setWorkspaceTrusted(true);
      hm.setProviderConfig({ model: 'test-model-99' });
      hm.setCwd(tmp);
      hm.loadFromFile(tmp);
      await hm.executeSessionStart({ sessionId: 's', cwd: tmp, source: 'resume', agentType: 'reviewer' });
      const payload = JSON.parse(fs.readFileSync(capture, 'utf-8'));
      assert.equal(payload.source, 'resume');
      assert.equal(payload.model, 'test-model-99');     // provider-config fallback
      assert.equal(payload.agent_type, 'reviewer');
      assert.equal(payload.hook_event_name, 'SessionStart');
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });

  it('explicit ctx.model overrides the provider-config fallback', async () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-g4b-'));
    try {
      const amiDir = path.join(tmp, '.ami');
      fs.mkdirSync(amiDir, { recursive: true });
      const capture = path.join(tmp, 'stdin.json');
      fs.writeFileSync(path.join(amiDir, 'hooks.json'), JSON.stringify({
        hooks: [{ event: 'sessionStart', hook: { type: 'command', command: `cat > ${capture}` } }],
      }));
      const hm = new HookManager();
      hm.setWorkspaceTrusted(true);
      hm.setProviderConfig({ model: 'fallback-model' });
      hm.setCwd(tmp);
      hm.loadFromFile(tmp);
      await hm.executeSessionStart({ sessionId: 's', cwd: tmp, model: 'explicit-model' });
      const payload = JSON.parse(fs.readFileSync(capture, 'utf-8'));
      assert.equal(payload.model, 'explicit-model');
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// Fail-soft discriminator validation (schema-parity): an unrecognized
// source/reason/trigger is logged but never throws (Claude Code accepts the
// documented enum; AMI warns rather than failing the lifecycle event).
// ---------------------------------------------------------------------------

describe('U007 — lifecycle discriminator validation (fail-soft)', () => {
  it('executeSessionStart tolerates a valid and an unknown source', async () => {
    const hm = new HookManager();
    assert.equal(await hm.executeSessionStart({ sessionId: 's', cwd: process.cwd(), source: 'startup' }), undefined);
    assert.equal(await hm.executeSessionStart({ sessionId: 's', cwd: process.cwd(), source: 'nonsense' }), undefined);
  });

  it('executeSessionEnd tolerates a valid and an unknown reason', async () => {
    const hm = new HookManager();
    let seen: string | undefined;
    hm.onSessionEnd(async (c) => { seen = c.reason; });
    await hm.executeSessionEnd({ sessionId: 's', cwd: process.cwd(), reason: 'logout' });
    assert.equal(seen, 'logout');
    await hm.executeSessionEnd({ sessionId: 's', cwd: process.cwd(), reason: 'bogus' });
    assert.equal(seen, 'bogus');   // hook still ran despite the unknown reason
    await hm.executeSessionEnd({ sessionId: 's', cwd: process.cwd() });   // undefined reason path
  });

  it('executeSetup tolerates a valid and an unknown trigger', async () => {
    const hm = new HookManager();
    let ran = 0;
    hm.onSetup(async () => { ran++; });
    await hm.executeSetup({ sessionId: 's', cwd: process.cwd(), trigger: 'init' });
    await hm.executeSetup({ sessionId: 's', cwd: process.cwd(), trigger: 'weird' });
    await hm.executeSetup({ sessionId: 's', cwd: process.cwd() });   // undefined trigger path
    assert.equal(ran, 3);
  });
});
