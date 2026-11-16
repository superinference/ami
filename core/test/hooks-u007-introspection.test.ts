/**
 * U007 — Hook introspection catalog parity (Claude Code → AMI). Covers:
 *  - G146 Per-event hook metadata catalog: matcher FIELD + enumerated allowed
 *         matcher VALUES for each event (hooksConfigManager.getHookEventMetadata,
 *         ref utils/hooks/hooksConfigManager.ts:L26-267). AMI already exposed
 *         `matcherField`; this adds `matcherValues` (static enums + dynamic
 *         toolNames/agentTypes/mcpServerNames).
 *  - G45  `eventSupportsMatcher` boundary — whether a matcher is meaningful for an
 *         event — plus the '(all)' matcher display label (ref ViewHookMode.tsx
 *         :L20-40).
 *  - G155 `sortMatchersByPriority` — deterministic ordering of matchers within an
 *         event by source priority then matcher name (ref hooksSettings.ts
 *         :L230-271 / hooksConfigManager.ts:L368-377).
 *  - G105 Introspection that reveals the ACTUAL registered hook commands/prompts/
 *         urls (not just counts) — `HookManager.describeHooks()` (ref utils/
 *         hooks.ts:L5005-5022).
 *  - G39  Per-event human-readable summary/description surfaced in the hooks
 *         introspection view — `HookManager.describeEvents()` (ref
 *         SelectEventMode.tsx:L84-88 / HooksConfigMenu.tsx:L449-453).
 */

import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  HookManager,
  getHookEventMetadata,
  eventSupportsMatcher,
  formatHookMatcherLabel,
  sortMatchersByPriority,
  HOOK_SOURCE_PRIORITY,
  SESSION_START_SOURCES,
  SETUP_TRIGGERS,
  type HookDescriptor,
  type HookEventDescription,
} from '../src/hooks';

const tmpDirs: string[] = [];
function mkHooksDir(hooks: unknown): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'hooks-introspect-'));
  tmpDirs.push(dir);
  fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
  fs.writeFileSync(path.join(dir, '.ami', 'hooks.json'), JSON.stringify({ hooks }));
  return dir;
}
after(() => { for (const d of tmpDirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch {} } });

// ---------------------------------------------------------------------------
// G146 — enumerated matcher values
// ---------------------------------------------------------------------------
describe('U007 G146 — getHookEventMetadata enumerated matcherValues', () => {
  it('static enumerations: sessionStart / setup / preCompact / configChange', () => {
    assert.deepEqual(getHookEventMetadata('sessionStart')!.matcherValues, [...SESSION_START_SOURCES]);
    assert.deepEqual(getHookEventMetadata('setup')!.matcherValues, [...SETUP_TRIGGERS]);
    assert.deepEqual(getHookEventMetadata('preCompact')!.matcherValues, ['manual', 'auto']);
    assert.deepEqual(getHookEventMetadata('postCompact')!.matcherValues, ['manual', 'auto']);
    assert.deepEqual(
      getHookEventMetadata('configChange')!.matcherValues,
      ['user_settings', 'project_settings', 'local_settings', 'policy_settings', 'skills'],
    );
    assert.ok(getHookEventMetadata('stopFailure')!.matcherValues!.includes('rate_limit'));
    assert.ok(getHookEventMetadata('instructionsLoaded')!.matcherValues!.includes('session_start'));
    assert.ok(getHookEventMetadata('notification')!.matcherValues!.includes('idle_prompt'));
  });

  it('dynamic enumerations: tool events use toolNames, subagent uses agentTypes, elicitation uses mcpServerNames', () => {
    assert.deepEqual(getHookEventMetadata('preToolUse', { toolNames: ['bash', 'file_edit'] })!.matcherValues, ['bash', 'file_edit']);
    // No toolNames supplied → empty (dynamic, unknown set), never undefined.
    assert.deepEqual(getHookEventMetadata('postToolUse')!.matcherValues, []);
    assert.deepEqual(getHookEventMetadata('subagentStart', { agentTypes: ['general', 'reviewer'] })!.matcherValues, ['general', 'reviewer']);
    assert.deepEqual(getHookEventMetadata('elicitation', { mcpServerNames: ['srv1'] })!.matcherValues, ['srv1']);
  });

  it('events with no matchable field have no matcherValues', () => {
    const md = getHookEventMetadata('worktreeCreate')!;
    assert.equal(md.matcherField, undefined);
    assert.equal(md.matcherValues, undefined);
  });

  it('unknown event → undefined', () => {
    assert.equal(getHookEventMetadata('nope'), undefined);
  });
});

// ---------------------------------------------------------------------------
// G45 — eventSupportsMatcher + '(all)' label
// ---------------------------------------------------------------------------
describe('U007 G45 — eventSupportsMatcher + formatHookMatcherLabel', () => {
  it('tool + field-matched events support a matcher; others do not', () => {
    assert.equal(eventSupportsMatcher('preToolUse'), true);
    assert.equal(eventSupportsMatcher('sessionStart'), true);
    assert.equal(eventSupportsMatcher('configChange'), true);
    assert.equal(eventSupportsMatcher('worktreeCreate'), false);
    assert.equal(eventSupportsMatcher('stop'), false);
    assert.equal(eventSupportsMatcher('nope'), false);
  });

  it('empty/absent matcher renders as (all)', () => {
    assert.equal(formatHookMatcherLabel(''), '(all)');
    assert.equal(formatHookMatcherLabel(undefined), '(all)');
    assert.equal(formatHookMatcherLabel('bash'), 'bash');
  });
});

// ---------------------------------------------------------------------------
// G155 — sortMatchersByPriority
// ---------------------------------------------------------------------------
describe('U007 G155 — sortMatchersByPriority', () => {
  it('orders by highest-priority source first, then matcher name', () => {
    const sorted = sortMatchersByPriority(
      ['zeta', 'alpha', 'beta'],
      { zeta: ['user'], alpha: ['user'], beta: ['project'] },
    );
    // beta (project, pri 1) before the two user-tier (pri 2); user ones alpha<zeta.
    assert.deepEqual(sorted, ['beta', 'alpha', 'zeta']);
  });

  it('project outranks user outranks config; plugin is lowest', () => {
    assert.ok(HOOK_SOURCE_PRIORITY.project < HOOK_SOURCE_PRIORITY.user);
    assert.ok(HOOK_SOURCE_PRIORITY.user < HOOK_SOURCE_PRIORITY.config);
    assert.ok(HOOK_SOURCE_PRIORITY.plugin >= 999);
    const sorted = sortMatchersByPriority(['a', 'b'], { a: ['config'], b: ['project'] });
    assert.deepEqual(sorted, ['b', 'a']);
  });

  it('does not mutate the input array', () => {
    const input = ['b', 'a'];
    sortMatchersByPriority(input, { a: ['user'], b: ['user'] });
    assert.deepEqual(input, ['b', 'a']);
  });
});

// ---------------------------------------------------------------------------
// G105 — describeHooks reveals the actual command/prompt/url
// ---------------------------------------------------------------------------
describe('U007 G105 — HookManager.describeHooks reveals content', () => {
  it('returns one descriptor per loaded hook with its real command/url/prompt', () => {
    const dir = mkHooksDir([
      { event: 'preToolUse', matcher: 'bash', hook: { type: 'command', command: 'echo hi' } },
      { event: 'preToolUse', hook: { type: 'command', command: 'guard.sh' } },
      { event: 'userPromptSubmit', hook: { type: 'prompt', prompt: 'is this safe?' } },
    ]);
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    const descs: HookDescriptor[] = hm.describeHooks();
    assert.equal(descs.length, 3);
    const preBash = descs.find(d => d.event === 'preToolUse' && d.matcher === 'bash')!;
    assert.equal(preBash.type, 'command');
    assert.equal(preBash.content, 'echo hi');
    assert.equal(preBash.source, 'project');
    const prompt = descs.find(d => d.event === 'userPromptSubmit')!;
    assert.equal(prompt.type, 'prompt');
    assert.equal(prompt.content, 'is this safe?');
  });

  it('reloading does not accumulate stale descriptors', () => {
    const dir = mkHooksDir([{ event: 'stop', hook: { type: 'command', command: 'a.sh' } }]);
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    hm.loadFromFile(dir);
    assert.equal(hm.describeHooks().filter(d => d.event === 'stop').length, 1);
  });
});

// ---------------------------------------------------------------------------
// G39 — describeEvents surfaces summary/description + matcher groups
// ---------------------------------------------------------------------------
describe('U007 G39 — HookManager.describeEvents introspection view', () => {
  it('surfaces active events with human-readable summary/description + matcher groups', () => {
    const dir = mkHooksDir([
      { event: 'preToolUse', matcher: 'bash', hook: { type: 'command', command: 'c1' } },
      { event: 'preToolUse', matcher: 'bash', hook: { type: 'command', command: 'c2' } },
      { event: 'preToolUse', hook: { type: 'command', command: 'c3' } },
      { event: 'sessionStart', hook: { type: 'command', command: 's1' } },
    ]);
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    const events: HookEventDescription[] = hm.describeEvents();

    const pre = events.find(e => e.event === 'preToolUse')!;
    assert.ok(pre, 'preToolUse should be listed as active');
    assert.equal(pre.count, 3);
    assert.ok(pre.summary && pre.summary.length > 0);
    assert.ok(pre.description && pre.description.includes('Exit code'));
    assert.equal(pre.supportsMatcher, true);
    assert.equal(pre.matcherField, 'toolName');
    // Matcher groups: 'bash' (count 2) and '' → label '(all)' (count 1).
    const bash = pre.matchers.find(m => m.matcher === 'bash')!;
    assert.equal(bash.count, 2);
    assert.equal(bash.label, 'bash');
    const all = pre.matchers.find(m => m.matcher === '')!;
    assert.equal(all.label, '(all)');
    assert.equal(all.count, 1);

    // Only active (count>0) events are returned; an un-configured event is absent.
    assert.equal(events.find(e => e.event === 'notification'), undefined);
  });

  it('passes dynamic toolNames through to matcherValues', () => {
    const dir = mkHooksDir([{ event: 'preToolUse', hook: { type: 'command', command: 'c' } }]);
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    const events = hm.describeEvents({ toolNames: ['bash', 'grep'] });
    const pre = events.find(e => e.event === 'preToolUse')!;
    assert.deepEqual(pre.matcherValues, ['bash', 'grep']);
  });
});
