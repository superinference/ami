/**
 * U007-w112 — two hook-introspection parity improvements:
 *
 *  - G (gap 1) groupHooksByEventAndMatcher per-matcher PLUGIN attribution:
 *    `HookManager.describeEvents()` matcher groups now carry `pluginNames` (the
 *    distinct plugin bundles that bound a hook under that matcher), mirroring
 *    Claude Code's `pluginHook`/`pluginName` tagging in groupHooksByEventAndMatcher
 *    (ref utils/hooks/hooksConfigManager.ts:L331-345). A user can now see WHICH
 *    plugin bound a matcher, not just that a `plugin` tier is involved.
 *
 *  - G (gap 2) Memoized hook-event metadata resolver keyed by a SORTED-JOIN of the
 *    dynamic matcher-value sources (ref hooksConfigManager.ts:L22-27, L266 —
 *    `toolNames => toolNames.slice().sort().join(',')`): repeated calls with the
 *    same tool/agent/MCP members (in any order) collapse onto ONE cached record
 *    instead of re-allocating per render.
 */

import { describe, it, after, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { HookManager, getHookEventMetadata, type HookEventDescription } from '../src/hooks';

// ---------------------------------------------------------------------------
// Gap 2 — memoized metadata resolver (sorted-join cache key)
// ---------------------------------------------------------------------------
describe('U007-w112 gap2 — getHookEventMetadata memoized by sorted-join key', () => {
  it('returns the SAME record for the same tool set regardless of array order', () => {
    const a = getHookEventMetadata('preToolUse', { toolNames: ['b', 'a', 'c'] });
    const b = getHookEventMetadata('preToolUse', { toolNames: ['c', 'b', 'a'] });
    assert.ok(a, 'metadata present for a known event');
    // Reference equality: a fresh array with the same members (every render) must
    // hit the cache, not allocate a new record. Fails pre-memoization (two distinct
    // objects), passes once memoized with the sorted-join resolver.
    assert.strictEqual(a, b, 'same-member tool sets collapse onto one cached record');
    // The enumerated values still reflect the (sorted-key but order-preserving) input.
    assert.deepEqual(a!.matcherValues, ['b', 'a', 'c']);
  });

  it('a different tool set yields a different cached record', () => {
    const a = getHookEventMetadata('preToolUse', { toolNames: ['a', 'b'] });
    const c = getHookEventMetadata('preToolUse', { toolNames: ['a', 'b', 'd'] });
    assert.notStrictEqual(a, c, 'distinct member sets must not share a cache entry');
    assert.deepEqual(c!.matcherValues, ['a', 'b', 'd']);
  });

  it('distinct events never collide on the cache key', () => {
    const pre = getHookEventMetadata('preToolUse', { toolNames: ['x'] });
    const post = getHookEventMetadata('postToolUse', { toolNames: ['x'] });
    assert.notStrictEqual(pre, post);
    assert.equal(pre!.summary, 'Before tool execution');
    assert.equal(post!.summary, 'After tool execution');
  });

  it('agent/MCP dynamic sources also key the cache by sorted join', () => {
    const a = getHookEventMetadata('subagentStart', { agentTypes: ['y', 'x'] });
    const b = getHookEventMetadata('subagentStart', { agentTypes: ['x', 'y'] });
    assert.strictEqual(a, b);
    const e1 = getHookEventMetadata('elicitation', { mcpServerNames: ['s2', 's1'] });
    const e2 = getHookEventMetadata('elicitation', { mcpServerNames: ['s1', 's2'] });
    assert.strictEqual(e1, e2);
  });

  it('an unknown event still returns undefined (not memoized)', () => {
    assert.equal(getHookEventMetadata('not-an-event'), undefined);
  });
});

// ---------------------------------------------------------------------------
// Gap 1 — per-matcher plugin attribution in describeEvents
// ---------------------------------------------------------------------------
describe('U007-w112 gap1 — describeEvents matcher groups carry pluginNames', () => {
  let tmp: string;
  let home: string;
  let origHome: string | undefined;
  let origManaged: string | undefined;
  let hm: HookManager;

  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-w112-'));
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-w112-home-'));
    origHome = process.env.HOME;
    origManaged = process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH;
    process.env.HOME = home;
    delete process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH;
    hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.setCwd(tmp);
  });

  afterEach(() => {
    if (origHome === undefined) delete process.env.HOME; else process.env.HOME = origHome;
    if (origManaged === undefined) delete process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH;
    else process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH = origManaged;
    for (const d of [tmp, home]) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } }
  });

  /** Create a project-scoped plugin bundle under <cwd>/.ami/plugins/<name>. */
  function writePluginBundle(name: string, hooks: unknown[]): void {
    const root = path.join(tmp, '.ami', 'plugins', name);
    fs.mkdirSync(path.join(root, '.ami-plugin'), { recursive: true });
    fs.writeFileSync(path.join(root, '.ami-plugin', 'plugin.json'), JSON.stringify({ name, version: '1.0.0' }));
    fs.writeFileSync(path.join(root, 'hooks.json'), JSON.stringify({ hooks }));
  }

  function writeProjectHooks(hooks: unknown[]): void {
    fs.mkdirSync(path.join(tmp, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(tmp, '.ami', 'hooks.json'), JSON.stringify({ hooks }));
  }

  it('names the plugin(s) that bound a matcher', () => {
    writePluginBundle('guardrails', [
      { event: 'preToolUse', matcher: 'bash', hook: { type: 'command', command: 'echo guard' } },
    ]);
    hm.loadFromFile(tmp);
    const events: HookEventDescription[] = hm.describeEvents();
    const pre = events.find(e => e.event === 'preToolUse')!;
    assert.ok(pre, 'preToolUse active');
    const bash = pre.matchers.find(m => m.matcher === 'bash')!;
    assert.ok(bash, 'bash matcher group present');
    assert.deepEqual(bash.pluginNames, ['guardrails'], 'matcher group names its plugin');
    // A plugin hook is attributed to its settings TIER (project) — the bare source
    // tier alone cannot reveal the plugin origin, which is exactly why pluginNames
    // is needed for per-matcher plugin attribution.
    assert.ok(bash.sources.length > 0, 'source tier still surfaced');
  });

  it('a filesystem-only matcher group carries no pluginNames', () => {
    writeProjectHooks([
      { event: 'preToolUse', matcher: 'grep', hook: { type: 'command', command: 'echo fs' } },
    ]);
    hm.loadFromFile(tmp);
    const events = hm.describeEvents();
    const pre = events.find(e => e.event === 'preToolUse')!;
    const grep = pre.matchers.find(m => m.matcher === 'grep')!;
    assert.equal(grep.pluginNames, undefined, 'no plugin attribution for a .ami/hooks.json matcher');
  });

  it('dedups and lists multiple plugins contributing the same matcher', () => {
    writePluginBundle('alpha', [
      { event: 'preToolUse', matcher: 'bash', hook: { type: 'command', command: 'echo a1' } },
      { event: 'preToolUse', matcher: 'bash', hook: { type: 'command', command: 'echo a2' } },
    ]);
    writePluginBundle('beta', [
      { event: 'preToolUse', matcher: 'bash', hook: { type: 'command', command: 'echo b1' } },
    ]);
    hm.loadFromFile(tmp);
    const events = hm.describeEvents();
    const bash = events.find(e => e.event === 'preToolUse')!.matchers.find(m => m.matcher === 'bash')!;
    assert.equal(bash.count, 3, 'all three plugin hooks grouped under bash');
    assert.deepEqual([...bash.pluginNames!].sort(), ['alpha', 'beta'], 'both plugins named, deduped');
  });
});

after(() => { /* temp dirs cleaned per-test */ });
