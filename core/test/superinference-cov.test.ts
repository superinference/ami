import { describe, it, mock, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as path from 'node:path';
import type { StreamChunk } from '../src/types';

import { Retriever } from '../src/superinference/retriever';
import { Critic } from '../src/superinference/critic';
import { Skillbook } from '../src/skillbook';
import { SkillManager } from '../src/skills';
import {
  sanitizeSkillArgs,
  validateSkillArgs,
  MAX_SKILL_ARG_NAMES,
  MAX_SKILL_ARG_NAME_LENGTH,
  MAX_SKILL_ARG_VALUE_LENGTH,
  MAX_SKILL_ARG_TOTAL_CHARS,
} from '../src/skills';

// Temp-dir bookkeeping so every fixture is cleaned up.
const tmpDirs: string[] = [];
function mkTmp(prefix: string): string {
  const d = fs.mkdtempSync(path.join(require('os').tmpdir(), prefix));
  tmpDirs.push(d);
  return d;
}
after(() => {
  for (const d of tmpDirs) {
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
});

// Minimal MemoryManager / MemoryGate duck-typed stand-ins. The Retriever only
// calls loadProjectInstructions/loadMemories and getApprovedContext, so plain
// objects suffice and keep the test off the filesystem.
function makeMemory(opts: {
  instructions?: string;
  memories?: Array<{ content: string }>;
}): any {
  return {
    loadProjectInstructions: () => opts.instructions ?? '',
    loadMemories: () => opts.memories ?? [],
  };
}
function makeGate(approved: string): any {
  return { getApprovedContext: () => approved };
}

// ===========================================================================
// Retriever — noise channel (Eq. 2) and relevance scoring
// ===========================================================================
describe('Retriever — noise channel and relevance', () => {
  it('clamps the noise level into [0,1]', () => {
    assert.equal(new Retriever(-5).noiseLevel, 0);
    assert.equal(new Retriever(5).noiseLevel, 1);
    assert.equal(new Retriever(0.3).noiseLevel, 0.3);
    assert.equal(new Retriever().noiseLevel, 0.1);
  });

  it('η=0 is perfect retrieval: nothing is dropped', () => {
    const r = new Retriever(0);
    const out = r.retrieve(
      'authentication token',
      makeMemory({
        instructions: 'PROJECT RULES',
        memories: [{ content: 'authentication token handling notes' }],
      }),
      makeGate('approved alpha\n\napproved beta'),
    );
    assert.ok(out.includes('PROJECT RULES'));
    assert.ok(out.includes('Previously Approved Results'));
    assert.ok(out.includes('approved alpha'));
    assert.ok(out.includes('Stored Memories'));
    assert.ok(out.includes('authentication token handling'));
  });

  it('η=1 is maximally degraded: approved + memory sections are dropped', () => {
    const r = new Retriever(1);
    const out = r.retrieve(
      'token',
      makeMemory({
        instructions: 'KEEP ME',
        memories: [{ content: 'token notes' }],
      }),
      makeGate('approved entry'),
    );
    // Instructions are NOT subject to the noise channel.
    assert.ok(out.includes('KEEP ME'));
    assert.ok(!out.includes('Previously Approved Results'));
    assert.ok(!out.includes('Stored Memories'));
  });

  it('0<η<1 exercises the per-entry drop filter — keep case', () => {
    const rnd = mock.method(Math, 'random', () => 0.9); // 0.9 > 0.5 → keep
    try {
      const r = new Retriever(0.5);
      const out = r.retrieve(
        'token',
        makeMemory({ memories: [{ content: 'token one' }, { content: 'token two' }] }),
        makeGate('aaa\n\nbbb'),
      );
      assert.ok(out.includes('Previously Approved Results'));
      assert.ok(out.includes('aaa'));
      assert.ok(out.includes('Stored Memories'));
      assert.ok(out.includes('token one'));
      assert.ok(rnd.mock.callCount() > 0);
    } finally {
      rnd.mock.restore();
    }
  });

  it('0<η<1 exercises the per-entry drop filter — drop case', () => {
    const rnd = mock.method(Math, 'random', () => 0.1); // 0.1 > 0.5 is false → drop
    try {
      const r = new Retriever(0.5);
      const out = r.retrieve(
        'token',
        makeMemory({ memories: [{ content: 'token one' }] }),
        makeGate('aaa\n\nbbb'),
      );
      assert.ok(!out.includes('Previously Approved Results'));
      assert.ok(!out.includes('Stored Memories'));
    } finally {
      rnd.mock.restore();
    }
  });

  it('empty query treats everything as relevant', () => {
    const r = new Retriever(0);
    const out = r.retrieve(
      '',
      makeMemory({ memories: [{ content: 'anything at all' }] }),
      makeGate(''),
    );
    assert.ok(out.includes('anything at all'));
  });

  it('query of only short tokens falls back to relevant', () => {
    const r = new Retriever(0);
    // "a an is" -> all tokens length<=2 -> queryTokens empty -> relevance 1
    const out = r.retrieve(
      'a an is',
      makeMemory({ memories: [{ content: 'some longer stored content' }] }),
      makeGate(''),
    );
    assert.ok(out.includes('some longer stored content'));
  });

  it('non-matching memories are filtered out by relevance', () => {
    const r = new Retriever(0);
    const out = r.retrieve(
      'kubernetes deployment',
      makeMemory({ memories: [{ content: 'completely unrelated cooking recipe' }] }),
      makeGate(''),
    );
    assert.ok(!out.includes('Stored Memories'));
  });

  it('truncates long memory content to 300 chars', () => {
    const r = new Retriever(0);
    const long = 'token ' + 'x'.repeat(1000);
    const out = r.retrieve(
      'token',
      makeMemory({ memories: [{ content: long }] }),
      makeGate(''),
    );
    assert.ok(out.includes('Stored Memories'));
    // The 'xxxx...' slice is capped: 300-char entry, not the full 1006.
    assert.ok(!out.includes('x'.repeat(400)));
  });

  it('returns empty string when nothing is available', () => {
    const r = new Retriever(0);
    const out = r.retrieve('q', makeMemory({}), makeGate(''));
    assert.equal(out, '');
  });
});

// ===========================================================================
// Critic — fail-closed catch path via DI seam (mjs catch block)
// ===========================================================================
describe('Critic — evaluate fail-closed path', () => {
  const provider = { apiKey: 'sk-test', model: 'gpt-4o', baseUrl: 'http://127.0.0.1:1/v1' } as any;

  it('fail-closes when the stream throws an Error (uses err.message)', async () => {
    const boom = (() => { throw new Error('stream exploded'); }) as unknown as typeof import('../src/provider').streamChatCompletion;
    const critic = new Critic(0.05, 0.1, boom);
    const res = await critic.evaluate('q', 'r', provider, new AbortController().signal);
    assert.equal(res.approved, false);
    assert.equal(res.score, 0);
    assert.match(res.reason, /Critic evaluation failed: stream exploded/);
  });

  it('fail-closes when the stream throws a non-Error (uses String(err))', async () => {
    // eslint-disable-next-line @typescript-eslint/only-throw-error
    const boom = (() => { throw 'raw-string-failure'; }) as unknown as typeof import('../src/provider').streamChatCompletion;
    const critic = new Critic(0.05, 0.1, boom);
    const res = await critic.evaluate('q', 'r', provider, new AbortController().signal);
    assert.equal(res.approved, false);
    assert.equal(res.score, 0);
    assert.match(res.reason, /Critic evaluation failed: raw-string-failure/);
  });

  it('parses a valid JSON decision streamed back (approved + clamped score)', async () => {
    async function* okStream(): AsyncGenerator<StreamChunk> {
      yield { type: 'content_delta', text: '{"approved": true, ' } as StreamChunk;
      yield { type: 'content_delta', text: '"score": 2.5, "reason": "good"}' } as StreamChunk;
      yield { type: 'done' } as StreamChunk;
    }
    const critic = new Critic(0.05, 0.1, (() => okStream()) as any);
    const res = await critic.evaluate('q', 'r', provider, new AbortController().signal);
    assert.equal(res.approved, true);
    assert.equal(res.score, 1); // clamped from 2.5
    assert.equal(res.reason, 'good');
  });

  it('defaults score to 1.0 on approval when score is missing/non-numeric', async () => {
    async function* s(): AsyncGenerator<StreamChunk> {
      yield { type: 'content_delta', text: '{"approved": true, "reason": "ok"}' } as StreamChunk;
    }
    const critic = new Critic(0.05, 0.1, (() => s()) as any);
    const res = await critic.evaluate('q', 'r', provider, new AbortController().signal);
    assert.equal(res.approved, true);
    assert.equal(res.score, 1.0);
  });

  it('defaults score to 0.0 on rejection when score is missing', async () => {
    async function* s(): AsyncGenerator<StreamChunk> {
      yield { type: 'content_delta', text: '{"approved": false}' } as StreamChunk;
    }
    const critic = new Critic(0.05, 0.1, (() => s()) as any);
    const res = await critic.evaluate('q', 'r', provider, new AbortController().signal);
    assert.equal(res.approved, false);
    assert.equal(res.score, 0.0);
    assert.equal(res.reason, '');
  });

  it('ignores non-content chunks and fails closed when no JSON arrives', async () => {
    async function* s(): AsyncGenerator<StreamChunk> {
      yield { type: 'content_delta' } as StreamChunk; // no .text → skipped
      yield { type: 'done' } as StreamChunk;
    }
    const critic = new Critic(0.05, 0.1, (() => s()) as any);
    const res = await critic.evaluate('q', 'r', provider, new AbortController().signal);
    assert.equal(res.approved, false);
    assert.match(res.reason, /no valid JSON/);
  });
});

describe('Critic — extractJSON edge cases', () => {
  const c = new Critic();
  it('returns null when there is no opening brace', () => {
    assert.equal(c.extractJSON('no json here'), null);
  });
  it('handles escaped quotes and braces inside strings', () => {
    const parsed = c.extractJSON('prefix {"reason": "a \\" } brace", "approved": true} suffix');
    assert.deepEqual(parsed, { reason: 'a " } brace', approved: true });
  });
  it('returns null on unbalanced braces (never closes)', () => {
    assert.equal(c.extractJSON('{"approved": true'), null);
  });
  it('returns null when the matched span is invalid JSON', () => {
    assert.equal(c.extractJSON('{not: valid, json}'), null);
  });
});

// ===========================================================================
// Skillbook — persistence, operations, search, context
// ===========================================================================
describe('Skillbook — operations and persistence', () => {
  it('ADD creates an entry and persists it; reload reads it back', () => {
    const cwd = mkTmp('sb-add-');
    const sb = new Skillbook(cwd);
    const added = sb.apply({
      type: 'ADD',
      entry: { section: 'context', keywords: ['retry', 'backoff'], issue: 'flaky net', insight: 'use jitter' },
    });
    assert.ok(added);
    assert.ok(fs.existsSync(path.join(cwd, '.superinference', 'skillbook.json')));

    const reloaded = new Skillbook(cwd);
    assert.equal(reloaded.getAll().length, 1);
    assert.equal(reloaded.getAll()[0].insight, 'use jitter');
  });

  it('TAG increments helpful/harmful/neutral counts and returns null for missing id', () => {
    const cwd = mkTmp('sb-tag-');
    const sb = new Skillbook(cwd);
    const e = sb.apply({ type: 'ADD', entry: { section: 'context', keywords: ['k'], issue: 'i', insight: 'x' } })!;
    sb.apply({ type: 'TAG', id: e.id, feedback: 'helpful' });
    sb.apply({ type: 'TAG', id: e.id, feedback: 'harmful' });
    const tagged = sb.apply({ type: 'TAG', id: e.id, feedback: 'neutral' })!;
    assert.equal(tagged.helpfulCount, 1);
    assert.equal(tagged.harmfulCount, 1);
    assert.equal(tagged.neutralCount, 1);
    assert.equal(sb.apply({ type: 'TAG', id: 'nope', feedback: 'helpful' }), null);
  });

  it('UPDATE replaces insight and/or keywords; returns null for missing id', () => {
    const cwd = mkTmp('sb-upd-');
    const sb = new Skillbook(cwd);
    const e = sb.apply({ type: 'ADD', entry: { section: 'harness', keywords: ['a'], issue: 'i', insight: 'old' } })!;
    const u1 = sb.apply({ type: 'UPDATE', id: e.id, insight: 'new insight' })!;
    assert.equal(u1.insight, 'new insight');
    assert.deepEqual(u1.keywords, ['a']);
    const u2 = sb.apply({ type: 'UPDATE', id: e.id, keywords: ['b', 'c'] })!;
    assert.deepEqual(u2.keywords, ['b', 'c']);
    // No-op update (neither field) keeps values.
    const u3 = sb.apply({ type: 'UPDATE', id: e.id })!;
    assert.equal(u3.insight, 'new insight');
    assert.equal(sb.apply({ type: 'UPDATE', id: 'missing', insight: 'z' }), null);
  });

  it('REMOVE deletes an existing entry and returns null for a missing id', () => {
    const cwd = mkTmp('sb-rm-');
    const sb = new Skillbook(cwd);
    const e = sb.apply({ type: 'ADD', entry: { section: 'context', keywords: ['a'], issue: 'i', insight: 'x' } })!;
    const removed = sb.apply({ type: 'REMOVE', id: e.id })!;
    assert.equal(removed.id, e.id);
    assert.equal(sb.getAll().length, 0);
    assert.equal(sb.apply({ type: 'REMOVE', id: 'gone' }), null);
  });

  it('getBySection filters by section', () => {
    const cwd = mkTmp('sb-sec-');
    const sb = new Skillbook(cwd);
    sb.apply({ type: 'ADD', entry: { section: 'context', keywords: ['a'], issue: 'i', insight: 'c1' } });
    sb.apply({ type: 'ADD', entry: { section: 'harness', keywords: ['b'], issue: 'i', insight: 'h1' } });
    assert.equal(sb.getBySection('context').length, 1);
    assert.equal(sb.getBySection('harness').length, 1);
  });

  it('search matches issue/insight/keywords and sorts by helpfulCount desc', () => {
    const cwd = mkTmp('sb-search-');
    const sb = new Skillbook(cwd);
    const low = sb.apply({ type: 'ADD', entry: { section: 'context', keywords: ['database'], issue: 'slow query', insight: 'add index' } })!;
    const high = sb.apply({ type: 'ADD', entry: { section: 'context', keywords: ['database'], issue: 'deadlock', insight: 'order locks' } })!;
    sb.apply({ type: 'TAG', id: high.id, feedback: 'helpful' });
    sb.apply({ type: 'TAG', id: high.id, feedback: 'helpful' });
    sb.apply({ type: 'TAG', id: low.id, feedback: 'helpful' });
    const results = sb.search('database');
    assert.equal(results.length, 2);
    assert.equal(results[0].id, high.id); // higher helpfulCount first
    assert.equal(sb.search('nonexistentterm').length, 0);
  });

  it('getContext renders only helpful context entries, empty otherwise', () => {
    const cwd = mkTmp('sb-ctx-');
    const sb = new Skillbook(cwd);
    assert.equal(sb.getContext(), '');
    const e = sb.apply({ type: 'ADD', entry: { section: 'context', keywords: ['perf', 'cache'], issue: 'i', insight: 'cache results' } })!;
    // Not yet helpful > harmful → still empty.
    assert.equal(sb.getContext(), '');
    sb.apply({ type: 'TAG', id: e.id, feedback: 'helpful' });
    const ctx = sb.getContext();
    assert.match(ctx, /Learned Strategies/);
    assert.match(ctx, /perf, cache/);
    assert.match(ctx, /cache results/);
    assert.match(ctx, /1 helpful/);
    // Harness-section helpful entry does not appear in context.
    const h = sb.apply({ type: 'ADD', entry: { section: 'harness', keywords: ['z'], issue: 'i', insight: 'y' } })!;
    sb.apply({ type: 'TAG', id: h.id, feedback: 'helpful' });
    assert.ok(!sb.getContext().includes('"y"'));
  });

  it('loadFromDisk tolerates corrupt JSON and missing entries key', () => {
    const corrupt = mkTmp('sb-corrupt-');
    fs.mkdirSync(path.join(corrupt, '.superinference'), { recursive: true });
    fs.writeFileSync(path.join(corrupt, '.superinference', 'skillbook.json'), '{ not valid json');
    const sb1 = new Skillbook(corrupt);
    assert.equal(sb1.getAll().length, 0);

    const noEntries = mkTmp('sb-noentries-');
    fs.mkdirSync(path.join(noEntries, '.superinference'), { recursive: true });
    fs.writeFileSync(path.join(noEntries, '.superinference', 'skillbook.json'), JSON.stringify({ version: 1 }));
    const sb2 = new Skillbook(noEntries);
    assert.equal(sb2.getAll().length, 0);
  });

  it('saveToDisk swallows write failures (best-effort persistence)', () => {
    const cwd = mkTmp('sb-fail-');
    // Put a FILE where the .superinference directory needs to be → mkdirSync throws.
    fs.writeFileSync(path.join(cwd, '.superinference'), 'blocking file');
    const sb = new Skillbook(cwd);
    // apply() calls saveToDisk() internally; must not throw.
    assert.doesNotThrow(() => {
      sb.apply({ type: 'ADD', entry: { section: 'context', keywords: ['a'], issue: 'i', insight: 'x' } });
    });
    // In-memory state still updated even though persistence failed.
    assert.equal(sb.getAll().length, 1);
  });
});

// ===========================================================================
// Skill argument safety
// ===========================================================================
describe('Skill argument safety', () => {
  it('sanitizeSkillArgs caps count, name length, value length, and total chars', () => {
    const many: Record<string, string> = {};
    for (let i = 0; i < MAX_SKILL_ARG_NAMES + 10; i++) many['k' + i] = 'v';
    assert.ok(Object.keys(sanitizeSkillArgs(many)).length <= MAX_SKILL_ARG_NAMES);

    const longName = 'n'.repeat(MAX_SKILL_ARG_NAME_LENGTH + 20);
    const out = sanitizeSkillArgs({ [longName]: 'v' });
    const key = Object.keys(out)[0];
    assert.equal(key.length, MAX_SKILL_ARG_NAME_LENGTH);

    const bigVal = sanitizeSkillArgs({ a: 'x'.repeat(MAX_SKILL_ARG_VALUE_LENGTH + 100) });
    assert.equal(bigVal.a.length, MAX_SKILL_ARG_VALUE_LENGTH);

    // Total-char cap: break out of the loop before overflowing.
    const totalArgs: Record<string, string> = {};
    for (let i = 0; i < 20; i++) totalArgs['key' + i] = 'y'.repeat(MAX_SKILL_ARG_VALUE_LENGTH);
    const capped = sanitizeSkillArgs(totalArgs);
    let sum = 0;
    for (const [k, v] of Object.entries(capped)) sum += k.length + v.length;
    assert.ok(sum <= MAX_SKILL_ARG_TOTAL_CHARS);
  });

  it('sanitizeSkillArgs coerces non-string values to strings', () => {
    const out = sanitizeSkillArgs({ n: 123 as unknown as string });
    assert.equal(out.n, '123');
  });

  it('validateSkillArgs warns on too many args, long names, long values, total overflow', () => {
    assert.deepEqual(validateSkillArgs({ a: 'b' }), { valid: true, warnings: [] });

    const many: Record<string, string> = {};
    for (let i = 0; i < MAX_SKILL_ARG_NAMES + 5; i++) many['k' + i] = 'v';
    assert.ok(validateSkillArgs(many).warnings.some(w => /Too many arguments/.test(w)));

    const longName = { ['n'.repeat(MAX_SKILL_ARG_NAME_LENGTH + 1)]: 'v' };
    assert.ok(validateSkillArgs(longName).warnings.some(w => /name too long/.test(w)));

    const longVal = { a: 'x'.repeat(MAX_SKILL_ARG_VALUE_LENGTH + 1) };
    assert.ok(validateSkillArgs(longVal).warnings.some(w => /value too long/.test(w)));

    const overflow: Record<string, string> = {};
    for (let i = 0; i < 12; i++) overflow['k' + i] = 'y'.repeat(MAX_SKILL_ARG_VALUE_LENGTH);
    assert.ok(validateSkillArgs(overflow).warnings.some(w => /Total argument size/.test(w)));
  });

  it('validateSkillArgs coerces non-string values for length checks', () => {
    const res = validateSkillArgs({ n: 42 as unknown as string });
    assert.equal(res.valid, true);
  });
});

// ===========================================================================
// SkillManager — loading, matching, content substitution, context
// ===========================================================================

// Build a fully-populated project tree in a temp dir and return its cwd.
function buildSkillTree(): string {
  const cwd = mkTmp('skills-');
  const skillsDir = path.join(cwd, '.superinference', 'skills');
  const agentsDir = path.join(cwd, '.superinference', 'agents');
  fs.mkdirSync(skillsDir, { recursive: true });
  fs.mkdirSync(agentsDir, { recursive: true });

  // 1. Subdir skill with a SKILL.md carrying a full frontmatter block,
  //    including `**` and `?` glob paths to drive matchGlob.
  const subA = path.join(skillsDir, 'deployer');
  fs.mkdirSync(subA, { recursive: true });
  fs.writeFileSync(path.join(subA, 'SKILL.md'), [
    '---',
    'description: Deploy helper',
    'when-to-use: before releases',
    'paths: ["src/**/*.ts", "config.???"]',
    'model: gpt-4o',
    'allowed-tools: bash, file_read',
    'disallowed-tools: [web_fetch]',
    'argument-hint: <env>',
    'context: fork',
    'disable-model-invocation: true',
    'user-invocable: false',
    'effort: high',
    '---',
    'Deploy to ${MODEL} from ${SKILL_DIR} in ${CWD} on ${DATE}.',
    'Args: $ARGUMENTS pos1=$1 pos9=$9 named=${CUSTOM}',
  ].join('\n'));

  // 2. Direct .md skill file in the skills dir (inline context, user-invocable true).
  fs.writeFileSync(path.join(skillsDir, 'greet.md'), [
    '---',
    'name: greeter',
    'description: Say hi',
    'when-to-use: when greeting the user',
    'context: inline',
    'user-invocable: true',
    '---',
    'Hello ${ARGUMENTS}! full=$0 braced=${ARGUMENTS}',
  ].join('\n'));

  // 3. Skill whose body embeds a live bash injection (success + failure markers).
  fs.writeFileSync(path.join(skillsDir, 'live.md'), [
    '---',
    'name: live',
    'description: live cmd',
    '---',
    'OK=!`printf ok` FAIL=!`exit 7`',
  ].join('\n'));

  // 4. Subdirectory WITHOUT a SKILL.md → parseSkillFile hits readFile catch → null.
  fs.mkdirSync(path.join(skillsDir, 'emptydir'), { recursive: true });

  // 5. A broken symlink entry → statSync throws → per-entry catch.
  try {
    fs.symlinkSync(path.join(cwd, 'does-not-exist-target'), path.join(skillsDir, 'broken-link'));
  } catch { /* symlink unsupported — other cases still cover the catch indirectly */ }

  // 6. A .md skill file with no body → parseSkillFile returns null.
  fs.writeFileSync(path.join(skillsDir, 'nobody.md'), '---\nname: nobody\ndescription: d\n---\n   ');

  // 6b. A .md skill with NO name field (basename-derived) + an empty-array
  //     frontmatter list + globs that exercise the fallback regex arm and a
  //     trailing `**`.
  // NOTE: no `description` field → exercises the `description || ''` fallback.
  fs.writeFileSync(path.join(skillsDir, 'extra.md'), [
    '---',
    'paths: ["*.ts", "docs/**"]',
    'allowed-tools: []',
    '---',
    'Extra body.',
  ].join('\n'));

  // 6c. A non-.md, non-directory file in the skills dir → skipped by loader.
  fs.writeFileSync(path.join(skillsDir, 'notes.txt'), 'ignore me');

  // Agents ------------------------------------------------------------------
  fs.writeFileSync(path.join(agentsDir, 'reviewer.md'), [
    '---',
    'description: Reviews code',
    'model: claude-opus',
    'max-turns: 7',
    'tools: bash, grep',
    'disallowed-tools: web_fetch',
    'permission-mode: ask',
    'auto-compact-window: 8000',
    '---',
    'You are a reviewer.',
  ].join('\n'));

  // Agent with invalid numeric fields and invalid permission-mode → undefined paths.
  // NOTE: no `description` field → exercises the agent `description || ''` fallback.
  fs.writeFileSync(path.join(agentsDir, 'loose.md'), [
    '---',
    'max-turns: notanumber',
    'permission-mode: bogus',
    'auto-compact-window: alsobad',
    '---',
    'Loose body.',
  ].join('\n'));

  // Agent with empty body → parseAgentFile returns null.
  fs.writeFileSync(path.join(agentsDir, 'empty.md'), '---\nname: emptyagent\n---\n');

  // A directory named like a .md file → readFileSync throws EISDIR → null.
  fs.mkdirSync(path.join(agentsDir, 'dir.md'), { recursive: true });

  // A non-.md file in the agents dir → filtered out before parsing.
  fs.writeFileSync(path.join(agentsDir, 'notes.txt'), 'ignore me too');

  // 7. Plugin bundle under .superinference/plugins → exercises plugin loading loop.
  const pluginRoot = path.join(cwd, '.superinference', 'plugins', 'myplugin');
  fs.mkdirSync(path.join(pluginRoot, '.superinference-plugin'), { recursive: true });
  fs.writeFileSync(
    path.join(pluginRoot, '.superinference-plugin', 'plugin.json'),
    JSON.stringify({ name: 'myplugin', version: '1.2.3' }),
  );
  fs.mkdirSync(path.join(pluginRoot, 'skills'), { recursive: true });
  fs.writeFileSync(path.join(pluginRoot, 'skills', 'plug.md'), [
    '---', 'name: plugskill', 'description: from plugin', '---', 'Plugin skill body.',
  ].join('\n'));
  fs.mkdirSync(path.join(pluginRoot, 'agents'), { recursive: true });
  fs.writeFileSync(path.join(pluginRoot, 'agents', 'plugagent.md'), [
    '---', 'name: plugagent', 'description: agent from plugin', '---', 'Plugin agent body.',
  ].join('\n'));

  // 7b. A barebones plugin bundle with a manifest but no skills/agents dirs →
  //     exercises the false branch of the bundle.skillsDir / agentsDir guards.
  const bare = path.join(cwd, '.superinference', 'plugins', 'barebones');
  fs.mkdirSync(path.join(bare, '.superinference-plugin'), { recursive: true });
  fs.writeFileSync(
    path.join(bare, '.superinference-plugin', 'plugin.json'),
    JSON.stringify({ name: 'barebones', version: '0.1.0' }),
  );

  // 8. AGENTS.md at project root → loadAgentsFile + getSkillContext section.
  fs.writeFileSync(path.join(cwd, 'AGENTS.md'), '# Project Rules\n\nFollow them.');

  return cwd;
}

describe('SkillManager — loading and discovery', () => {
  const cwd = buildSkillTree();
  const sm = new SkillManager(cwd);

  it('loads built-in skills and agents', () => {
    assert.ok(sm.getSkill('code-review'));
    assert.ok(sm.getSkill('explain'));
    assert.ok(sm.getAgent('verifier'));
    assert.ok(sm.getAgent('code-graph'));
  });

  it('loads subdir SKILL.md with full frontmatter parsed', () => {
    const s = sm.getSkill('deployer')!;
    assert.ok(s);
    assert.equal(s.whenToUse, 'before releases');
    assert.deepEqual(s.paths, ['src/**/*.ts', 'config.???']);
    assert.equal(s.model, 'gpt-4o');
    assert.deepEqual(s.allowedTools, ['bash', 'file_read']);
    assert.deepEqual(s.disallowedTools, ['web_fetch']);
    assert.equal(s.argumentHint, '<env>');
    assert.equal(s.context, 'fork');
    assert.equal(s.disableModelInvocation, true);
    assert.equal(s.userInvocable, false);
    assert.equal(s.effort, 'high');
  });

  it('loads direct .md skill and the plugin-provided skill/agent', () => {
    assert.equal(sm.getSkill('greeter')!.context, 'inline');
    assert.ok(sm.getSkill('plugskill'));
    assert.ok(sm.getAgent('plugagent'));
  });

  it('skips entries with no body and unreadable/dir entries', () => {
    assert.equal(sm.getSkill('nobody'), null);
    assert.equal(sm.getSkill('emptydir'), null);
    assert.equal(sm.getAgent('emptyagent'), null);
  });

  it('parses agent frontmatter including invalid-numeric fallbacks', () => {
    const a = sm.getAgent('reviewer')!;
    assert.equal(a.model, 'claude-opus');
    assert.equal(a.maxTurns, 7);
    assert.deepEqual(a.tools, ['bash', 'grep']);
    assert.deepEqual(a.disallowedTools, ['web_fetch']);
    assert.equal(a.permissionMode, 'ask');
    assert.equal(a.autoCompactWindow, 8000);

    const loose = sm.getAgent('loose')!;
    assert.equal(loose.maxTurns, undefined);
    assert.equal(loose.permissionMode, undefined);
    assert.equal(loose.autoCompactWindow, undefined);
    assert.equal(loose.description, ''); // description-less frontmatter
  });

  it('listSkills omits non-user-invocable skills', () => {
    const names = sm.listSkills().map(s => s.name);
    assert.ok(names.includes('greeter'));
    assert.ok(!names.includes('deployer')); // user-invocable: false
    assert.ok(sm.listAgents().length >= 2);
  });

  it('getSkill / getAgent return null for unknown names', () => {
    assert.equal(sm.getSkill('ghost'), null);
    assert.equal(sm.getAgent('ghost'), null);
  });
});

describe('SkillManager — glob matching', () => {
  const cwd = buildSkillTree();
  const sm = new SkillManager(cwd);

  it('matches ** globs across path segments', () => {
    const matched = sm.findMatchingSkills(['src/deep/nested/file.ts']);
    assert.ok(matched.some(s => s.name === 'deployer'));
  });

  it('matches ? single-char globs', () => {
    const matched = sm.findMatchingSkills(['config.abc']);
    assert.ok(matched.some(s => s.name === 'deployer'));
  });

  it('returns nothing for non-matching paths and skills without paths', () => {
    assert.equal(sm.findMatchingSkills(['README.md']).length, 0);
    // greeter has no paths → never contributes.
    assert.ok(!sm.findMatchingSkills(['anything']).some(s => s.name === 'greeter'));
  });

  it('matches *.ts via the suffix (non-anchored) regex arm', () => {
    const matched = sm.findMatchingSkills(['lib/util.ts']);
    assert.ok(matched.some(s => s.name === 'extra'));
  });

  it('evaluates a trailing ** pattern without matching a bare file', () => {
    // 'docs/readme' forces matchGlob('docs/**', ...) to run (the ** branch)
    // after '*.ts' misses; it does not match a non-directory leaf.
    assert.ok(!sm.findMatchingSkills(['docs/readme']).some(s => s.name === 'extra'));
  });

  it('derives skill name from basename and parses an empty-array list', () => {
    const extra = sm.getSkill('extra')!;
    assert.ok(extra);
    assert.deepEqual(extra.allowedTools, []);
    assert.equal(extra.description, ''); // description-less frontmatter
  });
});

describe('SkillManager — content substitution', () => {
  const cwd = buildSkillTree();
  const sm = new SkillManager(cwd);

  it('substitutes built-in vars and defaults MODEL when absent', () => {
    // greeter has no model → ${MODEL} would resolve to 'default' if present.
    const content = sm.getSkillContent('greeter', { ARGUMENTS: 'world' })!;
    assert.match(content, /Hello world!/);
    // $0 and ${ARGUMENTS} forms both expand to the full argument string.
    assert.match(content, /full=world braced=world/);
  });

  it('substitutes SKILL_DIR, CWD, MODEL, DATE, ARGUMENTS, positional, and named args', () => {
    const content = sm.getSkillContent('deployer', { ARGUMENTS: 'prod extra', CUSTOM: 'XYZ' })!;
    assert.match(content, /Deploy to gpt-4o from/);
    assert.ok(content.includes(path.join(cwd, '.superinference', 'skills', 'deployer')));
    assert.ok(content.includes(cwd));
    assert.match(content, /\d{4}-\d{2}-\d{2}/); // DATE
    assert.match(content, /Args: prod extra pos1=prod pos9= named=XYZ/);
  });

  it('getSkillContent without args leaves placeholders for arg vars', () => {
    const content = sm.getSkillContent('deployer')!;
    assert.match(content, /Args: \$ARGUMENTS pos1=\$1/);
  });

  it('getSkillContent with named args but no ARGUMENTS key skips positional expansion', () => {
    const content = sm.getSkillContent('deployer', { CUSTOM: 'ZZZ' })!;
    assert.match(content, /named=ZZZ/);
    // $ARGUMENTS / positional untouched because ARGUMENTS key is absent.
    assert.match(content, /Args: \$ARGUMENTS pos1=\$1/);
  });

  it('returns null for an unknown skill', () => {
    assert.equal(sm.getSkillContent('nope'), null);
  });

  it('expands live bash injections, marking failures with [error: ...]', () => {
    const content = sm.getSkillContent('live')!;
    assert.match(content, /OK=ok/);
    assert.match(content, /FAIL=\[error:/);
  });

  it('uses builtin SKILL_DIR = cwd for a built-in skill', () => {
    // code-review is built-in; its content has no vars but the <builtin> branch runs.
    const content = sm.getSkillContent('code-review');
    assert.ok(content && content.length > 0);
  });
});

describe('SkillManager — context assembly', () => {
  it('assembles AGENTS.md + skills (whenToUse) + agents (model/maxTurns)', () => {
    const cwd = buildSkillTree();
    const sm = new SkillManager(cwd);
    const ctx = sm.getSkillContext();
    assert.match(ctx, /## AGENTS\.md/);
    assert.match(ctx, /Project Rules/);
    assert.match(ctx, /## Available Skills/);
    assert.match(ctx, /\/greeter/);
    assert.match(ctx, /When: when greeting the user/); // user-invocable skill whenToUse
    assert.match(ctx, /## Available Agents/);
    assert.match(ctx, /Model: claude-opus/);
    assert.match(ctx, /Max turns: 7/);
  });

  it('loadAgentsFile returns empty string when AGENTS.md is absent', () => {
    const cwd = mkTmp('no-agents-');
    const sm = new SkillManager(cwd);
    assert.equal(sm.loadAgentsFile(), '');
    // getSkillContext still returns builtin skills/agents sections.
    const ctx = sm.getSkillContext();
    assert.ok(!ctx.includes('## AGENTS.md'));
    assert.match(ctx, /## Available Skills/);
  });
});
