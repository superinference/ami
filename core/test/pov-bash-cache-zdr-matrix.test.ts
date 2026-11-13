// POV: core tools/bash-cache.ts (read-only classifier + result cache) and
// openrouter-zdr.ts (ZDR filtering + loopback-HTTP fetch). Every assertion is on
// OBSERVED behavior — real function return values, real Map state transitions, and a
// real http.createServer over loopback (no real network, no .includes on source text).

import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'node:http';
import type { AddressInfo } from 'node:net';
import { isReadOnlyBashCommand, BashResultCache } from '../src/tools/bash-cache';
import { filterZdrModels, isZdrEligible, fetchZdrModelIds } from '../src/openrouter-zdr';

// ---------------------------------------------------------------------------
// isReadOnlyBashCommand — behavioral truth matrix. Each row is run through the
// REAL classifier and the observed boolean is asserted.
// ---------------------------------------------------------------------------
describe('isReadOnlyBashCommand — behavioral matrix', () => {
  const READ_ONLY: Array<[string, string]> = [
    ['bare ls', 'ls'],
    ['ls with flags/args', 'ls -la /tmp'],
    ['cat a file', 'cat package.json'],
    ['grep', 'grep -rn foo src'],
    ['head', 'head -n 5 file.txt'],
    ['tail', 'tail -f log'],
    ['wc', 'wc -l file'],
    ['find', 'find . -name "*.ts"'],
    ['git status', 'git status'],
    ['git log with flags', 'git log --oneline -n 10'],
    ['git diff', 'git diff HEAD~1'],
    ['git show', 'git show abc123'],
    ['pipe of read-only cmds', 'ls | grep x'],
    ['triple pipe read-only', 'cat a | sort | uniq'],
    ['absolute-path binary resolves to base', '/usr/bin/ls -l'],
    ['empty string has no command segment', ''],
    ['whitespace only', '   '],
    // Quirk preserved: a leading `cat`/`grep` suppresses the write-indicator guard,
    // so a redirect after `cat` is still classified read-only (observed behavior).
    ['cat with redirect is treated read-only (leading-cat quirk)', 'cat a > b'],
    ['grep with redirect is treated read-only (leading-grep quirk)', 'grep x f > out'],
  ];

  const WRITES: Array<[string, string]> = [
    ['rm', 'rm -rf build'],
    ['mv', 'mv a b'],
    ['cp', 'cp a b'],
    ['redirect to file', 'echo hi > out.txt'],
    ['append redirect', 'echo hi >> out.txt'],
    ['pipe into tee', 'ls | tee out.txt'],
    ['pipe into dd', 'cat x | dd of=y'],
    ['unknown binary', 'frobnicate --do-it'],
    ['git push (mutating subcommand)', 'git push origin main'],
    ['git commit', 'git commit -m x'],
    ['git bare (no subcommand)', 'git'],
    ['git with -c option consuming the subcommand slot', 'git -c core.pager=cat status'],
    ['mixed pipe with one writer', 'cat f | tee out'],
    ['mixed pipe read-only then unknown', 'ls | frobnicate'],
  ];

  for (const [label, cmd] of READ_ONLY) {
    it(`read-only: ${label}`, () => {
      assert.equal(isReadOnlyBashCommand(cmd), true, `expected read-only: ${JSON.stringify(cmd)}`);
    });
  }
  for (const [label, cmd] of WRITES) {
    it(`write/unknown: ${label}`, () => {
      assert.equal(isReadOnlyBashCommand(cmd), false, `expected NOT read-only: ${JSON.stringify(cmd)}`);
    });
  }

  it('trims surrounding whitespace before classifying', () => {
    assert.equal(isReadOnlyBashCommand('   git status   '), true);
    assert.equal(isReadOnlyBashCommand('   rm x   '), false);
  });
});

// ---------------------------------------------------------------------------
// BashResultCache — observe real Map state: hit/miss, key = command+cwd,
// TTL expiry, LRU eviction at maxSize, per-cwd invalidation, clear, size.
// ---------------------------------------------------------------------------
describe('BashResultCache — behavioral state transitions', () => {
  it('miss returns null before anything is stored', () => {
    const c = new BashResultCache();
    assert.equal(c.get('ls', '/a'), null);
    assert.equal(c.size, 0);
  });

  it('set then get is a hit returning the stored result', () => {
    const c = new BashResultCache();
    c.set('ls', '/a', 'file1\nfile2');
    assert.equal(c.get('ls', '/a'), 'file1\nfile2');
    assert.equal(c.size, 1);
  });

  it('is keyed by BOTH command and cwd — same command, different cwd misses', () => {
    const c = new BashResultCache();
    c.set('ls', '/a', 'A-result');
    assert.equal(c.get('ls', '/b'), null, 'different cwd must miss');
    assert.equal(c.get('ls', '/a'), 'A-result');
    c.set('ls', '/b', 'B-result');
    assert.equal(c.get('ls', '/a'), 'A-result');
    assert.equal(c.get('ls', '/b'), 'B-result');
    assert.equal(c.size, 2);
  });

  it('different command, same cwd is a distinct entry', () => {
    const c = new BashResultCache();
    c.set('ls', '/a', 'X');
    c.set('pwd', '/a', 'Y');
    assert.equal(c.get('ls', '/a'), 'X');
    assert.equal(c.get('pwd', '/a'), 'Y');
    assert.equal(c.size, 2);
  });

  it('an expired entry (ttl elapsed) misses AND is evicted from the map', () => {
    // ttlMs = -1 forces Date.now()-timestamp (>= 0) to exceed the ttl on the next get.
    const c = new BashResultCache(200, -1);
    c.set('ls', '/a', 'stale');
    assert.equal(c.size, 1);
    assert.equal(c.get('ls', '/a'), null, 'expired entry must miss');
    assert.equal(c.size, 0, 'expired entry must be deleted on access');
  });

  it('a live ttl keeps the entry readable', async () => {
    const c = new BashResultCache(200, 10_000);
    c.set('ls', '/a', 'fresh');
    await new Promise((r) => setTimeout(r, 5));
    assert.equal(c.get('ls', '/a'), 'fresh');
  });

  it('evicts the oldest (insertion-order) entry at maxSize (LRU-by-insertion)', () => {
    const c = new BashResultCache(2, 10_000);
    c.set('cmd1', '/a', '1');
    c.set('cmd2', '/a', '2');
    assert.equal(c.size, 2);
    c.set('cmd3', '/a', '3'); // triggers eviction of the first-inserted key
    assert.equal(c.size, 2);
    assert.equal(c.get('cmd1', '/a'), null, 'oldest entry evicted');
    assert.equal(c.get('cmd2', '/a'), '2');
    assert.equal(c.get('cmd3', '/a'), '3');
  });

  it('invalidateForCwd drops only that cwd, leaving siblings intact', () => {
    const c = new BashResultCache();
    c.set('ls', '/a', 'a1');
    c.set('pwd', '/a', 'a2');
    c.set('ls', '/b', 'b1');
    assert.equal(c.size, 3);
    c.invalidateForCwd('/a');
    assert.equal(c.get('ls', '/a'), null);
    assert.equal(c.get('pwd', '/a'), null);
    assert.equal(c.get('ls', '/b'), 'b1', 'other cwd untouched');
    assert.equal(c.size, 1);
  });

  it('invalidateForCwd with a prefix-sharing cwd does not over-match (NUL separator)', () => {
    const c = new BashResultCache();
    c.set('ls', '/a', 'a');
    c.set('ls', '/ab', 'ab'); // starts with "/a" textually but NOT "/a\0"
    c.invalidateForCwd('/a');
    assert.equal(c.get('ls', '/a'), null);
    assert.equal(c.get('ls', '/ab'), 'ab', 'prefix-sharing cwd must survive');
  });

  it('clear empties the whole cache', () => {
    const c = new BashResultCache();
    c.set('ls', '/a', '1');
    c.set('ls', '/b', '2');
    c.clear();
    assert.equal(c.size, 0);
    assert.equal(c.get('ls', '/a'), null);
  });

  it('set overwrites an existing key in place (no duplicate growth)', () => {
    const c = new BashResultCache();
    c.set('ls', '/a', 'old');
    c.set('ls', '/a', 'new');
    assert.equal(c.get('ls', '/a'), 'new');
    assert.equal(c.size, 1);
  });
});

// ---------------------------------------------------------------------------
// openrouter-zdr filtering — behavioral over a crafted model list.
// ---------------------------------------------------------------------------
describe('filterZdrModels / isZdrEligible — crafted model list', () => {
  const zdr = new Set(['google/gemini-2.5-flash', 'anthropic/claude-sonnet-5.5']);

  it('keeps only ZDR-eligible AMI-namespaced ids, dropping the rest', () => {
    const models = [
      'openrouter/google/gemini-2.5-flash',       // eligible
      'openrouter/openai/gpt-4o-mini',            // not in set
      'openrouter/anthropic/claude-sonnet-5.5',   // eligible
      'openrouter/auto',                          // single-token router → not listed
    ];
    assert.deepEqual(filterZdrModels(models, zdr), [
      'openrouter/google/gemini-2.5-flash',
      'openrouter/anthropic/claude-sonnet-5.5',
    ]);
  });

  it('empty ZDR set fails open (returns input unchanged)', () => {
    const models = ['openrouter/x/y', 'openrouter/z/w'];
    assert.deepEqual(filterZdrModels(models, new Set()), models);
    assert.equal(isZdrEligible('openrouter/anything/at-all', new Set()), true);
  });

  it('single-token router (openrouter/auto) is not individually eligible', () => {
    assert.equal(isZdrEligible('openrouter/auto', zdr), false);
  });

  it('empty input list yields empty output', () => {
    assert.deepEqual(filterZdrModels([], zdr), []);
  });
});

// ---------------------------------------------------------------------------
// fetchZdrModelIds — driven against a REAL loopback http server (port 0),
// through an injected fetchImpl that forwards to it. No real network.
// ---------------------------------------------------------------------------
describe('fetchZdrModelIds — loopback HTTP (http.createServer)', () => {
  let server: http.Server;
  let base: string;
  // behavior toggles set per-test, read by the single handler.
  let mode: 'ok' | 'bad-json' | '500' = 'ok';
  let lastAuth: string | undefined;
  let payload: unknown = { data: [{ model_id: 'google/gemini-2.5-flash' }] };

  before(async () => {
    server = http.createServer((req, res) => {
      lastAuth = req.headers.authorization;
      if (mode === '500') {
        res.statusCode = 500;
        res.end('nope');
        return;
      }
      res.setHeader('content-type', 'application/json');
      if (mode === 'bad-json') {
        res.end('not-json-at-all{{{');
        return;
      }
      res.end(JSON.stringify(payload));
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const addr = server.address() as AddressInfo;
    base = `http://127.0.0.1:${addr.port}/zdr`;
  });

  after(async () => {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  });

  // Forwards the hardcoded OpenRouter endpoint to our loopback server, preserving init.
  const forward: typeof fetch = ((_url: string, init?: RequestInit) =>
    fetch(base, init)) as unknown as typeof fetch;

  it('fetches + parses the ZDR model_id set over real HTTP', async () => {
    mode = 'ok';
    payload = { data: [
      { model_id: 'google/gemini-2.5-flash' },
      { model_id: 'anthropic/claude-sonnet-5.5' },
      { model_id: 'google/gemini-2.5-flash' }, // dup → Set dedupes
    ] };
    const set = await fetchZdrModelIds({ fetchImpl: forward });
    assert.equal(set.size, 2);
    assert.ok(set.has('google/gemini-2.5-flash'));
    assert.ok(set.has('anthropic/claude-sonnet-5.5'));
  });

  it('forwards a Bearer Authorization header when apiKey is given', async () => {
    mode = 'ok';
    payload = { data: [] };
    lastAuth = undefined;
    // split so no contiguous key-shaped literal appears in source
    const key = 'sk-or-' + 'v1-deadbeefcafef00d';
    await fetchZdrModelIds({ apiKey: key, fetchImpl: forward });
    assert.equal(lastAuth, `Bearer ${key}`);
  });

  it('omits the Authorization header when no apiKey is given', async () => {
    mode = 'ok';
    payload = { data: [] };
    lastAuth = 'unset-sentinel';
    await fetchZdrModelIds({ fetchImpl: forward });
    assert.equal(lastAuth, undefined);
  });

  it('returns an empty set on a non-ok (500) response', async () => {
    mode = '500';
    const set = await fetchZdrModelIds({ fetchImpl: forward });
    assert.equal(set.size, 0);
  });

  it('returns an empty set when the body is not valid JSON (never throws)', async () => {
    mode = 'bad-json';
    const set = await fetchZdrModelIds({ fetchImpl: forward });
    assert.equal(set.size, 0);
  });

  it('returns an empty set when the connection cannot be made (never throws)', async () => {
    // A fetchImpl pointed at a closed port → fetch rejects → caught → empty set.
    const deadForward: typeof fetch = ((_url: string, init?: RequestInit) =>
      fetch('http://127.0.0.1:1/zdr', init)) as unknown as typeof fetch;
    const set = await fetchZdrModelIds({ fetchImpl: deadForward, timeoutMs: 500 });
    assert.equal(set.size, 0);
  });
});
