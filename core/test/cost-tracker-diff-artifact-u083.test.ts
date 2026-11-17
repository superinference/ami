import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { readFileSync, existsSync } from 'node:fs';

import {
  CacheMissDiagnostics,
  buildDiffableContent,
  createUnifiedPatch,
  writeCacheBreakDiff,
  type CacheDiagRequest,
  type DiffableTool,
} from '../src/cost-tracker';

// ---------------------------------------------------------------------------
// U083 — Prompt-state diff artifact on a prompt-cache break.
//
// Claude Code serializes the before/after prompt state (model + system prompt +
// sorted per-tool name/description/schema) and writes a unified diff to a temp
// file whose path is surfaced in the break warning. These tests exercise the AMI
// port: buildDiffableContent(), createUnifiedPatch(), writeCacheBreakDiff(), and
// CacheMissDiagnostics.getLastDiffPath().
// ---------------------------------------------------------------------------

const TOOLS: DiffableTool[] = [
  { name: 'bash', description: 'run a shell command', inputSchema: { type: 'object', properties: { cmd: { type: 'string' } } } },
  { name: 'read', description: 'read a file', inputSchema: { type: 'object', properties: { path: { type: 'string' } } } },
];

describe('buildDiffableContent (U083)', () => {
  it('renders model + system prompt + sorted per-tool schema', () => {
    const out = buildDiffableContent('gpt-4o', 'You are helpful.', TOOLS);
    assert.match(out, /^Model: gpt-4o/);
    assert.match(out, /=== System Prompt ===\n\nYou are helpful\./);
    assert.match(out, /=== Tools \(2\) ===/);
    assert.match(out, /bash\n {2}description: run a shell command/);
    assert.match(out, /read\n {2}description: read a file/);
  });

  it('is order-stable: reordering the advertised tools yields identical content', () => {
    const a = buildDiffableContent('m', 'sys', TOOLS);
    const b = buildDiffableContent('m', 'sys', [TOOLS[1], TOOLS[0]]);
    assert.equal(a, b);
  });

  it('tolerates missing fields and unserializable schemas', () => {
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    const out = buildDiffableContent('m', 'sys', [{ name: 'x' }, { inputSchema: cyclic }]);
    assert.match(out, /x\n {2}description: \n {2}input_schema: $/m);
    assert.match(out, /input_schema: \[unserializable\]/);
  });
});

describe('createUnifiedPatch (U083)', () => {
  it('returns empty string for identical content', () => {
    assert.equal(createUnifiedPatch('f', 'a\nb\nc\n', 'a\nb\nc\n'), '');
  });

  it('emits a unified diff with a hunk header and +/- lines for a localized change', () => {
    const before = 'l1\nl2\nl3\nl4\nl5\nl6\n';
    const after = 'l1\nl2\nl3\nCHANGED\nl5\nl6\n';
    const patch = createUnifiedPatch('prompt-state', before, after, 'before', 'after');
    assert.match(patch, /^--- prompt-state\tbefore\n/);
    assert.match(patch, /\+\+\+ prompt-state\tafter\n/);
    assert.match(patch, /@@ -\d+,\d+ \+\d+,\d+ @@/);
    assert.match(patch, /^-l4$/m);
    assert.match(patch, /^\+CHANGED$/m);
    // Unchanged context lines are present (prefixed by a space), not every line.
    assert.match(patch, /^ l3$/m);
  });

  it('keeps distant changes in separate hunks', () => {
    const lines = Array.from({ length: 40 }, (_, i) => `line${i}`);
    const before = lines.join('\n') + '\n';
    const changed = [...lines];
    changed[1] = 'A';
    changed[38] = 'B';
    const after = changed.join('\n') + '\n';
    const patch = createUnifiedPatch('f', before, after);
    const hunks = patch.match(/@@ /g) ?? [];
    assert.equal(hunks.length, 2);
  });

  it('handles pure additions and pure deletions', () => {
    assert.match(createUnifiedPatch('f', '', 'new\n'), /^\+new$/m);
    assert.match(createUnifiedPatch('f', 'old\n', ''), /^-old$/m);
  });
});

describe('writeCacheBreakDiff (U083)', () => {
  it('writes a real unified-diff file and returns its path', () => {
    const prev = buildDiffableContent('gpt-4o', 'sys v1', TOOLS);
    const next = buildDiffableContent('gpt-4o', 'sys v2', TOOLS);
    const path = writeCacheBreakDiff(prev, next);
    assert.ok(path, 'expected a diff path');
    assert.ok(existsSync(path!));
    const content = readFileSync(path!, 'utf8');
    assert.match(content, /@@ /);
    assert.match(content, /-=== System Prompt ===|^-sys v1$/m);
  });

  it('returns undefined when the states are identical (nothing to diff)', () => {
    const same = buildDiffableContent('m', 'sys', TOOLS);
    assert.equal(writeCacheBreakDiff(same, same), undefined);
  });
});

describe('CacheMissDiagnostics.getLastDiffPath (U083)', () => {
  function req(over: Partial<CacheDiagRequest>): CacheDiagRequest {
    return {
      toolDefHash: 'h',
      systemPromptHash: 'sp',
      thinkingMode: 'none',
      model: 'gpt-4o',
      ...over,
    };
  }

  it('writes a diff on a full cache break when both requests supply diffable content', () => {
    const d = new CacheMissDiagnostics();
    // First request (baseline) — cache hit.
    d.recordRequest(req({
      systemPromptHash: 'sp1',
      buildDiffableContent: () => buildDiffableContent('gpt-4o', 'system v1', TOOLS),
    }));
    d.recordResponse(1000, 0);
    assert.equal(d.getLastDiffPath(), undefined);

    // Second request with a changed system prompt — full miss (cacheRead 0) → break.
    d.recordRequest(req({
      systemPromptHash: 'sp2',
      buildDiffableContent: () => buildDiffableContent('gpt-4o', 'system v2 EXTENDED', TOOLS),
    }));
    d.recordResponse(0, 500);

    const path = d.getLastDiffPath();
    assert.ok(path, 'expected a diff path after a break');
    assert.ok(existsSync(path!));
    const content = readFileSync(path!, 'utf8');
    assert.match(content, /system v1/);
    assert.match(content, /system v2 EXTENDED/);
  });

  it('does not write a diff when the break requests supply no diffable content', () => {
    const d = new CacheMissDiagnostics();
    d.recordRequest(req({ systemPromptHash: 'sp1' }));
    d.recordResponse(1000, 0);
    d.recordRequest(req({ systemPromptHash: 'sp2' }));
    d.recordResponse(0, 500);
    assert.equal(d.getLastDiffPath(), undefined);
  });

  it('clears the diff path when a subsequent request is a clean cache hit', () => {
    const d = new CacheMissDiagnostics();
    const mk = (sys: string): CacheDiagRequest => req({
      systemPromptHash: sys,
      buildDiffableContent: () => buildDiffableContent('gpt-4o', sys, TOOLS),
    });
    d.recordRequest(mk('a'));
    d.recordResponse(1000, 0);
    d.recordRequest(mk('b'));
    d.recordResponse(0, 100); // break
    assert.ok(d.getLastDiffPath());
    d.recordRequest(mk('b')); // same state → hit, cacheRead stays high
    d.recordResponse(1000, 0);
    assert.equal(d.getLastDiffPath(), undefined);
  });

  it('reset() clears the last diff path', () => {
    const d = new CacheMissDiagnostics();
    const mk = (sys: string): CacheDiagRequest => req({
      systemPromptHash: sys,
      buildDiffableContent: () => buildDiffableContent('gpt-4o', sys, TOOLS),
    });
    d.recordRequest(mk('a'));
    d.recordResponse(1000, 0);
    d.recordRequest(mk('b'));
    d.recordResponse(0, 100);
    assert.ok(d.getLastDiffPath());
    d.reset();
    assert.equal(d.getLastDiffPath(), undefined);
  });
});
