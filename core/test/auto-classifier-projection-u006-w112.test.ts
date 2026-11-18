import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { resolveAutoClassifierInput } from '../src/tools/auto-classifier-projection';
import { fileEditTool } from '../src/tools/file-edit';
import type { ToolDefinition } from '../src/types';

// ---------------------------------------------------------------------------
// U006 / w112 — canonical auto-mode-classifier INPUT PROJECTION resolver.
// Parity: Claude Code yoloClassifier `Tool.toAutoClassifierInput` (L384-424):
//   - per-tool hook is the single source of truth for the projected view,
//   - '' means "not security relevant" -> short-circuit to auto-allow,
//   - the hook runs on unvalidated model/history input so a throw falls back to
//     the raw object (never crashes the permission check),
//   - no hook -> bash collapses to its command, everything else keeps full input.
// ---------------------------------------------------------------------------

describe('resolveAutoClassifierInput', () => {
  it('uses a per-tool hook as the projected, security-relevant view', () => {
    const tool: Pick<ToolDefinition, 'name' | 'toAutoClassifierInput'> = {
      name: 'ask_user',
      toAutoClassifierInput: (input) => String(input.question ?? ''),
    };
    const r = resolveAutoClassifierInput(tool, { question: 'proceed?' });
    assert.deepEqual(r, { projected: 'proceed?', securityRelevant: true });
  });

  it("treats a hook returning '' as NOT security-relevant (short-circuit)", () => {
    const tool: Pick<ToolDefinition, 'name' | 'toAutoClassifierInput'> = {
      name: 'enter_worktree',
      toAutoClassifierInput: (input) => (typeof input.name === 'string' ? input.name : ''),
    };
    // No `name` field -> '' -> not security relevant.
    assert.deepEqual(resolveAutoClassifierInput(tool, {}), {
      projected: '',
      securityRelevant: false,
    });
    // With a name it is relevant again.
    assert.deepEqual(resolveAutoClassifierInput(tool, { name: 'wt-1' }), {
      projected: 'wt-1',
      securityRelevant: true,
    });
  });

  it('falls back to the RAW input (fail-safe, reviewable) when the hook throws', () => {
    const tool: Pick<ToolDefinition, 'name' | 'toAutoClassifierInput'> = {
      name: 'broken',
      toAutoClassifierInput: () => {
        throw new Error('malformed historical input');
      },
    };
    const input = { a: 1, b: 2 };
    const r = resolveAutoClassifierInput(tool, input);
    assert.equal(r.securityRelevant, true);
    assert.equal(r.projected, input);
  });

  it('defaults a bash call (no hook) to its command string', () => {
    const r = resolveAutoClassifierInput({ name: 'bash' }, { command: 'ls -la' });
    assert.deepEqual(r, { projected: 'ls -la', securityRelevant: true });
  });

  it('defaults a bash call with a non-string command to the full input', () => {
    const input = { command: 123 };
    const r = resolveAutoClassifierInput({ name: 'bash' }, input);
    assert.equal(r.projected, input);
    assert.equal(r.securityRelevant, true);
  });

  it('keeps the full input for a non-bash tool with no hook', () => {
    const input = { file_path: '/a', content: 'secret' };
    const r = resolveAutoClassifierInput({ name: 'unknown' }, input);
    assert.equal(r.projected, input);
    assert.equal(r.securityRelevant, true);
  });

  it('keeps the full input when the tool is undefined', () => {
    const input = { x: 1 };
    assert.equal(resolveAutoClassifierInput(undefined, input).projected, input);
  });
});

describe('file_edit toAutoClassifierInput (data-minimization)', () => {
  it('never exposes file content/new_string to the classifier — only path+action', () => {
    const projected = fileEditTool.toAutoClassifierInput!({
      file_path: '/etc/app/creds.env',
      content: 'API_KEY=sk-super-hidden-value',
    });
    assert.equal(projected, 'write /etc/app/creds.env');
    assert.doesNotMatch(projected, /hidden/);
    assert.doesNotMatch(projected, /API_KEY/);
  });

  it('reports an in-place edit (old_string present) as action=edit', () => {
    const projected = fileEditTool.toAutoClassifierInput!({
      file_path: 'src/x.ts',
      old_string: 'a',
      new_string: 'b',
    });
    assert.equal(projected, 'edit src/x.ts');
  });

  it('reports an edits[] batch as action=edit', () => {
    const projected = fileEditTool.toAutoClassifierInput!({
      file_path: 'src/x.ts',
      edits: [{ old_string: 'a', new_string: 'b' }],
    });
    assert.equal(projected, 'edit src/x.ts');
  });

  it('surfaces explicit mode bits (a 600/755 create is security-relevant)', () => {
    const projected = fileEditTool.toAutoClassifierInput!({
      file_path: 'run.sh',
      content: '#!/bin/sh\n',
      mode: '755',
    });
    assert.equal(projected, 'write run.sh mode=755');
  });

  it("projects to '' (not security-relevant) when the path is missing/blank", () => {
    assert.equal(fileEditTool.toAutoClassifierInput!({}), '');
    assert.equal(fileEditTool.toAutoClassifierInput!({ file_path: '   ' }), '');
  });

  it('is resolved end-to-end to a short-circuit-free, minimized projection', () => {
    const r = resolveAutoClassifierInput(fileEditTool, {
      file_path: '/a/b.ts',
      content: 'x'.repeat(10_000),
    });
    assert.equal(r.projected, 'write /a/b.ts');
    assert.equal(r.securityRelevant, true);
  });
});
