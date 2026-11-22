/**
 * Wave-2 coverage gap closure: shell Buffer stdout/stderr arms, task-kill
 * manager_unavailable / prune_idle type guard, path-suggest misses, and a few
 * remaining enter/exit worktree + workflow affordance branches.
 */
import { describe, it, mock, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { EventEmitter } from 'events';

import { execFileNoThrow, shellHooks } from '../src/utils/shell';
import { taskKillTool } from '../src/tools/task-kill';
import {
  suggestSimilarPaths,
  suggestPathCorrection,
  suggestPathUnderCwd,
  suggestByBasename,
} from '../src/tools/path-suggest';
import type { ToolContext } from '../src/types';

afterEach(() => mock.restoreAll());

describe('shell execFileNoThrow Buffer + optional-arg arms', () => {
  it('coerces Buffer stdout/stderr and honors env / shell / abortSignal spreads', async () => {
    const ac = new AbortController();
    mock.method(shellHooks, 'execFile', (
      _file: string,
      _args: string[],
      opts: Record<string, unknown>,
      cb: (err: Error | null, stdout: string | Buffer, stderr: string | Buffer) => void,
    ) => {
      assert.equal(opts.env && typeof opts.env === 'object', true);
      assert.ok('signal' in opts);
      // Return Buffers so the typeof !== 'string' arms fire.
      queueMicrotask(() => cb(null, Buffer.from('OUT'), Buffer.from('ERR')));
      return { stdin: null } as unknown as ReturnType<typeof shellHooks.execFile>;
    });
    const r = await execFileNoThrow('echo', ['hi'], {
      cwd: os.tmpdir(),
      env: { ...process.env, AMI_COV: '1' },
      abortSignal: ac.signal,
      preserveOutputOnError: true,
    });
    assert.equal(r.code, 0);
    assert.equal(r.stdout, 'OUT');
    assert.equal(r.stderr, 'ERR');
  });

  it('maps a non-number errno launch failure with preserveOutputOnError false', async () => {
    mock.method(shellHooks, 'execFile', (
      _f: string,
      _a: string[],
      _o: unknown,
      cb: (err: NodeJS.ErrnoException | null, stdout: string, stderr: string) => void,
    ) => {
      queueMicrotask(() => {
        const err = Object.assign(new Error('spawn ENOENT'), { code: 'ENOENT' });
        cb(err, 'ignored', 'ignored');
      });
      return { stdin: null } as unknown as ReturnType<typeof shellHooks.execFile>;
    });
    const r = await execFileNoThrow('missing-bin-xyz', [], {
      preserveOutputOnError: false,
    });
    assert.equal(r.code, 1);
    assert.equal(r.stdout, '');
    assert.equal(r.stderr, '');
  });

  it('preserveOutputOnError keeps buffers and surfaces signal-based errors', async () => {
    mock.method(shellHooks, 'execFile', (
      _f: string,
      _a: string[],
      _o: unknown,
      cb: (err: NodeJS.ErrnoException & { signal?: NodeJS.Signals } | null, stdout: Buffer, stderr: Buffer) => void,
    ) => {
      queueMicrotask(() => {
        const err = Object.assign(new Error('killed'), { code: 'SIGTERM', signal: 'SIGTERM' as NodeJS.Signals });
        cb(err, Buffer.from('partial'), Buffer.from('e'));
      });
      return { stdin: null } as unknown as ReturnType<typeof shellHooks.execFile>;
    });
    const r = await execFileNoThrow('sleep', ['1'], { preserveOutputOnError: true });
    assert.equal(r.code, 1);
    assert.equal(r.stdout, 'partial');
    assert.ok(r.error);
  });
});

describe('task-kill gap closure', () => {
  it('returns manager_unavailable when processManager is missing', async () => {
    const r = await taskKillTool.execute!(
      { task_id: 't1' },
      { cwd: '/tmp' } as ToolContext,
    );
    assert.equal(r.isError, true);
    assert.equal(r.metadata?.reason, 'manager_unavailable');
  });

  it('rejects a non-boolean prune_idle', async () => {
    const r = await taskKillTool.execute!(
      { prune_idle: 'yes' },
      { cwd: '/tmp', processManager: { list: () => [] } } as unknown as ToolContext,
    );
    assert.equal(r.isError, true);
    assert.equal(r.metadata?.reason, 'invalid_input');
    assert.match(r.output, /prune_idle/);
  });

  it('rejects unknown keys before looking up a task', async () => {
    const r = await taskKillTool.execute!(
      { task_id: 'x', nope: 1 },
      { cwd: '/tmp', processManager: {} } as unknown as ToolContext,
    );
    assert.equal(r.isError, true);
    assert.equal(r.metadata?.reason, 'invalid_input');
  });
});

describe('path-suggest gap closure', () => {
  // Prefer a home-backed temp root — /tmp can hit EDQUOT under heavy parallel suites.
  const TEMP_ROOT = fs.existsSync('/home/ccamacho/tmp-ami-cov')
    ? '/home/ccamacho/tmp-ami-cov'
    : os.tmpdir();

  it('suggestSimilarPaths surfaces same-stem + case-only corrections and skips ignored', () => {
    const d = fs.mkdtempSync(path.join(TEMP_ROOT, 'ps-'));
    try {
      fs.writeFileSync(path.join(d, 'config.toml'), 'x');
      fs.writeFileSync(path.join(d, 'README.md'), 'x');
      fs.writeFileSync(path.join(d, '.env'), 'secret');
      // Same-stem (config.ts requested, config.toml present) even with large ext distance.
      const stem = suggestSimilarPaths(path.join(d, 'config.ts'), d, 5, 1);
      assert.ok(stem.some((s) => s.endsWith('config.toml')), `stem hits: ${stem.join(',')}`);
      // Case-only mismatch.
      const cas = suggestSimilarPaths(path.join(d, 'readme.md'), d);
      assert.ok(cas.some((s) => /README\.md$/i.test(s)));
      // Dotfile must not leak via a case variant.
      const hidden = suggestSimilarPaths(path.join(d, '.ENV'), d);
      assert.ok(!hidden.some((s) => s.endsWith('.env')));
    } finally {
      fs.rmSync(d, { recursive: true, force: true });
    }
  });

  it('suggestPathCorrection recovers a misspelled directory component', () => {
    const d = fs.mkdtempSync(path.join(TEMP_ROOT, 'ps2-'));
    try {
      fs.mkdirSync(path.join(d, 'src'));
      fs.writeFileSync(path.join(d, 'src', 'index.ts'), 'x');
      const r = suggestPathCorrection(path.join(d, 'scr', 'index.ts'), d);
      assert.equal(r.kind, 'missing-dir');
      assert.ok(r.suggestions.some((s) => s.includes(`${path.sep}src${path.sep}index.ts`)));
      // Unreadable/missing deep parent → none.
      const none = suggestPathCorrection(path.join(d, 'nope', 'deep', 'x.ts'), d, { maxDistance: 0 });
      assert.equal(none.kind, 'none');
    } finally {
      fs.rmSync(d, { recursive: true, force: true });
    }
  });

  it('suggestPathUnderCwd relocates a sibling-of-cwd path into cwd', () => {
    const parent = fs.mkdtempSync(path.join(TEMP_ROOT, 'ps3-'));
    const cwd = path.join(parent, 'proj');
    try {
      fs.mkdirSync(cwd);
      fs.writeFileSync(path.join(cwd, 'file.ts'), 'x');
      // Request a path sitting NEXT TO cwd (under the shared parent) — the
      // helper remaps it into cwd when that corrected path exists.
      const requested = path.join(parent, 'file.ts');
      const under = suggestPathUnderCwd(requested, cwd);
      assert.equal(under, path.join(cwd, 'file.ts'));
      const byBase = suggestByBasename(path.join(cwd, 'wrong', 'file.ts'), cwd);
      assert.ok(byBase.some((s) => s.endsWith('file.ts')));
    } finally {
      fs.rmSync(parent, { recursive: true, force: true });
    }
  });
});
