import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  replTool,
  runReplCode,
  isReplModeEnabled,
  getReplPrimitiveTools,
  getReplOnlyToolNames,
  filterReplOnlyTools,
  REPL_TOOL_NAME,
  REPL_MODE_ENV,
  REPL_MODE_ENV_COMPAT,
  DEFAULT_REPL_TIMEOUT_MS,
  MAX_REPL_TIMEOUT_MS,
  createDefaultTools,
} from '../src/tools';
import type { ToolContext } from '../src/types';

const ctx = (signal?: AbortSignal, cwd = os.tmpdir()): ToolContext =>
  ({ cwd, abortSignal: signal ?? new AbortController().signal } as unknown as ToolContext);

const ENV_KEYS = [REPL_MODE_ENV, REPL_MODE_ENV_COMPAT];
const saved: Record<string, string | undefined> = {};
for (const k of ENV_KEYS) saved[k] = process.env[k];

afterEach(() => {
  for (const k of ENV_KEYS) {
    if (saved[k] === undefined) delete process.env[k];
    else process.env[k] = saved[k];
  }
});

describe('repl-tool: mode gate', () => {
  it('is OFF by default and ON via either env flag', () => {
    assert.equal(isReplModeEnabled({}), false);
    assert.equal(isReplModeEnabled({ [REPL_MODE_ENV]: '1' }), true);
    assert.equal(isReplModeEnabled({ [REPL_MODE_ENV_COMPAT]: 'true' }), true);
  });

  it('treats an explicit falsey token as OFF', () => {
    assert.equal(isReplModeEnabled({ [REPL_MODE_ENV]: '0' }), false);
    assert.equal(isReplModeEnabled({ [REPL_MODE_ENV]: 'off' }), false);
  });

  it('replTool.isEnabled() follows process.env', () => {
    delete process.env[REPL_MODE_ENV];
    delete process.env[REPL_MODE_ENV_COMPAT];
    assert.equal(replTool.isEnabled!(), false);
    process.env[REPL_MODE_ENV] = '1';
    assert.equal(replTool.isEnabled!(), true);
  });
});

describe('repl-tool: primitive set', () => {
  it('is the curated ordered literal including the search tools (getAllBaseTools bypass)', () => {
    const names = getReplPrimitiveTools().map((t) => t.name);
    assert.deepEqual(names, ['file_read', 'file_edit', 'glob', 'grep', 'bash', 'notebook_edit', 'task']);
    assert.ok(names.includes('grep'));
    assert.ok(names.includes('glob'));
  });

  it('is memoized (same reference across calls)', () => {
    assert.strictEqual(getReplPrimitiveTools(), getReplPrimitiveTools());
  });

  it('getReplOnlyToolNames mirrors the primitive names exactly and is memoized', () => {
    const only = getReplOnlyToolNames();
    assert.strictEqual(only, getReplOnlyToolNames());
    assert.deepEqual([...only].sort(), getReplPrimitiveTools().map((t) => t.name).sort());
  });
});

describe('repl-tool: advertise-time filter', () => {
  const pool = [{ name: 'repl' }, { name: 'bash' }, { name: 'grep' }, { name: 'web_search' }];

  it('returns an unchanged copy when REPL mode is off', () => {
    const out = filterReplOnlyTools(pool, {});
    assert.deepEqual(out.map((t) => t.name), pool.map((t) => t.name));
    assert.notStrictEqual(out, pool);
  });

  it('returns an unchanged copy when REPL mode is on but the repl tool is absent', () => {
    const noRepl = [{ name: 'bash' }, { name: 'grep' }];
    const out = filterReplOnlyTools(noRepl, { [REPL_MODE_ENV]: '1' });
    assert.deepEqual(out.map((t) => t.name), ['bash', 'grep']);
  });

  it('hides the REPL-only primitives when REPL mode is on and the repl tool is present', () => {
    const out = filterReplOnlyTools(pool, { [REPL_MODE_ENV]: '1' }).map((t) => t.name);
    assert.deepEqual(out, ['repl', 'web_search']);
  });
});

describe('repl-tool: registration wiring', () => {
  it('is registered only when REPL mode is enabled', () => {
    delete process.env[REPL_MODE_ENV];
    delete process.env[REPL_MODE_ENV_COMPAT];
    assert.equal(createDefaultTools(os.tmpdir()).get(REPL_TOOL_NAME), undefined);
    process.env[REPL_MODE_ENV] = '1';
    const reg = createDefaultTools(os.tmpdir());
    assert.ok(reg.get(REPL_TOOL_NAME));
  });
});

describe('repl-tool: schema + input validation', () => {
  it('requires a non-empty code string', async () => {
    for (const bad of [undefined, '', '   ', 42 as unknown as string]) {
      const r = await replTool.execute({ code: bad } as Record<string, unknown>, ctx());
      assert.equal(r.isError, true);
      assert.match(r.output, /non-empty string/);
    }
  });

  it('declares code required and advertises the primitives', () => {
    assert.deepEqual(replTool.inputSchema.required, ['code']);
    assert.equal(replTool.isReadOnly, false);
    assert.equal(replTool.isConcurrencySafe, false);
    assert.match(replTool.description, /file_read/);
    assert.match(replTool.description, /orchestrate/i);
  });
});

describe('repl-tool: execution', () => {
  it('captures console output and the returned value', async () => {
    const r = await runReplCode('console.log("hi", 7); return { ok: true };', ctx(), 5000);
    assert.equal(r.isError, undefined);
    assert.match(r.output, /hi 7/);
    assert.match(r.output, /=> \{"ok":true\}/);
  });

  it('reports "(no output)" when nothing is logged or returned', async () => {
    const r = await runReplCode('const x = 1 + 1;', ctx(), 5000);
    assert.equal(r.output, '(no output)');
  });

  it('orchestrates a real primitive tool from inside the VM', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'repl-'));
    const file = path.join(dir, 'note.txt');
    fs.writeFileSync(file, 'HELLO-FROM-REPL');
    try {
      const code = `const r = await file_read({ file_path: ${JSON.stringify(file)} }); return r.output;`;
      const r = await runReplCode(code, ctx(undefined, dir), 10000);
      assert.equal(r.isError, undefined);
      assert.match(r.output, /HELLO-FROM-REPL/);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('also exposes the primitives via the tools map', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'repl-'));
    const file = path.join(dir, 'm.txt');
    fs.writeFileSync(file, 'VIA-MAP');
    try {
      const code = `const r = await tools.file_read({ file_path: ${JSON.stringify(file)} }); return r.output;`;
      const r = await runReplCode(code, ctx(undefined, dir), 10000);
      assert.match(r.output, /VIA-MAP/);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('reports a thrown error (with any captured logs) as isError', async () => {
    const r = await runReplCode('console.log("before"); throw new Error("boom");', ctx(), 5000);
    assert.equal(r.isError, true);
    assert.match(r.output, /before/);
    assert.match(r.output, /REPL error: boom$/);
  });

  it('reports a non-Error string throw via String()', async () => {
    const r = await runReplCode('throw "plain-string-failure";', ctx(), 5000);
    assert.equal(r.isError, true);
    assert.match(r.output, /plain-string-failure/);
  });

  it('reports an object throw without a string message via String()', async () => {
    const r = await runReplCode('throw { code: 5 };', ctx(), 5000);
    assert.equal(r.isError, true);
    assert.match(r.output, /REPL error: \[object Object\]/);
  });

  it('times out a never-resolving block', async () => {
    const r = await runReplCode('await new Promise(() => {});', ctx(), 50);
    assert.equal(r.isError, true);
    assert.match(r.output, /exceeded 50ms/);
  });

  it('rejects immediately when the abort signal is already aborted', async () => {
    const r = await runReplCode('await new Promise(() => {});', ctx(AbortSignal.abort()), 5000);
    assert.equal(r.isError, true);
    assert.match(r.output, /aborted/);
  });

  it('aborts an in-flight block when the signal fires', async () => {
    const ac = new AbortController();
    const p = runReplCode('await new Promise(() => {});', ctx(ac.signal), 5000);
    setTimeout(() => ac.abort(), 20);
    const r = await p;
    assert.equal(r.isError, true);
    assert.match(r.output, /aborted/);
  });

  it('falls back to String() for a non-JSON-serializable return value', async () => {
    const r = await runReplCode('return 10n;', ctx(), 5000);
    assert.match(r.output, /=> 10/);
  });
});

describe('repl-tool: timeout clamping', () => {
  it('clamps timeout_ms above the hard maximum and ignores invalid values', async () => {
    assert.ok(MAX_REPL_TIMEOUT_MS > DEFAULT_REPL_TIMEOUT_MS);
    // A huge timeout must not prevent a quick block from returning promptly.
    const r = await replTool.execute({ code: 'return 1;', timeout_ms: MAX_REPL_TIMEOUT_MS * 10 }, ctx());
    assert.match(r.output, /=> 1/);
    const r2 = await replTool.execute({ code: 'return 2;', timeout_ms: -5 }, ctx());
    assert.match(r2.output, /=> 2/);
  });
});
