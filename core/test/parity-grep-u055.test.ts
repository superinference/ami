import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { EventEmitter } from 'node:events';
import { grepTool, grepHooks, searchFilesForPattern } from '../src/tools/grep';
import type { ToolContext } from '../src/types';

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return { cwd: tmpDir, abortSignal: new AbortController().signal, ...overrides };
}

const savedTimeoutMs = grepHooks.timeoutMs;
const savedKillGraceMs = grepHooks.killGraceMs;
const savedEscalationMs = grepHooks.sigkillEscalationMs;
const savedPlatform = grepHooks.platform;

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-grep-u055-'));
});

afterEach(() => {
  mock.restoreAll();
  grepHooks.timeoutMs = savedTimeoutMs;
  grepHooks.killGraceMs = savedKillGraceMs;
  grepHooks.sigkillEscalationMs = savedEscalationMs;
  grepHooks.platform = savedPlatform;
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Gap: rich tool description guiding the model how to search well
// Gap: pattern param description with regex/ripgrep escaping + multiline examples
// ---------------------------------------------------------------------------
describe('grep – rich tool + pattern description (parity)', () => {
  it('tool description explains ripgrep, output modes, glob/type and multiline', () => {
    const d = grepTool.description;
    assert.match(d, /ripgrep/i);
    assert.match(d, /files_with_matches/);
    assert.match(d, /content/);
    assert.match(d, /count/);
    assert.match(d, /multiline/i);
    assert.match(d, /glob|include/i);
    assert.match(d, /\btype\b/);
  });

  it('pattern param description covers regex syntax, metacharacter escaping and multiline', () => {
    const desc = (grepTool.inputSchema.properties as Record<string, { description?: string }>)
      .pattern.description ?? '';
    assert.match(desc, /regex|regular expression/i);
    assert.match(desc, /escape/i);
    assert.match(desc, /multiline/i);
    assert.match(desc, /fixed_strings/);
  });
});

// ---------------------------------------------------------------------------
// Gap: semantic coercion of numeric/boolean params (semanticNumber/semanticBoolean)
// ---------------------------------------------------------------------------
describe('grep – semantic coercion of string-serialized params (parity)', () => {
  beforeEach(() => {
    fs.writeFileSync(path.join(tmpDir, 'a.ts'), 'const HELLO = 1;\nconst world = 2;\n');
  });

  it('coerces case_insensitive:"true" (string) to a real true', async () => {
    const strRes = await grepTool.execute!(
      { pattern: 'hello', case_insensitive: 'true', output_mode: 'content' },
      ctx(),
    );
    const boolRes = await grepTool.execute!(
      { pattern: 'hello', case_insensitive: true, output_mode: 'content' },
      ctx(),
    );
    assert.notEqual(strRes.isError, true);
    // "true" must behave like true: the uppercase HELLO is matched.
    assert.match(strRes.output, /HELLO/);
    assert.equal(strRes.output, boolRes.output);
  });

  it('coerces case_insensitive:"false" (string) to a real false (not truthy)', async () => {
    const res = await grepTool.execute!(
      { pattern: 'hello', case_insensitive: 'false', output_mode: 'content' },
      ctx(),
    );
    // "false" must NOT be treated as truthy — uppercase HELLO must not match.
    assert.ok(!/HELLO/.test(res.output), 'string "false" must disable case-insensitive matching');
  });

  it('coerces head_limit:"1" (string) like the number 1', async () => {
    fs.writeFileSync(path.join(tmpDir, 'b.ts'), 'match_me one\nmatch_me two\nmatch_me three\n');
    const res = await grepTool.execute!(
      { pattern: 'match_me', head_limit: '1', output_mode: 'content' },
      ctx(),
    );
    const bodyLines = res.output
      .split('\n')
      .filter(l => /match_me/.test(l));
    assert.equal(bodyLines.length, 1, 'head_limit "1" must cap content to a single match line');
  });
});

// ---------------------------------------------------------------------------
// Gap: graceful SIGTERM-before-SIGKILL termination on timeout/abort
// ---------------------------------------------------------------------------
describe('grep – SIGTERM-before-SIGKILL escalation on timeout (parity)', () => {
  it('sends SIGTERM first, then escalates to SIGKILL when the child stays alive', async () => {
    grepHooks.timeoutMs = 10;
    grepHooks.sigkillEscalationMs = 15;
    grepHooks.killGraceMs = 60;
    grepHooks.platform = 'linux';
    const signals: (string | number | undefined)[] = [];
    mock.method(grepHooks, 'spawn', () => {
      const child: any = new EventEmitter();
      child.stdout = new EventEmitter();
      child.stderr = new EventEmitter();
      child.kill = (sig?: string | number) => { signals.push(sig); return true; };
      // never closes → timeout fires, then the escalation timer fires
      return child;
    });
    const res = await grepTool.execute!({ pattern: 'x' }, ctx());
    assert.equal(res.isError, true);
    assert.match(res.output, /timed out/);
    assert.deepEqual(signals, ['SIGTERM', 'SIGKILL'], 'must SIGTERM first, then SIGKILL');
  });

  it('uses the default signal on win32 (SIGTERM throws there)', async () => {
    grepHooks.timeoutMs = 10;
    grepHooks.sigkillEscalationMs = 15;
    grepHooks.killGraceMs = 40;
    grepHooks.platform = 'win32';
    const signals: (string | number | undefined)[] = [];
    mock.method(grepHooks, 'spawn', () => {
      const child: any = new EventEmitter();
      child.stdout = new EventEmitter();
      child.stderr = new EventEmitter();
      child.kill = (sig?: string | number) => { signals.push(sig); return true; };
      return child;
    });
    await grepTool.execute!({ pattern: 'x' }, ctx());
    assert.deepEqual(signals, [undefined], 'win32 must call child.kill() with the default signal, no escalation');
  });
});

// ---------------------------------------------------------------------------
// Gap: mtime ordering of file matches in fallback paths
// ---------------------------------------------------------------------------
describe('searchFilesForPattern – mtime ordering (parity)', () => {
  it('returns matches newest-first by file mtime', () => {
    // Names are chosen so ALPHABETICAL order (a_old before z_new) is the OPPOSITE of
    // mtime order (z_new is newer) — a readdir-order walk would fail this assertion.
    const older = path.join(tmpDir, 'a_old.txt');
    const newer = path.join(tmpDir, 'z_new.txt');
    fs.writeFileSync(older, 'needle in older\n');
    fs.writeFileSync(newer, 'needle in newer\n');
    const base = Date.now() / 1000;
    fs.utimesSync(older, base - 1000, base - 1000);
    fs.utimesSync(newer, base, base);

    const out = searchFilesForPattern(tmpDir, 'needle', {});
    const lines = out.split('\n').filter(Boolean);
    assert.equal(lines.length, 2);
    assert.match(lines[0], /z_new\.txt/, 'newest-modified file must come first');
    assert.match(lines[1], /a_old\.txt/);
  });

  it('breaks mtime ties deterministically by filename', () => {
    const b = path.join(tmpDir, 'b.txt');
    const a = path.join(tmpDir, 'a.txt');
    fs.writeFileSync(b, 'tie\n');
    fs.writeFileSync(a, 'tie\n');
    const t = Date.now() / 1000;
    fs.utimesSync(a, t, t);
    fs.utimesSync(b, t, t);

    const out = searchFilesForPattern(tmpDir, 'tie', {});
    const lines = out.split('\n').filter(Boolean);
    assert.match(lines[0], /a\.txt/, 'equal mtime → filename order');
    assert.match(lines[1], /b\.txt/);
  });
});
