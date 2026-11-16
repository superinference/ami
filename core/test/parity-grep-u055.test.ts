import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { EventEmitter } from 'node:events';
import { grepTool, grepHooks, searchFilesForPattern, runSearch, grepLineFilePath } from '../src/tools/grep';
import type { ToolContext } from '../src/types';

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return { cwd: tmpDir, abortSignal: new AbortController().signal, ...overrides };
}

const savedTimeoutMs = grepHooks.timeoutMs;
const savedKillGraceMs = grepHooks.killGraceMs;
const savedEscalationMs = grepHooks.sigkillEscalationMs;
const savedPlatform = grepHooks.platform;
const savedBudgetMs = grepHooks.inProcessBudgetMs;

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-grep-u055-'));
});

afterEach(() => {
  mock.restoreAll();
  grepHooks.timeoutMs = savedTimeoutMs;
  grepHooks.killGraceMs = savedKillGraceMs;
  grepHooks.sigkillEscalationMs = savedEscalationMs;
  grepHooks.platform = savedPlatform;
  grepHooks.inProcessBudgetMs = savedBudgetMs;
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

// ---------------------------------------------------------------------------
// Gap: Claude-Code-compatible single-letter param aliases (-A/-B/-C/-n/-i)
// ---------------------------------------------------------------------------
/** Mock grepHooks.spawn to capture the rg args and resolve as an empty (no-match) run. */
function captureRgArgs(): { args: string[][] } {
  const captured: string[][] = [];
  mock.method(grepHooks, 'spawn', (_cmd: string, args: string[]) => {
    captured.push(args);
    const child: any = new EventEmitter();
    child.stdout = new EventEmitter();
    child.stderr = new EventEmitter();
    child.kill = () => true;
    // exit 0 with no stdout → "No matches" (no grep/in-process fallback triggered)
    queueMicrotask(() => { child.emit('close', 0, null); });
    return child;
  });
  return { args: captured };
}

describe('grep – Claude-Code single-letter aliases translate to rg flags (parity)', () => {
  it('-C maps to context_lines, -i to case_insensitive', async () => {
    const cap = captureRgArgs();
    await grepTool.execute!(
      { pattern: 'x', output_mode: 'content', '-C': 3, '-i': true },
      ctx(),
    );
    const args = cap.args[0];
    assert.ok(args.includes('-C') && args[args.indexOf('-C') + 1] === '3', 'alias -C → rg -C 3');
    assert.ok(args.includes('-i'), 'alias -i → rg -i');
  });

  it('-n:false maps to line_numbers:false (suppresses --line-number)', async () => {
    const cap = captureRgArgs();
    await grepTool.execute!(
      { pattern: 'x', output_mode: 'content', '-n': false },
      ctx(),
    );
    assert.ok(!cap.args[0].includes('--line-number'), 'alias -n:false → no rg --line-number');
  });

  it('-A and -B map to after/before context', async () => {
    const cap = captureRgArgs();
    await grepTool.execute!(
      { pattern: 'x', output_mode: 'content', '-A': 2, '-B': 1 },
      ctx(),
    );
    const args = cap.args[0];
    assert.ok(args.includes('-A') && args[args.indexOf('-A') + 1] === '2', 'alias -A → rg -A 2');
    assert.ok(args.includes('-B') && args[args.indexOf('-B') + 1] === '1', 'alias -B → rg -B 1');
  });

  it('the descriptive name wins when both it and the alias are supplied', async () => {
    const cap = captureRgArgs();
    await grepTool.execute!(
      { pattern: 'x', output_mode: 'content', context_lines: 5, '-C': 9 },
      ctx(),
    );
    const args = cap.args[0];
    assert.equal(args[args.indexOf('-C') + 1], '5', 'context_lines must take precedence over -C');
  });

  it('end-to-end: `-i` alias performs a real case-insensitive search (no mocks)', async () => {
    fs.writeFileSync(path.join(tmpDir, 'a.ts'), 'const HELLO = 1;\n');
    const res = await grepTool.execute!(
      { pattern: 'hello', '-i': true, output_mode: 'content' },
      ctx(),
    );
    assert.notEqual(res.isError, true);
    assert.match(res.output, /HELLO/, 'the -i alias must enable case-insensitive matching end to end');
  });

  it('schema advertises the Claude aliases and content-mode-only warnings', () => {
    const props = (grepTool.inputSchema as any).properties;
    for (const k of ['-A', '-B', '-C', '-n', '-i']) {
      assert.ok(props[k], `schema must advertise alias ${k}`);
    }
    assert.match(props.before_context.description, /Requires output_mode: "content"/);
    assert.match(props.line_numbers.description, /Requires output_mode: "content"/);
    // Affordance hint delegating open-ended multi-round searches to the task subagent.
    assert.match(grepTool.description, /task.*subagent|subagent.*task/i);
  });
});

// ---------------------------------------------------------------------------
// Gap: Windows-drive-safe ripgrep line parsing (first :<digits>: boundary)
// ---------------------------------------------------------------------------
describe('grepLineFilePath – drive-safe file-path extraction (parity)', () => {
  it('extracts the path up to the first :<digits>: in a content line', () => {
    assert.equal(grepLineFilePath('src/a.ts:42:const x = 1;'), 'src/a.ts');
  });
  it('does not shred a Windows drive-letter path', () => {
    assert.equal(grepLineFilePath('C:\\dir\\file.ts:5:text'), 'C:\\dir\\file.ts');
  });
  it('extracts the path from a count line (path:COUNT)', () => {
    assert.equal(grepLineFilePath('src/a.ts:3'), 'src/a.ts');
  });
  it('returns the whole line for a bare files_with_matches path', () => {
    assert.equal(grepLineFilePath('src/a.ts'), 'src/a.ts');
    assert.equal(grepLineFilePath('C:\\dir\\file.ts'), 'C:\\dir\\file.ts');
  });
  it('a Windows path is counted as ONE file, not inflated by the drive colon', () => {
    // Two matches in the SAME drive-letter file must report a single file.
    const out = 'C:\\dir\\file.ts:1:a\nC:\\dir\\file.ts:2:b';
    const files = new Set(out.split('\n').map(grepLineFilePath));
    assert.equal(files.size, 1);
  });
});

// ---------------------------------------------------------------------------
// Gap: critical error-code distinction (EACCES/EPERM surfaced, not swallowed)
// ---------------------------------------------------------------------------
describe('runSearch – EACCES/EPERM surfaced as search error (parity)', () => {
  function spawnError(code: string) {
    mock.method(grepHooks, 'spawn', () => {
      const child: any = new EventEmitter();
      child.stdout = new EventEmitter();
      child.stderr = new EventEmitter();
      child.kill = () => true;
      queueMicrotask(() => child.emit('error', Object.assign(new Error('boom'), { code })));
      return child;
    });
  }

  it('EACCES becomes a searchError (so the real permission problem is reported)', async () => {
    spawnError('EACCES');
    const outcome = await runSearch(['x', tmpDir], 'rg', ctx(), 0);
    assert.equal(outcome.output, null);
    assert.match(outcome.searchError ?? '', /EACCES/);
  });

  it('EPERM becomes a searchError', async () => {
    spawnError('EPERM');
    const outcome = await runSearch(['x', tmpDir], 'rg', ctx(), 0);
    assert.match(outcome.searchError ?? '', /EPERM/);
  });

  it('ENOENT (binary missing) still falls back silently (no searchError)', async () => {
    spawnError('ENOENT');
    const outcome = await runSearch(['x', tmpDir], 'rg', ctx(), 0);
    assert.equal(outcome.output, null);
    assert.equal(outcome.searchError, undefined);
  });
});

// ---------------------------------------------------------------------------
// Gap: ReDoS-safe in-process fallback — wall-clock deadline aborts the walk
// ---------------------------------------------------------------------------
describe('searchFilesForPattern – wall-clock deadline (ReDoS guard)', () => {
  it('returns [] / stops immediately when the budget is already exhausted', () => {
    for (let i = 0; i < 5; i++) fs.writeFileSync(path.join(tmpDir, `f${i}.txt`), 'needle\n');
    grepHooks.inProcessBudgetMs = 0; // deadline = now → first expired() check bails
    const out = searchFilesForPattern(tmpDir, 'needle', {});
    assert.equal(out, '', 'an exhausted budget must abort the walk with no matches');
  });

  it('with a normal budget it still finds matches (deadline not hit)', () => {
    fs.writeFileSync(path.join(tmpDir, 'f.txt'), 'needle\n');
    grepHooks.inProcessBudgetMs = 20_000;
    const out = searchFilesForPattern(tmpDir, 'needle', {});
    assert.match(out, /needle/);
  });
});

// ---------------------------------------------------------------------------
// Gap: TOCTOU symlink re-check in the recursive fallback walk
// ---------------------------------------------------------------------------
describe('searchFilesForPattern – symlink TOCTOU guard (parity)', () => {
  it('does not follow a symlink encountered during the walk', () => {
    const outside = fs.mkdtempSync(path.join(os.tmpdir(), 'si-grep-outside-'));
    try {
      fs.writeFileSync(path.join(outside, 'secret.txt'), 'needle_secret\n');
      fs.writeFileSync(path.join(tmpDir, 'real.txt'), 'needle_real\n');
      try {
        fs.symlinkSync(path.join(outside, 'secret.txt'), path.join(tmpDir, 'link.txt'));
      } catch {
        return; // platform without symlink permission — skip
      }
      const out = searchFilesForPattern(tmpDir, 'needle', {});
      assert.match(out, /needle_real/, 'the real in-tree file is searched');
      assert.ok(!/needle_secret/.test(out), 'the symlinked outside file must NOT be followed');
    } finally {
      fs.rmSync(outside, { recursive: true, force: true });
    }
  });
});
