import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { fileEditTool, verifyWriteTarget } from '../src/tools/file-edit';
import { fileReadTool, CYBER_RISK_MITIGATION_REMINDER } from '../src/tools/file-read';
import { writeFileAtomicSync, GIT_NO_PROMPT_ENV } from '../src/tools/tool-utils';
import { splitGlobFilter, isEagainStderr } from '../src/tools/grep';
import { globTimeoutMs, globHooks, globTool } from '../src/tools/glob';
import { fuzzyFindAndReplace, desanitizeMatchString, DESANITIZATIONS } from '../src/tools/fuzzy-match';
import { getFileCache } from '../src/file-cache';
import { scoutTool, scoutHooks } from '../src/tools/scout';
import type { ToolContext } from '../src/types';

function ctx(cwd: string, over: Partial<ToolContext> = {}): ToolContext {
  return { cwd, abortSignal: new AbortController().signal, ...over } as ToolContext;
}

let tmp: string;
beforeEach(() => { tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'parity-tf-')); });
afterEach(() => { fs.rmSync(tmp, { recursive: true, force: true }); });

// --- gaps 4/27/28: replace_all quoted-boolean coercion -----------------------
describe('file_edit replace_all quoted-boolean coercion', () => {
  it('"replace_all":"false" does NOT replace all (must require uniqueness)', async () => {
    const fp = path.join(tmp, 'a.txt');
    fs.writeFileSync(fp, 'x\nx\nx\n');
    const r = await fileEditTool.execute(
      { file_path: fp, old_string: 'x', new_string: 'y', replace_all: 'false' },
      ctx(tmp),
    );
    // With "false" correctly coerced, the ambiguous single edit must error (3 matches),
    // NOT replace everything (which the Boolean("false")===true bug would do).
    assert.ok(r.isError);
    assert.match(r.output, /matches 3 locations/);
    assert.equal(fs.readFileSync(fp, 'utf8'), 'x\nx\nx\n');
  });

  it('"replace_all":"true" replaces all occurrences', async () => {
    const fp = path.join(tmp, 'b.txt');
    fs.writeFileSync(fp, 'x\nx\nx\n');
    const r = await fileEditTool.execute(
      { file_path: fp, old_string: 'x', new_string: 'y', replace_all: 'true' },
      ctx(tmp),
    );
    assert.ok(!r.isError);
    assert.equal(fs.readFileSync(fp, 'utf8'), 'y\ny\ny\n');
  });

  it('edits[].replace_all:"false" is coerced per-edit', async () => {
    const fp = path.join(tmp, 'c.txt');
    fs.writeFileSync(fp, 'x\nx\n');
    const r = await fileEditTool.execute(
      { file_path: fp, edits: [{ old_string: 'x', new_string: 'y', replace_all: 'false' }] },
      ctx(tmp),
    );
    assert.ok(r.isError);
  });
});

// --- gap 2: dangling-symlink write escape ------------------------------------
describe('verifyWriteTarget dangling-symlink escape', () => {
  it('blocks a dangling symlink pointing outside the workspace', () => {
    const outside = path.join(os.tmpdir(), `parity-escape-${Date.now()}.txt`);
    const link = path.join(tmp, 'link');
    fs.symlinkSync(outside, link); // target does not exist → dangling
    const err = verifyWriteTarget(link, tmp);
    assert.ok(err, 'should block');
    assert.match(err!, /dangling symlink|symlink-escape/);
  });

  it('allows a dangling symlink pointing inside the workspace', () => {
    const inside = path.join(tmp, 'future.txt');
    const link = path.join(tmp, 'ilink');
    fs.symlinkSync(inside, link);
    const err = verifyWriteTarget(link, tmp);
    assert.equal(err, undefined);
  });
});

// --- gaps 10/11/12: crash-safe atomic write ----------------------------------
describe('writeFileAtomicSync', () => {
  it('writes content and leaves no temp files behind', () => {
    const fp = path.join(tmp, 'atomic.txt');
    writeFileAtomicSync(fp, 'hello');
    assert.equal(fs.readFileSync(fp, 'utf8'), 'hello');
    assert.deepEqual(fs.readdirSync(tmp).filter(f => f.includes('.tmp.')), []);
  });

  it('preserves the existing file mode on rewrite', () => {
    const fp = path.join(tmp, 'exe.sh');
    fs.writeFileSync(fp, 'old');
    fs.chmodSync(fp, 0o750);
    writeFileAtomicSync(fp, 'new');
    assert.equal(fs.statSync(fp).mode & 0o777, 0o750);
    assert.equal(fs.readFileSync(fp, 'utf8'), 'new');
  });
});

// --- gap 6: malware-refusal reminder on text reads ---------------------------
describe('file_read cyber-risk reminder', () => {
  it('appends the malware-refusal reminder to text reads', async () => {
    const fp = path.join(tmp, 'code.js');
    fs.writeFileSync(fp, 'console.log(1)\n');
    const r = await fileReadTool.execute({ file_path: fp }, ctx(tmp));
    assert.ok(!r.isError);
    assert.ok(r.output.includes(CYBER_RISK_MITIGATION_REMINDER.trim()));
  });
});

// --- gaps 20/21/22: PDF size cap ---------------------------------------------
describe('file_read PDF size cap', () => {
  it('rejects an oversized PDF before loading it', async () => {
    const fp = path.join(tmp, 'big.pdf');
    // Sparse file > 20MB without allocating real bytes.
    const fd = fs.openSync(fp, 'w');
    fs.ftruncateSync(fd, 21 * 1024 * 1024);
    fs.closeSync(fd);
    const r = await fileReadTool.execute({ file_path: fp }, ctx(tmp));
    assert.ok(r.isError);
    assert.match(r.output, /PDF too large/);
  });
});

// --- gap 24: file-read abort -------------------------------------------------
describe('file_read abort', () => {
  it('returns a cancelled error when the signal is already aborted', async () => {
    const fp = path.join(tmp, 'x.txt');
    fs.writeFileSync(fp, 'data');
    const ac = new AbortController();
    ac.abort();
    const r = await fileReadTool.execute({ file_path: fp }, ctx(tmp, { abortSignal: ac.signal }));
    assert.ok(r.isError);
    assert.match(r.output, /cancelled/);
  });
});

// --- gap 7: brace-aware glob splitting ---------------------------------------
describe('splitGlobFilter', () => {
  it('keeps a brace group intact', () => {
    assert.deepEqual(splitGlobFilter('*.{ts,tsx}'), ['*.{ts,tsx}']);
  });
  it('comma-splits brace-free fragments', () => {
    assert.deepEqual(splitGlobFilter('*.ts,*.js'), ['*.ts', '*.js']);
  });
  it('splits on whitespace and preserves braces per fragment', () => {
    assert.deepEqual(splitGlobFilter('*.{ts,tsx} *.md'), ['*.{ts,tsx}', '*.md']);
  });
});

// --- gap 26: EAGAIN-from-stderr detection ------------------------------------
describe('isEagainStderr', () => {
  it('detects os error 11', () => assert.ok(isEagainStderr('rg: failed: os error 11')));
  it('detects Resource temporarily unavailable', () => assert.ok(isEagainStderr('Resource temporarily unavailable')));
  it('ignores unrelated stderr', () => assert.ok(!isEagainStderr('no such file')));
});

// --- gap 5: desanitize matching strategy -------------------------------------
describe('fuzzy-match desanitize_normalized', () => {
  it('maps sanitized tokens back and re-applies to the replacement', () => {
    const content = 'before <function_results>raw</function_results> after\n';
    const r = fuzzyFindAndReplace(content, '<fnr>raw</fnr>', '<fnr>done</fnr>');
    assert.equal(r.strategy, 'desanitize_normalized');
    assert.ok(r.newContent!.includes('<function_results>done</function_results>'));
  });
  it('desanitizeMatchString is a no-op for non-sanitized input', () => {
    assert.equal(desanitizeMatchString('plain text'), 'plain text');
  });
  it('DESANITIZATIONS is non-empty', () => assert.ok(DESANITIZATIONS.length > 0));
});

// --- gap 13: isPartialView blocks blind overwrite ----------------------------
describe('partial-view overwrite guard', () => {
  it('blocks a full overwrite after only a partial (size-capped) read', async () => {
    const fp = path.join(tmp, 'large.txt');
    const big = 'Z'.repeat(300 * 1024); // > 256KB cap, single line
    fs.writeFileSync(fp, big);
    const filesRead = new Set<string>();
    const c = ctx(tmp, { filesRead });
    const read = await fileReadTool.execute({ file_path: fp }, c);
    assert.match(read.output, /PARTIAL VIEW/);
    assert.ok(getFileCache(tmp).isPartialView(path.resolve(fp)));
    const write = await fileEditTool.execute({ file_path: fp, content: 'tiny' }, c);
    assert.ok(write.isError);
    assert.match(write.output, /PARTIAL view/);
    getFileCache(tmp).clear();
  });
});

// --- gaps 14/25: glob abort + timeout ----------------------------------------
describe('glob abort + timeout', () => {
  it('globTimeoutMs honors the env override (seconds)', () => {
    assert.equal(globTimeoutMs({ AMI_GLOB_TIMEOUT_SECONDS: '5' } as NodeJS.ProcessEnv), 5000);
    assert.equal(globTimeoutMs({} as NodeJS.ProcessEnv), 20000);
  });
  it('returns cancelled when the signal is already aborted', async () => {
    const ac = new AbortController();
    ac.abort();
    const r = await globTool.execute({ pattern: '**/*' }, ctx(tmp, { abortSignal: ac.signal }));
    assert.ok(r.isError);
    assert.match(r.output, /cancelled/);
  });
  it('times out a slow in-process walk', async () => {
    fs.writeFileSync(path.join(tmp, 'f.ts'), 'x');
    const origTimeout = globHooks.timeoutMs;
    const origGlob = globHooks.glob;
    globHooks.timeoutMs = 20;
    globHooks.glob = (() => new Promise(() => {})) as typeof globHooks.glob; // never resolves
    try {
      const r = await globTool.execute({ pattern: '**/*.ts' }, ctx(tmp));
      assert.ok(r.isError);
      assert.match(r.output, /timed out/);
    } finally {
      globHooks.timeoutMs = origTimeout;
      globHooks.glob = origGlob;
    }
  });
});

// --- gaps 3/9: scout git-no-prompt env + abort -------------------------------
describe('scout git-no-prompt + abort', () => {
  it('GIT_NO_PROMPT_ENV disables interactive git prompts', () => {
    assert.equal(GIT_NO_PROMPT_ENV.GIT_TERMINAL_PROMPT, '0');
    assert.equal(GIT_NO_PROMPT_ENV.GIT_ASKPASS, '');
  });

  it('clone passes the no-prompt env to git', async () => {
    const calls: Array<{ args: string[]; env?: Record<string, string | undefined> }> = [];
    const origExecFile = scoutHooks.execFileSync;
    const origExec = scoutHooks.execSync;
    const origExists = scoutHooks.existsSync;
    const origMkdir = scoutHooks.mkdirSync;
    scoutHooks.existsSync = (() => false) as typeof scoutHooks.existsSync;
    scoutHooks.mkdirSync = (() => undefined) as unknown as typeof scoutHooks.mkdirSync;
    scoutHooks.execFileSync = ((cmd: string, args: string[], opts: any) => {
      calls.push({ args, env: opts?.env });
      return '' as unknown as Buffer;
    }) as typeof scoutHooks.execFileSync;
    scoutHooks.execSync = (() => '' as unknown as Buffer) as typeof scoutHooks.execSync;
    try {
      await scoutTool.execute({ repo: 'owner/repo' }, ctx(tmp));
      const clone = calls.find(c => c.args.includes('clone'));
      assert.ok(clone, 'git clone called');
      assert.equal(clone!.env?.GIT_TERMINAL_PROMPT, '0');
    } finally {
      scoutHooks.execFileSync = origExecFile;
      scoutHooks.execSync = origExec;
      scoutHooks.existsSync = origExists;
      scoutHooks.mkdirSync = origMkdir;
    }
  });

  it('returns cancelled when aborted before clone', async () => {
    const ac = new AbortController();
    ac.abort();
    const r = await scoutTool.execute({ repo: 'owner/repo' }, ctx(tmp, { abortSignal: ac.signal }));
    assert.ok(r.isError);
    assert.match(r.output, /cancelled/);
  });
});
