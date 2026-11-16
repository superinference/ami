import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  bashTool,
  _bashHooks,
  stripRedundantCdPrefix,
  rewriteWindowsNullRedirect,
  normalizeBashInput,
  stripEmptyLines,
} from '../src/tools/bash';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// Unit: pre-execution normalization (parity: Claude Code api.ts normalizeToolInput
// + shellQuoting.rewriteWindowsNullRedirect)
// ---------------------------------------------------------------------------

describe('U030 stripRedundantCdPrefix', () => {
  it('strips a redundant leading `cd <cwd> && ` prefix', () => {
    assert.equal(stripRedundantCdPrefix('cd /proj && npm test', '/proj'), 'npm test');
  });
  it('only strips the first occurrence', () => {
    assert.equal(
      stripRedundantCdPrefix('cd /proj && echo cd /proj && done', '/proj'),
      'echo cd /proj && done',
    );
  });
  it('preserves a cd into a DIFFERENT directory', () => {
    assert.equal(stripRedundantCdPrefix('cd /other && ls', '/proj'), 'cd /other && ls');
  });
  it('treats the cwd path literally (no regex metachar surprise)', () => {
    assert.equal(stripRedundantCdPrefix('cd /a.b+c && ls', '/a.b+c'), 'ls');
  });
  it('is a no-op when cwd is undefined/empty', () => {
    assert.equal(stripRedundantCdPrefix('cd /proj && ls', undefined), 'cd /proj && ls');
    assert.equal(stripRedundantCdPrefix('cd /proj && ls', ''), 'cd /proj && ls');
  });
});

describe('U030 rewriteWindowsNullRedirect', () => {
  it('rewrites >nul, 2>nul, &>nul, >>nul (case-insensitive)', () => {
    assert.equal(rewriteWindowsNullRedirect('ls >nul'), 'ls >/dev/null');
    assert.equal(rewriteWindowsNullRedirect('ls 2>nul'), 'ls 2>/dev/null');
    assert.equal(rewriteWindowsNullRedirect('ls &>NUL'), 'ls &>/dev/null');
    assert.equal(rewriteWindowsNullRedirect('ls >> Nul'), 'ls >> /dev/null');
  });
  it('does NOT rewrite >null, >nul.txt, or a nul filename argument', () => {
    assert.equal(rewriteWindowsNullRedirect('ls >null'), 'ls >null');
    assert.equal(rewriteWindowsNullRedirect('ls >nul.txt'), 'ls >nul.txt');
    assert.equal(rewriteWindowsNullRedirect('cat nul.txt'), 'cat nul.txt');
  });
  it('rewrites a redirect followed by a pipe boundary', () => {
    assert.equal(rewriteWindowsNullRedirect('a 2>nul | b'), 'a 2>/dev/null | b');
  });
});

describe('U030 normalizeBashInput (combined pipeline)', () => {
  it('applies both transforms together', () => {
    assert.equal(
      normalizeBashInput('cd /proj && grep x file 2>nul', '/proj'),
      'grep x file 2>/dev/null',
    );
  });
});

// ---------------------------------------------------------------------------
// Unit: stripEmptyLines (parity: Claude Code BashTool/utils.ts)
// ---------------------------------------------------------------------------

describe('U030 stripEmptyLines', () => {
  it('removes leading/trailing empty lines', () => {
    assert.equal(stripEmptyLines('\n\nhello\n\n'), 'hello');
  });
  it('preserves interior blank lines', () => {
    assert.equal(stripEmptyLines('a\n\nb'), 'a\n\nb');
  });
  it('preserves leading indentation of content lines (unlike trim)', () => {
    assert.equal(stripEmptyLines('\n  indented\n'), '  indented');
  });
  it('treats whitespace-only lines as empty at the boundary', () => {
    assert.equal(stripEmptyLines('   \nreal\n\t'), 'real');
  });
  it('returns empty string when every line is blank', () => {
    assert.equal(stripEmptyLines('\n   \n\t\n'), '');
  });
});

// ---------------------------------------------------------------------------
// Integration: normalization is applied end-to-end through bashTool.execute
// ---------------------------------------------------------------------------

describe('U030 bashTool.execute — normalization reaches the executed command', () => {
  afterEach(() => mock.restoreAll());

  function captureExec() {
    let seen = '';
    mock.method(_bashHooks, 'shouldUseSandbox', () => false);
    mock.method(_bashHooks, 'execCommand', async (cmd: string) => {
      seen = cmd;
      return { stdout: 'ok', stderr: '', exitCode: 0 };
    });
    return () => seen;
  }

  it('strips a redundant `cd <cwd> && ` prefix before execution', async () => {
    const getSeen = captureExec();
    await bashTool.execute({ command: `cd ${process.cwd()} && echo hi` }, ctx());
    assert.equal(getSeen(), 'echo hi');
  });

  it('rewrites a Windows `>nul` redirect before execution', async () => {
    const getSeen = captureExec();
    await bashTool.execute({ command: 'echo hi 2>nul' }, ctx());
    assert.equal(getSeen(), 'echo hi 2>/dev/null');
  });

});

describe('U030 bashTool.execute — stripEmptyLines cleans stream boundaries', () => {
  afterEach(() => mock.restoreAll());

  it('drops trailing blank lines of stdout before the [stderr] section', async () => {
    mock.method(_bashHooks, 'shouldUseSandbox', () => false);
    mock.method(_bashHooks, 'execCommand', async () => ({
      stdout: 'line1\n\n',
      stderr: 'err1',
      exitCode: 0,
    }));
    const res = await bashTool.execute({ command: 'run-it' }, ctx());
    // GREEN: stdout's trailing blank lines are stripped, so only the single
    // join-newline + label precedes [stderr].
    assert.ok(res.output.includes('line1\n\n[stderr]'), res.output);
    // DISCRIMINATOR: pre-fix the raw trailing blanks survived, yielding >=3 \n.
    assert.ok(!res.output.includes('line1\n\n\n[stderr]'), res.output);
  });
});
