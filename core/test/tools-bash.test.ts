import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as childProcess from 'child_process';
import { bashTool, detectSelfKill } from '../src/tools/bash';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// Tool definition
// ---------------------------------------------------------------------------

describe('bashTool – definition', () => {
  it('has the correct name', () => {
    assert.equal(bashTool.name, 'bash');
  });

  it('has a description', () => {
    assert.ok(bashTool.description.length > 0);
  });

  it('is not read-only', () => {
    assert.equal(bashTool.isReadOnly, false);
  });

  it('schema requires "command"', () => {
    assert.ok(bashTool.inputSchema.required?.includes('command'));
  });

  it('schema defines command, timeout, and description properties', () => {
    const props = bashTool.inputSchema.properties;
    assert.ok('command' in props);
    assert.ok('timeout' in props);
    assert.ok('description' in props);
  });
});

// ---------------------------------------------------------------------------
// Execute – basic commands
// ---------------------------------------------------------------------------

describe('bashTool – execute', () => {
  it('runs a simple echo command', async () => {
    const result = await bashTool.execute({ command: 'echo hello' }, ctx());
    assert.ok(result.output.includes('hello'));
    assert.ok(!result.isError);
  });

  it('returns exit code 0 for successful commands', async () => {
    const result = await bashTool.execute({ command: 'true' }, ctx());
    assert.ok(result.output.includes('Exit code: 0'));
    assert.ok(!result.isError);
  });

  it('returns non-zero exit code and isError for failing commands', async () => {
    const result = await bashTool.execute({ command: 'false' }, ctx());
    assert.ok(result.output.includes('Exit code: 1'));
    assert.equal(result.isError, true);
  });

  it('captures stderr', async () => {
    const result = await bashTool.execute(
      { command: 'echo errormsg >&2' },
      ctx(),
    );
    assert.ok(result.output.includes('errormsg'));
  });

  it('returns (no output) for commands with no stdout/stderr and exit 0', async () => {
    const result = await bashTool.execute({ command: 'true' }, ctx());
    // exit code is still appended
    assert.ok(result.output.includes('Exit code: 0'));
  });
});

// ---------------------------------------------------------------------------
// Execute – empty/invalid command
// ---------------------------------------------------------------------------

describe('bashTool – empty command', () => {
  it('rejects empty command string', async () => {
    const result = await bashTool.execute({ command: '' }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('must not be empty'));
  });

  it('rejects whitespace-only command', async () => {
    const result = await bashTool.execute({ command: '   ' }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('must not be empty'));
  });
});

// ---------------------------------------------------------------------------
// Execute – timeout
// ---------------------------------------------------------------------------

describe('bashTool – timeout', () => {
  it('kills long-running commands after custom timeout', async () => {
    const result = await bashTool.execute(
      { command: 'sleep 60', timeout: 200 },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('timed out'));
  });
});

// ---------------------------------------------------------------------------
// Execute – abort signal
// ---------------------------------------------------------------------------

describe('bashTool – abort signal', () => {
  it('aborts immediately if signal already aborted', async () => {
    const ac = new AbortController();
    ac.abort();
    const result = await bashTool.execute(
      { command: 'echo hi' },
      ctx({ abortSignal: ac.signal }),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('abort') || result.output.includes('Abort'),
      `Expected abort message, got: ${result.output}`);
  });

  it('aborts a running command when signal fires', async () => {
    const ac = new AbortController();
    const promise = bashTool.execute(
      { command: 'sleep 60' },
      ctx({ abortSignal: ac.signal }),
    );
    setTimeout(() => ac.abort(), 100);
    const result = await promise;
    assert.equal(result.isError, true);
  });
});

// ---------------------------------------------------------------------------
// Execute – onProgress
// ---------------------------------------------------------------------------

describe('bashTool – onProgress', () => {
  it('streams stdout chunks via onProgress callback', async () => {
    const chunks: string[] = [];
    const result = await bashTool.execute(
      { command: 'echo line1 && echo line2' },
      ctx({ onProgress: (c) => chunks.push(c) }),
    );
    assert.ok(!result.isError);
    const joined = chunks.join('');
    assert.ok(joined.includes('line1'));
    assert.ok(joined.includes('line2'));
  });

  it('streams stderr chunks via onProgress callback', async () => {
    const chunks: string[] = [];
    await bashTool.execute(
      { command: 'echo err >&2' },
      ctx({ onProgress: (c) => chunks.push(c) }),
    );
    assert.ok(chunks.join('').includes('err'));
  });
});

// ---------------------------------------------------------------------------
// Execute – cwd
// ---------------------------------------------------------------------------

describe('bashTool – cwd', () => {
  it('executes in the specified cwd', async () => {
    const result = await bashTool.execute(
      { command: 'pwd' },
      ctx({ cwd: '/tmp' }),
    );
    assert.ok(result.output.includes('/tmp'));
  });
});

// ---------------------------------------------------------------------------
// Output truncation
// ---------------------------------------------------------------------------

describe('bashTool – output truncation', () => {
  it('truncates very long stdout', async () => {
    // Generate ~70000 characters
    const result = await bashTool.execute(
      { command: 'python3 -c "print(\'x\' * 70000)"' },
      ctx(),
    );
    // The output should contain the truncation marker
    assert.ok(
      result.output.includes('truncated') || result.output.length <= 70000,
      'Very long output should be truncated',
    );
  });
});

// ---------------------------------------------------------------------------
// Timeout handling (lines 72-77, 83-84)
// ---------------------------------------------------------------------------

describe('bashTool – timeout', () => {
  it('kills command after timeout and reports timeout message', async () => {
    const result = await bashTool.execute(
      { command: 'sleep 30', timeout: 200 },
      ctx(),
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('timed out') || result.output.includes('Timed out'),
      `Expected timeout message, got: ${result.output}`);
  });
});

// ---------------------------------------------------------------------------
// Abort during execution (lines 80-84, 107-112)
// ---------------------------------------------------------------------------

describe('bashTool – abort during execution', () => {
  it('returns abort message when signal fires mid-execution', async () => {
    const ac = new AbortController();
    const promise = bashTool.execute(
      { command: 'sleep 5' },
      ctx({ abortSignal: ac.signal }),
    );
    // Abort after a short delay
    setTimeout(() => ac.abort(), 200);
    const result = await promise;
    assert.ok(result.isError);
    assert.ok(
      result.output.includes('abort') || result.output.includes('Abort') ||
      result.output.includes('aborted') || result.output.includes('Command aborted'),
      `Expected abort message, got: ${result.output}`,
    );
  });

  it('returns abort immediately when signal is already aborted', async () => {
    const ac = new AbortController();
    ac.abort();
    const result = await bashTool.execute(
      { command: 'echo should-not-run' },
      ctx({ abortSignal: ac.signal }),
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('aborted') || result.output.includes('abort'));
  });
});

// ---------------------------------------------------------------------------
// Error handling for spawn failure
// ---------------------------------------------------------------------------

describe('bashTool – spawn error', () => {
  it('handles error event from child process (lines 107-112)', async () => {
    // Trigger error by executing in a non-existent cwd
    // This may trigger an error event on the child process
    const result = await bashTool.execute(
      { command: 'echo test' },
      ctx({ cwd: '/nonexistent/dir/xyz' }),
    );
    assert.ok(result.isError);
  });
});

// ---------------------------------------------------------------------------
// Non-interactive environment forcing
// ---------------------------------------------------------------------------

describe('bashTool – non-interactive env forcing', () => {
  it('sets GIT_EDITOR to false to prevent interactive git editors', async () => {
    const result = await bashTool.execute({ command: 'echo $GIT_EDITOR' }, ctx());
    assert.ok(result.output.includes('false'));
  });

  it('sets PAGER to cat to prevent interactive pagers', async () => {
    const result = await bashTool.execute({ command: 'echo $PAGER' }, ctx());
    assert.ok(result.output.includes('cat'));
  });

  it('sets GIT_PAGER to cat to prevent interactive git pagers', async () => {
    const result = await bashTool.execute({ command: 'echo $GIT_PAGER' }, ctx());
    assert.ok(result.output.includes('cat'));
  });

  it('sets GIT_TERMINAL_PROMPT to 0 to prevent git credential prompts', async () => {
    const result = await bashTool.execute({ command: 'echo $GIT_TERMINAL_PROMPT' }, ctx());
    assert.ok(result.output.includes('0'));
  });

  it('sets DEBIAN_FRONTEND to noninteractive for apt', async () => {
    const result = await bashTool.execute({ command: 'echo $DEBIAN_FRONTEND' }, ctx());
    assert.ok(result.output.includes('noninteractive'));
  });

  it('sets AI_AGENT to superinference for tool identification', async () => {
    const result = await bashTool.execute({ command: 'echo $AI_AGENT' }, ctx());
    assert.ok(result.output.includes('superinference'));
  });

  it('sets EDITOR and VISUAL to false to block editor launches', async () => {
    const result = await bashTool.execute({ command: 'echo $EDITOR:$VISUAL' }, ctx());
    assert.ok(result.output.includes('false:false'));
  });
});

// ---------------------------------------------------------------------------
// Command chaining detection
// ---------------------------------------------------------------------------

describe('bashTool – command chaining detection', () => {
  it('appends chaining note when 3+ commands are chained', async () => {
    const result = await bashTool.execute(
      { command: 'echo a && echo b && echo c' },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('[Note:'));
    assert.ok(result.output.includes('chains 3 commands'));
  });

  it('does not append note for 2-command chain', async () => {
    const result = await bashTool.execute(
      { command: 'echo a && echo b' },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.ok(!result.output.includes('[Note:'));
  });

  it('does not append note for simple commands', async () => {
    const result = await bashTool.execute(
      { command: 'echo hello' },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.ok(!result.output.includes('[Note:'));
  });

  it('does not append note for pipe-only commands', async () => {
    const result = await bashTool.execute(
      { command: 'echo hello | cat' },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.ok(!result.output.includes('[Note:'));
  });
});

// ---------------------------------------------------------------------------
// Git commit guard
// ---------------------------------------------------------------------------

describe('bashTool – git commit guard', () => {
  const GUARD_MSG = 'Use the git_commit tool instead';

  it('blocks git commit -m and suggests git_commit tool', async () => {
    const result = await bashTool.execute(
      { command: 'git commit -m "test message"' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes(GUARD_MSG));
    assert.ok(result.output.includes('Co-Authored-By'));
  });

  it('blocks git commit --amend', async () => {
    const result = await bashTool.execute(
      { command: 'git commit --amend' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes(GUARD_MSG));
  });

  it('blocks chained commands containing git commit', async () => {
    const result = await bashTool.execute(
      { command: 'git add . && git commit -m "chained"' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes(GUARD_MSG));
  });

  it('does not block git log', async () => {
    const result = await bashTool.execute(
      { command: 'git log --oneline -1' },
      ctx(),
    );
    assert.ok(!result.output.includes(GUARD_MSG));
  });

  it('does not block git status', async () => {
    const result = await bashTool.execute(
      { command: 'git status' },
      ctx(),
    );
    assert.ok(!result.output.includes(GUARD_MSG));
  });

  it('does not block git show', async () => {
    const result = await bashTool.execute(
      { command: 'git show HEAD --stat' },
      ctx(),
    );
    assert.ok(!result.output.includes(GUARD_MSG));
  });

  it('does not false-positive on echo "git commit"', async () => {
    const result = await bashTool.execute(
      { command: 'echo "git commit"' },
      ctx(),
    );
    assert.ok(!result.output.includes(GUARD_MSG));
  });

  it('does not false-positive on single-quoted git commit', async () => {
    const result = await bashTool.execute(
      { command: "echo 'git commit -m test'" },
      ctx(),
    );
    assert.ok(!result.output.includes(GUARD_MSG));
  });

  it('blocks git commit with env var prefix', async () => {
    const result = await bashTool.execute(
      { command: 'GIT_AUTHOR_NAME=bot git commit -m "test"' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes(GUARD_MSG));
  });

  it('does not block git diff or git add', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-bash-test-'));
    try {
      childProcess.execSync('git init', { cwd: tmpDir, stdio: 'pipe' });
      childProcess.execSync('git config user.name "Test"', { cwd: tmpDir, stdio: 'pipe' });
      childProcess.execSync('git config user.email "t@t.com"', { cwd: tmpDir, stdio: 'pipe' });
      fs.writeFileSync(path.join(tmpDir, 'f.txt'), 'x');

      const result1 = await bashTool.execute(
        { command: 'git diff --cached' },
        ctx({ cwd: tmpDir }),
      );
      assert.ok(!result1.output.includes(GUARD_MSG));

      const result2 = await bashTool.execute(
        { command: 'git add .' },
        ctx({ cwd: tmpDir }),
      );
      assert.ok(!result2.output.includes(GUARD_MSG));
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// detectSelfKill — runtime PID guard
// ---------------------------------------------------------------------------

describe('detectSelfKill', () => {
  it('detects kill targeting own PID', () => {
    const result = detectSelfKill(`kill ${process.pid}`);
    assert.ok(result, 'Should detect kill of own PID');
    assert.ok(result!.includes('AMI itself'));
  });

  it('detects kill -9 targeting own PID', () => {
    const result = detectSelfKill(`kill -9 ${process.pid}`);
    assert.ok(result, 'Should detect kill -9 of own PID');
  });

  it('detects kill targeting parent PID', () => {
    const result = detectSelfKill(`kill ${process.ppid}`);
    assert.ok(result, 'Should detect kill of parent PID');
    assert.ok(result!.includes('parent'));
  });

  it('allows kill of unrelated PID', () => {
    const fakePid = 99999;
    if (fakePid !== process.pid && fakePid !== process.ppid) {
      const result = detectSelfKill(`kill ${fakePid}`);
      assert.equal(result, null, 'Should allow kill of unrelated PID');
    }
  });

  it('allows non-kill commands', () => {
    assert.equal(detectSelfKill('echo hello'), null);
    assert.equal(detectSelfKill('ls -la'), null);
    assert.equal(detectSelfKill('node server.js'), null);
  });

  it('ignores PIDs inside quotes', () => {
    const result = detectSelfKill(`echo "kill ${process.pid}"`);
    assert.equal(result, null, 'Should ignore PIDs in quoted strings');
  });
});
