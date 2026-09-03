/**
 * Tests for the run_tests / build guardrail integration and the extended
 * BASH_TEST_RUN_RE regex. Covers the three changes made to tool-guardrails.ts:
 *
 * 1. run_tests and build count toward totalBashCalls and reset bash counters
 * 2. run_tests increments bashTestRunsSinceLastEdit; build does not
 * 3. BASH_TEST_RUN_RE covers all supported languages (go test, cargo test, etc.)
 * 4. run_tests repetition guard (beforeCall) fires at 3 and 5 calls without edits
 * 5. recoveryHint covers run_tests and build
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { ToolCallGuardrailController } from '../src/tool-guardrails';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeCtrl(detached = true): ToolCallGuardrailController {
  return new ToolCallGuardrailController(detached);
}

function bash(ctrl: ToolCallGuardrailController, cmd: string, failed = false): void {
  ctrl.beforeCall('bash', { command: cmd });
  ctrl.afterCall('bash', { command: cmd }, failed ? 'error' : 'ok', failed);
}

function runTests(ctrl: ToolCallGuardrailController, failed = false): void {
  ctrl.beforeCall('run_tests', {});
  ctrl.afterCall('run_tests', {}, failed ? 'FAIL' : 'ok', failed);
}

function build(ctrl: ToolCallGuardrailController, failed = false): void {
  ctrl.beforeCall('build', {});
  ctrl.afterCall('build', {}, failed ? 'error' : 'ok', failed);
}

function readFile(ctrl: ToolCallGuardrailController): void {
  ctrl.beforeCall('file_read', { file_path: '/src/foo.go' });
  ctrl.afterCall('file_read', { file_path: '/src/foo.go' }, 'content', false);
}

function editFile(ctrl: ToolCallGuardrailController, failed = false): void {
  ctrl.beforeCall('file_edit', { file_path: '/src/foo.go', old_string: 'a', new_string: 'b' });
  ctrl.afterCall('file_edit', { file_path: '/src/foo.go', old_string: 'a', new_string: 'b' }, failed ? 'error' : 'ok', failed);
}

// ---------------------------------------------------------------------------
// run_tests counts as totalBashCalls
// ---------------------------------------------------------------------------

describe('run_tests — counts as bash-equivalent in progress', () => {
  it('increments totalBashCalls', () => {
    const ctrl = makeCtrl();
    runTests(ctrl);
    assert.equal(ctrl.getProgress().totalBashCalls, 1);
  });

  it('increments totalToolCalls', () => {
    const ctrl = makeCtrl();
    runTests(ctrl);
    assert.equal(ctrl.getProgress().totalToolCalls, 1);
  });

  it('resets editsSinceLastBash', () => {
    const ctrl = makeCtrl();
    editFile(ctrl);
    assert.equal(ctrl.getProgress().editsSinceLastBash, 1);
    runTests(ctrl);
    assert.equal(ctrl.getProgress().editsSinceLastBash, 0);
  });

  it('resets toolsSinceLastBash', () => {
    const ctrl = makeCtrl();
    readFile(ctrl);
    readFile(ctrl);
    assert.ok(ctrl.getProgress().toolsSinceLastBash > 0);
    runTests(ctrl);
    assert.equal(ctrl.getProgress().toolsSinceLastBash, 0);
  });

  it('multiple run_tests calls accumulate totalBashCalls', () => {
    const ctrl = makeCtrl();
    runTests(ctrl);
    runTests(ctrl);
    runTests(ctrl);
    assert.equal(ctrl.getProgress().totalBashCalls, 3);
  });
});

// ---------------------------------------------------------------------------
// build counts as totalBashCalls
// ---------------------------------------------------------------------------

describe('build — counts as bash-equivalent in progress', () => {
  it('increments totalBashCalls', () => {
    const ctrl = makeCtrl();
    build(ctrl);
    assert.equal(ctrl.getProgress().totalBashCalls, 1);
  });

  it('resets editsSinceLastBash', () => {
    const ctrl = makeCtrl();
    editFile(ctrl);
    build(ctrl);
    assert.equal(ctrl.getProgress().editsSinceLastBash, 0);
  });

  it('resets toolsSinceLastBash', () => {
    const ctrl = makeCtrl();
    readFile(ctrl);
    build(ctrl);
    assert.equal(ctrl.getProgress().toolsSinceLastBash, 0);
  });
});

// ---------------------------------------------------------------------------
// bashTestRunsSinceLastEdit — run_tests increments, build does not
// ---------------------------------------------------------------------------

describe('bashTestRunsSinceLastEdit tracking', () => {
  it('run_tests increments the test-run counter', () => {
    const ctrl = makeCtrl();
    runTests(ctrl);
    // Verify: a 4th run_tests without edits should warn (counter = 1 here, no warn yet)
    const d = ctrl.beforeCall('run_tests', {});
    assert.equal(d.action, 'allow');
  });

  it('build does NOT increment the test-run counter', () => {
    const ctrl = makeCtrl();
    // 4 builds with no edits — should not trigger run_tests repetition warning
    build(ctrl); build(ctrl); build(ctrl); build(ctrl);
    const d = ctrl.beforeCall('run_tests', {});
    assert.equal(d.action, 'allow');
  });

  it('run_tests counter resets after a successful file_edit', () => {
    const ctrl = makeCtrl();
    runTests(ctrl);
    runTests(ctrl);
    runTests(ctrl); // counter = 3, warn threshold
    editFile(ctrl); // resets counter to 0
    // Now run_tests should be allowed again (counter reset)
    const d = ctrl.beforeCall('run_tests', {});
    assert.equal(d.action, 'allow');
  });

  it('bash go test increments the counter — warns at 3+, blocks at 5+', () => {
    const ctrl = makeCtrl();
    // 3 calls → counter=3 → next beforeCall warns
    bash(ctrl, 'go test ./...'); bash(ctrl, 'go test ./...'); bash(ctrl, 'go test ./...');
    assert.equal(ctrl.beforeCall('bash', { command: 'go test ./...' }).action, 'warn');
    // 2 more → counter=5 → next beforeCall blocks
    bash(ctrl, 'go test ./...'); bash(ctrl, 'go test ./...');
    assert.equal(ctrl.beforeCall('bash', { command: 'go test ./...' }).action, 'block');
  });
});

// ---------------------------------------------------------------------------
// run_tests repetition guard (beforeCall)
// ---------------------------------------------------------------------------

describe('run_tests repetition guard', () => {
  it('allows first 2 run_tests without edits', () => {
    const ctrl = makeCtrl();
    runTests(ctrl);
    runTests(ctrl);
    const d = ctrl.beforeCall('run_tests', {});
    assert.equal(d.action, 'allow');
  });

  it('warns at 3 run_tests without edits', () => {
    const ctrl = makeCtrl();
    runTests(ctrl); runTests(ctrl); runTests(ctrl);
    const d = ctrl.beforeCall('run_tests', {});
    assert.equal(d.action, 'warn');
    assert.ok(d.reason?.includes('3+ times'));
  });

  it('blocks at 5 run_tests without edits', () => {
    const ctrl = makeCtrl();
    runTests(ctrl); runTests(ctrl); runTests(ctrl); runTests(ctrl); runTests(ctrl);
    const d = ctrl.beforeCall('run_tests', {});
    assert.equal(d.action, 'block');
    assert.ok(d.reason?.includes('5+ times'));
  });

  it('resets after file_edit — allows run_tests again', () => {
    const ctrl = makeCtrl();
    runTests(ctrl); runTests(ctrl); runTests(ctrl); runTests(ctrl); runTests(ctrl);
    editFile(ctrl);
    const d = ctrl.beforeCall('run_tests', {});
    assert.equal(d.action, 'allow');
  });

  it('no guard in non-detached mode', () => {
    const ctrl = makeCtrl(false);
    for (let i = 0; i < 10; i++) runTests(ctrl);
    const d = ctrl.beforeCall('run_tests', {});
    assert.equal(d.action, 'allow');
  });

  it('warn and block messages mention the right action', () => {
    const ctrl = makeCtrl();
    runTests(ctrl); runTests(ctrl); runTests(ctrl);
    const warn = ctrl.beforeCall('run_tests', {});
    assert.ok(warn.reason?.includes('edit'));
    runTests(ctrl); runTests(ctrl);
    const block = ctrl.beforeCall('run_tests', {});
    assert.ok(block.reason?.includes('BLOCKED'));
  });
});

// ---------------------------------------------------------------------------
// BASH_TEST_RUN_RE — extended language coverage
// ---------------------------------------------------------------------------

describe('BASH_TEST_RUN_RE — language detection in bash commands', () => {
  const languages: [string, string][] = [
    ['go test ./...', 'go test'],
    ['go test -run TestFoo ./pkg/', 'go test with filter'],
    ['cargo test', 'cargo test'],
    ['cargo test my_fn -- --nocapture', 'cargo test with args'],
    ['mvn test --fail-at-end', 'mvn test'],
    ['dotnet test', 'dotnet test'],
    ['ctest --test-dir build -V', 'ctest'],
    ['./gradlew test', 'gradlew test'],
    ['bundle exec rspec', 'rspec'],
    ['./vendor/bin/phpunit', 'phpunit'],
    ['npm test', 'npm test'],
    ['yarn test', 'yarn test'],
    ['pnpm test', 'pnpm test'],
    ['pytest tests/', 'pytest'],
    ['python -m unittest', 'unittest'],
    ['runtests.py', 'runtests'],
  ];

  for (const [cmd, label] of languages) {
    it(`detects "${label}" — 3 calls warns`, () => {
      const ctrl = makeCtrl();
      bash(ctrl, cmd); bash(ctrl, cmd); bash(ctrl, cmd);
      const d = ctrl.beforeCall('bash', { command: cmd });
      assert.equal(d.action, 'warn', `Expected warn for: ${cmd}`);
    });

    it(`detects "${label}" — 5 calls blocks`, () => {
      const ctrl = makeCtrl();
      for (let i = 0; i < 5; i++) bash(ctrl, cmd);
      const d = ctrl.beforeCall('bash', { command: cmd });
      assert.equal(d.action, 'block', `Expected block for: ${cmd}`);
    });
  }

  it('does not trigger for non-test bash commands', () => {
    const ctrl = makeCtrl();
    for (let i = 0; i < 10; i++) bash(ctrl, 'echo hello');
    const d = ctrl.beforeCall('bash', { command: 'echo hello' });
    // Only the exact-failure guard applies, not the test-run guard
    assert.equal(d.action, 'allow');
  });
});

// ---------------------------------------------------------------------------
// recoveryHint — run_tests and build
// ---------------------------------------------------------------------------

describe('recoveryHint for run_tests and build', () => {
  it('run_tests gets the bash-recovery hint (not the generic "different approach" hint)', () => {
    // recoveryHint('run_tests') should return the fix-code hint, not the fallback
    // Verify via afterCall exact-failure which embeds recoveryHint in its message
    const ctrl = makeCtrl();
    // Use args that avoid the repetition guard: provide a command arg so it differs
    for (let i = 0; i < 4; i++) {
      ctrl.afterCall('run_tests', { command: 'cargo test' }, 'FAIL', true);
    }
    const d = ctrl.beforeCall('run_tests', { command: 'cargo test' });
    assert.equal(d.action, 'block');
    assert.ok(
      d.reason?.includes('file_read') || d.reason?.includes('fix the code'),
      `Got: ${d.reason}`,
    );
  });

  it('build gets the bash-recovery hint after 4 identical failures', () => {
    const ctrl = makeCtrl();
    for (let i = 0; i < 4; i++) {
      ctrl.afterCall('build', { command: 'cargo build' }, 'error', true);
    }
    const d = ctrl.beforeCall('build', { command: 'cargo build' });
    assert.equal(d.action, 'block');
    assert.ok(
      d.reason?.includes('file_read') || d.reason?.includes('fix the code'),
      `Got: ${d.reason}`,
    );
  });
});

// ---------------------------------------------------------------------------
// Integration: Qwen-style workflow — run_tests used instead of bash
// ---------------------------------------------------------------------------

describe('integration — Qwen workflow using run_tests', () => {
  it('progress correctly reflects run_tests as commands executed', () => {
    const ctrl = makeCtrl();
    readFile(ctrl);                // reads: 1
    readFile(ctrl);                // reads: 2
    runTests(ctrl);                // bash: 1, testRuns: 1
    editFile(ctrl);                // edits: 1, testRuns reset to 0
    runTests(ctrl);                // bash: 2, testRuns: 1
    const p = ctrl.getProgress();
    assert.equal(p.totalToolCalls, 5);
    assert.equal(p.totalBashCalls, 2);
    assert.equal(p.totalEdits, 1);
    assert.equal(p.totalReads, 2);
    assert.equal(p.editsSinceLastBash, 0);
  });

  it('build + run_tests workflow: build does not trigger test-repetition guard', () => {
    const ctrl = makeCtrl();
    readFile(ctrl);
    editFile(ctrl);          // testRuns reset
    build(ctrl);             // bash call, not test run
    runTests(ctrl);          // testRuns: 1
    runTests(ctrl);          // testRuns: 2
    const d = ctrl.beforeCall('run_tests', {});
    assert.equal(d.action, 'allow'); // only 2 test runs, not yet at warn threshold
  });

  it('mixed bash go test + run_tests: both count toward test-run limit', () => {
    const ctrl = makeCtrl();
    bash(ctrl, 'go test ./...');  // testRuns: 1
    bash(ctrl, 'go test ./...');  // testRuns: 2
    runTests(ctrl);               // testRuns: 3 → warn threshold reached
    const d = ctrl.beforeCall('run_tests', {});
    assert.equal(d.action, 'warn');
  });

  it('engine nudge scenario: totalBashCalls > 0 when run_tests used', () => {
    // Mirrors the engine.ts check: progress.totalBashCalls === 0 fires wrong nudge
    const ctrl = makeCtrl();
    readFile(ctrl);
    readFile(ctrl);
    runTests(ctrl); // previously this left totalBashCalls=0 causing wrong engine nudge
    const p = ctrl.getProgress();
    assert.ok(p.totalBashCalls > 0, 'run_tests must count as a bash call for engine nudges to work');
    assert.equal(p.totalEdits, 0); // no edits yet — correct
  });

  it('complete successful workflow produces expected final progress', () => {
    const ctrl = makeCtrl();
    readFile(ctrl);                       // explore
    readFile(ctrl);
    runTests(ctrl);                       // discover failures
    readFile(ctrl);                       // read failing code
    editFile(ctrl);                       // fix
    build(ctrl);                          // rebuild (compiled lang)
    runTests(ctrl);                       // verify
    const p = ctrl.getProgress();
    assert.equal(p.totalBashCalls, 3);    // 1 run_tests + 1 build + 1 run_tests
    assert.equal(p.totalEdits, 1);
    assert.equal(p.totalReads, 3);
    assert.equal(p.editsSinceLastBash, 0); // run_tests reset it
  });
});

// ---------------------------------------------------------------------------
// reset() clears all new state
// ---------------------------------------------------------------------------

describe('reset clears all state including run_tests tracking', () => {
  it('reset after run_tests calls restores clean state', () => {
    const ctrl = makeCtrl();
    runTests(ctrl); runTests(ctrl); runTests(ctrl); runTests(ctrl); runTests(ctrl);
    // Block state
    assert.equal(ctrl.beforeCall('run_tests', {}).action, 'block');
    ctrl.reset();
    // After reset, guard is cleared
    assert.equal(ctrl.beforeCall('run_tests', {}).action, 'allow');
    assert.equal(ctrl.getProgress().totalBashCalls, 0);
    assert.equal(ctrl.getProgress().totalToolCalls, 0);
  });
});

// ---------------------------------------------------------------------------
// Pre-existing uncovered paths — bash guards, file_edit limits, recovery hints
// ---------------------------------------------------------------------------

describe('bash guards — formatting and file-mutation blockers', () => {
  const check = (cmd: string, action: 'block' | 'warn') =>
    it(`${action}s: ${cmd.slice(0, 60)}`, () => {
      const ctrl = makeCtrl();
      const d = ctrl.beforeCall('bash', { command: cmd });
      assert.equal(d.action, action);
    });

  check('black src/', 'block');
  check('autopep8 --in-place foo.py', 'block');
  check('prettier --write src/', 'block');
  check('ruff format .', 'block');
  check('sed -i "s/foo/bar/" file.go', 'block');
  check('sed -ri "s/x/y/" *.go', 'block');
  check('git apply patch.diff', 'block');
  check('git am < mbox', 'block');
  check('patch -p1 < fix.diff', 'block');
  check('python -c "open(\'f\').write(\'x\')"', 'block');
  check('git checkout src/foo.go', 'warn');
});

describe('file_edit size guards', () => {
  function makeEdit(lines: number) {
    return { file_path: '/src/x.go', old_string: Array(lines).fill('a').join('\n'), new_string: Array(lines).fill('b').join('\n') };
  }

  it('blocks edits spanning more than 30 lines', () => {
    const ctrl = makeCtrl();
    const d = ctrl.beforeCall('file_edit', makeEdit(31));
    assert.equal(d.action, 'block');
    assert.ok(d.reason?.includes('31'));
  });

  it('warns on edits spanning 16-30 lines', () => {
    const ctrl = makeCtrl();
    const d = ctrl.beforeCall('file_edit', makeEdit(16));
    assert.equal(d.action, 'warn');
    assert.ok(d.reason?.includes('16'));
  });

  it('blocks after 8 successful edits', () => {
    const ctrl = makeCtrl();
    for (let i = 0; i < 8; i++) {
      ctrl.afterCall('file_edit', { file_path: `/src/f${i}.go`, old_string: 'a', new_string: 'b' }, 'ok', false);
    }
    const d = ctrl.beforeCall('file_edit', { file_path: '/src/new.go', old_string: 'x', new_string: 'y' });
    assert.equal(d.action, 'block');
    assert.ok(d.reason?.includes('8 successful edits'));
  });
});

describe('no-progress and stall guards', () => {
  it('warns when file_read is repeated 10+ times without bash or edits', () => {
    const ctrl = makeCtrl();
    for (let i = 0; i < 10; i++) {
      ctrl.afterCall('file_read', { file_path: `/src/f${i}.go` }, 'content', false);
    }
    const d = ctrl.beforeCall('file_read', { file_path: '/src/x.go' });
    assert.equal(d.action, 'warn');
    assert.ok(d.reason?.includes('10+'));
  });

  it('warns when edits accumulate with failures and no bash', () => {
    const ctrl = makeCtrl();
    // 5 edits with 3+ failures and no bash
    for (let i = 0; i < 3; i++) ctrl.afterCall('file_edit', { file_path: '/a', old_string: 'x', new_string: 'y' }, 'error', true);
    for (let i = 0; i < 2; i++) ctrl.afterCall('file_edit', { file_path: '/b', old_string: 'x', new_string: 'y' }, 'ok', false);
    const d = ctrl.beforeCall('file_edit', { file_path: '/c', old_string: 'x', new_string: 'y' });
    assert.equal(d.action, 'warn');
    assert.ok(d.reason?.includes('multiple file edits'));
  });

  it('no-progress blocks idempotent tool returning same result 5 times', () => {
    // Interleave a different tool call between each grep to prevent loop detection,
    // which fires before the no-progress check.
    const ctrl = makeCtrl();
    const args = { pattern: 'foo', path: '.' };
    let last;
    for (let i = 0; i < 5; i++) {
      last = ctrl.afterCall('grep', args, 'same result', false);
      ctrl.afterCall('file_read', { file_path: `/src/f${i}.go` }, 'content', false); // break loop
    }
    assert.equal(last!.action, 'block');
  });

  it('no-progress warns at 3 identical results', () => {
    const ctrl = makeCtrl();
    const args = { pattern: 'bar', path: '.' };
    ctrl.afterCall('grep', args, 'same', false);
    ctrl.afterCall('file_read', { file_path: '/f1' }, 'x', false); // break loop
    ctrl.afterCall('grep', args, 'same', false);
    ctrl.afterCall('file_read', { file_path: '/f2' }, 'x', false);
    const d = ctrl.afterCall('grep', args, 'same', false);
    assert.equal(d.action, 'warn');
  });
});

// getRecoveryAction is embedded in afterCall return values (not beforeCall).
// After 4 identical failures, afterCall returns block with the specific hint.
describe('getRecoveryAction — all branches', () => {
  function failUntilBlock(toolName: string, args: Record<string, unknown>, output: string) {
    const ctrl = makeCtrl();
    let d;
    // EXACT_FAILURE_BLOCK=4: 4th afterCall returns block
    for (let i = 0; i < 4; i++) {
      d = ctrl.afterCall(toolName, args, output, true);
    }
    return d!;
  }

  it('file_edit "modified since" recovery — hints file_write', () => {
    const d = failUntilBlock('file_edit', { file_path: '/x', old_string: 'a', new_string: 'b' }, 'modified since you last read');
    assert.equal(d.action, 'block');
    assert.ok(d.reason?.includes('file_write'), `Got: ${d.reason}`);
  });

  it('file_edit "not found" recovery — includes file_path in hint', () => {
    const d = failUntilBlock('file_edit', { file_path: '/src/foo.go', old_string: 'missing', new_string: 'x' }, 'not found in file');
    assert.equal(d.action, 'block');
    assert.ok(d.reason?.includes('file_read') || d.reason?.includes('/src/foo.go'), `Got: ${d.reason}`);
  });

  it('file_edit "not found" without file_path — generic hint', () => {
    const d = failUntilBlock('file_edit', { old_string: 'missing', new_string: 'x' }, 'not found in file');
    assert.equal(d.action, 'block');
    assert.ok(d.reason?.includes('file_read'), `Got: ${d.reason}`);
  });

  it('file_edit "multiple matches" recovery — hints replace_all', () => {
    const d = failUntilBlock('file_edit', { file_path: '/x', old_string: 'dup', new_string: 'z' }, 'multiple matches found');
    assert.equal(d.action, 'block');
    assert.ok(d.reason?.includes('replace_all'), `Got: ${d.reason}`);
  });

  it('bash "command not found" recovery — hints tool_search', () => {
    const d = failUntilBlock('bash', { command: 'xyzzy' }, 'xyzzy: command not found');
    assert.equal(d.action, 'block');
    assert.ok(d.reason?.includes('tool_search'), `Got: ${d.reason}`);
  });

  it('bash "permission denied" recovery — hints permissions', () => {
    const d = failUntilBlock('bash', { command: 'cat /etc/shadow' }, 'permission denied');
    assert.equal(d.action, 'block');
    assert.ok(d.reason?.includes('permission'), `Got: ${d.reason}`);
  });

  it('bash LaTeX error recovery — hints web_search', () => {
    const d = failUntilBlock('bash', { command: 'pdflatex doc.tex' }, 'LaTeX Error: file not found');
    assert.equal(d.action, 'block');
    assert.ok(d.reason?.includes('web_search'), `Got: ${d.reason}`);
  });

  it('grep timed out recovery — hints narrow search', () => {
    const d = failUntilBlock('grep', { pattern: '.*', path: '/' }, 'timed out');
    assert.equal(d.action, 'block');
    assert.ok(d.reason?.includes('narrow') || d.reason?.includes('search path'), `Got: ${d.reason}`);
  });

  it('web_fetch SSRF recovery — hints private address', () => {
    const d = failUntilBlock('web_fetch', { url: 'http://internal/' }, 'SSRF: private address');
    assert.equal(d.action, 'block');
    assert.ok(d.reason?.includes('private') || d.reason?.includes('internal'), `Got: ${d.reason}`);
  });
});

describe('per-file edit and failure guards', () => {
  it('warns when same file is edited 5+ times', () => {
    const ctrl = makeCtrl();
    // Record 5 successful edits on the same file
    for (let i = 0; i < 5; i++) {
      ctrl.afterCall('file_edit', { file_path: '/src/foo.go', old_string: `a${i}`, new_string: `b${i}` }, 'ok', false);
    }
    const d = ctrl.beforeCall('file_edit', { file_path: '/src/foo.go', old_string: 'x', new_string: 'y' });
    assert.equal(d.action, 'warn');
    assert.ok(d.reason?.includes('/src/foo.go'));
  });

  it('warns on replace_all usage', () => {
    const ctrl = makeCtrl();
    const d = ctrl.beforeCall('file_edit', { file_path: '/src/x.go', old_string: 'a', new_string: 'b', replace_all: true });
    assert.equal(d.action, 'warn');
    assert.ok(d.reason?.includes('replace_all'));
  });

  it('blocks when same file fails 4+ times across different args', () => {
    const ctrl = makeCtrl();
    // Use different old_string each time so exact-failure sig differs, but same file_path
    for (let i = 0; i < 4; i++) {
      ctrl.afterCall('file_edit', { file_path: '/src/foo.go', old_string: `unique_${i}`, new_string: 'b' }, 'error', true);
    }
    // The 4th afterCall should have returned a block already — verify by checking 5th
    const d = ctrl.afterCall('file_edit', { file_path: '/src/foo.go', old_string: 'unique_4', new_string: 'b' }, 'error', true);
    assert.equal(d.action, 'block');
    assert.ok(d.reason?.includes('file_write'));
  });

  it('warns when same file fails 3 times across different args', () => {
    const ctrl = makeCtrl();
    // The 3rd afterCall for same file_path (different args) returns warn
    let d;
    for (let i = 0; i < 3; i++) {
      d = ctrl.afterCall('file_edit', { file_path: '/src/bar.go', old_string: `u_${i}`, new_string: 'b' }, 'error', true);
    }
    assert.equal(d!.action, 'warn');
    assert.ok(d!.reason?.includes('file_write'));
  });
});

describe('test-file editing guard', () => {
  const testPaths = [
    '/src/tests/foo_test.py',
    '/src/test_bar.py',
    '/src/conftest.py',
    '/src/foo.test.ts',
    '/repo/test/foo_test.go',
  ];
  for (const fp of testPaths) {
    it(`blocks file_edit on test file: ${fp}`, () => {
      const ctrl = makeCtrl();
      const d = ctrl.beforeCall('file_edit', { file_path: fp, old_string: 'a', new_string: 'b' });
      assert.equal(d.action, 'block');
      assert.ok(d.reason?.includes('BLOCKED'));
    });
  }
});

describe('artifact contamination guardrails', () => {
  it('warns when redirecting test output to local json file', () => {
    const ctrl = makeCtrl();
    const d = ctrl.beforeCall('bash', { command: 'go test ./... -json > test-results.json' });
    assert.equal(d.action, 'warn');
    assert.ok(d.reason?.includes('/tmp'), `Got: ${d.reason}`);
  });

  it('warns when redirecting cargo test output to local file', () => {
    const ctrl = makeCtrl();
    const d = ctrl.beforeCall('bash', { command: 'cargo test -- --format json > out.json' });
    assert.equal(d.action, 'warn');
  });

  it('warns when pytest output redirected to local json', () => {
    const ctrl = makeCtrl();
    const d = ctrl.beforeCall('bash', { command: 'pytest tests/ -v --json-report > test-output.json' });
    assert.equal(d.action, 'warn');
  });

  it('allows test output redirected to /tmp', () => {
    const ctrl = makeCtrl();
    const d = ctrl.beforeCall('bash', { command: 'go test ./... -json > /tmp/test-results.json' });
    assert.equal(d.action, 'allow');
  });

  it('allows test output to /dev/null', () => {
    const ctrl = makeCtrl();
    const d = ctrl.beforeCall('bash', { command: 'go test ./... > /dev/null 2>&1' });
    assert.equal(d.action, 'allow');
  });

  it('warns when extracting Go SDK into working directory', () => {
    const ctrl = makeCtrl();
    const d = ctrl.beforeCall('bash', { command: 'tar -C .local -xzf go1.24.1.linux-amd64.tar.gz' });
    assert.equal(d.action, 'warn');
    assert.ok(d.reason?.includes('/tmp'), `Got: ${d.reason}`);
  });

  it('allows extracting to /tmp', () => {
    const ctrl = makeCtrl();
    const d = ctrl.beforeCall('bash', { command: 'tar -C /tmp -xzf go.tar.gz' });
    assert.equal(d.action, 'allow');
  });

  it('no warning in non-detached mode', () => {
    const ctrl = makeCtrl(false);
    const d = ctrl.beforeCall('bash', { command: 'go test ./... -json > test-results.json' });
    assert.equal(d.action, 'allow');
  });
});
