import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  normalizeCommand,
  deobfuscateEnvExpansion,
  collapseWhitespace,
} from '../src/tools/command-normalize';
import {
  detectToolConflict,
  isReservedToolName,
  RESERVED_TOOL_PREFIXES,
  ToolRegistryGuard,
} from '../src/tools/tool-registry-guard';

// POV: core tools/command-normalize.ts + tool-registry-guard.ts.
// Observable behaviour only: every assertion drives the real exported function
// and checks its RESULT. No source-text inspection.

describe('collapseWhitespace — observable collapsing', () => {
  it('collapses runs of spaces to one', () => {
    assert.equal(collapseWhitespace('ls     -la       /x'), 'ls -la /x');
  });

  it('collapses mixed tabs and spaces to a single space', () => {
    assert.equal(collapseWhitespace('a\t \t b'), 'a b');
  });

  it('trims leading and trailing horizontal whitespace', () => {
    assert.equal(collapseWhitespace('  \t hello \t  '), 'hello');
  });

  it('is idempotent (collapsing twice equals collapsing once)', () => {
    const once = collapseWhitespace('x   y\tz');
    assert.equal(collapseWhitespace(once), once);
  });

  it('leaves an already-normalized command unchanged', () => {
    assert.equal(collapseWhitespace('git status'), 'git status');
  });

  it('returns empty string for whitespace-only input', () => {
    assert.equal(collapseWhitespace('   \t\t  '), '');
  });

  it('returns empty string for empty input', () => {
    assert.equal(collapseWhitespace(''), '');
  });

  it('does NOT collapse newlines, only tabs/spaces (trims edge newline)', () => {
    // [\t ]+ excludes \n; interior newline survives, single spaces around it stay.
    assert.equal(collapseWhitespace('a \n b'), 'a \n b');
    // trim() removes a trailing newline at the edge.
    assert.equal(collapseWhitespace('a\n'), 'a');
  });
});

describe('deobfuscateEnvExpansion — empty quote/subst/escape collapsing', () => {
  const cases: Array<[string, string, string]> = [
    ['empty double-quote pair', 'r""m', 'rm'],
    ['empty single-quote pair', "r''m", 'rm'],
    ['empty command substitution', 'r$()m', 'rm'],
    ['empty parameter expansion', 'r${}m', 'rm'],
    ['empty backtick pair', 'r``m', 'rm'],
    ['backslash-escaped letter', 'r\\m', 'rm'],
  ];
  for (const [label, input, expected] of cases) {
    it(`collapses ${label}: ${JSON.stringify(input)} -> ${expected}`, () => {
      assert.equal(deobfuscateEnvExpansion(input), expected);
    });
  }

  it('iterates stacked empty quotes to a fixpoint', () => {
    assert.equal(deobfuscateEnvExpansion('r""""m'), 'rm');
  });

  it('collapses mixed stacked tricks to a fixpoint', () => {
    assert.equal(deobfuscateEnvExpansion('r""$()m'), 'rm');
    assert.equal(deobfuscateEnvExpansion('r${}``""m'), 'rm');
  });

  it('collapses whitespace inside empty substitution/expansion', () => {
    assert.equal(deobfuscateEnvExpansion('r$(   )m'), 'rm');
    assert.equal(deobfuscateEnvExpansion('r${ }m'), 'rm');
  });

  it('reconstructs a fully obfuscated destructive word', () => {
    // r""m -rf -> rm -rf : the obfuscated token is unmasked for matching.
    assert.equal(deobfuscateEnvExpansion('r""m -rf /'), 'rm -rf /');
    assert.equal(deobfuscateEnvExpansion('r\\m -r\\f /tmp'), 'rm -rf /tmp');
  });

  it('leaves content-bearing double quotes untouched', () => {
    assert.equal(
      deobfuscateEnvExpansion('echo "hello world"'),
      'echo "hello world"',
    );
  });

  it('leaves content-bearing single quotes untouched', () => {
    assert.equal(deobfuscateEnvExpansion("echo 'a b'"), "echo 'a b'");
  });

  it('un-escapes a backslash followed by a letter (shell \\x === x)', () => {
    // \n here is a backslash + the letter n, not a newline.
    assert.equal(deobfuscateEnvExpansion('r\\nm'), 'rnm');
  });

  it('leaves a backslash before a non-letter untouched', () => {
    assert.equal(deobfuscateEnvExpansion('r\\.m'), 'r\\.m');
    assert.equal(deobfuscateEnvExpansion('r\\1m'), 'r\\1m');
  });

  it('returns the input unchanged when no obfuscation is present', () => {
    assert.equal(deobfuscateEnvExpansion('git commit -m msg'), 'git commit -m msg');
  });

  it('returns empty string for empty input', () => {
    assert.equal(deobfuscateEnvExpansion(''), '');
  });

  it('is idempotent on its own output', () => {
    const once = deobfuscateEnvExpansion('r""$()\\m');
    assert.equal(deobfuscateEnvExpansion(once), once);
  });
});

describe('normalizeCommand — full normalization pipeline', () => {
  it('returns empty string for empty input (short-circuit)', () => {
    assert.equal(normalizeCommand(''), '');
  });

  it('strips ANSI color escape sequences', () => {
    assert.equal(normalizeCommand('\x1b[31mred\x1b[0m'), 'red');
  });

  it('strips an ANSI sequence embedded mid-command and collapses spaces', () => {
    assert.equal(normalizeCommand('ls\x1b[1m   -la'), 'ls -la');
  });

  it('joins backslash-newline line continuations', () => {
    assert.equal(normalizeCommand('echo \\\n  hi'), 'echo hi');
  });

  it('joins multiple continuations into one line', () => {
    assert.equal(normalizeCommand('a \\\n b \\\n c'), 'a b c');
  });

  it('strips a trailing inline comment preceded by whitespace', () => {
    assert.equal(normalizeCommand('echo hi # a comment'), 'echo hi');
  });

  it('strips a whole-line comment (hash at index 0)', () => {
    assert.equal(normalizeCommand('# just a comment'), '');
  });

  it('does NOT treat a hash glued to a word as a comment', () => {
    assert.equal(normalizeCommand('echo hi#x'), 'echo hi#x');
  });

  it('does NOT strip a hash inside double quotes', () => {
    assert.equal(normalizeCommand('echo "a # b"'), 'echo "a # b"');
  });

  it('does NOT strip a hash inside single quotes', () => {
    assert.equal(normalizeCommand("echo 'a # b'"), "echo 'a # b'");
  });

  it('collapses surrounding whitespace to a tidy single-spaced command', () => {
    assert.equal(normalizeCommand('   git    status   '), 'git status');
  });

  it('combines ANSI, continuation, comment and whitespace in one pass', () => {
    const raw = '  \x1b[32mgit\x1b[0m   add \\\n   .   # stage all\n';
    assert.equal(normalizeCommand(raw), 'git add .');
  });

  it('keeps a hash that opens the command after leading spaces stripped', () => {
    // leading whitespace precedes the hash, so the whole line is a comment.
    assert.equal(normalizeCommand('   # hidden'), '');
  });
});

describe('isReservedToolName — reserved-prefix membership', () => {
  it('matches an exact non-underscore reserved name', () => {
    assert.equal(isReservedToolName('bash'), true);
    assert.equal(isReservedToolName('grep'), true);
    assert.equal(isReservedToolName('glob'), true);
  });

  it('matches an underscore-boundary extension of a reserved name', () => {
    assert.equal(isReservedToolName('bash_v2'), true);
    assert.equal(isReservedToolName('file_edit_strict'), true);
  });

  it('matches the underscore-suffixed prefixes exactly', () => {
    assert.equal(isReservedToolName('task_'), true);
    assert.equal(isReservedToolName('agent_'), true);
    assert.equal(isReservedToolName('task_run'), true);
    assert.equal(isReservedToolName('agent_spawn'), true);
  });

  it('does NOT match a name that merely shares a prefix without the boundary', () => {
    // 'bashful' is not 'bash' and not 'bash_*'
    assert.equal(isReservedToolName('bashful'), false);
    assert.equal(isReservedToolName('file_editor'), false);
    assert.equal(isReservedToolName('grepper'), false);
  });

  it('does NOT match the stem of an underscore-only prefix', () => {
    // 'task' alone is neither 'task_' nor 'task_*'
    assert.equal(isReservedToolName('task'), false);
    assert.equal(isReservedToolName('agent'), false);
  });

  it('does NOT match an unrelated plugin tool name', () => {
    assert.equal(isReservedToolName('my_plugin_tool'), false);
    assert.equal(isReservedToolName('deploy'), false);
    assert.equal(isReservedToolName(''), false);
  });

  it('every declared reserved prefix reports as reserved', () => {
    for (const prefix of RESERVED_TOOL_PREFIXES) {
      assert.equal(isReservedToolName(prefix), true, `prefix ${prefix}`);
    }
  });
});

describe('detectToolConflict — duplicate-source detection', () => {
  it('returns null for a brand-new tool in an empty registry', () => {
    assert.equal(detectToolConflict('deploy', 'plugin-a', new Map()), null);
  });

  it('returns null when the same source re-registers its own tool', () => {
    const reg = new Map([['deploy', 'plugin-a']]);
    assert.equal(detectToolConflict('deploy', 'plugin-a', reg), null);
  });

  it('reports a conflict when a different source claims an existing tool', () => {
    const reg = new Map([['deploy', 'plugin-a']]);
    const c = detectToolConflict('deploy', 'plugin-b', reg);
    assert.ok(c);
    assert.equal(c.toolName, 'deploy');
    assert.equal(c.existingSource, 'plugin-a');
    assert.equal(c.conflictingSource, 'plugin-b');
    assert.match(c.reason, /already registered by "plugin-a"/);
  });

  it('duplicate-source conflict takes precedence over reserved-prefix check', () => {
    // 'bash' is reserved, but it is already in the registry from plugin-a,
    // so the duplicate message (not the reserved message) is returned.
    const reg = new Map([['bash', 'plugin-a']]);
    const c = detectToolConflict('bash', 'plugin-b', reg);
    assert.ok(c);
    assert.match(c.reason, /already registered by/);
  });
});

describe('detectToolConflict — reserved-prefix protection', () => {
  it('blocks a plugin from registering a reserved tool name', () => {
    const c = detectToolConflict('bash', 'plugin-a', new Map());
    assert.ok(c);
    assert.equal(c.existingSource, 'core');
    assert.equal(c.conflictingSource, 'plugin-a');
    assert.match(c.reason, /conflicts with reserved prefix "bash"/);
  });

  it('blocks an underscore-boundary extension of a reserved name', () => {
    const c = detectToolConflict('file_edit_fast', 'plugin-a', new Map());
    assert.ok(c);
    assert.match(c.reason, /reserved prefix "file_edit"/);
  });

  it('blocks a task_/agent_ prefixed plugin tool', () => {
    const c = detectToolConflict('task_run', 'plugin-a', new Map());
    assert.ok(c);
    assert.match(c.reason, /reserved prefix "task_"/);
  });

  it('allows "core" source to register a reserved name', () => {
    assert.equal(detectToolConflict('bash', 'core', new Map()), null);
  });

  it('allows "builtin" source to register a reserved name', () => {
    assert.equal(detectToolConflict('file_edit', 'builtin', new Map()), null);
  });

  it('allows a plugin to register a non-reserved look-alike name', () => {
    assert.equal(detectToolConflict('bashful', 'plugin-a', new Map()), null);
    assert.equal(detectToolConflict('file_editor', 'plugin-a', new Map()), null);
  });
});

describe('ToolRegistryGuard — stateful registration', () => {
  let guard: ToolRegistryGuard;
  beforeEach(() => {
    guard = new ToolRegistryGuard();
  });

  it('registers a fresh tool and reports no conflict', () => {
    assert.equal(guard.register('deploy', 'plugin-a'), null);
    assert.equal(guard.isRegistered('deploy'), true);
    assert.equal(guard.getSource('deploy'), 'plugin-a');
    assert.deepEqual([...guard.getConflicts()], []);
  });

  it('is a no-op-ish idempotent re-register from the same source', () => {
    guard.register('deploy', 'plugin-a');
    assert.equal(guard.register('deploy', 'plugin-a'), null);
    assert.equal(guard.getSource('deploy'), 'plugin-a');
    assert.equal(guard.getConflicts().length, 0);
  });

  it('records a conflict and does NOT overwrite the original source', () => {
    guard.register('deploy', 'plugin-a');
    const c = guard.register('deploy', 'plugin-b');
    assert.ok(c);
    assert.equal(guard.getSource('deploy'), 'plugin-a');
    assert.equal(guard.getConflicts().length, 1);
    assert.equal(guard.getConflicts()[0].conflictingSource, 'plugin-b');
  });

  it('rejects a reserved-prefix registration from a plugin and does not store it', () => {
    const c = guard.register('bash', 'plugin-a');
    assert.ok(c);
    assert.equal(guard.isRegistered('bash'), false);
    assert.equal(guard.getSource('bash'), undefined);
    assert.equal(guard.getConflicts().length, 1);
  });

  it('accumulates multiple conflicts across registrations', () => {
    guard.register('deploy', 'plugin-a');
    guard.register('deploy', 'plugin-b'); // dup
    guard.register('grep', 'plugin-c'); // reserved
    assert.equal(guard.getConflicts().length, 2);
  });

  it('getRegisteredTools returns a defensive copy', () => {
    guard.register('deploy', 'plugin-a');
    const snapshot = guard.getRegisteredTools();
    snapshot.set('injected', 'attacker');
    assert.equal(guard.isRegistered('injected'), false);
    assert.equal(snapshot.get('deploy'), 'plugin-a');
  });

  it('clear() resets registry and conflicts', () => {
    guard.register('deploy', 'plugin-a');
    guard.register('deploy', 'plugin-b');
    guard.clear();
    assert.equal(guard.isRegistered('deploy'), false);
    assert.equal(guard.getConflicts().length, 0);
    // after clear, the same tool can be registered cleanly again
    assert.equal(guard.register('deploy', 'plugin-b'), null);
    assert.equal(guard.getSource('deploy'), 'plugin-b');
  });

  it('allows core to claim a reserved name then blocks a plugin duplicate', () => {
    assert.equal(guard.register('bash', 'core'), null);
    assert.equal(guard.isRegistered('bash'), true);
    const c = guard.register('bash', 'plugin-a');
    assert.ok(c);
    assert.match(c.reason, /already registered by "core"/);
  });
});
