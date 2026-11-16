import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  shouldUseSandbox,
  buildExcludedRules,
} from '../src/tools/bash-sandbox';
import { bashTool, _bashHooks } from '../src/tools/bash';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// Structured sandbox `excludedCommands` matching — parity with Claude Code's
// containsExcludedCommand (shouldUseSandbox.ts). Each user excludedCommands
// entry is parsed into a structured prefix/wildcard rule and matched against the
// BASE command of every (env/wrapper-normalized) subcommand instead of as a
// loose `\b<token>\b` substring. Covered gaps:
//   #5  anchor exemption to the base command token (not arbitrary substrings)
//   #37 rule-type expressiveness (prefix / wildcard; prefix also matches exact)
//   #64 compound-split + env-var/safe-wrapper stripping before matching
//   #66 fixed-point env-var/wrapper stripping with BINARY_HIJACK_VARS heuristic
//
// Every assertion marked RED below returns the OPPOSITE result under the old
// loose-regex implementation (`new RegExp('\\b'+escaped+'\\b')` tested anywhere
// in the subcommand), so these are genuine reproducers, not vacuous checks.
// ---------------------------------------------------------------------------

describe('shouldUseSandbox — base-command anchoring (#5)', () => {
  it('an exempt token as a bare ARGUMENT no longer suppresses a trigger', () => {
    // RED on revert: old `\bdocker\b` matches the trailing `docker` argument of
    // the curl trigger and returns false (escape); the anchored prefix rule does
    // not. `docker` is used (not a hardcoded default) to isolate the user path.
    assert.equal(
      shouldUseSandbox('curl https://example.com docker', { excludedCommands: ['docker'] }),
      true,
      'a prefix rule must not match an exempt keyword used as an argument',
    );
  });

  it('a prefix rule still exempts the command it names', () => {
    assert.equal(
      shouldUseSandbox('docker run curl', { excludedCommands: ['docker'] }),
      false,
    );
  });

  it('metacharacters in a prefix entry are compared literally (no regex)', () => {
    assert.equal(
      shouldUseSandbox('curl https://example.com', { excludedCommands: ['c.rl'] }),
      true,
      'c.rl must not match curl',
    );
  });
});

describe('shouldUseSandbox — rule-type expressiveness (#37)', () => {
  it('a wildcard rule exempts matching trigger commands', () => {
    // RED on revert: the old escaped-regex path (with URL args stripped before
    // matching) cannot express this and leaves `curl https://…` sandboxed.
    assert.equal(
      shouldUseSandbox('curl https://example.com', { excludedCommands: ['curl http*'] }),
      false,
      'wildcard rule must exempt the matching command',
    );
  });

  it('a wildcard rule does NOT exempt a non-matching command', () => {
    assert.equal(
      shouldUseSandbox('curl ftp://example.com/x', { excludedCommands: ['curl http*'] }),
      true,
      'wildcard rule must only exempt what it matches',
    );
  });

  it('a legacy `:*` suffix is treated as a prefix rule', () => {
    assert.equal(
      shouldUseSandbox('curl https://example.com', { excludedCommands: ['curl:*'] }),
      false,
    );
    // `curl:*` must not exempt an unrelated trigger.
    assert.equal(
      shouldUseSandbox('wget https://example.com', { excludedCommands: ['curl:*'] }),
      true,
    );
  });
});

describe('shouldUseSandbox — env-var / safe-wrapper normalization (#64/#66)', () => {
  it('strips a SAFE leading env-var before matching an exemption', () => {
    assert.equal(
      shouldUseSandbox('FOO=bar curl https://example.com', { excludedCommands: ['curl'] }),
      false,
      'a safe env-var prefix must not defeat a prefix exemption',
    );
  });

  it('strips a safe wrapper before matching an exemption', () => {
    assert.equal(
      shouldUseSandbox('timeout 30 curl https://example.com', { excludedCommands: ['curl'] }),
      false,
    );
  });

  it('strips interleaved wrapper + env-var to a fixed point', () => {
    assert.equal(
      shouldUseSandbox('timeout 300 FOO=bar curl https://example.com', { excludedCommands: ['curl'] }),
      false,
      'timeout 300 FOO=bar curl must normalize to `curl …` and match `curl`',
    );
  });

  it('does NOT strip a binary-hijack env var (LD_PRELOAD) — stays sandboxed', () => {
    // RED on revert: old `\bcurl\b` matches regardless of the LD_PRELOAD prefix
    // and exempts the command — the exact hijack the BINARY_HIJACK_VARS heuristic
    // blocks. The command must remain SANDBOXED.
    assert.equal(
      shouldUseSandbox('LD_PRELOAD=/tmp/evil.so curl https://example.com', { excludedCommands: ['curl'] }),
      true,
      'an LD_PRELOAD-hijacked binary must not inherit the exemption',
    );
  });

  it('does NOT strip a PATH hijack env var — stays sandboxed', () => {
    assert.equal(
      shouldUseSandbox('PATH=/tmp/evil curl https://example.com', { excludedCommands: ['curl'] }),
      true,
    );
  });

  it('does NOT transparently strip `sudo` (arbitrary-code wrapper)', () => {
    // sudo is deliberately NOT in the safe-wrapper list, so `sudo curl …` is NOT
    // exempted by a `curl` rule — it must stay sandboxed.
    assert.equal(
      shouldUseSandbox('sudo curl https://example.com', { excludedCommands: ['curl'] }),
      true,
    );
  });
});

describe('buildExcludedRules — rule parsing', () => {
  it('parses bare tokens and `:*` suffixes as prefix rules', () => {
    assert.deepEqual(buildExcludedRules({ excludedCommands: ['git'] }), [
      { type: 'prefix', prefix: 'git' },
    ]);
    assert.deepEqual(buildExcludedRules({ excludedCommands: ['bazel build:*'] }), [
      { type: 'prefix', prefix: 'bazel build' },
    ]);
  });

  it('parses unescaped `*` as a wildcard rule', () => {
    assert.deepEqual(buildExcludedRules({ excludedCommands: ['bazel build //*'] }), [
      { type: 'wildcard', pattern: 'bazel build //*' },
    ]);
  });

  it('drops empty / whitespace-only entries', () => {
    assert.deepEqual(buildExcludedRules({ excludedCommands: ['', '   '] }), []);
    assert.deepEqual(buildExcludedRules(), []);
  });
});

// ---------------------------------------------------------------------------
// E2E red-green: drive the real bashTool.execute (non-container host path) and
// intercept the exact command string handed to the shell. A command whose BASE
// is in context.sandboxExcludedCommands runs UNWRAPPED; a DIFFERENT trigger
// command that merely mentions the excluded token as an argument must still be
// WRAPPED (sandboxed) — base-command anchoring, not loose substring matching.
//
// RED on revert: the old loose-regex matcher (`\bdocker\b` anywhere) exempts the
// `curl … docker` trigger and runs it raw — a sandbox escape. GREEN: the anchored
// prefix rule keeps the trigger sandboxed.
// ---------------------------------------------------------------------------
describe('bashTool.execute — excluded token as argument stays sandboxed (e2e)', () => {
  function baseCtx(overrides: Partial<ToolContext> = {}): ToolContext {
    return {
      cwd: process.cwd(),
      abortSignal: new AbortController().signal,
      ...overrides,
    } as ToolContext;
  }

  async function captureExecuted(command: string, ctx: ToolContext): Promise<string> {
    let captured = '';
    const spy = mock.method(_bashHooks, 'execCommand', async (cmd: string) => {
      captured = cmd;
      return { stdout: '', stderr: '', exitCode: 0 };
    });
    try {
      const result = await bashTool.execute({ command }, ctx);
      assert.equal(result.isError, false, `execute should succeed: ${JSON.stringify(result)}`);
      assert.equal(spy.mock.callCount(), 1, 'execCommand should run exactly once');
      return captured;
    } finally {
      mock.restoreAll();
    }
  }

  afterEach(() => mock.restoreAll());

  it('a command whose base is excluded runs unwrapped (premise guard)', async () => {
    // `docker …` is excluded and is not itself a sandbox trigger, so it runs raw.
    const executed = await captureExecuted(
      'docker run curl',
      baseCtx({ sandboxExcludedCommands: ['docker'] }),
    );
    assert.equal(executed, 'docker run curl', `expected raw command, got: ${executed}`);
  });

  it('a trigger mentioning the excluded token as an argument is still WRAPPED', async () => {
    const cmd = 'curl https://example.com docker';
    const executed = await captureExecuted(
      cmd,
      baseCtx({ sandboxExcludedCommands: ['docker'] }),
    );
    assert.notEqual(executed, cmd, 'the trigger must not run raw');
    assert.match(
      executed,
      /unshare|ulimit/,
      `expected a sandbox wrapper (unshare/ulimit), got: ${executed}`,
    );
  });
});
