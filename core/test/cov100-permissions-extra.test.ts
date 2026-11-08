import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import * as os from 'node:os';
import * as path from 'node:path';
import { PermissionManager } from '../src/permissions';

// ---------------------------------------------------------------------------
// classifyBashCommandCore — the standalone `rm -rf`/`rm -f` regex fallback.
//
// `rm` itself is in DESTRUCTIVE_COMMANDS, so a command whose *base* word is `rm`
// is classified destructive by the base-command scan BEFORE this regex ever
// runs. The regex at the "Check if rm with -rf or -r flags" guard is therefore
// only reached when the literal `rm <flag>` text appears somewhere that is NOT
// the extracted base command — e.g. inside the argument of another command.
// These tests drive that exact path and pin down its real (deliberately
// over-broad) behavior.
// ---------------------------------------------------------------------------

describe('classifyBashCommand — rm flag regex fallback (non-base rm occurrence)', () => {
  const pm = new PermissionManager();

  it('flags destructive when "rm -rf" appears inside another command\'s argument (-r alternative)', () => {
    // Base command is `git commit` (not `rm`), so the destructive-base scan does
    // not fire; the `rm\s+.*-[^\s]*r` alternative matches the quoted text and
    // escalates the whole command to destructive.
    assert.equal(
      pm.classifyBashCommand('git commit -m "fix rm -rf bug"'),
      'destructive',
    );
  });

  it('flags destructive via the -f alternative alone (no "r" flag present)', () => {
    // `echo` is a safe base command; the only thing that makes this destructive
    // is the second regex alternative `rm\s+.*-[^\s]*f` matching "rm -f".
    assert.equal(
      pm.classifyBashCommand('echo "rm -f solo"'),
      'destructive',
    );
  });

  it('does NOT fire for a substring "rm" without a word boundary + flag', () => {
    // "performance" contains the substring "rm" but has no \brm word boundary
    // followed by a flag, so the regex guard stays false and the command is
    // classified by the normal safe-command path.
    assert.equal(
      pm.classifyBashCommand('echo "performance tuning"'),
      'safe',
    );
  });

  it('still classifies a genuine base `rm -rf` as destructive (base scan path)', () => {
    // Control: when `rm` IS the base command the earlier DESTRUCTIVE_COMMANDS
    // scan already returns destructive — same verdict, different code path.
    assert.equal(pm.classifyBashCommand('rm -rf /tmp/scratch'), 'destructive');
  });
});

// ---------------------------------------------------------------------------
// registerAllowedRoot — normalization of odd-but-valid string inputs.
//
// The method resolves + normalizes the path inside a try/catch; the catch is a
// defensive no-op ("a bad path just isn't granted"). For any valid string on a
// POSIX platform path.resolve/normalize do not throw, so these assertions pin
// the happy-path behavior: absolute paths are normalized, relative paths are
// resolved against cwd, duplicates are deduped, and empty/blank input is a no-op.
// ---------------------------------------------------------------------------

describe('registerAllowedRoot — resolution + normalization', () => {
  it('normalizes an absolute path with redundant segments', () => {
    const pm = new PermissionManager();
    pm.registerAllowedRoot('/tmp/./a/../b');
    assert.deepEqual(pm.getAllowedReadRoots(), [path.normalize('/tmp/b')]);
  });

  it('resolves a relative path against process.cwd()', () => {
    const pm = new PermissionManager();
    pm.registerAllowedRoot('sub/dir');
    assert.deepEqual(
      pm.getAllowedReadRoots(),
      [path.normalize(path.resolve('sub/dir'))],
    );
  });

  it('is idempotent for the same normalized root', () => {
    const pm = new PermissionManager();
    pm.registerAllowedRoot('/tmp/x');
    pm.registerAllowedRoot('/tmp/x');
    pm.registerAllowedRoot('/tmp/./x');
    assert.deepEqual(pm.getAllowedReadRoots(), [path.normalize('/tmp/x')]);
  });

  it('ignores empty / non-string input without registering a root', () => {
    const pm = new PermissionManager();
    pm.registerAllowedRoot('');
    // @ts-expect-error — exercising the runtime guard against non-string input
    pm.registerAllowedRoot(undefined);
    assert.deepEqual(pm.getAllowedReadRoots(), []);
  });

  it('a registered out-of-cwd root makes a path under it classify as allow', () => {
    const pm = new PermissionManager();
    const root = path.join(os.tmpdir(), 'cov100-root');
    pm.registerAllowedRoot(root);
    // A file under the granted root resolves to 'allow' even though it is
    // outside the launch cwd — proving the root is actually honored.
    assert.equal(
      pm.classifyReadAccess(path.join(root, 'note.txt'), '/some/other/cwd'),
      'allow',
    );
  });
});
