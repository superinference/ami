import { test } from 'node:test';
import assert from 'node:assert/strict';
import { unwrapWrapperChain, WRAPPER_PREFIX_COMMANDS } from '../src/tools/command-prefix';
import { SUBCOMMAND_AWARE_ROOTS } from '../src/permissions';

// U-perm1 / w128 — wrapper-vs-subcommand disambiguation invariant.
//
// Parity reference: Claude Code's prefix.ts `isKnownSubcommand` reclassifies a command
// that a spec marks as a wrapper (e.g. git, whose spec has `isCommand` alias args) back
// to a REGULAR command when args[0] is a declared subcommand — so `git status` is never
// mis-handled as a generic wrapper invocation.
//
// AMI reaches the SAME outcome by construction rather than by a runtime disambiguation
// branch: the wrapper set (WRAPPER_PREFIX_COMMANDS / SHELL_WRAPPERS) and the
// subcommand-aware roots (SUBCOMMAND_AWARE_ROOTS, handled by the spec-prefix path) are
// DISJOINT, so a subcommand-aware root like `git`/`docker`/`npm` is never a wrapper —
// `unwrapWrapperChain` returns null for it and the command flows through the
// spec/subcommand-aware derivation instead. These tests LOCK that invariant: if a
// future change added a subcommand-aware root to the wrapper set (re-introducing exactly
// the misclassification Claude's `isKnownSubcommand` guards against), they go red.

test('INVARIANT: no wrapper-prefix command is a subcommand-aware root (disjoint sets)', () => {
  const overlap = [...WRAPPER_PREFIX_COMMANDS].filter(w => SUBCOMMAND_AWARE_ROOTS.has(w));
  assert.deepEqual(
    overlap,
    [],
    `A subcommand-aware root must never be a wrapper (would be mis-unwrapped like the reference's git-as-wrapper bug): ${overlap.join(', ')}`,
  );
});

test('a subcommand-aware root is NOT treated as a wrapper (git/docker/npm → null)', () => {
  // null ⇒ "not wrapped" ⇒ the caller uses the spec/subcommand-aware derivation,
  // exactly the branch Claude's isKnownSubcommand forces.
  for (const cmd of ['git status', 'docker ps', 'npm run build', 'kubectl get pods', 'make test']) {
    assert.equal(unwrapWrapperChain(cmd), null, `${cmd} must not be treated as a wrapper`);
  }
});

test('a real wrapper around a subcommand-aware root still unwraps to the inner command', () => {
  // The disambiguation must NOT over-fire: `sudo git status` is genuinely wrapped —
  // sudo is the wrapper, `git status` the inner command.
  assert.deepEqual(unwrapWrapperChain('sudo git status'), { wrappers: ['sudo'], inner: 'git status' });
  assert.deepEqual(unwrapWrapperChain('xargs git commit'), { wrappers: ['xargs'], inner: 'git commit' });
  assert.deepEqual(unwrapWrapperChain('timeout 5 docker ps'), { wrappers: ['timeout'], inner: 'docker ps' });
});
