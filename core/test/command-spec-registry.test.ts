import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  getCommandSpec,
  loadFigSpec,
  isSafeCommandSpecName,
  valueFlagsForCommand,
  BUILTIN_COMMAND_SPECS,
  type CommandSpec,
} from '../src/tools/command-spec-registry';
import { getSimpleCommandPrefix, suggestAllowPrefix } from '../src/permissions';

// ---------------------------------------------------------------------------
// Unit: path-safety validator (parity: registry.ts loadFigSpec guard L30-34)
// ---------------------------------------------------------------------------
describe('isSafeCommandSpecName', () => {
  it('accepts plain command names', () => {
    assert.equal(isSafeCommandSpecName('make'), true);
    assert.equal(isSafeCommandSpecName('go'), true);
    assert.equal(isSafeCommandSpecName('git'), true);
    // a bare dash is the one allowed leading-dash form (names stdin in some specs)
    assert.equal(isSafeCommandSpecName('-'), true);
  });

  it('rejects path-traversal, separators, and flag-shaped names', () => {
    assert.equal(isSafeCommandSpecName(''), false);
    assert.equal(isSafeCommandSpecName('../etc/passwd'), false);
    assert.equal(isSafeCommandSpecName('a/b'), false);
    assert.equal(isSafeCommandSpecName('a\\b'), false);
    assert.equal(isSafeCommandSpecName('..'), false);
    assert.equal(isSafeCommandSpecName('-rf'), false);
    assert.equal(isSafeCommandSpecName('--force'), false);
  });
});

// ---------------------------------------------------------------------------
// Unit: loadFigSpec (optional @withfig/autocomplete fallback)
// ---------------------------------------------------------------------------
describe('loadFigSpec', () => {
  it('returns null for unsafe names without attempting an import', async () => {
    assert.equal(await loadFigSpec('../evil'), null);
    assert.equal(await loadFigSpec('a/b'), null);
    assert.equal(await loadFigSpec('-x'), null);
  });

  it('returns null when the optional @withfig/autocomplete package is absent', async () => {
    // The package is not a declared dependency, so the dynamic import throws and
    // is caught — exactly like the reference when the dependency is not installed.
    assert.equal(await loadFigSpec('definitely-not-a-real-command-xyz'), null);
    assert.equal(await loadFigSpec('ls'), null);
  });
});

// ---------------------------------------------------------------------------
// Unit: getCommandSpec (built-in → fig → null, memoized)
// ---------------------------------------------------------------------------
describe('getCommandSpec', () => {
  it('resolves a built-in spec by name', async () => {
    const spec = await getCommandSpec('make');
    assert.ok(spec);
    assert.equal(spec!.name, 'make');
    assert.ok(spec!.options && spec!.options.length > 0);
  });

  it('resolves go including its subcommands', async () => {
    const spec = await getCommandSpec('go');
    assert.ok(spec);
    assert.equal(spec!.name, 'go');
    const subs = (spec!.subcommands ?? []).map((s) => s.name);
    assert.deepEqual(subs.sort(), ['build', 'run', 'test', 'vet']);
  });

  it('returns null for an unknown command (no built-in, no fig spec)', async () => {
    assert.equal(await getCommandSpec('totally-unknown-cmd-123'), null);
  });

  it('memoizes by command name (same promise returned for repeat lookups)', () => {
    const first = getCommandSpec('make');
    const second = getCommandSpec('make');
    assert.strictEqual(first, second);
  });

  it('every built-in spec round-trips through getCommandSpec', async () => {
    for (const builtin of BUILTIN_COMMAND_SPECS) {
      const resolved: CommandSpec | null = await getCommandSpec(builtin.name);
      assert.strictEqual(resolved, builtin);
    }
  });
});

// ---------------------------------------------------------------------------
// Unit: valueFlagsForCommand (derive value-taking flags from a built-in spec)
// ---------------------------------------------------------------------------
describe('valueFlagsForCommand', () => {
  it('derives every flag spelling that declares args, for a covered command', () => {
    const flags = valueFlagsForCommand('make');
    assert.ok(flags);
    assert.equal(flags!.has('-C'), true);
    assert.equal(flags!.has('--directory'), true);
    assert.equal(flags!.has('-f'), true);
    assert.equal(flags!.has('--file'), true);
    assert.equal(flags!.has('-I'), true);
  });

  it('returns undefined for a command with no built-in spec', () => {
    assert.equal(valueFlagsForCommand('totally-unknown-cmd-123'), undefined);
    // git is modeled by the permission layer's own table, not this registry
    assert.equal(valueFlagsForCommand('git'), undefined);
  });
});

// ---------------------------------------------------------------------------
// Integration / E2E: the registry widens flag-value skipping in the bash
// permission classifier for commands the hand-maintained table does not cover.
// RED on revert: without the registry fallback, `make -C build test` yields the
// WRONG `make build` prefix (the value `build` is treated as the subcommand).
// ---------------------------------------------------------------------------
describe('command-spec registry wiring into bash prefix extraction', () => {
  it('skips a registry-modeled global value-flag + its value for make', () => {
    // `-C build` consumes `build` as the directory → the real target is `test`.
    assert.equal(getSimpleCommandPrefix('make -C build test'), 'make test');
    assert.equal(getSimpleCommandPrefix('make --directory=build test'), 'make test');
    assert.equal(getSimpleCommandPrefix('make -f other.mk lint'), 'make lint');
  });

  it('skips the go -C value flag to reach the subcommand', () => {
    assert.equal(getSimpleCommandPrefix('go -C ./pkg build'), 'go build');
    assert.equal(getSimpleCommandPrefix('go -C ./pkg test'), 'go test');
  });

  it('end-to-end: suggestAllowPrefix derives the correctly-scoped rule', () => {
    assert.equal(suggestAllowPrefix('make -C build test'), 'make test');
  });

  it('leaves commands with no registry/table spec on the plain two-word contract', () => {
    // No spec for `brew`: a leading flag is skipped without eating a value.
    assert.equal(getSimpleCommandPrefix('brew -v install wget'), 'brew install');
  });
});
