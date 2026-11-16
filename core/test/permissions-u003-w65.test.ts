import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  PermissionManager,
  validateCommandFlags,
  normalizeCaseForComparison,
} from '../src/permissions.ts';

// ===========================================================================
// UNIT U003 — wave w65 parity gaps in core/core/src/permissions.ts
// Each block cites the Claude reference whose behavior AMI is matching.
// ===========================================================================

// ---------------------------------------------------------------------------
// Gap: DROP SCHEMA in destructive SQL patterns
//   claudeRef .../BashTool/destructiveCommandWarning.ts:L71-74
//   Claude's SQL pattern covers DROP of TABLE/DATABASE/SCHEMA; AMI lacked the
//   SCHEMA arm, so `DROP SCHEMA public CASCADE` escaped the SQL destructive branch.
// ---------------------------------------------------------------------------
describe('U003/w65: DROP SCHEMA classified destructive', () => {
  const pm = new PermissionManager('ask');

  it('`DROP SCHEMA public CASCADE` is destructive', () => {
    assert.equal(pm.classifyBashCommand('psql -c "DROP SCHEMA public CASCADE"'), 'destructive');
  });

  it('existing DROP TABLE still destructive (no regression)', () => {
    assert.equal(pm.classifyBashCommand('psql -c "DROP TABLE foo"'), 'destructive');
  });

  it('a plain SELECT is not destructive via the SQL branch', () => {
    assert.notEqual(pm.classifyBashCommand('psql -c "SELECT 1"'), 'destructive');
  });
});

// ---------------------------------------------------------------------------
// Gap: `srun` (SLURM) + `stdbuf` treated as command wrappers so the inner
//      command is unwrapped and classified.
//   claudeRef .../utils/bash/specs/srun.ts:L3-29, specs/index.ts:L6-17
//   `srun`/`stdbuf` were absent from permissions.ts SHELL_WRAPPERS, so
//   `srun mkfs /dev/sda` classified on the opaque `srun` token and the inner
//   destructive command was never reached by base-name matching.
// ---------------------------------------------------------------------------
describe('U003/w65: srun/stdbuf wrapper unwrapping', () => {
  const pm = new PermissionManager('ask');

  it('`srun mkfs /dev/sda` unwraps to the inner destructive mkfs', () => {
    assert.equal(pm.classifyBashCommand('srun mkfs /dev/sda'), 'destructive');
  });

  it('`stdbuf -oL mkfs /dev/sda` unwraps to the inner destructive mkfs', () => {
    assert.equal(pm.classifyBashCommand('stdbuf -oL mkfs /dev/sda'), 'destructive');
  });

  it('`srun rm -rf /path` reaches the inner rm (destructive)', () => {
    assert.equal(pm.classifyBashCommand('srun rm -rf /path'), 'destructive');
  });

  // srun option-arity: -n/--ntasks/-N/--nodes each consume a following count
  // operand before the wrapped command (srun.ts:L6-23). Without that, the count
  // is mis-read as the command.
  it('`srun -n 4 mkfs /dev/sda` skips the -n count operand (destructive)', () => {
    assert.equal(pm.classifyBashCommand('srun -n 4 mkfs /dev/sda'), 'destructive');
  });

  it('`srun --ntasks 4 mkfs /dev/sda` skips the --ntasks count (destructive)', () => {
    assert.equal(pm.classifyBashCommand('srun --ntasks 4 mkfs /dev/sda'), 'destructive');
  });

  it('`srun -N 2 rm -rf /path` skips the -N count (destructive)', () => {
    assert.equal(pm.classifyBashCommand('srun -N 2 rm -rf /path'), 'destructive');
  });

  it('`srun -n4 mkfs /dev/sda` (attached count) reaches mkfs', () => {
    assert.equal(pm.classifyBashCommand('srun -n4 mkfs /dev/sda'), 'destructive');
  });
});

// ---------------------------------------------------------------------------
// Gap: respectsDoubleDash — POSIX `--` end-of-options handling in the flag
//      validator, with a per-command opt-out for tools that ignore `--`.
//   claudeRef .../utils/shell/readOnlyCommandValidation.ts:L26-38 + readOnlyValidation.ts:L46-49
//   validateCommandFlags had no `--` concept; a `--`-respecting tool's post-`--`
//   token is a positional, not a flag. find does NOT honor `--` for its
//   expression primaries (respectsDoubleDash:false) so a post-`--` `-delete`
//   must still be caught.
// ---------------------------------------------------------------------------
describe('U003/w65: respectsDoubleDash / POSIX `--` handling', () => {
  const pm = new PermissionManager('ask');

  // untyped flag walker
  it('`sort -o out f` blocks the output flag (unsafe)', () => {
    assert.equal(pm.classifyBashCommand('sort -o out f'), 'unsafe');
  });

  it('`sort -- -o out` treats -o as a positional after `--` (safe)', () => {
    assert.equal(pm.classifyBashCommand('sort -- -o out'), 'safe');
  });

  // strict allowlist command: `--` stops the strict scan
  it('`which --bad` is unsafe (strict), `which -- --bad` is safe', () => {
    assert.equal(pm.classifyBashCommand('which --bad'), 'unsafe');
    assert.equal(pm.classifyBashCommand('which -- --bad'), 'safe');
  });

  // typed flag walker (fd has argTypes)
  it('`fd -t f --exec rm` is blocked, `fd -t f -- --exec` is safe', () => {
    assert.equal(pm.classifyBashCommand('fd -t f --exec rm'), 'unsafe');
    assert.equal(pm.classifyBashCommand('fd -t f -- --exec'), 'safe');
  });

  // find opts out of `--` handling: a post-`--` write primary still fails the walker.
  it('find does NOT honor `--` (respectsDoubleDash:false): blocked flags still caught', () => {
    assert.equal(validateCommandFlags('find', ['.', '--', '-delete']), false);
    assert.equal(validateCommandFlags('find', ['.', '--', '-exec', 'rm', '{}']), false);
  });

  it('a `--`-respecting command stops scanning at `--`', () => {
    // -o would be blocked without `--` handling; after `--` it is a positional.
    assert.equal(validateCommandFlags('sort', ['-o', 'x']), false);
    assert.equal(validateCommandFlags('sort', ['--', '-o', 'x']), true);
  });

  it('typed walker: fd blocks --exec, honors `--`', () => {
    assert.equal(validateCommandFlags('fd', ['--exec', 'rm']), false);
    assert.equal(validateCommandFlags('fd', ['--', '--exec']), true);
  });
});

// ---------------------------------------------------------------------------
// Gap: normalizeCaseForComparison — case-fold path-boundary comparisons on
//      case-insensitive filesystems (macOS/Windows).
//   claudeRef .../utils/permissions/filesystem.ts:L81-92 + permissionOptions.tsx:L20-39
//   On a case-insensitive FS `.SSH/` and `.ssh/` name the same file; AMI's
//   case-sensitive safety-path `includes()` failed open there, and the workspace
//   containment prefix test wrongly treated a differently-cased path as outside.
// ---------------------------------------------------------------------------
describe('U003/w65: normalizeCaseForComparison helper', () => {
  const origPlatform = process.platform;
  afterEach(() => {
    Object.defineProperty(process, 'platform', { value: origPlatform, configurable: true });
  });

  const setPlatform = (p: string) =>
    Object.defineProperty(process, 'platform', { value: p, configurable: true });

  it('lowercases on darwin/win32, no-op on linux', () => {
    setPlatform('darwin');
    assert.equal(normalizeCaseForComparison('/Users/X/.SSH/'), '/users/x/.ssh/');
    setPlatform('win32');
    assert.equal(normalizeCaseForComparison('C:\\Users\\X\\.SSH'), 'c:\\users\\x\\.ssh');
    setPlatform('linux');
    assert.equal(normalizeCaseForComparison('/Users/X/.SSH/'), '/Users/X/.SSH/');
  });

  it('case-insensitive FS: a differently-cased sensitive path still prompts (no fail-open)', async () => {
    setPlatform('darwin');
    const pm = new PermissionManager('auto-allow');
    assert.equal(await pm.check('file_edit', { file_path: '/home/x/.SSH/authorized_keys' }), 'ask');
    assert.equal(await pm.check('file_edit', { file_path: '/home/x/.Ami/permissions.json' }), 'ask');
    // a non-sensitive path is still auto-allowed
    assert.equal(await pm.check('file_edit', { file_path: '/home/x/proj/a.ts' }), 'allow');
  });

  it('case-sensitive FS: distinct casing is NOT merged (`.SSH/` not a sensitive match)', async () => {
    setPlatform('linux');
    const pm = new PermissionManager('auto-allow');
    assert.equal(await pm.check('file_edit', { file_path: '/home/x/.SSH/authorized_keys' }), 'allow');
    // the canonical lowercase path still matches the safety guard
    assert.equal(await pm.check('file_edit', { file_path: '/home/x/.ssh/authorized_keys' }), 'ask');
  });

  it('case-insensitive FS: mixed-case workspace path counts as contained', () => {
    setPlatform('darwin');
    const pm = new PermissionManager('ask');
    assert.equal(pm.isPathAllowed('/Users/foo/project/file.ts', '/Users/foo/Project'), true);
  });

  it('case-sensitive FS: mixed-case workspace path is NOT contained', () => {
    setPlatform('linux');
    const pm = new PermissionManager('ask');
    assert.equal(pm.isPathAllowed('/Users/foo/project/file.ts', '/Users/foo/Project'), false);
  });
});
