import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  PermissionManager,
  validateCommandFlags,
  isGitInvocationReadOnly,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// U003 (w74): read-only command-allowlist parity for base64 / checksum (sha*sum,
// md5sum) / grep flag enforcement / dotnet SDK queries / pyright type-checking /
// git ls-remote (incl. --server-option exfil exclusion) / fd -l exec exclusion.
// Mirrors Claude's readOnlyValidation.ts / readOnlyCommandValidation.ts configs.
// ---------------------------------------------------------------------------

const pm = new PermissionManager();

test('base64 encode/decode to stdout is auto-allowed; typed flags validated', () => {
  assert.equal(pm.classifyBashCommand('base64 file.txt'), 'safe');
  assert.equal(pm.classifyBashCommand('base64 -d file.txt'), 'safe');
  assert.equal(pm.classifyBashCommand('base64 --decode file.txt'), 'safe');
  assert.equal(pm.classifyBashCommand('base64 -w 0 file'), 'safe');
  assert.equal(pm.classifyBashCommand('base64 --help'), 'safe');
  // `-w` is numeric: a non-numeric value is rejected (parser-differential guard).
  assert.equal(pm.classifyBashCommand('base64 -w abc file'), 'unsafe');
  // An unknown flag (strict allowlist) falls through to a prompt.
  assert.equal(pm.classifyBashCommand('base64 -o out file'), 'unsafe');
});

test('checksum commands (sha256sum/sha1sum/md5sum) are read-only reads', () => {
  for (const c of ['sha256sum', 'sha1sum', 'md5sum']) {
    assert.equal(pm.classifyBashCommand(`${c} file`), 'safe', `${c} file`);
    assert.equal(pm.classifyBashCommand(`${c} -c sums.txt`), 'safe', `${c} -c`);
    assert.equal(pm.classifyBashCommand(`${c} --tag --status file`), 'safe', `${c} --tag`);
    // Unknown flag → prompt (strict).
    assert.equal(pm.classifyBashCommand(`${c} --bogus file`), 'unsafe', `${c} --bogus`);
  }
});

test('grep gains a strict safe-flag allowlist (unknown flags prompt)', () => {
  assert.equal(pm.classifyBashCommand('grep -i foo file'), 'safe');
  assert.equal(pm.classifyBashCommand('grep -rn foo .'), 'safe');
  assert.equal(pm.classifyBashCommand('grep -A 3 foo f'), 'safe');
  // RED discriminator: before the allowlist, an unrecognised grep flag was
  // silently auto-approved (validateCommandFlags returned true with no flagDef).
  assert.equal(pm.classifyBashCommand('grep --pre cat file'), 'unsafe');
  assert.equal(validateCommandFlags('grep', ['--pre', 'cat']), false);
  assert.equal(validateCommandFlags('grep', ['-i', 'foo']), true);
});

test('dotnet read-only SDK queries auto-allow; subcommands prompt', () => {
  assert.equal(pm.classifyBashCommand('dotnet --version'), 'safe');
  assert.equal(pm.classifyBashCommand('dotnet --info'), 'safe');
  assert.equal(pm.classifyBashCommand('dotnet --list-runtimes'), 'safe');
  assert.equal(pm.classifyBashCommand('dotnet --list-sdks'), 'safe');
  // A bare `dotnet` (help) and any build/run subcommand require approval.
  assert.equal(pm.classifyBashCommand('dotnet'), 'unsafe');
  assert.equal(pm.classifyBashCommand('dotnet build'), 'unsafe');
  assert.equal(pm.classifyBashCommand('dotnet run'), 'unsafe');
  // A read-only flag mixed with a subcommand is NOT all-read → prompt.
  assert.equal(pm.classifyBashCommand('dotnet --version build'), 'unsafe');
});

test('pyright type-checking auto-allows; --watch/-w and unknown flags prompt', () => {
  assert.equal(pm.classifyBashCommand('pyright src/'), 'safe');
  assert.equal(pm.classifyBashCommand('pyright --outputjson'), 'safe');
  assert.equal(pm.classifyBashCommand('pyright --project cfg.json'), 'safe');
  assert.equal(pm.classifyBashCommand('pyright --level warning'), 'safe');
  // Long-running watch mode is not a read action.
  assert.equal(pm.classifyBashCommand('pyright --watch'), 'unsafe');
  assert.equal(pm.classifyBashCommand('pyright -w'), 'unsafe');
  // Unknown flag → prompt (strict).
  assert.equal(pm.classifyBashCommand('pyright --createstub os'), 'unsafe');
  // respectsDoubleDash:false — a write-shaped flag cannot be smuggled past `--`.
  assert.equal(pm.classifyBashCommand('pyright -- --watch'), 'unsafe');
});

test('git ls-remote is read-only but --server-option/-o exfil is excluded', () => {
  assert.equal(pm.classifyBashCommand('git ls-remote origin'), 'safe');
  assert.equal(pm.classifyBashCommand('git ls-remote --heads origin'), 'safe');
  assert.equal(pm.classifyBashCommand('git ls-remote --tags origin'), 'safe');
  // RED discriminator: --server-option exfiltrates an attacker string to the
  // remote (protocol-v2 capability advertisement) — a network write.
  assert.equal(pm.classifyBashCommand('git ls-remote --server-option=secret origin'), 'unsafe');
  assert.equal(pm.classifyBashCommand('git ls-remote -o secret origin'), 'unsafe');
  // Unit-level guard directly.
  assert.equal(isGitInvocationReadOnly(['git', 'ls-remote', 'origin']), true);
  assert.equal(isGitInvocationReadOnly(['git', 'ls-remote', '--server-option=x', 'origin']), false);
});

test('fd -l/--list-details is excluded (it execs ls as a subprocess)', () => {
  assert.equal(pm.classifyBashCommand('fd -e txt'), 'safe');
  // RED discriminator: -l / --list-details internally run `ls` (PATH-hijack vector).
  assert.equal(pm.classifyBashCommand('fd -l'), 'unsafe');
  assert.equal(pm.classifyBashCommand('fd --list-details'), 'unsafe');
  assert.equal(validateCommandFlags('fd', ['-l']), false);
  assert.equal(validateCommandFlags('fdfind', ['--list-details']), false);
});
