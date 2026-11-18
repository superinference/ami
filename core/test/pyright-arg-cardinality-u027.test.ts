import { test } from 'node:test';
import assert from 'node:assert/strict';
import { PermissionManager, validateCommandFlags } from '../src/permissions';

// ---------------------------------------------------------------------------
// U027 — pyright per-command arg-cardinality parity (regression/characterization).
//
// The parity audit flagged that AMI supposedly "has no per-command schema for
// pyright" and "cannot distinguish arg-consuming flags from positional file args
// for pyright." AMI DOES model this — the declarative `PYRIGHT_ARG_TYPES` map +
// the strict `pyright` entry in `SAFE_COMMAND_FLAGS` drive the getopt-accurate
// walker `validateTypedCommandFlags`, which mirrors Claude Code's
// PYRIGHT_READ_ONLY_COMMANDS (utils/shell/readOnlyCommandValidation.ts L1504)
// flag-for-flag: same safeFlags, same `--watch`/`-w` danger callback, same
// `respectsDoubleDash:false`.
//
// These tests LOCK that behavior: the value token after an arg-taking option is
// consumed as the option's VALUE (not mistaken for a positional file) while real
// positional files are still recognised, and the exact set of auto-approvable
// flags matches Claude's (write-shaped / unlisted flags prompt, and none can be
// smuggled past `--`). A regression that drops the pyright schema, mis-attributes
// a value token, or broadens the safe set beyond Claude's would fail here.
// ---------------------------------------------------------------------------

const pm = new PermissionManager();

test('pyright: arg-taking option consumes its value, positional files still recognised', () => {
  // `--project ./cfg` consumes ./cfg as the config VALUE; `src/` is a positional
  // file. Both forms (separated and fused `=`) auto-allow.
  assert.equal(pm.classifyBashCommand('pyright --project ./cfg src/'), 'safe');
  assert.equal(pm.classifyBashCommand('pyright --project ./cfg'), 'safe');
  assert.equal(pm.classifyBashCommand('pyright --project=./cfg'), 'safe');
  // Path-valued directory option (--typeshedpath) followed by a positional dir.
  assert.equal(pm.classifyBashCommand('pyright --typeshedpath ./stubs tests/'), 'safe');
  // Non-path valued option (--level LEVEL): the value token `warning` is consumed,
  // then real positional files follow — all auto-allowed.
  assert.equal(pm.classifyBashCommand('pyright --level warning src/ lib/mod.py'), 'safe');
  // Pure variadic positional files.
  assert.equal(pm.classifyBashCommand('pyright src/ tests/ lib/mod.py'), 'safe');
});

test('pyright: a missing or dash-led option value is rejected (type confusion)', () => {
  // An arg-taking option with NO following value is not auto-safe.
  assert.equal(validateCommandFlags('pyright', ['--project']), false);
  assert.equal(pm.classifyBashCommand('pyright --project'), 'unsafe');
  // A following FLAG is not a valid string value — the walker must not swallow it
  // as the option's value and wave the command through.
  assert.equal(validateCommandFlags('pyright', ['--project', '--outputjson']), false);
  // The happy path: value then positional validates.
  assert.equal(validateCommandFlags('pyright', ['--project', './cfg', 'src/']), true);
});

test('pyright: auto-approvable flag set matches Claude; unlisted/write flags prompt', () => {
  // In Claude's PYRIGHT_READ_ONLY_COMMANDS safeFlags (and AMI's PYRIGHT_ARG_TYPES).
  assert.equal(pm.classifyBashCommand('pyright --outputjson'), 'safe');
  assert.equal(pm.classifyBashCommand('pyright --stats --verbose'), 'safe');
  assert.equal(pm.classifyBashCommand('pyright --pythonversion 3.11 --pythonplatform Linux'), 'safe');
  // Deliberately NOT in Claude's safe list → must prompt (not silently allowed):
  //  --createstub / --verifytypes WRITE stub files; --pythonpath/--threads and the
  //  -t/-v aliases are omitted by Claude, so AMI (strict) must prompt too.
  assert.equal(pm.classifyBashCommand('pyright --createstub os'), 'unsafe');
  assert.equal(pm.classifyBashCommand('pyright --verifytypes mypkg'), 'unsafe');
  assert.equal(pm.classifyBashCommand('pyright --pythonpath /usr/bin/python3'), 'unsafe');
  assert.equal(pm.classifyBashCommand('pyright --threads 4'), 'unsafe');
  assert.equal(pm.classifyBashCommand('pyright -t ./stubs'), 'unsafe');
});

test('pyright: --watch/-w prompt and no write flag can be smuggled past `--`', () => {
  assert.equal(pm.classifyBashCommand('pyright --watch'), 'unsafe');
  assert.equal(pm.classifyBashCommand('pyright -w'), 'unsafe');
  // respectsDoubleDash:false — pyright treats `--` as a file path, so the walker
  // keeps scanning and a write-shaped flag after `--` is still caught.
  assert.equal(pm.classifyBashCommand('pyright -- --createstub os'), 'unsafe');
  assert.equal(pm.classifyBashCommand('pyright -- --watch'), 'unsafe');
});
