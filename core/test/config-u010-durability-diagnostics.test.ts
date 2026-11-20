import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  safeWriteConfig,
  loadConfigFileChecked,
  validateConfigJson,
  fsHooks,
  CONFIG_FILE_MODE,
  JSON_SYNTAX_RECOVERY_HINT,
} from '../src/config';

// ---------------------------------------------------------------------------
// UNIT U010 (durability + diagnostics) — core/core/src/config.ts
//
//   Gap #18 — owner-only 0o600 permissions on config writes: a newly-created
//             config file is created owner-read/write only; an existing file's
//             mode is PRESERVED across a rewrite. Parity: the reference writes config
//             with mode 0o600.
//   Gap #84 — durable (fsync-flushed) config writes: the temp file is written
//             with `flush: true` so a crash/power-loss right after the rename
//             cannot leave a zero-length/half-flushed config. Parity: the reference's
//             writeFileSyncAndFlush.
//   Gap #50 — distinguish ENOENT/ENOTDIR (absent) from EACCES/EIO (present but
//             unreadable) in loadConfigFileChecked, surfacing the latter as an
//             `unreadable` diagnostic instead of silently treating it as "no
//             config". Parity: the reference's handleFileSystemError.
//   Gap #57 — JSON syntax-error recovery guidance on a corrupt config load.
//   Gap #130 — JSON syntax-error recovery guidance from validateConfigJson.
//
// Each assertion discriminates the NEW behavior from the pre-fix behavior (the
// old code wrote without a mode/flush, swallowed every read error as absent, and
// surfaced only the raw parser message).
// ---------------------------------------------------------------------------

function mkTemp(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'cfg-u010-dd-'));
}

// ===========================================================================
// Gap #18 — owner-only 0o600 on new file; preserve mode on rewrite
// ===========================================================================
describe('U010 Gap #18 — owner-only file permissions', () => {
  let dir: string;
  beforeEach(() => { dir = mkTemp(); });
  afterEach(() => { fs.rmSync(dir, { recursive: true, force: true }); });

  it('creates a NEW config file with mode 0o600 (owner-only)', { skip: process.platform === 'win32' }, () => {
    const fp = path.join(dir, 'config.json');
    safeWriteConfig(fp, { model: 'opus' });
    const mode = fs.statSync(fp).mode & 0o777;
    assert.equal(mode, CONFIG_FILE_MODE, `expected 0o600, got 0o${mode.toString(8)}`);
  });

  it('PRESERVES an existing file mode across a rewrite (does not reset to 0o600)', { skip: process.platform === 'win32' }, () => {
    const fp = path.join(dir, 'config.json');
    safeWriteConfig(fp, { model: 'a' });
    fs.chmodSync(fp, 0o644);
    safeWriteConfig(fp, { model: 'b' });
    const mode = fs.statSync(fp).mode & 0o777;
    assert.equal(mode, 0o644, `expected preserved 0o644, got 0o${mode.toString(8)}`);
    assert.equal(JSON.parse(fs.readFileSync(fp, 'utf-8')).model, 'b');
  });
});

// ===========================================================================
// Gap #84 — durable write: the temp write passes flush:true + mode
// ===========================================================================
describe('U010 Gap #84 — durable (flushed) writes', () => {
  let dir: string;
  beforeEach(() => { dir = mkTemp(); });
  afterEach(() => { mock.restoreAll(); fs.rmSync(dir, { recursive: true, force: true }); });

  it('writes the temp file with { flush: true } and a mode (fsync durability)', () => {
    const fp = path.join(dir, 'config.json');
    const seen: Array<{ file: string; opts: unknown }> = [];
    const real = fsHooks.writeFileSync;
    mock.method(fsHooks, 'writeFileSync', ((file: fs.PathOrFileDescriptor, data: string | NodeJS.ArrayBufferView, opts?: unknown) => {
      seen.push({ file: String(file), opts });
      return (real as (...a: unknown[]) => unknown)(file, data, opts);
    }) as typeof fs.writeFileSync);

    safeWriteConfig(fp, { model: 'opus' });

    const tmpWrite = seen.find(s => s.file.endsWith('.tmp'));
    assert.ok(tmpWrite, 'expected a temp-file write');
    const opts = tmpWrite!.opts as { flush?: boolean; mode?: number; encoding?: string };
    assert.equal(opts.flush, true, 'temp write must request flush:true for durability');
    assert.equal(opts.mode, CONFIG_FILE_MODE, 'new temp write must pass the owner-only mode');
    // Sanity: the data really landed.
    assert.equal(JSON.parse(fs.readFileSync(fp, 'utf-8')).model, 'opus');
  });
});

// ===========================================================================
// Gap #50 — ENOENT vs EACCES in loadConfigFileChecked
// ===========================================================================
describe('U010 Gap #50 — unreadable-but-present config is a diagnostic', () => {
  afterEach(() => { mock.restoreAll(); });

  it('a missing file (ENOENT) → absent, NO diagnostic', () => {
    const dir = mkTemp();
    try {
      const res = loadConfigFileChecked(path.join(dir, 'nope.json'));
      assert.equal(res.config, null);
      assert.equal(res.diagnostic, undefined);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('a present-but-unreadable file (EACCES) → `unreadable` diagnostic, not silent absence', () => {
    const fp = '/some/config.json';
    mock.method(fsHooks, 'existsSync', (() => true) as typeof fs.existsSync);
    mock.method(fsHooks, 'readFileSync', (() => {
      throw Object.assign(new Error('EACCES: permission denied'), { code: 'EACCES' });
    }) as typeof fs.readFileSync);

    const res = loadConfigFileChecked(fp);
    assert.equal(res.config, null);
    assert.ok(res.diagnostic, 'an unreadable present file must produce a diagnostic');
    assert.equal(res.diagnostic!.kind, 'unreadable');
    assert.match(res.diagnostic!.message, /EACCES/);
  });

  it('ENOTDIR (a path component is not a directory) → absent, NO diagnostic', () => {
    const fp = '/x/config.json';
    mock.method(fsHooks, 'existsSync', (() => true) as typeof fs.existsSync);
    mock.method(fsHooks, 'readFileSync', (() => {
      throw Object.assign(new Error('ENOTDIR'), { code: 'ENOTDIR' });
    }) as typeof fs.readFileSync);

    const res = loadConfigFileChecked(fp);
    assert.equal(res.config, null);
    assert.equal(res.diagnostic, undefined);
  });
});

// ===========================================================================
// Gaps #57 / #130 — JSON syntax-error recovery guidance
// ===========================================================================
describe('U010 Gaps #57/#130 — syntax-error recovery guidance', () => {
  let dir: string;
  beforeEach(() => { dir = mkTemp(); });
  afterEach(() => { fs.rmSync(dir, { recursive: true, force: true }); });

  it('loadConfigFileChecked appends the recovery hint to a parse-error diagnostic (#57)', () => {
    const fp = path.join(dir, 'config.json');
    fs.writeFileSync(fp, '{ "model": "opus", }}}'); // malformed JSON
    const res = loadConfigFileChecked(fp);
    assert.equal(res.config, null);
    assert.ok(res.diagnostic);
    assert.equal(res.diagnostic!.kind, 'parse-error');
    assert.ok(res.diagnostic!.message.includes(JSON_SYNTAX_RECOVERY_HINT),
      `parse-error message must carry the recovery hint; got: ${res.diagnostic!.message}`);
  });

  it('validateConfigJson appends the recovery hint to an invalid-JSON error (#130)', () => {
    const r = validateConfigJson('{ not: valid json ');
    assert.equal(r.valid, false);
    assert.ok(r.error!.startsWith('Invalid JSON:'));
    assert.ok(r.error!.includes(JSON_SYNTAX_RECOVERY_HINT),
      `validateConfigJson error must carry the recovery hint; got: ${r.error}`);
  });

  it('validateConfigJson stays clean (no hint) for valid JSON objects', () => {
    assert.deepEqual(validateConfigJson('{"model":"opus"}'), { valid: true });
  });
});
