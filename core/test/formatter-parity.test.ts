/**
 * Formatter parity/hardening tests (additive superset).
 *
 * Covers correctness/robustness gaps in the multi-language file formatter
 * dispatcher: invalid-input guards (no throw on undefined/empty paths or null
 * configs), extension normalization (case + leading-dot), extension-less files,
 * and the previously-dead custom-formatter registration wired into formatFile /
 * getAvailableFormatters.
 *
 * Each behavioral fix is red-green validated against the pre-change source.
 */
import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as path from 'path';
import {
  formatFile,
  getAvailableFormatters,
  isFormatterAvailable,
  execHooks,
  __clearFormatterCache,
  normalizeExtension,
  getFormatterForExtension,
  isFormattableFile,
  registerFormatter,
  clearCustomFormatters,
  listRegisteredFormatters,
  type FormatterConfig,
} from '../src/formatter';

describe('formatter hardening/parity', () => {
  beforeEach(() => {
    __clearFormatterCache();
    clearCustomFormatters();
  });
  afterEach(() => {
    mock.restoreAll();
    __clearFormatterCache();
    clearCustomFormatters();
  });

  // --- Gap 1: formatFile must not throw on invalid filePath --------------
  it('formatFile returns {formatted:false} for undefined/non-string path (no throw)', () => {
    // RED (pre-fix): path.extname(undefined) throws ERR_INVALID_ARG_TYPE.
    assert.deepEqual(formatFile(undefined as unknown as string, '/tmp'), { formatted: false });
    assert.deepEqual(formatFile(null as unknown as string, '/tmp'), { formatted: false });
    assert.deepEqual(formatFile(42 as unknown as string, '/tmp'), { formatted: false });
  });

  it('formatFile returns {formatted:false} for empty path and extension-less files', () => {
    assert.deepEqual(formatFile('', '/tmp'), { formatted: false });
    assert.deepEqual(formatFile('/repo/Makefile', '/tmp'), { formatted: false });
    assert.deepEqual(formatFile('/repo/.prettierrc', '/tmp'), { formatted: false });
  });

  // --- Gap 2: isFormatterAvailable must not throw on null/undefined config
  it('isFormatterAvailable returns false for null/undefined config (no throw)', () => {
    // RED (pre-fix): `!config.check` dereferences null → TypeError.
    assert.equal(isFormatterAvailable(undefined as unknown as FormatterConfig), false);
    assert.equal(isFormatterAvailable(null as unknown as FormatterConfig), false);
    assert.equal(isFormatterAvailable({ extensions: ['.x'], command: 'x', args: [] }), false);
  });

  // --- Gap 3: extension normalization -----------------------------------
  it('normalizeExtension canonicalizes case and leading dots', () => {
    assert.equal(normalizeExtension('TS'), '.ts');
    assert.equal(normalizeExtension('.TS'), '.ts');
    assert.equal(normalizeExtension('..Ts'), '.ts');
    assert.equal(normalizeExtension('  .JS  '), '.js');
    assert.equal(normalizeExtension('ts'), '.ts');
    assert.equal(normalizeExtension(''), '');
    assert.equal(normalizeExtension('.'), '');
    assert.equal(normalizeExtension('...'), '');
    assert.equal(normalizeExtension(undefined as unknown as string), '');
    assert.equal(normalizeExtension(123 as unknown as string), '');
  });

  it('getFormatterForExtension matches regardless of case/leading dot', () => {
    assert.equal(getFormatterForExtension('ts')?.command, 'npx');
    assert.equal(getFormatterForExtension('.TS')?.command, 'npx');
    assert.equal(getFormatterForExtension('go')?.command, 'gofmt');
    assert.equal(getFormatterForExtension('nope-ext'), undefined);
    assert.equal(getFormatterForExtension(''), undefined);
    assert.equal(getFormatterForExtension(undefined as unknown as string), undefined);
  });

  it('isFormattableFile reports built-in coverage and is input-safe', () => {
    assert.equal(isFormattableFile('/a/b/c.ts'), true);
    assert.equal(isFormattableFile('/a/b/C.TSX'), true);
    assert.equal(isFormattableFile('/a/b/c.unknownext'), false);
    assert.equal(isFormattableFile('/a/b/Makefile'), false);
    assert.equal(isFormattableFile(''), false);
    assert.equal(isFormattableFile(undefined as unknown as string), false);
  });

  // --- Gap 4: custom-formatter registration wired into dispatch ---------
  it('registerFormatter validates config and throws on invalid input', () => {
    assert.throws(() => registerFormatter(null as unknown as FormatterConfig), TypeError);
    assert.throws(() => registerFormatter({ extensions: ['.ts'], command: '', args: [] }), TypeError);
    assert.throws(() => registerFormatter({ extensions: ['.ts'], command: '   ', args: [] }), TypeError);
    assert.throws(() => registerFormatter({ extensions: [], command: 'x', args: [] }), TypeError);
    assert.throws(() => registerFormatter({ extensions: ['', '.'], command: 'x', args: [] }), TypeError);
    assert.throws(
      () => registerFormatter({ extensions: 'ts' as unknown as string[], command: 'x', args: [] }),
      TypeError,
    );
    assert.equal(listRegisteredFormatters().length, 0);
  });

  it('registerFormatter normalizes extensions and dedups, and is listed defensively', () => {
    registerFormatter({ extensions: ['TS', '.ts', '.Tsx'], command: 'myfmt', args: ['--fix'], check: 'myfmt -v' });
    const list = listRegisteredFormatters();
    assert.equal(list.length, 1);
    assert.deepEqual(list[0].extensions, ['.ts', '.tsx']);
    // mutating the snapshot must not affect internal state
    list[0].extensions.push('.zzz');
    assert.deepEqual(listRegisteredFormatters()[0].extensions, ['.ts', '.tsx']);
  });

  it('registered custom formatter wins over the built-in for formatFile', () => {
    // RED (pre-wiring): formatFile only consults BUILT_IN_FORMATTERS, so the
    // captured format command starts with the built-in `npx prettier --write`.
    registerFormatter({ extensions: ['.ts'], command: 'myfmt', args: ['--fix'], check: 'myfmt --version' });
    const calls: string[] = [];
    mock.method(execHooks, 'execSync', (cmd: string) => { calls.push(cmd); return Buffer.from(''); });

    const res = formatFile('/tmp/a.ts', '/tmp');
    assert.equal(res.formatted, true);

    const fmtCall = calls.find(c => c.includes('/tmp/a.ts'));
    assert.ok(fmtCall, 'expected a format invocation for the file');
    assert.ok(fmtCall!.startsWith('myfmt --fix '), `expected custom command, got: ${fmtCall}`);
    assert.ok(!fmtCall!.includes('prettier'), 'must not fall back to the built-in prettier');
  });

  it('registering a new extension makes it formattable and listed in getAvailableFormatters', () => {
    // RED (pre-wiring): a brand-new extension is never formatted or advertised.
    assert.equal(isFormattableFile('/x/y.foo'), false);
    registerFormatter({ extensions: ['.foo'], command: 'foofmt', args: ['-w'], check: 'foofmt --version' });
    assert.equal(isFormattableFile('/x/y.foo'), true);

    const calls: string[] = [];
    mock.method(execHooks, 'execSync', (cmd: string) => { calls.push(cmd); return Buffer.from(''); });

    const res = formatFile('/x/y.foo', '/x');
    assert.equal(res.formatted, true);
    assert.ok(calls.some(c => c.startsWith('foofmt -w ') && c.includes('/x/y.foo')));

    const avail = getAvailableFormatters('/x');
    assert.ok(avail.some(line => line.includes('.foo') && line.includes('foofmt -w')));
  });

  it('custom formatter without a check command is treated as unavailable (consistent with built-ins)', () => {
    registerFormatter({ extensions: ['.bar'], command: 'barfmt', args: [] });
    mock.method(execHooks, 'execSync', () => Buffer.from(''));
    // no check → isFormatterAvailable false → not run
    assert.deepEqual(formatFile('/x/y.bar', '/x'), { formatted: false });
  });

  it('clearCustomFormatters restores built-in-only behavior', () => {
    registerFormatter({ extensions: ['.ts'], command: 'myfmt', args: [], check: 'myfmt --version' });
    assert.equal(getFormatterForExtension('.ts')?.command, 'myfmt');
    clearCustomFormatters();
    assert.equal(getFormatterForExtension('.ts')?.command, 'npx');
    assert.equal(listRegisteredFormatters().length, 0);
  });

  // --- Capability-depth probe (F2098): version-depth is sufficient --------
  // Rationale: a deep "probe the real operation" availability check is only
  // needed when the version probe can pass while the gated operation fails
  // with NO fallback. For formatters that premise does not hold: formatFile
  // already degrades gracefully when the real invocation fails even though the
  // availability (version) check passed — it returns {formatted:false,error}
  // rather than throwing. This test locks that fallback so the shallow probe
  // stays correct (and a deep operation probe is unnecessary).
  it('formatFile degrades gracefully when the format op fails after the check passed', () => {
    registerFormatter({ extensions: ['.ts'], command: 'myfmt', args: [], check: 'myfmt --version' });
    let call = 0;
    mock.method(execHooks, 'execSync', (cmd: string) => {
      call += 1;
      // 1st call = the `check` (version probe) → passes, proving "available".
      if (cmd.includes('--version')) return Buffer.from('myfmt 1.0.0');
      // 2nd call = the real format operation → fails at use time.
      throw new Error('format op exploded');
    });

    assert.equal(isFormatterAvailable({ extensions: ['.ts'], command: 'myfmt', args: [], check: 'myfmt --version' }), true);
    const res = formatFile('/tmp/a.ts', '/tmp');
    // No throw: the operation-time failure is captured as a fallback result.
    assert.equal(res.formatted, false);
    assert.ok(typeof res.error === 'string' && res.error.includes('format op exploded'));
    assert.ok(call >= 2, 'both the check and the real operation were attempted');
  });

  it('filePath with single quotes is safely escaped in the custom command', () => {
    registerFormatter({ extensions: ['.ts'], command: 'myfmt', args: [], check: 'myfmt --version' });
    const calls: string[] = [];
    mock.method(execHooks, 'execSync', (cmd: string) => { calls.push(cmd); return Buffer.from(''); });
    const weird = path.join('/tmp', "o'brien.ts");
    formatFile(weird, '/tmp');
    const fmtCall = calls.find(c => c.includes('.ts') && c.startsWith('myfmt'));
    assert.ok(fmtCall);
    assert.ok(fmtCall!.includes("'\\''"), 'single quote must be POSIX-escaped');
  });
});
