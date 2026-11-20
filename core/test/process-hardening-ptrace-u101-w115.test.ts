/**
 * Unit tests (U101/w115) for the live-ptrace block added to best-effort process
 * hardening — the pure port of the reference's setNonDumpable()/prctl(PR_SET_DUMPABLE,0)
 * call (upstreamproxy.ts:L220-252). This complements the existing core-dump-filter
 * measure: prctl(PR_SET_DUMPABLE,0) ALSO blocks a same-UID `gdb -p $PPID` /
 * /proc/<pid>/mem read of the live heap, not just the post-crash core file. Node has
 * no built-in prctl, so the measure activates only on a runtime with libc FFI
 * (Bun-style `bun:ffi`) and fails open to `no-ffi` on plain Node. ALL FFI/platform/
 * prctl dependencies are injected so NO real syscall is ever made.
 *
 * Run: cd core/core && node --import tsx --test test/process-hardening-ptrace-u101-w115.test.ts
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  hardenProcess,
  blockPtrace,
  loadLibcPrctl,
  bunFfiRequire,
  describeProcessHardening,
  PR_SET_DUMPABLE,
  PROCESS_HARDENING_DISABLE_ENV,
  COREDUMP_FILTER_PATH,
  type PrctlFn,
} from '../src/utils/process-hardening';

/** A fake `bun:ffi` whose dlopen returns a prctl stub recording its args. */
function fakeFfiRequire(onPrctl: (args: Array<number | bigint>) => number) {
  const calls: Array<Array<number | bigint>> = [];
  const req = (id: string): unknown => {
    assert.equal(id, 'bun:ffi');
    return {
      dlopen: (path: string, symbols: Record<string, unknown>) => {
        assert.equal(path, 'libc.so.6');
        assert.ok('prctl' in symbols, 'declares the prctl symbol');
        return {
          symbols: {
            prctl: (...args: Array<number | bigint>) => {
              calls.push(args);
              return onPrctl(args);
            },
          },
        };
      },
    };
  };
  return { req, calls };
}

describe('bunFfiRequire', () => {
  it('delegates to a runtime global require when present', () => {
    const g = globalThis as { require?: (id: string) => unknown };
    const prev = g.require;
    try {
      g.require = (id: string) => `loaded:${id}`;
      assert.equal(bunFfiRequire('bun:ffi'), 'loaded:bun:ffi');
    } finally {
      if (prev === undefined) delete g.require;
      else g.require = prev;
    }
  });

  it('throws when the runtime has no global require (plain Node ESM)', () => {
    const g = globalThis as { require?: (id: string) => unknown };
    const prev = g.require;
    try {
      delete g.require;
      assert.throws(() => bunFfiRequire('bun:ffi'), /no runtime require/);
    } finally {
      if (prev !== undefined) g.require = prev;
    }
  });
});

describe('loadLibcPrctl', () => {
  it('returns null on a runtime without FFI (plain Node — default + explicit)', () => {
    assert.equal(loadLibcPrctl(), null);
    assert.equal(loadLibcPrctl({ hasFfi: false }), null);
  });

  it('loads a working prctl invoker through the FFI and forwards PR_SET_DUMPABLE args', () => {
    const { req, calls } = fakeFfiRequire(() => 0);
    const prctl = loadLibcPrctl({ hasFfi: true, ffiRequire: req });
    assert.equal(typeof prctl, 'function');
    const rc = prctl!(PR_SET_DUMPABLE, 0n);
    assert.equal(rc, 0);
    // The invoker pads the remaining prctl args with zero (PR_SET_DUMPABLE takes none).
    assert.deepEqual(calls[0], [PR_SET_DUMPABLE, 0n, 0n, 0n, 0n]);
  });

  it('fails open to null when the FFI loader throws (no bun:ffi / dlopen error)', () => {
    const throwing = loadLibcPrctl({
      hasFfi: true,
      ffiRequire: () => { throw new Error('Cannot find module bun:ffi'); },
    });
    assert.equal(throwing, null);
  });

  it('falls back to bunFfiRequire when no ffiRequire is injected', () => {
    const g = globalThis as { require?: (id: string) => unknown };
    const prev = g.require;
    try {
      // A require that returns a non-FFI object → dlopen access throws → null.
      g.require = () => ({});
      assert.equal(loadLibcPrctl({ hasFfi: true }), null);
    } finally {
      if (prev === undefined) delete g.require;
      else g.require = prev;
    }
  });
});

describe('blockPtrace', () => {
  it('applies PR_SET_DUMPABLE(0) on Linux with a working prctl (applied)', () => {
    let seen: Array<number | bigint> | undefined;
    const prctl: PrctlFn = (opt, arg) => { seen = [opt, arg]; return 0; };
    const r = blockPtrace({ platform: 'linux', env: {}, prctl });
    assert.equal(r.status, 'applied');
    assert.equal(r.method, 'prctl-set-dumpable');
    assert.deepEqual(seen, [PR_SET_DUMPABLE, 0n]);
  });

  it('is unsupported (fail-open) on a non-Linux platform — never calls prctl', () => {
    let called = false;
    const r = blockPtrace({ platform: 'darwin', env: {}, prctl: () => { called = true; return 0; } });
    assert.equal(r.status, 'unsupported');
    assert.equal(called, false);
    assert.match(r.detail, /Linux-only/);
  });

  it('is disabled when the opt-out env var is set — never calls prctl', () => {
    let called = false;
    const r = blockPtrace({
      platform: 'linux',
      env: { [PROCESS_HARDENING_DISABLE_ENV]: '1' },
      prctl: () => { called = true; return 0; },
    });
    assert.equal(r.status, 'disabled');
    assert.equal(called, false);
  });

  it('reports no-ffi when no prctl invoker is available (forced null)', () => {
    const r = blockPtrace({ platform: 'linux', env: {}, prctl: null });
    assert.equal(r.status, 'no-ffi');
    assert.match(r.detail, /no libc FFI/);
  });

  it('reports no-ffi on plain Node when prctl is left to the default loader', () => {
    // deps.prctl undefined → loadLibcPrctl() → null on a non-FFI runtime.
    const r = blockPtrace({ platform: 'linux', env: {} });
    assert.equal(r.status, 'no-ffi');
  });

  it('fails open when prctl returns nonzero (failed)', () => {
    const r = blockPtrace({ platform: 'linux', env: {}, prctl: () => -1 });
    assert.equal(r.status, 'failed');
    assert.match(r.detail, /returned -1/);
  });

  it('fails open when the FFI call throws (error) — Error and non-Error', () => {
    const e1 = blockPtrace({ platform: 'linux', env: {}, prctl: () => { throw new Error('EPERM'); } });
    assert.equal(e1.status, 'error');
    assert.match(e1.detail, /EPERM/);
    const e2 = blockPtrace({ platform: 'linux', env: {}, prctl: () => { throw 'boom'; } });
    assert.equal(e2.status, 'error');
    assert.match(e2.detail, /boom/);
  });

  it('never throws and forwards a debug log on each path', () => {
    const logs: string[] = [];
    const log = (m: string) => logs.push(m);
    assert.doesNotThrow(() => blockPtrace({ platform: 'darwin', env: {}, log }));
    assert.doesNotThrow(() => blockPtrace({ platform: 'linux', env: { [PROCESS_HARDENING_DISABLE_ENV]: '1' }, log }));
    assert.doesNotThrow(() => blockPtrace({ platform: 'linux', env: {}, prctl: null, log }));
    assert.doesNotThrow(() => blockPtrace({ platform: 'linux', env: {}, prctl: () => 0, log }));
    assert.doesNotThrow(() => blockPtrace({ platform: 'linux', env: {}, prctl: () => 7, log }));
    assert.doesNotThrow(() => blockPtrace({ platform: 'linux', env: {}, prctl: () => { throw new Error('x'); }, log }));
    assert.ok(logs.length >= 6, 'each path should emit one debug log');
  });
});

describe('hardenProcess (both measures)', () => {
  it('applies the core-dump filter AND the ptrace block on Linux', () => {
    const writes: Array<{ path: string; data: string }> = [];
    const r = hardenProcess({
      platform: 'linux',
      env: {},
      readFileSync: () => '33\n',
      writeFileSync: (p, d) => writes.push({ path: p, data: d }),
      prctl: () => 0,
    });
    // Measure 1 (top-level, back-compat fields) still works.
    assert.equal(r.status, 'applied');
    assert.equal(r.method, 'coredump-filter');
    assert.deepEqual(writes, [{ path: COREDUMP_FILTER_PATH, data: '0' }]);
    // Measure 2 is attached.
    assert.equal(r.ptrace.status, 'applied');
    assert.equal(r.ptrace.method, 'prctl-set-dumpable');
  });

  it('attaches a fail-open ptrace result when the runtime has no FFI', () => {
    const r = hardenProcess({
      platform: 'linux',
      env: {},
      readFileSync: () => '0',
      writeFileSync: () => {},
      prctl: null,
    });
    assert.equal(r.status, 'already');
    assert.equal(r.ptrace.status, 'no-ffi');
  });

  it('both measures are unsupported (fail-open) on a non-Linux platform', () => {
    const r = hardenProcess({ platform: 'win32', env: {} });
    assert.equal(r.status, 'unsupported');
    assert.equal(r.ptrace.status, 'unsupported');
  });

  it('both measures are skipped when opted out via env', () => {
    const r = hardenProcess({ platform: 'linux', env: { [PROCESS_HARDENING_DISABLE_ENV]: 'yes' } });
    assert.equal(r.status, 'disabled');
    assert.equal(r.ptrace.status, 'disabled');
  });
});

describe('describeProcessHardening (ptrace introspection view, read-only)', () => {
  it('reports the ptrace capability on Linux (supported, FFI-gated, inactive on plain Node)', () => {
    const d = describeProcessHardening({ platform: 'linux', env: {} });
    assert.equal(d.ptrace.method, 'prctl-set-dumpable');
    assert.equal(d.ptrace.prctlOption, PR_SET_DUMPABLE);
    assert.equal(d.ptrace.supported, true);
    // Plain Node (the test runtime) has no libc FFI → not active.
    assert.equal(d.ptrace.ffiAvailable, false);
    assert.equal(d.ptrace.active, false);
    assert.match(d.ptrace.note, /libc FFI|Bun/);
  });

  it('reports ptrace unsupported on a non-Linux platform', () => {
    const d = describeProcessHardening({ platform: 'darwin', env: {} });
    assert.equal(d.ptrace.supported, false);
    assert.equal(d.ptrace.active, false);
    assert.match(d.ptrace.note, /Linux-only/);
  });

  it('reflects the opt-out state in ptrace.active', () => {
    const d = describeProcessHardening({
      platform: 'linux',
      env: { [PROCESS_HARDENING_DISABLE_ENV]: '1' },
    });
    assert.equal(d.disabled, true);
    assert.equal(d.ptrace.active, false);
  });

  it('is pure — callable without any deps', () => {
    assert.doesNotThrow(() => describeProcessHardening());
  });
});
