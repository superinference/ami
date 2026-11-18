/**
 * Unit tests (U019/w108) for best-effort process hardening — the pure-Node port of
 * Claude Code's setNonDumpable()/PR_SET_DUMPABLE defense-in-depth. On Linux it
 * clears /proc/self/coredump_filter so heap pages (holding live API keys/session
 * tokens) are excluded from any core dump, blocking same-UID heap-scraping via a
 * core file. All platform/fs/env dependencies are injected so NO real /proc is
 * touched.
 *
 * Run: cd core/core && node --import tsx --test test/process-hardening-u019-w108.test.ts
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  hardenProcess,
  describeProcessHardening,
  processHardeningDisabled,
  PROCESS_HARDENING_DISABLE_ENV,
  COREDUMP_FILTER_PATH,
  type ProcessHardeningDeps,
} from '../src/utils/process-hardening';

/** A recording fs pair plus the captured writes, for assertions. */
function recordingFs(filterValue: string | Error) {
  const writes: Array<{ path: string; data: string }> = [];
  const deps: Partial<ProcessHardeningDeps> = {
    readFileSync: (p: string) => {
      if (filterValue instanceof Error) throw filterValue;
      assert.equal(p, COREDUMP_FILTER_PATH);
      return `${filterValue}\n`;
    },
    writeFileSync: (p: string, data: string) => {
      writes.push({ path: p, data });
    },
  };
  return { deps, writes };
}

describe('processHardeningDisabled', () => {
  it('is false when the opt-out env var is unset', () => {
    assert.equal(processHardeningDisabled({}), false);
  });
  it('is true for truthy opt-out values (case/space-insensitive)', () => {
    for (const v of ['1', 'true', 'ON', ' yes ', 'Enable', 'enabled']) {
      assert.equal(
        processHardeningDisabled({ [PROCESS_HARDENING_DISABLE_ENV]: v }),
        true,
        `"${v}" should disable`,
      );
    }
  });
  it('is false for a non-truthy value', () => {
    assert.equal(processHardeningDisabled({ [PROCESS_HARDENING_DISABLE_ENV]: '0' }), false);
    assert.equal(processHardeningDisabled({ [PROCESS_HARDENING_DISABLE_ENV]: 'nope' }), false);
  });
});

describe('hardenProcess', () => {
  it('clears a non-zero core-dump filter on Linux (applied)', () => {
    const { deps, writes } = recordingFs('33');
    const r = hardenProcess({ platform: 'linux', env: {}, ...deps });
    assert.equal(r.status, 'applied');
    assert.equal(r.method, 'coredump-filter');
    assert.equal(r.coredumpFilterBefore, '33');
    assert.equal(r.coredumpFilterAfter, '0');
    assert.deepEqual(writes, [{ path: COREDUMP_FILTER_PATH, data: '0' }]);
  });

  it('is a no-op when the filter is already cleared (already)', () => {
    for (const already of ['0', '0x0', '00', '0x00']) {
      const { deps, writes } = recordingFs(already);
      const r = hardenProcess({ platform: 'linux', env: {}, ...deps });
      assert.equal(r.status, 'already', `"${already}" should be treated as cleared`);
      assert.equal(writes.length, 0, 'must not re-write an already-cleared filter');
    }
  });

  it('skips when opted out via env (disabled) — never reads/writes', () => {
    const { deps, writes } = recordingFs('33');
    let read = false;
    const r = hardenProcess({
      platform: 'linux',
      env: { [PROCESS_HARDENING_DISABLE_ENV]: '1' },
      readFileSync: () => { read = true; return '33'; },
      writeFileSync: deps.writeFileSync,
    });
    assert.equal(r.status, 'disabled');
    assert.equal(read, false, 'disabled path must not read /proc');
    assert.equal(writes.length, 0);
  });

  it('is unsupported (fail-open) on a non-Linux platform', () => {
    for (const platform of ['darwin', 'win32'] as NodeJS.Platform[]) {
      const r = hardenProcess({ platform, env: {} });
      assert.equal(r.status, 'unsupported');
      assert.equal(r.platform, platform);
      assert.match(r.detail, /failing open/);
    }
  });

  it('is unsupported (fail-open) when the filter file cannot be read (no /proc)', () => {
    const { deps } = recordingFs(new Error('ENOENT'));
    const r = hardenProcess({ platform: 'linux', env: {}, ...deps });
    assert.equal(r.status, 'unsupported');
    assert.match(r.detail, /unreadable/);
  });

  it('fails open when the filter is readable but not writable', () => {
    const r = hardenProcess({
      platform: 'linux',
      env: {},
      readFileSync: () => '33',
      writeFileSync: () => { throw new Error('EACCES'); },
    });
    assert.equal(r.status, 'failed');
    assert.equal(r.coredumpFilterBefore, '33');
    assert.match(r.detail, /unwritable/);
  });

  it('never throws and forwards a log message on each path', () => {
    const logs: string[] = [];
    const log = (m: string) => logs.push(m);
    assert.doesNotThrow(() => hardenProcess({ platform: 'darwin', env: {}, log }));
    assert.doesNotThrow(() =>
      hardenProcess({ platform: 'linux', env: { [PROCESS_HARDENING_DISABLE_ENV]: '1' }, log }),
    );
    assert.doesNotThrow(() =>
      hardenProcess({ platform: 'linux', env: {}, readFileSync: () => '33', writeFileSync: () => {}, log }),
    );
    assert.ok(logs.length >= 3, 'each path should emit one debug log');
  });
});

describe('describeProcessHardening (introspection view, read-only)', () => {
  it('reports supported:true on Linux with the /proc path and method', () => {
    const d = describeProcessHardening({ platform: 'linux', env: {} });
    assert.equal(d.supported, true);
    assert.equal(d.method, 'coredump-filter');
    assert.equal(d.coredumpFilterPath, COREDUMP_FILTER_PATH);
    assert.equal(d.disableEnv, PROCESS_HARDENING_DISABLE_ENV);
    assert.equal(d.disabled, false);
    assert.match(d.note, /coredump_filter/);
  });

  it('reports supported:false on non-Linux and reflects the opt-out state', () => {
    const d = describeProcessHardening({
      platform: 'darwin',
      env: { [PROCESS_HARDENING_DISABLE_ENV]: 'yes' },
    });
    assert.equal(d.supported, false);
    assert.equal(d.disabled, true);
    assert.match(d.note, /Linux-only/);
  });

  it('does not mutate anything (pure) — callable without fs deps', () => {
    assert.doesNotThrow(() => describeProcessHardening({ platform: 'linux', env: {} }));
  });
});
