import { describe, it, afterEach, mock } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import { loadConfigFileChecked, fsHooks } from '../src/config';
import * as errno from '../src/utils/errno';
import * as cron from '../src/cron';

// ===========================================================================
// UNIT U471 — core/core/src/utils/errno.ts
//
// Gap (shellConfig.ts::fs error classification): optional-file readers must
// classify a caught fs error through ONE canonical errno classifier — returning
// "absent" only for the genuinely-unreachable codes and surfacing any UNEXPECTED
// failure (EIO, a decode error, a non-errno throw) instead of a blanket
// catch -> null that masks real faults. errno.ts is that single source
// (getErrnoCode / isFsInaccessible, parity with the reference's utils/errors.ts).
//
// This unit's remaining work was CONSOLIDATION (invariant #6, no semantic
// duplication): config.ts's reader used an ad-hoc inline `err.code` cast and
// cron.ts re-declared a verbatim-identical `FS_INACCESSIBLE_CODES` + its own
// `isFsInaccessible`. Both now route through errno.ts. These tests lock that
// single-source wiring so a regression re-introducing a parallel classifier (or
// a blanket swallow) fails.
// ===========================================================================

describe('U471 — one canonical fs-error classifier (no parallel errno set)', () => {
  it('cron.isFsInaccessible IS the canonical errno.isFsInaccessible (single source)', () => {
    // A verbatim-duplicate private classifier in cron.ts would make these two
    // different function objects — this identity check fails if the dup returns.
    assert.strictEqual(cron.isFsInaccessible, errno.isFsInaccessible);
  });

  it('the canonical classifier covers exactly the Claude-parity inaccessible set', () => {
    for (const code of ['ENOENT', 'EACCES', 'EPERM', 'ENOTDIR', 'ELOOP']) {
      assert.equal(cron.isFsInaccessible(Object.assign(new Error('x'), { code })), true, code);
    }
    // Unexpected / unrelated fs codes are NOT "inaccessible" — they must surface.
    for (const code of ['EIO', 'EISDIR', 'EBUSY']) {
      assert.equal(cron.isFsInaccessible(Object.assign(new Error('x'), { code })), false, code);
    }
    // Non-errno throws (no code / non-string code / non-object) are never absent.
    assert.equal(cron.isFsInaccessible(new Error('plain')), false);
    assert.equal(cron.isFsInaccessible({ code: 42 }), false);
    assert.equal(cron.isFsInaccessible(null), false);
  });
});

describe('U471 — config reader classifies via the canonical getErrnoCode', () => {
  afterEach(() => { mock.restoreAll(); });

  it('a missing file (ENOENT) → absent, NO diagnostic', () => {
    mock.method(fsHooks, 'existsSync', (() => true) as typeof fs.existsSync);
    mock.method(fsHooks, 'readFileSync', (() => {
      throw Object.assign(new Error('ENOENT: no such file'), { code: 'ENOENT' });
    }) as typeof fs.readFileSync);

    const res = loadConfigFileChecked('/x/config.json');
    assert.equal(res.config, null);
    assert.equal(res.diagnostic, undefined);
  });

  it('a transient read fault (EIO) on a PRESENT file → surfaced as `unreadable`, not swallowed', () => {
    mock.method(fsHooks, 'existsSync', (() => true) as typeof fs.existsSync);
    mock.method(fsHooks, 'readFileSync', (() => {
      throw Object.assign(new Error('EIO: i/o error'), { code: 'EIO' });
    }) as typeof fs.readFileSync);

    const res = loadConfigFileChecked('/x/config.json');
    assert.equal(res.config, null);
    assert.ok(res.diagnostic, 'an unexpected IO fault must produce a diagnostic');
    assert.equal(res.diagnostic!.kind, 'unreadable');
    assert.match(res.diagnostic!.message, /EIO/);
  });

  it('a non-errno throw (no `code`, e.g. a decode failure) → surfaced as `unreadable`', () => {
    // getErrnoCode returns undefined here; the reader must NOT mistake "no errno"
    // for ENOENT/ENOTDIR absence — it is an unexpected failure to surface.
    mock.method(fsHooks, 'existsSync', (() => true) as typeof fs.existsSync);
    mock.method(fsHooks, 'readFileSync', (() => {
      throw new Error('stream closed unexpectedly');
    }) as typeof fs.readFileSync);

    const res = loadConfigFileChecked('/x/config.json');
    assert.equal(res.config, null);
    assert.ok(res.diagnostic, 'a codeless failure must still surface a diagnostic');
    assert.equal(res.diagnostic!.kind, 'unreadable');
    assert.match(res.diagnostic!.message, /stream closed/);
  });
});
