import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { isWSL } from '../src/utils/env';
import { isWSL as grepIsWSL } from '../src/tools/grep';

// U021-w70: the canonical WSL detector lives in utils/env and is re-exported by
// tools/grep so the ripgrep timeout and the session-lock cross-OS reclaim guard
// share ONE implementation (no duplicated platform probe).
describe('isWSL (canonical, utils/env)', () => {
  it('detects the WSL distro env flag', () => {
    assert.equal(isWSL({ WSL_DISTRO_NAME: 'Ubuntu' } as NodeJS.ProcessEnv), true);
  });

  it('detects the WSL interop env flag', () => {
    assert.equal(isWSL({ WSL_INTEROP: '/run/WSL/1_interop' } as NodeJS.ProcessEnv), true);
  });

  it('returns a boolean for a plain (non-WSL) env', () => {
    assert.equal(typeof isWSL({} as NodeJS.ProcessEnv), 'boolean');
  });

  it('is the SAME function re-exported from tools/grep (single source)', () => {
    assert.equal(grepIsWSL, isWSL);
  });
});
