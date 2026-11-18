import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { isEnvDefinedFalsy } from '../src/utils/env';
import {
  isEnvDefinedFalsy as managerIsEnvDefinedFalsy,
  shouldAutoConnectIde,
  MCP_IDE_AUTOCONNECT_ENV,
} from '../src/mcp/manager';

/**
 * U440 — consolidate the three-state `isEnvDefinedFalsy` onto ONE canonical home
 * in `core/utils/env` (the gap's suggested env-utils target) and route the MCP
 * IDE auto-connect waterfall through it.
 *
 * Before this unit the helper was re-derived per site (a copy in `mcp/manager.ts`
 * and another in `cli/update-check.ts`), the exact semantic duplication §0b.6
 * forbids. These tests lock the canonical helper's three-state vocabulary AND the
 * identity of the re-export (manager must be the SAME function object, not a
 * parallel definition), so a future divergence fails the suite.
 *
 * RED: the import of `isEnvDefinedFalsy` from `../src/utils/env` does not resolve
 * until the canonical helper exists, so the whole file fails to load pre-fix.
 */
describe('U440 canonical isEnvDefinedFalsy (core/utils/env)', () => {
  it('is true ONLY for an explicit falsy word, case- and whitespace-insensitive', () => {
    for (const v of ['0', 'false', 'no', 'off', ' OFF ', 'False', '\tNO\n']) {
      assert.equal(isEnvDefinedFalsy(v), true, v);
    }
  });

  it('is false for unset / empty / truthy / arbitrary text (distinct from set-to-false)', () => {
    for (const v of [undefined, '', '   ', '1', 'true', 'yes', 'on', 'maybe', 'banana']) {
      assert.equal(isEnvDefinedFalsy(v), false, String(v));
    }
  });

  it('accepts a boolean for parity with the reference signature', () => {
    assert.equal(isEnvDefinedFalsy(false), true);
    assert.equal(isEnvDefinedFalsy(true), false);
  });

  it('never classifies a value as both truthy-looking and defined-falsy', () => {
    for (const v of ['0', 'false', 'no', 'off', '1', 'true', 'yes', 'on', '', 'x']) {
      const falsy = isEnvDefinedFalsy(v);
      const truthyWord = ['1', 'true', 'yes', 'on'].includes(v.trim().toLowerCase());
      assert.equal(falsy && truthyWord, false, v);
    }
  });
});

describe('U440 single canonical source — no parallel definition', () => {
  it('mcp/manager re-exports the SAME function object as core/utils/env', () => {
    assert.equal(managerIsEnvDefinedFalsy, isEnvDefinedFalsy);
  });

  it('the IDE auto-connect hard opt-out is driven by the canonical helper', () => {
    // An explicit falsy override is a hard opt-out that wins over every enabling
    // input — proving the waterfall consumes the consolidated helper end-to-end.
    assert.equal(
      shouldAutoConnectIde({
        configEnabled: true,
        flag: true,
        env: { TERM_PROGRAM: 'vscode', [MCP_IDE_AUTOCONNECT_ENV]: ' OFF ' },
      }),
      false,
    );
    // An empty override is NOT defined-falsy, so an otherwise-enabling input wins.
    assert.equal(
      shouldAutoConnectIde({ configEnabled: true, env: { [MCP_IDE_AUTOCONNECT_ENV]: '' } }),
      true,
    );
  });
});
