import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { isXaaEnabled, MCP_ENABLE_XAA_ENV } from '../src/mcp/oauth';
import { parseEnvBool } from '../src/utils/env';

/**
 * U218 — consolidate the divergent inline `isEnvTruthy` copies onto the single
 * canonical tolerant env-boolean parser {@link parseEnvBool} (core/utils/env),
 * the same home cron/web-fetch/git-commit were routed to in U228. Before this
 * unit, `mcp/oauth.ts` carried its OWN private `isEnvTruthy` accepting only the
 * strict `{1,true,yes,on}` set, so the same env var parsed differently than at
 * sites that already used `parseEnvBool`.
 *
 * RED discriminator: each case below uses a tolerant spelling (`enable`/`y`/`t`/
 * `enabled`) that the removed strict copy did NOT recognise but `parseEnvBool`
 * does. Reverting the consolidation (restoring the strict private copy) flips
 * these from `true` back to `false`, so the test fails on revert.
 */
describe('U218 isXaaEnabled routes through the canonical parseEnvBool', () => {
  // Sanity: the old strict tokens still resolve truthy (additive superset).
  it('keeps the strict truthy spellings working', () => {
    for (const v of ['1', 'true', 'yes', 'on', 'YES', ' On ']) {
      assert.equal(isXaaEnabled({ [MCP_ENABLE_XAA_ENV]: v }), true, `${v} should enable XAA`);
    }
  });

  it('now honours the tolerant truthy spellings the strict copy rejected (RED on revert)', () => {
    for (const v of ['enable', 'enabled', 'y', 't']) {
      // parseEnvBool accepts these; the removed strict {1,true,yes,on} copy did not.
      assert.equal(parseEnvBool(v), true, `parseEnvBool(${v}) is the canonical truth`);
      assert.equal(isXaaEnabled({ [MCP_ENABLE_XAA_ENV]: v }), true, `${v} should enable XAA`);
    }
  });

  it('stays off for unset, empty and falsy spellings', () => {
    assert.equal(isXaaEnabled({}), false);
    for (const v of ['', '0', 'false', 'no', 'off', 'disable', 'garbage']) {
      assert.equal(isXaaEnabled({ [MCP_ENABLE_XAA_ENV]: v }), false, `${v} should not enable XAA`);
    }
  });
});
