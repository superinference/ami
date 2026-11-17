import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  isSafeBetaToken,
  getExtraAnthropicBetas,
  resolvePromptCacheScope,
  buildAnthropicCacheControl,
  computeAnthropicBetas,
  PROMPT_CACHE_SCOPE_ENV,
  PROMPT_CACHING_SCOPE_BETA,
} from '../src/provider';

// ---------------------------------------------------------------------------
// U020 / w80 — two additive provider parity improvements:
//
//  1. Header-safe validation of externally-supplied `anthropic-beta` tokens
//     (references/claude/.../src/utils/betas.ts:L33-87 `filterAllowedSdkBetas`).
//     AMI keeps its operator ANTHROPIC_BETAS escape hatch but now drops any
//     malformed/injection-unsafe token instead of forwarding it verbatim.
//
//  2. Cross-session prompt-cache scope knob `AMI_PROMPT_CACHE_SCOPE`
//     (references/claude/.../src/utils/api.ts:L80-84 `CacheScope`): a
//     'global'/'org' scope on cache_control + the accompanying
//     PROMPT_CACHING_SCOPE_BETA so scoped requests are not rejected.
//
// RED on revert:
//  - dropping the `.filter(isSafeBetaToken)` lets `"bad beta"` / CRLF tokens
//    survive, failing the getExtraAnthropicBetas sanitization assertions.
//  - removing the scope param / beta push fails the cache_control + betas
//    assertions below.
// ---------------------------------------------------------------------------

const ANTHROPIC_BETAS_ENV = 'ANTHROPIC_BETAS';

afterEach(() => {
  delete process.env[ANTHROPIC_BETAS_ENV];
  delete process.env[PROMPT_CACHE_SCOPE_ENV];
});

describe('isSafeBetaToken', () => {
  it('accepts real beta token shapes', () => {
    assert.equal(isSafeBetaToken('context-1m-2025-08-07'), true);
    assert.equal(isSafeBetaToken('extended-cache-ttl-2025-04-11'), true);
    assert.equal(isSafeBetaToken('some_future.beta-9'), true);
    assert.equal(isSafeBetaToken('A1'), true);
  });

  it('rejects injection-unsafe or malformed tokens', () => {
    assert.equal(isSafeBetaToken(''), false);
    assert.equal(isSafeBetaToken('has space'), false);
    assert.equal(isSafeBetaToken('-leading-hyphen'), false);
    assert.equal(isSafeBetaToken('bad\r\nx-injected: 1'), false);
    assert.equal(isSafeBetaToken('comma,inside'), false);
    assert.equal(isSafeBetaToken('tab\tchar'), false);
    assert.equal(isSafeBetaToken('ctrl\u0000char'), false);
  });
});

describe('getExtraAnthropicBetas sanitization', () => {
  it('keeps well-formed betas and drops malformed ones', () => {
    process.env[ANTHROPIC_BETAS_ENV] = 'context-1m-2025-08-07, bad beta ,, good-one-2026-01-01,bad\r\ninject';
    assert.deepEqual(getExtraAnthropicBetas(), ['context-1m-2025-08-07', 'good-one-2026-01-01']);
  });

  it('is empty when unset', () => {
    delete process.env[ANTHROPIC_BETAS_ENV];
    assert.deepEqual(getExtraAnthropicBetas(), []);
  });

  it('drops a value that would smuggle an extra header line', () => {
    process.env[ANTHROPIC_BETAS_ENV] = 'x-evil: 1\r\nanthropic-beta';
    assert.deepEqual(getExtraAnthropicBetas(), []);
  });
});

describe('resolvePromptCacheScope', () => {
  it('honors only global/org (case-insensitive, trimmed)', () => {
    assert.equal(resolvePromptCacheScope({ [PROMPT_CACHE_SCOPE_ENV]: 'global' }), 'global');
    assert.equal(resolvePromptCacheScope({ [PROMPT_CACHE_SCOPE_ENV]: ' ORG ' }), 'org');
  });

  it('returns undefined for unset/junk', () => {
    assert.equal(resolvePromptCacheScope({}), undefined);
    assert.equal(resolvePromptCacheScope({ [PROMPT_CACHE_SCOPE_ENV]: 'nonsense' }), undefined);
    assert.equal(resolvePromptCacheScope({ [PROMPT_CACHE_SCOPE_ENV]: '' }), undefined);
  });
});

describe('buildAnthropicCacheControl scope', () => {
  it('omits scope by default (env unset) and keeps ttl behavior', () => {
    delete process.env[PROMPT_CACHE_SCOPE_ENV];
    assert.deepEqual(buildAnthropicCacheControl(), { type: 'ephemeral' });
    assert.deepEqual(buildAnthropicCacheControl('1h'), { type: 'ephemeral', ttl: '1h' });
  });

  it('attaches an explicit scope', () => {
    assert.deepEqual(buildAnthropicCacheControl('1h', 'org'), { type: 'ephemeral', ttl: '1h', scope: 'org' });
    assert.deepEqual(buildAnthropicCacheControl(undefined, 'global'), { type: 'ephemeral', scope: 'global' });
  });

  it('defaults scope from the env knob', () => {
    process.env[PROMPT_CACHE_SCOPE_ENV] = 'global';
    assert.deepEqual(buildAnthropicCacheControl('5m'), { type: 'ephemeral', scope: 'global' });
  });
});

describe('computeAnthropicBetas scope beta', () => {
  it('adds the scope beta only when a scope is active', () => {
    delete process.env[PROMPT_CACHE_SCOPE_ENV];
    assert.equal(computeAnthropicBetas('gpt-5').includes(PROMPT_CACHING_SCOPE_BETA), false);

    process.env[PROMPT_CACHE_SCOPE_ENV] = 'org';
    assert.equal(computeAnthropicBetas('gpt-5').includes(PROMPT_CACHING_SCOPE_BETA), true);
  });

  it('respects the disableBetas kill switch', () => {
    process.env[PROMPT_CACHE_SCOPE_ENV] = 'global';
    assert.deepEqual(computeAnthropicBetas('gpt-5', { disableBetas: true }), []);
  });
});
