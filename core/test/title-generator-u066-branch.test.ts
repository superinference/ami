import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  generateBranchName,
  sanitizeBranchSlug,
  parseBranchJson,
  parseTitleJson,
  BRANCH_NAME_GENERATION_PROMPT,
} from '../src/title-generator';
import type { ProviderConfig, StreamChunk } from '../src/types';

const provider: ProviderConfig = { baseUrl: 'http://x/v1', apiKey: 'k', model: 'm', maxTokens: 50 };

/** Build a fake streamer that emits `text` as a single content_delta then done.
 *  Honors an already-aborted signal by emitting nothing (mirrors a cancelled call). */
function streamerOf(text: string) {
  return async function* (
    _c: ProviderConfig,
    _m: unknown[],
    _t: unknown[],
    signal: AbortSignal,
  ): AsyncGenerator<StreamChunk> {
    if (signal.aborted) { yield { type: 'done' }; return; }
    yield { type: 'content_delta', text };
    yield { type: 'done' };
  };
}

/** A streamer that throws (LLM transport failure). */
async function* throwingStreamer(): AsyncGenerator<StreamChunk> {
  throw new Error('boom');
  // eslint-disable-next-line no-unreachable
  yield { type: 'done' };
}

describe('sanitizeBranchSlug (U066)', () => {
  it('kebab-cases a free-form phrase', () => {
    assert.equal(sanitizeBranchSlug('Fix the Login Button'), 'fix-the-login-button');
  });
  it('drops a single leading vendor/namespace prefix segment', () => {
    assert.equal(sanitizeBranchSlug('claude/fix-login'), 'fix-login');
    assert.equal(sanitizeBranchSlug('feature/add-auth'), 'add-auth');
  });
  it('collapses punctuation and repeated/edge dashes', () => {
    assert.equal(sanitizeBranchSlug('  --Add,   OAuth!!-- '), 'add-oauth');
  });
  it('returns empty for junk with no alnum', () => {
    assert.equal(sanitizeBranchSlug('!!!///'), '');
    assert.equal(sanitizeBranchSlug(''), '');
  });
  it('truncates on a dash boundary within the max budget', () => {
    const out = sanitizeBranchSlug('alpha beta gamma delta epsilon zeta eta theta', 20);
    assert.ok(out.length <= 20, `len ${out.length}`);
    assert.ok(!out.endsWith('-'));
    // boundary break keeps whole words
    assert.ok(out.split('-').every(w => w.length > 0));
  });
  it('hard-cuts a single oversized word (no early boundary)', () => {
    const out = sanitizeBranchSlug('supercalifragilisticexpialidocious', 10);
    assert.equal(out, 'supercalif');
  });
});

describe('parseBranchJson (U066)', () => {
  it('reads the branch field', () => {
    assert.equal(parseBranchJson('{"branch": "fix-login"}'), 'fix-login');
  });
  it('ignores a title field (branch path only)', () => {
    assert.equal(parseBranchJson('{"title": "Fix login"}'), undefined);
  });
  it('extracts a JSON block wrapped in prose/fences', () => {
    assert.equal(parseBranchJson('here you go:\n```json\n{"branch":"add-auth"}\n```'), 'add-auth');
  });
  it('returns undefined for empty / non-JSON / empty-field', () => {
    assert.equal(parseBranchJson(''), undefined);
    assert.equal(parseBranchJson('just text'), undefined);
    assert.equal(parseBranchJson('{"branch": "  "}'), undefined);
  });
});

describe('parseTitleJson regression after shared-extractor refactor (U066)', () => {
  it('title wins over name; block extraction still works', () => {
    assert.equal(parseTitleJson('{"title":"T","name":"N"}'), 'T');
    assert.equal(parseTitleJson('{"name":"N"}'), 'N');
    assert.equal(parseTitleJson('noise {"title":"Wrapped"} tail'), 'Wrapped');
    assert.equal(parseTitleJson('[]'), undefined);
  });
});

describe('generateBranchName (U066)', () => {
  it('returns the structured kebab slug the model emitted', async () => {
    const out = await generateBranchName(
      'Fix the mobile login button', provider, undefined,
      streamerOf('{"branch": "fix-mobile-login-button"}'),
    );
    assert.equal(out, 'fix-mobile-login-button');
  });

  it('strips a vendor prefix the model prepended', async () => {
    const out = await generateBranchName(
      'add oauth', provider, undefined, streamerOf('{"branch": "claude/add-oauth"}'),
    );
    assert.equal(out, 'add-oauth');
  });

  it('sanitizes free-form (non-JSON) model output into a slug', async () => {
    const out = await generateBranchName(
      'update readme', provider, undefined, streamerOf('Update README Now'),
    );
    assert.equal(out, 'update-readme-now');
  });

  it('strips <think> blocks before parsing', async () => {
    const out = await generateBranchName(
      'debug ci', provider, undefined,
      streamerOf('<think>pondering</think>{"branch":"debug-failing-ci"}'),
    );
    assert.equal(out, 'debug-failing-ci');
  });

  it('falls back to a description-derived slug when the model yields nothing usable', async () => {
    const out = await generateBranchName(
      'Refactor the API client', provider, undefined, streamerOf('{"branch": "   "}'),
    );
    assert.equal(out, 'refactor-the-api-client');
  });

  it('falls back on a streamer throw (LLM transport failure)', async () => {
    const out = await generateBranchName(
      'Add dark mode toggle', provider, undefined, throwingStreamer,
    );
    assert.equal(out, 'add-dark-mode-toggle');
  });

  it('returns the literal "task" floor for an empty description', async () => {
    const out = await generateBranchName('   ', provider, undefined, streamerOf('{"branch":"x"}'));
    assert.equal(out, 'task');
  });

  it('short-circuits and falls back when the caller signal is already aborted', async () => {
    const ac = new AbortController();
    ac.abort();
    const out = await generateBranchName(
      'Improve data processing', provider, undefined, streamerOf('{"branch":"from-model"}'), ac.signal,
    );
    assert.equal(out, 'improve-data-processing');
  });

  it('prompt advertises a JSON branch contract (no vendor prefix leak)', () => {
    assert.match(BRANCH_NAME_GENERATION_PROMPT, /"branch"/);
    assert.match(BRANCH_NAME_GENERATION_PROMPT, /kebab-case/);
    assert.doesNotMatch(BRANCH_NAME_GENERATION_PROMPT, /claude\//);
  });
});
