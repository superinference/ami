/**
 * U027 parity: LSP-plugin recommendation matching pipeline + response/anti-nag
 * state machine (Claude Code's utils/plugins/lspRecommendation.ts
 * `getMatchingLspPlugins`, components/LspRecommendation/LspRecommendationMenu.tsx
 * response schema, and hooks/useLspPluginRecommendation.tsx session gating).
 *
 * Covers:
 *  - extension match (case-insensitive) + no-extension early return;
 *  - global-disable gate (explicit off-switch AND ignored-count threshold);
 *  - never-suggest list, already-installed, and binary-gate filtering;
 *  - dedup by pluginId (first wins) and official-first stable ordering;
 *  - getTopLspRecommendation = official-prioritized matches[0];
 *  - applyLspRecommendationResponse for yes/no/never/disable (pure, no mutation);
 *  - formatLspRecommendationHint wording;
 *  - LspRecommendationSession: per-file dedup + once-per-session cap + reset;
 *  - getSymbolAtPosition1Based 1-based→0-based conversion + guards.
 */
import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  getMatchingLspPlugins,
  getTopLspRecommendation,
  applyLspRecommendationResponse,
  formatLspRecommendationHint,
  lspFileExtension,
  LspRecommendationSession,
  type LspPluginCandidate,
  type LspRecommendationConfig,
} from '../src/lsp/recommendation';
import { MAX_LSP_RECOMMENDATION_IGNORED_COUNT } from '../src/types';
import { getSymbolAtPosition1Based } from '../src/lsp/symbol-context';

// Binary-presence stub: pretend every listed command is installed.
const installed = (names: string[]) => (cmd: string) => names.includes(cmd);

function candidate(over: Partial<LspPluginCandidate> = {}): LspPluginCandidate {
  return {
    pluginId: 'rust@official',
    pluginName: 'rust',
    marketplaceName: 'official',
    description: 'Rust language support',
    isOfficial: true,
    extensions: ['.rs'],
    command: 'rust-analyzer',
    ...over,
  };
}

describe('U027 LSP recommendation — lspFileExtension', () => {
  it('lowercases the extension and returns "" when absent', () => {
    assert.equal(lspFileExtension('/a/b/Main.RS'), '.rs');
    assert.equal(lspFileExtension('/a/b/README'), '');
    assert.equal(lspFileExtension(''), '');
    assert.equal(lspFileExtension(undefined as unknown as string), '');
  });
});

describe('U027 LSP recommendation — getMatchingLspPlugins', () => {
  const opts = { isInstalled: installed(['rust-analyzer', 'gopls']) };

  it('matches on a case-insensitive extension', () => {
    const recs = getMatchingLspPlugins('/x/lib.RS', [candidate()], opts);
    assert.equal(recs.length, 1);
    assert.equal(recs[0].pluginId, 'rust@official');
    assert.equal(recs[0].fileExtension, '.rs');
    assert.deepEqual(recs[0].extensions, ['.rs']);
  });

  it('returns [] when the file has no extension', () => {
    assert.deepEqual(getMatchingLspPlugins('/x/Makefile', [candidate()], opts), []);
  });

  it('returns [] when recommendations are globally disabled', () => {
    const config: LspRecommendationConfig = { lspRecommendationDisabled: true };
    assert.deepEqual(getMatchingLspPlugins('/x/a.rs', [candidate()], { ...opts, config }), []);
  });

  it('returns [] once the ignored-count threshold is reached', () => {
    const config: LspRecommendationConfig = {
      lspRecommendationIgnoredCount: MAX_LSP_RECOMMENDATION_IGNORED_COUNT,
    };
    assert.deepEqual(getMatchingLspPlugins('/x/a.rs', [candidate()], { ...opts, config }), []);
  });

  it('skips a plugin on the never-suggest list', () => {
    const config: LspRecommendationConfig = { lspRecommendationNeverPlugins: ['rust@official'] };
    assert.deepEqual(getMatchingLspPlugins('/x/a.rs', [candidate()], { ...opts, config }), []);
  });

  it('skips an already-installed plugin', () => {
    assert.deepEqual(
      getMatchingLspPlugins('/x/a.rs', [candidate({ installed: true })], opts),
      [],
    );
  });

  it('skips a plugin whose server binary is not installed', () => {
    const recs = getMatchingLspPlugins('/x/a.rs', [candidate({ command: 'absent-ls' })], opts);
    assert.deepEqual(recs, []);
  });

  it('dedups by pluginId, keeping the first entry', () => {
    const dup = [candidate({ description: 'first' }), candidate({ description: 'second' })];
    const recs = getMatchingLspPlugins('/x/a.rs', dup, opts);
    assert.equal(recs.length, 1);
    assert.equal(recs[0].description, 'first');
  });

  it('orders official marketplaces first (stable for ties)', () => {
    const community = candidate({
      pluginId: 'rust@community',
      marketplaceName: 'community',
      isOfficial: false,
    });
    const official = candidate();
    // Pass community FIRST — official must still sort ahead of it.
    const recs = getMatchingLspPlugins('/x/a.rs', [community, official], opts);
    assert.deepEqual(recs.map((r) => r.pluginId), ['rust@official', 'rust@community']);
  });

  it('defaults the binary probe to the real isBinaryInstalled when none is injected', () => {
    // No isInstalled override → uses utils/which; a nonsense binary is not present,
    // so the pipeline filters it out without throwing.
    const recs = getMatchingLspPlugins('/x/a.rs', [candidate({ command: 'definitely-not-a-real-binary-xyz' })]);
    assert.deepEqual(recs, []);
  });
});

describe('U027 LSP recommendation — getTopLspRecommendation', () => {
  const opts = { isInstalled: installed(['rust-analyzer']) };
  it('returns the official-prioritized matches[0]', () => {
    const community = candidate({ pluginId: 'rust@community', isOfficial: false });
    const official = candidate();
    const top = getTopLspRecommendation('/x/a.rs', [community, official], opts);
    assert.equal(top?.pluginId, 'rust@official');
  });
  it('returns null when nothing matches', () => {
    assert.equal(getTopLspRecommendation('/x/a.go', [candidate()], opts), null);
  });
});

describe('U027 LSP recommendation — applyLspRecommendationResponse', () => {
  it('"yes" leaves gating untouched and does not mutate input', () => {
    const config: LspRecommendationConfig = { lspRecommendationIgnoredCount: 2 };
    const next = applyLspRecommendationResponse(config, 'yes', 'rust@official');
    assert.deepEqual(next, { lspRecommendationIgnoredCount: 2 });
    assert.notEqual(next, config);
  });

  it('"no" increments the ignored count (from 0 when absent)', () => {
    assert.equal(applyLspRecommendationResponse({}, 'no', 'p').lspRecommendationIgnoredCount, 1);
    assert.equal(
      applyLspRecommendationResponse({ lspRecommendationIgnoredCount: 4 }, 'no', 'p')
        .lspRecommendationIgnoredCount,
      5,
    );
  });

  it('"never" appends the pluginId to the never list, deduped', () => {
    const once = applyLspRecommendationResponse({}, 'never', 'rust@official');
    assert.deepEqual(once.lspRecommendationNeverPlugins, ['rust@official']);
    const twice = applyLspRecommendationResponse(once, 'never', 'rust@official');
    assert.deepEqual(twice.lspRecommendationNeverPlugins, ['rust@official']);
    const other = applyLspRecommendationResponse(once, 'never', 'go@official');
    assert.deepEqual(other.lspRecommendationNeverPlugins, ['rust@official', 'go@official']);
  });

  it('"disable" sets the global off-switch', () => {
    assert.equal(applyLspRecommendationResponse({}, 'disable', 'p').lspRecommendationDisabled, true);
  });

  it('does not mutate the input config object', () => {
    const config: LspRecommendationConfig = { lspRecommendationNeverPlugins: ['a'] };
    applyLspRecommendationResponse(config, 'never', 'b');
    assert.deepEqual(config.lspRecommendationNeverPlugins, ['a']);
  });
});

describe('U027 LSP recommendation — formatLspRecommendationHint', () => {
  it('names the extension, plugin, and server binary', () => {
    const [rec] = getMatchingLspPlugins('/x/a.rs', [candidate()], {
      isInstalled: installed(['rust-analyzer']),
    });
    const hint = formatLspRecommendationHint(rec);
    assert.match(hint, /\.rs files/);
    assert.match(hint, /"rust"/);
    assert.match(hint, /rust-analyzer/);
    assert.match(hint, /Rust language support/);
  });

  it('omits the description clause when absent', () => {
    const [rec] = getMatchingLspPlugins('/x/a.rs', [candidate({ description: undefined })], {
      isInstalled: installed(['rust-analyzer']),
    });
    const hint = formatLspRecommendationHint(rec);
    assert.ok(hint.trim().endsWith('error checking.'));
  });
});

describe('U027 LSP recommendation — LspRecommendationSession', () => {
  it('checks each file at most once', () => {
    const s = new LspRecommendationSession();
    assert.equal(s.shouldCheckFile('/a.rs'), true);
    assert.equal(s.shouldCheckFile('/a.rs'), false);
    assert.equal(s.shouldCheckFile('/b.rs'), true);
  });

  it('enforces the once-per-session cap via canShow/markShown', () => {
    const s = new LspRecommendationSession();
    assert.equal(s.canShow({}), true);
    assert.equal(s.hasShownThisSession, false);
    s.markShown();
    assert.equal(s.hasShownThisSession, true);
    assert.equal(s.canShow({}), false);
  });

  it('canShow is false when recommendations are disabled', () => {
    const s = new LspRecommendationSession();
    assert.equal(s.canShow({ lspRecommendationDisabled: true }), false);
    assert.equal(
      s.canShow({ lspRecommendationIgnoredCount: MAX_LSP_RECOMMENDATION_IGNORED_COUNT }),
      false,
    );
  });

  it('reset clears both the checked-file set and the shown flag', () => {
    const s = new LspRecommendationSession();
    s.shouldCheckFile('/a.rs');
    s.markShown();
    s.reset();
    assert.equal(s.hasShownThisSession, false);
    assert.equal(s.shouldCheckFile('/a.rs'), true);
  });
});

describe('U027 — getSymbolAtPosition1Based', () => {
  let dir: string;
  let sample: string;
  before(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lsp-sym1based-'));
    sample = path.join(dir, 'sample.ts');
    // Line 1 (1-based): const fooBar = 1 + baz;
    fs.writeFileSync(sample, 'const fooBar = 1 + baz;\n');
  });
  after(() => {
    try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* ignore */ }
  });

  it('converts 1-based (line,char) to 0-based and resolves the symbol', () => {
    // "fooBar" starts at 0-based column 6 → 1-based column 7.
    assert.equal(getSymbolAtPosition1Based(sample, 1, 7), 'fooBar');
  });

  it('returns null for non-positive or non-integer coordinates', () => {
    assert.equal(getSymbolAtPosition1Based(sample, 0, 7), null);
    assert.equal(getSymbolAtPosition1Based(sample, 1, 0), null);
    assert.equal(getSymbolAtPosition1Based(sample, 1.5, 7), null);
    assert.equal(getSymbolAtPosition1Based(sample, 1, 2.2), null);
  });

  it('returns null when the position is out of range', () => {
    assert.equal(getSymbolAtPosition1Based(sample, 99, 1), null);
  });
});
