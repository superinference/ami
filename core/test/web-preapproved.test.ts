import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  PREAPPROVED_HOSTS,
  isPreapprovedHost,
  matchDomainRule,
  shouldReturnRawMarkdown,
} from '../src/tools/web-preapproved';

// ---------------------------------------------------------------------------
// U447 — direct unit coverage for the preapproved-docs-host module. The module
// is the single home of the web_fetch preapproval decision (host allowlist,
// path-scoped domain-rule matching, and the raw-markdown passthrough gate); it
// previously had no dedicated test file of its own.
// ---------------------------------------------------------------------------

describe('PREAPPROVED_HOSTS table', () => {
  it('contains curated hostname-only code/doc hosts', () => {
    assert.ok(PREAPPROVED_HOSTS.has('docs.python.org'));
    assert.ok(PREAPPROVED_HOSTS.has('developer.mozilla.org'));
    assert.ok(PREAPPROVED_HOSTS.has('pkg.go.dev'));
  });
  it('contains path-scoped entries for multi-tenant hosts', () => {
    assert.ok(PREAPPROVED_HOSTS.has('github.com/anthropics'));
  });
});

describe('isPreapprovedHost', () => {
  it('matches a hostname-only entry regardless of path, case-insensitively', () => {
    assert.ok(isPreapprovedHost('docs.python.org', '/3/library/os.html'));
    assert.ok(isPreapprovedHost('DOCS.PYTHON.ORG', '/'));
  });
  it('rejects an unlisted host', () => {
    assert.equal(isPreapprovedHost('evil.example', '/'), false);
  });
  it('enforces path-segment boundaries for path-scoped entries', () => {
    assert.ok(isPreapprovedHost('github.com', '/anthropics'));
    assert.ok(isPreapprovedHost('github.com', '/anthropics/claude-code'));
    assert.equal(isPreapprovedHost('github.com', '/anthropics-evil/malware'), false);
    assert.equal(isPreapprovedHost('github.com', '/other'), false);
  });
});

describe('matchDomainRule', () => {
  it('matches a host-only rule exactly or by subdomain', () => {
    assert.ok(matchDomainRule('example.com', '/x', 'example.com'));
    assert.ok(matchDomainRule('api.example.com', '/x', 'example.com'));
    assert.equal(matchDomainRule('evil.com', '/x', 'example.com'), false);
  });
  it('matches a path-scoped rule with segment-boundary enforcement', () => {
    assert.ok(matchDomainRule('github.com', '/anthropics', 'github.com/anthropics'));
    assert.ok(matchDomainRule('github.com', '/anthropics/repo', 'github.com/anthropics'));
    assert.equal(matchDomainRule('github.com', '/anthropics-evil/malware', 'github.com/anthropics'), false);
  });
  it('fails a path-scoped rule when the host does not match', () => {
    assert.equal(matchDomainRule('gitlab.com', '/anthropics', 'github.com/anthropics'), false);
  });
});

// ---------------------------------------------------------------------------
// U447 — shouldReturnRawMarkdown: the raw-markdown passthrough gate. Mirrors
// Claude's WebFetchTool preapproved branch (isPreapprovedUrl && length ceiling);
// the decision is owned by this module so it is independently testable and has a
// single source of truth, rather than living inline in web-fetch.ts.
// ---------------------------------------------------------------------------

describe('shouldReturnRawMarkdown', () => {
  const MAX = 100_000;
  it('returns true for a preapproved host under the length ceiling', () => {
    assert.equal(shouldReturnRawMarkdown('docs.python.org', '/3/library/os.html', 10, MAX), true);
  });
  it('returns false for an unlisted host even under the ceiling', () => {
    assert.equal(shouldReturnRawMarkdown('untrusted.example', '/', 10, MAX), false);
  });
  it('returns false for a preapproved host at or over the length ceiling', () => {
    assert.equal(shouldReturnRawMarkdown('docs.python.org', '/', MAX, MAX), false);
    assert.equal(shouldReturnRawMarkdown('docs.python.org', '/', MAX + 1, MAX), false);
  });
  it('honours path-segment boundaries for path-scoped hosts', () => {
    assert.equal(shouldReturnRawMarkdown('github.com', '/anthropics/repo', 10, MAX), true);
    assert.equal(shouldReturnRawMarkdown('github.com', '/anthropics-evil', 10, MAX), false);
  });
});
