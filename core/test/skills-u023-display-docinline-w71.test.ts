/**
 * U023 (w71) — pure core building blocks added to skills.ts:
 *   - agent-listing display affordances: agentMemoryScopeDisplay,
 *     formatAgentMemoryDimension, formatAgentSkillsList
 *   - relevance-filtered doc inlining: getFilesForLanguage, buildInlineReference
 *   - required-MCP-server readiness classification: requiredMcpServerReadiness
 *   - named positional-argument substitution (braced + bare): substituteNamedArgPlaceholders
 *
 * Run: node --import tsx --test core/core/test/skills-u023-display-docinline-w71.test.ts
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  agentMemoryScopeDisplay,
  formatAgentMemoryDimension,
  formatAgentSkillsList,
  getFilesForLanguage,
  buildInlineReference,
  requiredMcpServerReadiness,
  substituteNamedArgPlaceholders,
  contentReferencesArgName,
} from '../src/skills';

describe('agentMemoryScopeDisplay', () => {
  it('maps each scope to a descriptive label', () => {
    assert.match(agentMemoryScopeDisplay('user')!, /User/);
    assert.match(agentMemoryScopeDisplay('project')!, /Project/);
    assert.match(agentMemoryScopeDisplay('local')!, /Local/);
  });
  it('returns undefined for unset / unknown scope', () => {
    assert.equal(agentMemoryScopeDisplay(undefined), undefined);
    // @ts-expect-error — exercise the unknown-value branch
    assert.equal(agentMemoryScopeDisplay('cloud'), undefined);
  });
});

describe('formatAgentMemoryDimension', () => {
  it('renders "<scope> memory" when a valid scope is declared', () => {
    assert.equal(formatAgentMemoryDimension({ memory: 'project' }), 'project memory');
    assert.equal(formatAgentMemoryDimension({ memory: 'user' }), 'user memory');
  });
  it('returns empty string for no / invalid scope', () => {
    assert.equal(formatAgentMemoryDimension({}), '');
    assert.equal(formatAgentMemoryDimension({ memory: undefined }), '');
    // @ts-expect-error — garbled scope must not surface a dimension
    assert.equal(formatAgentMemoryDimension({ memory: 'bogus' }), '');
  });
});

describe('formatAgentSkillsList', () => {
  it('joins up to 10 skills with a comma', () => {
    assert.equal(formatAgentSkillsList(['a', 'b', 'c']), 'a, b, c');
    const ten = Array.from({ length: 10 }, (_v, i) => `s${i}`);
    assert.equal(formatAgentSkillsList(ten), ten.join(', '));
  });
  it('collapses more than 10 skills to a count', () => {
    const eleven = Array.from({ length: 11 }, (_v, i) => `s${i}`);
    assert.equal(formatAgentSkillsList(eleven), '11 skills');
  });
  it('returns empty string for undefined / empty', () => {
    assert.equal(formatAgentSkillsList(undefined), '');
    assert.equal(formatAgentSkillsList([]), '');
  });
});

describe('getFilesForLanguage', () => {
  const files = {
    'python/quickstart.md': 'py',
    'python/advanced.md': 'py2',
    'typescript/quickstart.md': 'ts',
    'shared/overview.md': 'sh',
    'other.md': 'x',
  };
  it('selects language-prefixed and shared docs only', () => {
    const got = getFilesForLanguage(files, 'python').sort();
    assert.deepEqual(got, ['python/advanced.md', 'python/quickstart.md', 'shared/overview.md']);
  });
  it('trims the language and keeps shared when no lang match', () => {
    const got = getFilesForLanguage(files, '  ruby  ').sort();
    assert.deepEqual(got, ['shared/overview.md']);
  });
  it('returns [] for missing files or blank language', () => {
    assert.deepEqual(getFilesForLanguage(undefined, 'python'), []);
    assert.deepEqual(getFilesForLanguage(files, ''), []);
    assert.deepEqual(getFilesForLanguage(files, '   '), []);
    // @ts-expect-error — non-string language
    assert.deepEqual(getFilesForLanguage(files, 42), []);
  });
});

describe('buildInlineReference', () => {
  const files = {
    'python/b.md': 'B body',
    'python/a.md': '<!-- hidden -->A body with {{name}}',
    'shared/c.md': '  padded  ',
  };
  it('sorts keys, strips comments, applies vars, trims, wraps in <doc> tags', () => {
    const out = buildInlineReference(files, ['python/b.md', 'python/a.md'], { name: 'AMI' });
    // deterministic sort → a before b
    assert.ok(out.indexOf('python/a.md') < out.indexOf('python/b.md'), out);
    assert.ok(out.includes('<doc path="python/a.md">\nA body with AMI\n</doc>'), out);
    assert.ok(out.includes('<doc path="python/b.md">\nB body\n</doc>'), out);
    assert.ok(!out.includes('hidden'), 'HTML comment must be stripped');
  });
  it('skips requested keys with no matching file (no empty tag)', () => {
    const out = buildInlineReference(files, ['shared/c.md', 'missing/x.md']);
    assert.ok(out.includes('<doc path="shared/c.md">\npadded\n</doc>'), out);
    assert.ok(!out.includes('missing/x.md'), out);
  });
  it('returns "" for no files or no resolved keys', () => {
    assert.equal(buildInlineReference(undefined, ['a']), '');
    assert.equal(buildInlineReference(files, ['none']), '');
  });
  it('leaves unknown {{tokens}} intact when no vars supplied', () => {
    const out = buildInlineReference({ 'shared/c.md': 'x {{name}} y' }, ['shared/c.md']);
    assert.ok(out.includes('x {{name}} y'), out);
  });
});

describe('requiredMcpServerReadiness', () => {
  it('is immediately ready with no requirements', () => {
    assert.deepEqual(requiredMcpServerReadiness(undefined, {}), {
      ready: true, pending: [], failed: [], missing: [],
    });
    assert.deepEqual(requiredMcpServerReadiness([], { kali: 'connected' }).ready, true);
  });
  it('is ready when every required server is connected (substring, case-insensitive)', () => {
    const r = requiredMcpServerReadiness(['kali'], { 'Kali-Linux': 'connected' });
    assert.equal(r.ready, true);
  });
  it('reports pending while a required server is still connecting', () => {
    const r = requiredMcpServerReadiness(['kali'], { kali: 'pending' });
    assert.deepEqual(r, { ready: false, pending: ['kali'], failed: [], missing: [] });
  });
  it('reports failed when every matching server failed', () => {
    const r = requiredMcpServerReadiness(['kali'], { kali: 'failed' });
    assert.deepEqual(r, { ready: false, pending: [], failed: ['kali'], missing: [] });
  });
  it('a single connected match among failures still satisfies', () => {
    const r = requiredMcpServerReadiness(['svc'], { 'svc-a': 'failed', 'svc-b': 'connected' });
    assert.equal(r.ready, true);
  });
  it('reports missing when no server matches', () => {
    const r = requiredMcpServerReadiness(['nope'], { kali: 'connected' });
    assert.deepEqual(r, { ready: false, pending: [], failed: [], missing: ['nope'] });
  });
  it('classifies a mix across requirements', () => {
    const r = requiredMcpServerReadiness(['a', 'b', 'c', 'd'], {
      a: 'connected', b: 'pending', c: 'failed',
    });
    assert.equal(r.ready, false);
    assert.deepEqual(r.pending, ['b']);
    assert.deepEqual(r.failed, ['c']);
    assert.deepEqual(r.missing, ['d']);
  });
});

describe('substituteNamedArgPlaceholders', () => {
  it('replaces both braced ${name} and bare $name', () => {
    const out = substituteNamedArgPlaceholders(
      'edit ${file} in $mode mode', ['file', 'mode'], ['app.ts', 'strict'],
    );
    assert.equal(out, 'edit app.ts in strict mode');
  });
  it('bare $name respects the word-boundary guard ($namexyz / $name[0] untouched)', () => {
    const out = substituteNamedArgPlaceholders(
      '$file $filename $file[0]', ['file'], ['app.ts'],
    );
    assert.equal(out, 'app.ts $filename $file[0]');
  });
  it('a declared name with no supplied token resolves to empty (never literal)', () => {
    assert.equal(substituteNamedArgPlaceholders('[$a][${b}]', ['a', 'b'], ['x']), '[x][]');
  });
  it('skips names the caller binds explicitly', () => {
    const out = substituteNamedArgPlaceholders('$a $b', ['a', 'b'], ['1', '2'], ['a']);
    assert.equal(out, '$a 2');
  });
  it('escapes regex metacharacters in a name', () => {
    assert.equal(substituteNamedArgPlaceholders('${a.b}', ['a.b'], ['v']), 'v');
  });
  it('returns content unchanged with no declared names', () => {
    assert.equal(substituteNamedArgPlaceholders('$x', undefined, []), '$x');
    assert.equal(substituteNamedArgPlaceholders('$x', [], ['v']), '$x');
  });
});

describe('contentReferencesArgName', () => {
  it('detects both braced ${name} and bare $name references', () => {
    assert.equal(contentReferencesArgName('use ${file}', 'file'), true);
    assert.equal(contentReferencesArgName('use $file here', 'file'), true);
  });
  it('honors the bare word-boundary guard', () => {
    assert.equal(contentReferencesArgName('$filename', 'file'), false);
    assert.equal(contentReferencesArgName('$file[0]', 'file'), false);
    // braced still matches even with adjacent text
    assert.equal(contentReferencesArgName('${file}name', 'file'), true);
  });
  it('is false when the name is absent, blank, or inputs are non-string', () => {
    assert.equal(contentReferencesArgName('nothing here', 'file'), false);
    assert.equal(contentReferencesArgName('$file', ''), false);
    // @ts-expect-error — non-string content
    assert.equal(contentReferencesArgName(undefined, 'file'), false);
  });
  it('escapes regex metacharacters in the name', () => {
    assert.equal(contentReferencesArgName('$a.b', 'a.b'), true);
    assert.equal(contentReferencesArgName('$axb', 'a.b'), false);
  });
});
