import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  AnalyticsTracker,
  normalizeModelForAnalytics,
  truncateDevVersion,
  KNOWN_ANALYTICS_MODEL_FAMILIES,
  isCodeEditingTool,
  CODE_EDITING_TOOLS,
  getLanguageForAnalytics,
  deriveCodeEditLanguage,
  sanitizeAnalyticsData,
  hashFilePath,
} from '../src/analytics';

// ---------------------------------------------------------------------------
// U024-w70: attribute cardinality-reduction normalization (Claude datadog.ts
// parity) + code-editing-tool language segmentation (permissionLogging parity).
// ---------------------------------------------------------------------------

describe('normalizeModelForAnalytics', () => {
  it('collapses a raw model id to its low-cardinality family', () => {
    assert.equal(normalizeModelForAnalytics('claude-opus-4-20250101'), 'opus');
    assert.equal(normalizeModelForAnalytics('claude-3-5-sonnet'), 'sonnet');
    assert.equal(normalizeModelForAnalytics('claude-haiku-4'), 'haiku');
    assert.equal(normalizeModelForAnalytics('gpt-4o'), 'gpt');
    assert.equal(normalizeModelForAnalytics('gemini-2.0-flash'), 'gemini');
    assert.equal(normalizeModelForAnalytics('grok-2'), 'grok');
    assert.equal(normalizeModelForAnalytics('deepseek-r1'), 'deepseek');
  });
  it('maps an unknown model id to "other"', () => {
    assert.equal(normalizeModelForAnalytics('some-private-model-x9'), 'other');
  });
  it('strips a trailing [1m] long-context marker before matching', () => {
    assert.equal(normalizeModelForAnalytics('claude-sonnet-4[1m]'), 'sonnet');
  });
  it('passes an empty string through unchanged', () => {
    assert.equal(normalizeModelForAnalytics(''), '');
  });
  it('prefers the longest family token (o1/o3/o4 not shadowed)', () => {
    // 'o1' is a known family; a bare o1 id resolves to o1 (not accidentally 'other').
    assert.equal(normalizeModelForAnalytics('o1-preview'), 'o1');
    assert.ok((KNOWN_ANALYTICS_MODEL_FAMILIES as readonly string[]).includes('opus'));
  });
});

describe('truncateDevVersion', () => {
  it('truncates a dev build version to base+date (drops timestamp+sha)', () => {
    assert.equal(
      truncateDevVersion('2.0.53-dev.20251124.t173302.sha526cc6a'),
      '2.0.53-dev.20251124',
    );
  });
  it('leaves a release version unchanged', () => {
    assert.equal(truncateDevVersion('0.7.4'), '0.7.4');
    assert.equal(truncateDevVersion('1.2.3-dev.20250101'), '1.2.3-dev.20250101');
  });
});

describe('isCodeEditingTool / CODE_EDITING_TOOLS', () => {
  it('recognizes AMI canonical + Claude-Code edit tool names', () => {
    for (const t of ['file_edit', 'notebook_edit', 'Edit', 'Write', 'MultiEdit', 'NotebookEdit']) {
      assert.equal(isCodeEditingTool(t), true, t);
      assert.ok(CODE_EDITING_TOOLS.has(t));
    }
  });
  it('rejects non-editing tools', () => {
    for (const t of ['bash', 'grep', 'file_read', 'glob', '']) {
      assert.equal(isCodeEditingTool(t), false, t);
    }
  });
});

describe('getLanguageForAnalytics', () => {
  it('maps an extension to its language', () => {
    assert.equal(getLanguageForAnalytics('/repo/src/engine.ts'), 'TypeScript');
    assert.equal(getLanguageForAnalytics('a.py'), 'Python');
    assert.equal(getLanguageForAnalytics('x/y/z.rs'), 'Rust');
    assert.equal(getLanguageForAnalytics('nb.ipynb'), 'Jupyter');
  });
  it('returns "unknown" for no / unmapped extension', () => {
    assert.equal(getLanguageForAnalytics('/etc/hosts'), 'unknown');
    assert.equal(getLanguageForAnalytics('weird.zzzzz'), 'unknown');
    assert.equal(getLanguageForAnalytics('.env'), 'unknown');
  });
});

describe('deriveCodeEditLanguage', () => {
  it('adds file_language for a code-edit tool with a path', () => {
    const out = deriveCodeEditLanguage({ tool: 'file_edit', file_path: '/a/b.ts' });
    assert.equal(out.file_language, 'TypeScript');
    assert.equal(out.file_path, '/a/b.ts'); // raw path untouched here (hashed later)
  });
  it('recognizes the toolName key (lowercased -> toolname)', () => {
    const out = deriveCodeEditLanguage({ toolName: 'notebook_edit', file_path: '/x/n.ipynb' });
    assert.equal(out.file_language, 'Jupyter');
  });
  it('is a no-op for a non-editing tool', () => {
    const data = { tool: 'bash', file_path: '/a/b.ts' };
    assert.equal(deriveCodeEditLanguage(data), data);
    assert.equal(data.hasOwnProperty('file_language'), false);
  });
  it('is a no-op when no path field is present', () => {
    const data = { tool: 'file_edit', durationMs: 5 };
    assert.equal(deriveCodeEditLanguage(data), data);
  });
  it('does not overwrite an existing file_language', () => {
    const data = { tool: 'file_edit', file_path: '/a/b.ts', file_language: 'preset' };
    assert.equal(deriveCodeEditLanguage(data), data);
    assert.equal(data.file_language, 'preset');
  });
});

describe('sanitizeAnalyticsData — cardinality normalization wiring', () => {
  it('adds a model_family companion while keeping the raw model', () => {
    const out = sanitizeAnalyticsData({ model: 'claude-opus-4-20250101' }, { env: {} });
    assert.equal(out.model, 'claude-opus-4-20250101'); // raw kept (AMI superset)
    assert.equal(out.model_family, 'opus');            // bounded dimension added
  });
  it('truncates a dev version field on egress', () => {
    const out = sanitizeAnalyticsData({ version: '2.0.53-dev.20251124.t173302.sha526cc6a' }, { env: {} });
    assert.equal(out.version, '2.0.53-dev.20251124');
  });
  it('derives file_language for an edit event while hashing the raw path', () => {
    const out = sanitizeAnalyticsData({ toolName: 'file_edit', file_path: '/secret/project/app.py' }, { env: {} });
    assert.equal(out.file_language, 'Python');
    assert.equal(out.file_path_hash, hashFilePath('/secret/project/app.py'));
    assert.equal('file_path' in out, false); // raw path never egresses
  });
  it('does not add file_language for a non-edit tool event', () => {
    const out = sanitizeAnalyticsData({ toolName: 'bash', file_path: '/a/b.ts' }, { env: {} });
    assert.equal('file_language' in out, false);
  });
});

describe('AnalyticsTracker end-to-end — egress carries bounded dimensions', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-analytics-w70-'));
  after(() => { try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* best effort */ } });

  it('keeps raw data in-memory but minimizes + enriches the on-disk JSONL', () => {
    const t = new AnalyticsTracker(tmp, { env: { NODE_ENV: 'test' } });
    t.setSessionId('s1');
    t.log('tool_call', { toolName: 'file_edit', file_path: '/home/me/proj/main.go', durationMs: 7 });
    t.log('api_call_start', { model: 'gpt-4o-mini', messageCount: 3 });

    // In-memory keeps the RAW values for faithful local introspection.
    const mem = t.getEvents();
    assert.equal(mem[0].data.file_path, '/home/me/proj/main.go');
    assert.equal(mem[1].data.model, 'gpt-4o-mini');

    // On-disk egress is minimized (path hashed) + enriched (file_language/model_family).
    const lines = fs.readFileSync(path.join(tmp, 'analytics.jsonl'), 'utf8').trim().split('\n');
    const edit = JSON.parse(lines[0]);
    assert.equal(edit.data.file_language, 'Go');
    assert.equal(edit.data.file_path_hash, hashFilePath('/home/me/proj/main.go'));
    assert.equal('file_path' in edit.data, false);
    const api = JSON.parse(lines[1]);
    assert.equal(api.data.model, 'gpt-4o-mini');     // raw kept
    assert.equal(api.data.model_family, 'gpt');      // bounded dimension
  });
});
