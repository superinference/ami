import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  scanForSecrets,
  redactSecrets,
  partitionAndSortTools,
  isMcpToolName,
  detectFileEncoding,
  encodeText,
} from '../src/tools/tool-utils';
import { toolDefinitionsToOpenAI } from '../src/provider';
import { fileEditTool } from '../src/tools/file-edit';
import type { ToolDefinition, ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// U069 / w74 — tool-utils.ts the reference parity additions:
//  - Additional curated secret-provider rules (Sentry, Grafana SA / legacy API
//    key, Slack app token, Shopify) with escaped-boundary alternation.
//  - Deterministic built-in/MCP partition-sort of the tool pool for prompt-cache
//    stability, wired into toolDefinitionsToOpenAI.
//  - Cheap leading-bytes file-encoding probe (detectFileEncoding) for write-back.
// ---------------------------------------------------------------------------

describe('additional provider secret rules (scan + redact)', () => {
  const cases: Array<{ label: string; ruleId: string; sample: string }> = [
    { label: 'Sentry User Token', ruleId: 'sentry-user-token', sample: 'sntryu_' + 'a1b2c3d4'.repeat(8) },
    { label: 'Grafana Service Account Token', ruleId: 'grafana-service-account-token', sample: 'glsa_' + 'Ab3xYz9Q'.repeat(4) + '_deadBEEF' },
    { label: 'Grafana API Key', ruleId: 'grafana-api-key', sample: 'eyJrIjoi' + 'AbCdEf12'.repeat(10) },
    { label: 'Slack App Token', ruleId: 'slack-app-token', sample: 'xapp-1-A024BE7LH-645951204405-423b5x7fz' },
    { label: 'Shopify Access Token', ruleId: 'shopify-access-token', sample: 'shpat_' + 'ab12cd34'.repeat(4) },
    { label: 'Shopify Shared Secret', ruleId: 'shopify-shared-secret', sample: 'shpss_' + 'ab12cd34'.repeat(4) },
  ];

  for (const { ruleId, sample } of cases) {
    it(`detects ${ruleId} without surfacing the secret`, () => {
      const matches = scanForSecrets(`const key = "${sample}";`);
      const ids = matches.map(m => m.ruleId);
      assert.ok(ids.includes(ruleId), `expected ${ruleId} to fire, got: ${ids.join(',')}`);
      // The matched bytes must never appear in a SecretMatch.
      for (const m of matches) {
        assert.ok(!JSON.stringify(m).includes(sample), 'secret bytes leaked into match');
      }
    });

    it(`redacts ${ruleId} while preserving surrounding text`, () => {
      const out = redactSecrets(`token=${sample}\n`);
      assert.ok(!out.includes(sample), `secret not redacted: ${out}`);
      assert.ok(out.includes('[REDACTED-SECRET]'), `no redaction marker: ${out}`);
      assert.ok(out.startsWith('token='), 'boundary text clobbered');
    });
  }

  it('treats an escaped newline as a valid trailing boundary', () => {
    // A secret embedded inside a serialized JSON string value, terminated by the
    // two-char escaped newline sequence \n — must still be detected + redacted.
    const sample = 'sntryu_' + 'a1b2c3d4'.repeat(8);
    const serialized = `{"dsn":"${sample}\\n"}`;
    assert.ok(scanForSecrets(serialized).some(m => m.ruleId === 'sentry-user-token'));
    assert.ok(!redactSecrets(serialized).includes(sample));
  });

  it('does not fire on ordinary non-secret text', () => {
    assert.deepEqual(scanForSecrets('the quick brown fox shpat jumps xapp over'), []);
  });
});

describe('partitionAndSortTools', () => {
  const t = (name: string): { name: string } => ({ name });

  it('identifies MCP tool names by the mcp__ prefix', () => {
    assert.equal(isMcpToolName('mcp__server__tool'), true);
    assert.equal(isMcpToolName('bash'), false);
    assert.equal(isMcpToolName('mcp_not_double'), false);
  });

  it('puts sorted built-ins first, then sorted MCP tools', () => {
    const input = [t('mcp__z__a'), t('grep'), t('bash'), t('mcp__a__b'), t('web_fetch')];
    const out = partitionAndSortTools(input).map(x => x.name);
    assert.deepEqual(out, ['bash', 'grep', 'web_fetch', 'mcp__a__b', 'mcp__z__a']);
  });

  it('is stable regardless of input order (cache-stable)', () => {
    const a = partitionAndSortTools([t('mcp__b__x'), t('a'), t('c')]).map(x => x.name);
    const b = partitionAndSortTools([t('c'), t('mcp__b__x'), t('a')]).map(x => x.name);
    assert.deepEqual(a, b);
  });

  it('de-duplicates by name, first occurrence wins, and does not mutate input', () => {
    const input = [t('dup'), t('dup'), t('aaa')];
    const out = partitionAndSortTools(input).map(x => x.name);
    assert.deepEqual(out, ['aaa', 'dup']);
    assert.equal(input.length, 3);
  });

  it('orders the provider tool schema deterministically via toolDefinitionsToOpenAI', () => {
    const def = (name: string): ToolDefinition => ({
      name,
      description: `desc ${name}`,
      inputSchema: { type: 'object', properties: {} },
      isReadOnly: true,
      execute: async () => ({ output: '' }),
    });
    const tools = [def('mcp__srv__run'), def('write_file'), def('bash'), def('mcp__aaa__go')];
    const names = toolDefinitionsToOpenAI(tools).map(s => s.function.name);
    assert.deepEqual(names, ['bash', 'write_file', 'mcp__aaa__go', 'mcp__srv__run']);
  });
});

describe('detectFileEncoding (leading-bytes probe)', () => {
  let dir: string;
  beforeEach(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-enc-')); });
  afterEach(() => { fs.rmSync(dir, { recursive: true, force: true }); });

  it('returns utf-8 for a plain file', () => {
    const f = path.join(dir, 'plain.txt');
    fs.writeFileSync(f, 'hello world, no BOM here');
    assert.equal(detectFileEncoding(f), 'utf-8');
  });

  it('detects a UTF-8 BOM', () => {
    const f = path.join(dir, 'bom.txt');
    fs.writeFileSync(f, encodeText('content', 'utf-8-bom'));
    assert.equal(detectFileEncoding(f), 'utf-8-bom');
  });

  it('detects UTF-16LE and UTF-16BE BOMs', () => {
    const le = path.join(dir, 'le.txt');
    fs.writeFileSync(le, encodeText('x', 'utf-16le'));
    assert.equal(detectFileEncoding(le), 'utf-16le');
    const be = path.join(dir, 'be.txt');
    fs.writeFileSync(be, encodeText('x', 'utf-16be'));
    assert.equal(detectFileEncoding(be), 'utf-16be');
  });

  it('defaults to utf-8 for an absent file (no throw)', () => {
    assert.equal(detectFileEncoding(path.join(dir, 'nope.txt')), 'utf-8');
  });

  it('defaults to utf-8 for an empty file', () => {
    const f = path.join(dir, 'empty.txt');
    fs.writeFileSync(f, '');
    assert.equal(detectFileEncoding(f), 'utf-8');
  });

  it('does not read the whole file — only the leading bytes', () => {
    // A large BOM-prefixed file must still be classified correctly without a
    // full read (behavior-preserving vs the old whole-file re-read).
    const f = path.join(dir, 'big.txt');
    fs.writeFileSync(f, encodeText('a'.repeat(5_000_000), 'utf-8-bom'));
    assert.equal(detectFileEncoding(f), 'utf-8-bom');
  });
});

describe('file_edit preserves on-disk encoding end-to-end (detectFileEncoding wiring)', () => {
  let dir: string;
  beforeEach(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-edit-enc-')); });
  afterEach(() => { fs.rmSync(dir, { recursive: true, force: true }); });

  const ctx = (): ToolContext => ({ cwd: dir, abortSignal: new AbortController().signal });

  it('keeps a UTF-16LE BOM after an edit (no whole-file re-read regression)', async () => {
    const f = path.join(dir, 'u16.txt');
    fs.writeFileSync(f, encodeText('hello world', 'utf-16le'));
    const res = await fileEditTool.execute(
      { file_path: f, old_string: 'world', new_string: 'there' },
      ctx(),
    );
    assert.equal(res.isError, undefined, res.output);
    const raw = fs.readFileSync(f);
    // BOM bytes intact and content re-encoded as UTF-16LE.
    assert.equal(detectFileEncoding(f), 'utf-16le');
    assert.equal(raw[0], 0xFF);
    assert.equal(raw[1], 0xFE);
    assert.equal(raw.subarray(2).toString('utf16le'), 'hello there');
  });

  it('keeps a UTF-8 BOM after an edit', async () => {
    const f = path.join(dir, 'u8bom.txt');
    fs.writeFileSync(f, encodeText('alpha beta', 'utf-8-bom'));
    const res = await fileEditTool.execute(
      { file_path: f, old_string: 'beta', new_string: 'gamma' },
      ctx(),
    );
    assert.equal(res.isError, undefined, res.output);
    assert.equal(detectFileEncoding(f), 'utf-8-bom');
    const raw = fs.readFileSync(f);
    assert.equal(raw.subarray(3).toString('utf-8'), 'alpha gamma');
  });
});
