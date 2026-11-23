/**
 * The AMI_DUMP_PROMPTS JSONL keeps lines in PRODUCTION order.
 *
 * Bug: every dump line was an independent async append (mkdir + appendFile) fired
 * without waiting for the previous one, so a slower append for the request lines could
 * be overtaken by the response line — the file recorded `response` before the `init` /
 * `message` of the same request (seen as a flaky "init is undefined" under suite load).
 * Appends to one file are now serialized (serializedDumpWrite).
 *
 * Run: cd core/core && node --import tsx --test test/provider-prompt-dump-order.test.ts
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { serializedDumpWrite, withPromptDump } from '../src/provider';

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

describe('prompt-dump write ordering', () => {
  it('a slow earlier append is never overtaken by a fast later one on the same file', async () => {
    const landed: string[] = [];
    const a = serializedDumpWrite('/virtual/f.jsonl', async () => { await sleep(40); landed.push('init'); });
    const b = serializedDumpWrite('/virtual/f.jsonl', async () => { await sleep(20); landed.push('message'); });
    const c = serializedDumpWrite('/virtual/f.jsonl', async () => { landed.push('response'); });
    await Promise.all([a, b, c]);
    assert.deepEqual(landed, ['init', 'message', 'response']);
  });

  it('a failed append does not block or reorder the following ones', async () => {
    const landed: string[] = [];
    const bad = serializedDumpWrite('/virtual/g.jsonl', async () => { await sleep(10); throw new Error('disk full'); });
    const ok = serializedDumpWrite('/virtual/g.jsonl', async () => { landed.push('next'); });
    await assert.rejects(bad, /disk full/);
    await ok;
    assert.deepEqual(landed, ['next']);
  });

  it('the real on-disk dump lists init and message before the response', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-dump-order-'));
    try {
      const base = (async () => new Response(JSON.stringify({ id: 'r1' }), {
        status: 200, headers: { 'content-type': 'application/json' },
      })) as unknown as typeof globalThis.fetch;
      const wrapped = withPromptDump(base, { enabled: true, dir, sessionId: 'order', defer: (fn) => fn() });
      await wrapped('http://api' as unknown as string, {
        method: 'POST', body: JSON.stringify({ model: 'm', messages: [{ role: 'user', content: 'hi' }] }),
      });
      const file = path.join(dir, 'prompts-order.jsonl');
      for (let i = 0; i < 200 && !(fs.existsSync(file) && fs.readFileSync(file, 'utf-8').includes('"response"')); i++) await sleep(10);
      const types = fs.readFileSync(file, 'utf-8').trim().split('\n').map((l) => (JSON.parse(l) as { type: string }).type);
      assert.deepEqual(types, ['init', 'message', 'response']);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});
