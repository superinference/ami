import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  writeMemoryTool,
  SAVE_ACKNOWLEDGEMENTS,
  pickSaveAcknowledgement,
} from '../src/tools/write-memory';
import type { ToolContext } from '../src/types';

let tmp: string;
const ctx = () => ({ cwd: tmp, abortSignal: new AbortController().signal } as unknown as ToolContext);

beforeEach(() => { tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'write-mem-w99-')); });
afterEach(() => { fs.rmSync(tmp, { recursive: true, force: true }); });

describe('pickSaveAcknowledgement — warm, varied save wording (gap: UserMemoryInputMessage sample)', () => {
  it('exposes the exact warm acknowledgement set Claude samples', () => {
    assert.deepEqual([...SAVE_ACKNOWLEDGEMENTS], ['Got it.', 'Good to know.', 'Noted.']);
  });

  it('maps the low end of the rng range to the first phrase', () => {
    assert.equal(pickSaveAcknowledgement(() => 0), 'Got it.');
  });

  it('maps the middle of the rng range to the middle phrase', () => {
    assert.equal(pickSaveAcknowledgement(() => 0.5), 'Good to know.');
  });

  it('maps the high end of the rng range to the last phrase', () => {
    assert.equal(pickSaveAcknowledgement(() => 0.99), 'Noted.');
  });

  it('clamps an rng returning exactly 1 to the last valid index (no overflow)', () => {
    assert.equal(pickSaveAcknowledgement(() => 1), 'Noted.');
  });

  it('defaults to Math.random and always returns a member of the set', () => {
    for (let i = 0; i < 50; i++) {
      assert.ok(
        (SAVE_ACKNOWLEDGEMENTS as readonly string[]).includes(pickSaveAcknowledgement()),
        'default picker returns a known warm phrase',
      );
    }
  });
});

describe('write_memory — success output carries a warm acknowledgement tail', () => {
  it('appends one of the warm phrases after the "/memory" hint', async () => {
    const r = await writeMemoryTool.execute({ name: 'warm', content: 'hi' }, ctx());
    assert.notEqual(r.isError, true);
    const out = String(r.output);
    // The existing confirmation (count + /memory hint) is preserved...
    assert.match(out, /You now have 1 private memory\. View with \/memory\. /);
    // ...and a warm acknowledgement is now the final sentence.
    const tail = out.slice(out.indexOf('View with /memory. ') + 'View with /memory. '.length);
    assert.ok(
      (SAVE_ACKNOWLEDGEMENTS as readonly string[]).includes(tail),
      `expected a warm acknowledgement tail, got: ${JSON.stringify(tail)}`,
    );
  });

  it('keeps the save verb at the start of the output (does not prepend the warmth)', async () => {
    const r = await writeMemoryTool.execute({ name: 'ordered', content: 'x' }, ctx());
    assert.match(String(r.output), /^Saved memory "ordered"/);
  });

  it('a non-error save result is never flagged isError just because of the tail', async () => {
    const r = await writeMemoryTool.execute({ name: 'ok', content: 'y' }, ctx());
    assert.notEqual(r.isError, true);
  });
});
