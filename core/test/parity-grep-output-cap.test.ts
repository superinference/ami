import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { runSearch, grepHooks, MAX_SEARCH_OUTPUT_BYTES } from '../src/tools/grep';
import type { ToolContext } from '../src/types';

function makeChild() {
  const child: any = new EventEmitter();
  child.stdout = new EventEmitter();
  child.stderr = new EventEmitter();
  child.killCalls = [];
  child.kill = (signal?: string) => { child.killCalls.push(signal); return true; };
  return child;
}

function ctx(): ToolContext {
  return { cwd: process.cwd(), abortSignal: new AbortController().signal };
}

const savedGrace = grepHooks.killGraceMs;
afterEach(() => { mock.restoreAll(); grepHooks.killGraceMs = savedGrace; });

describe('runSearch – bounded stdout accumulation (DoS cap)', () => {
  it('cuts the search short, SIGKILLs the child, and flags truncation past the byte cap', async () => {
    const child = makeChild();
    mock.method(grepHooks, 'spawn', () => child as any);
    grepHooks.killGraceMs = 5;

    const p = runSearch(['x', '/tmp'], 'rg', ctx());
    // Emit a chunk larger than the cap in one go.
    child.stdout.emit('data', Buffer.alloc(MAX_SEARCH_OUTPUT_BYTES + 1024, 0x61));
    const outcome = await p;

    assert.equal(outcome.outputTruncated, true);
    assert.ok(outcome.output && outcome.output.length > 0);
    assert.deepEqual(child.killCalls, ['SIGKILL']);
  });

  it('does not flag truncation for small output', async () => {
    const child = makeChild();
    mock.method(grepHooks, 'spawn', () => child as any);
    const p = runSearch(['x', '/tmp'], 'rg', ctx());
    child.stdout.emit('data', Buffer.from('a.ts\nb.ts\n'));
    child.emit('close', 0, null);
    const outcome = await p;
    assert.notEqual(outcome.outputTruncated, true);
    assert.equal(outcome.output, 'a.ts\nb.ts\n');
  });
});
