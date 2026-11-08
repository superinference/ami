import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { executeWorkflow, workflowAgentCacheKey, DiskWorkflowCache } from '../src/tools/workflow-runtime';
import type { WorkflowAgentCache } from '../src/tools/workflow-runtime';

// B2: an opt-in agent-result cache lets a re-run/resume skip agents that already
// completed, instead of re-invoking the handler for the same (prompt, opts).

describe('workflowAgentCacheKey (B2)', () => {
  it('is stable for identical prompt + opts', () => {
    assert.equal(workflowAgentCacheKey('p', { model: 'm' }), workflowAgentCacheKey('p', { model: 'm' }));
  });
  it('differs by prompt and by semantic opts', () => {
    assert.notEqual(workflowAgentCacheKey('p1'), workflowAgentCacheKey('p2'));
    assert.notEqual(workflowAgentCacheKey('p', { model: 'a' }), workflowAgentCacheKey('p', { model: 'b' }));
    assert.notEqual(workflowAgentCacheKey('p', { agentType: 'x' }), workflowAgentCacheKey('p'));
  });
  it('ignores display-only opts (label/phase)', () => {
    assert.equal(
      workflowAgentCacheKey('p', { model: 'm' } as never),
      workflowAgentCacheKey('p', { model: 'm', label: 'A', phase: 'P' } as never),
    );
  });
});

describe('DiskWorkflowCache (B2)', () => {
  const dirs: string[] = [];
  afterEach(() => { for (const d of dirs.splice(0)) fs.rmSync(d, { recursive: true, force: true }); });
  it('round-trips a value; a miss returns undefined', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wfc-')); dirs.push(dir);
    const c = new DiskWorkflowCache(dir);
    assert.equal(c.get('k'), undefined);
    c.set('k', { a: 1, s: 'x' });
    assert.deepEqual(c.get('k'), { a: 1, s: 'x' });
  });
});

describe('executeWorkflow agent cache (B2)', () => {
  const script = `
export const meta = { name: 'c', description: 'd' }
const a = await agent('fixed prompt', { model: 'm' })
return a
`;

  function memCache(): { cache: WorkflowAgentCache; map: Map<string, unknown> } {
    const map = new Map<string, unknown>();
    return { map, cache: { get: (k) => map.get(k), set: (k, v) => { map.set(k, v); } } };
  }

  it('re-run with the same cache skips the handler for identical (prompt, opts)', async () => {
    const { cache } = memCache();
    let calls = 0;
    const handler = async () => { calls++; return `result-${calls}`; };
    const r1 = await executeWorkflow(script, handler, { cache });
    const r2 = await executeWorkflow(script, handler, { cache });
    assert.equal(calls, 1, 'handler must run only once across the two runs');
    assert.equal(r1.result, 'result-1');
    assert.equal(r2.result, 'result-1'); // cached replay
  });

  it('without a cache, both runs invoke the handler (default behavior unchanged)', async () => {
    let calls = 0;
    const handler = async () => { calls++; return 'x'; };
    await executeWorkflow(script, handler);
    await executeWorkflow(script, handler);
    assert.equal(calls, 2);
  });

  it('does not cache a null/failed result (it re-runs)', async () => {
    const { cache } = memCache();
    const failScript = `export const meta = { name: 'c', description: 'd' }\nreturn await agent('p', {})`;
    let calls = 0;
    const handler = async () => { calls++; throw new Error('boom'); }; // agent() swallows -> null
    await executeWorkflow(failScript, handler, { cache });
    await executeWorkflow(failScript, handler, { cache });
    assert.equal(calls, 2, 'a null result must not be cached');
  });

  it('is durable across runtimes via DiskWorkflowCache', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wfc-disk-'));
    try {
      let calls = 0;
      const handler = async () => { calls++; return `r${calls}`; };
      await executeWorkflow(script, handler, { cache: new DiskWorkflowCache(dir) });
      await executeWorkflow(script, handler, { cache: new DiskWorkflowCache(dir) }); // fresh instance, same dir
      assert.equal(calls, 1);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

// Functional: the workflow TOOL wires resumeKey -> DiskWorkflowCache (B2).
import { workflowTool } from '../src/tools/workflow';

describe('workflowTool resumeKey wiring (B2)', () => {
  const dirs: string[] = [];
  afterEach(() => { for (const d of dirs.splice(0)) fs.rmSync(d, { recursive: true, force: true }); });
  const mkcwd = () => { const d = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wftool-')); dirs.push(d); return d; };
  const script = `export const meta = { name: 'c', description: 'd' }\nreturn await agent('p', {})`;

  it('creates a resume cache under .cache/<resumeKey> when resumeKey is given', async () => {
    const cwd = mkcwd();
    const r = await workflowTool.execute!({ script, resumeKey: 'my-run' }, { cwd } as never);
    assert.equal(r.isError, false);
    const cacheDir = path.join(cwd, '.superinference', 'workflows', '.cache', 'my-run');
    assert.ok(fs.existsSync(cacheDir), 'resume cache dir must be created');
    assert.ok(fs.readdirSync(cacheDir).length >= 1, 'a completed agent result must be cached');
  });

  it('sanitizes the resumeKey to block path traversal', async () => {
    const cwd = mkcwd();
    await workflowTool.execute!({ script, resumeKey: '../../etc/evil' }, { cwd } as never);
    // nothing escaped the .cache dir; the sanitized segment stays contained
    const base = path.join(cwd, '.superinference', 'workflows', '.cache');
    assert.ok(fs.existsSync(base));
    for (const entry of fs.readdirSync(base)) assert.ok(!entry.includes('..') && !entry.includes('/'));
  });

  it('creates no cache when resumeKey is absent', async () => {
    const cwd = mkcwd();
    await workflowTool.execute!({ script }, { cwd } as never);
    assert.ok(!fs.existsSync(path.join(cwd, '.superinference', 'workflows', '.cache')));
  });
});
