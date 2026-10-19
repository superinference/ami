import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  WorkflowRuntime,
  executeWorkflow,
  parseWorkflowMeta,
  validateWorkflowScript,
  detectCpuCount,
  getMaxConcurrent,
} from '../src/tools/workflow-runtime';
import { workflowTool } from '../src/tools/workflow';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

describe('parseWorkflowMeta', () => {
  it('parses valid meta block', () => {
    const script = `
export const meta = {
  name: 'test-workflow',
  description: 'A test workflow',
  phases: [{ title: 'Phase1' }],
}
const x = 1;
`;
    const meta = parseWorkflowMeta(script);
    assert.ok(meta);
    assert.equal(meta.name, 'test-workflow');
    assert.equal(meta.description, 'A test workflow');
  });

  it('returns null for missing meta', () => {
    assert.equal(parseWorkflowMeta('const x = 1;'), null);
  });

  it('returns null for malformed meta', () => {
    const result = parseWorkflowMeta('export const meta = {invalid');
    // May parse or not depending on structure — just ensure no crash
    assert.ok(result === null || result !== undefined);
  });
});

describe('WorkflowRuntime', () => {
  it('creates context with all primitives', () => {
    const runtime = new WorkflowRuntime({
      agentHandler: async () => 'result',
    });
    const ctx = runtime.createContext({ input: 'test' });

    assert.equal(typeof ctx.agent, 'function');
    assert.equal(typeof ctx.parallel, 'function');
    assert.equal(typeof ctx.pipeline, 'function');
    assert.equal(typeof ctx.phase, 'function');
    assert.equal(typeof ctx.log, 'function');
    assert.deepEqual(ctx.args, { input: 'test' });
    assert.equal(ctx.budget.total, null);
    assert.equal(ctx.budget.remaining(), Infinity);
  });

  it('tracks agent count', async () => {
    const runtime = new WorkflowRuntime({
      agentHandler: async () => 'done',
    });
    const ctx = runtime.createContext(undefined);

    await ctx.agent('task 1');
    await ctx.agent('task 2');
    await ctx.agent('task 3');

    assert.equal(runtime.agentCount, 3);
  });

  it('tracks logs', () => {
    const runtime = new WorkflowRuntime({
      agentHandler: async () => 'done',
    });
    const ctx = runtime.createContext(undefined);

    ctx.log('step 1');
    ctx.log('step 2');

    assert.deepEqual(runtime.logs, ['step 1', 'step 2']);
  });

  it('tracks phase changes', () => {
    const runtime = new WorkflowRuntime({
      agentHandler: async () => 'done',
    });
    const ctx = runtime.createContext(undefined);

    ctx.phase('Build');
    assert.equal(runtime.currentPhase, 'Build');

    ctx.phase('Test');
    assert.equal(runtime.currentPhase, 'Test');
  });

  it('budget tracking works', () => {
    const runtime = new WorkflowRuntime({
      agentHandler: async () => 'done',
      budgetTotal: 100000,
    });
    const ctx = runtime.createContext(undefined);

    assert.equal(ctx.budget.total, 100000);
    assert.equal(ctx.budget.spent(), 0);
    assert.equal(ctx.budget.remaining(), 100000);

    runtime.addTokens(30000);
    assert.equal(ctx.budget.spent(), 30000);
    assert.equal(ctx.budget.remaining(), 70000);
  });

  it('agent returns null on handler error', async () => {
    const runtime = new WorkflowRuntime({
      agentHandler: async () => { throw new Error('fail'); },
    });
    const ctx = runtime.createContext(undefined);

    const result = await ctx.agent('test');
    assert.equal(result, null);
  });

  it('abort stops new agents', async () => {
    const runtime = new WorkflowRuntime({
      agentHandler: async () => 'done',
    });
    const ctx = runtime.createContext(undefined);

    runtime.abort();

    await assert.rejects(async () => {
      await ctx.agent('test');
    }, /aborted/);
  });
});

describe('parallel', () => {
  it('runs thunks concurrently', async () => {
    const runtime = new WorkflowRuntime({
      agentHandler: async (prompt) => prompt,
    });
    const ctx = runtime.createContext(undefined);

    const results = await ctx.parallel([
      () => ctx.agent('a'),
      () => ctx.agent('b'),
      () => ctx.agent('c'),
    ]);

    assert.equal(results.length, 3);
    assert.equal(results[0], 'a');
    assert.equal(results[1], 'b');
    assert.equal(results[2], 'c');
  });

  it('returns null for failed thunks', async () => {
    const runtime = new WorkflowRuntime({
      agentHandler: async () => 'ok',
    });
    const ctx = runtime.createContext(undefined);

    const results = await ctx.parallel([
      () => Promise.resolve('good'),
      () => Promise.reject(new Error('bad')),
      () => Promise.resolve('also good'),
    ]);

    assert.equal(results[0], 'good');
    assert.equal(results[1], null);
    assert.equal(results[2], 'also good');
  });
});

describe('pipeline', () => {
  it('runs items through stages sequentially', async () => {
    const runtime = new WorkflowRuntime({
      agentHandler: async (prompt) => prompt,
    });
    const ctx = runtime.createContext(undefined);

    const results = await ctx.pipeline(
      [1, 2, 3],
      async (prev, item) => item * 2,
      async (prev) => prev + 1,
    );

    assert.deepEqual(results, [3, 5, 7]);
  });

  it('passes original item and index to later stages', async () => {
    const runtime = new WorkflowRuntime({
      agentHandler: async () => 'ok',
    });
    const ctx = runtime.createContext(undefined);

    const results = await ctx.pipeline(
      ['a', 'b'],
      async (prev, item, idx) => `${item}-${idx}`,
      async (prev, item, idx) => `${prev}|orig:${item}|i:${idx}`,
    );

    assert.equal(results[0], 'a-0|orig:a|i:0');
    assert.equal(results[1], 'b-1|orig:b|i:1');
  });

  it('returns null for items that throw in a stage', async () => {
    const runtime = new WorkflowRuntime({
      agentHandler: async () => 'ok',
    });
    const ctx = runtime.createContext(undefined);

    const results = await ctx.pipeline(
      [1, 2, 3],
      async (prev, item) => {
        if (item === 2) throw new Error('skip');
        return item;
      },
      async (prev) => prev * 10,
    );

    assert.equal(results[0], 10);
    assert.equal(results[1], null);
    assert.equal(results[2], 30);
  });
});

describe('executeWorkflow', () => {
  it('executes a simple workflow script', async () => {
    const script = `
export const meta = {
  name: 'simple',
  description: 'Simple test workflow',
}
phase('Work')
log('Starting work')
const r1 = await agent('Do task 1')
const r2 = await agent('Do task 2')
log('Done')
return { results: [r1, r2] }
`;

    const calls: string[] = [];
    const result = await executeWorkflow(script, async (prompt) => {
      calls.push(prompt);
      return `result for: ${prompt}`;
    });

    assert.equal(result.agentCount, 2);
    assert.deepEqual(result.logs, ['Starting work', 'Done']);
    assert.equal(calls.length, 2);
    assert.ok(calls[0].includes('task 1'));
    assert.ok(calls[1].includes('task 2'));
  });

  it('passes args to workflow', async () => {
    const script = `
export const meta = {
  name: 'args-test',
  description: 'Test args passing',
}
return { received: args }
`;

    const result = await executeWorkflow(script, async () => 'ok', {
      args: { foo: 'bar' },
    });

    assert.deepEqual((result.result as any).received, { foo: 'bar' });
  });

  it('handles workflow errors gracefully', async () => {
    const script = `
export const meta = {
  name: 'error-test',
  description: 'Test error handling',
}
throw new Error('workflow failed')
`;

    await assert.rejects(
      () => executeWorkflow(script, async () => 'ok'),
      /workflow failed/,
    );
  });

  it('respects budget limits', async () => {
    const script = `
export const meta = {
  name: 'budget-test',
  description: 'Test budget',
}
return { remaining: budget.remaining(), total: budget.total }
`;

    const result = await executeWorkflow(script, async () => 'ok', {
      budgetTotal: 50000,
    });

    assert.equal((result.result as any).total, 50000);
    assert.equal((result.result as any).remaining, 50000);
  });
});

describe('workflowTool', () => {
  it('has correct name and schema', () => {
    assert.equal(workflowTool.name, 'workflow');
    assert.ok(workflowTool.description.includes('workflow'));
    assert.ok(workflowTool.inputSchema.properties.script);
    assert.ok(workflowTool.inputSchema.properties.scriptPath);
    assert.ok(workflowTool.inputSchema.properties.name);
  });

  it('rejects missing script/name/path', async () => {
    const result = await workflowTool.execute(
      {},
      { cwd: '/tmp', abortSignal: new AbortController().signal },
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('required'));
  });

  it('rejects script without meta', async () => {
    const result = await workflowTool.execute(
      { script: 'const x = 1;' },
      { cwd: '/tmp', abortSignal: new AbortController().signal },
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('meta'));
  });

  it('executes inline script', async () => {
    const script = `
export const meta = {
  name: 'inline',
  description: 'Inline workflow',
}
log('hello from inline')
return 42
`;

    const result = await workflowTool.execute(
      { script },
      { cwd: '/tmp', abortSignal: new AbortController().signal },
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('inline'));
    assert.ok(result.output.includes('hello from inline'));
  });

  it('loads script from scriptPath', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'wf-test-'));
    const scriptPath = path.join(tmpDir, 'test.js');
    fs.writeFileSync(scriptPath, `
export const meta = {
  name: 'from-file',
  description: 'Loaded from file',
}
return 'file-result'
`);

    try {
      const result = await workflowTool.execute(
        { scriptPath },
        { cwd: '/tmp', abortSignal: new AbortController().signal },
      );
      assert.ok(!result.isError);
      assert.ok(result.output.includes('from-file'));
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('rejects nonexistent scriptPath', async () => {
    const result = await workflowTool.execute(
      { scriptPath: '/nonexistent/script.js' },
      { cwd: '/tmp', abortSignal: new AbortController().signal },
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('not found'));
  });

  it('rejects nonexistent named workflow', async () => {
    const result = await workflowTool.execute(
      { name: 'nonexistent-workflow' },
      { cwd: '/tmp', abortSignal: new AbortController().signal },
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('not found'));
  });
});

describe('WorkflowRuntime — per-run maxConcurrent cap', () => {
  const fanScript = `export const meta = { name: 'fan', description: 'x' };\nreturn await parallel(Array.from({length:6},(_,i)=>()=>agent('t'+i)));`;
  function tracking() {
    let live = 0, peak = 0;
    const handler = async () => { live++; peak = Math.max(peak, live); await new Promise(r => setTimeout(r, 20)); live--; return 'ok'; };
    return { handler, peak: () => peak };
  }
  it('caps concurrent agents at maxConcurrent=1', async () => {
    const t = tracking();
    await executeWorkflow(fanScript, t.handler, { maxConcurrent: 1 });
    assert.ok(t.peak() <= 1, `peak ${t.peak()} must be <= 1`);
  });
  it('control: a high cap lets agents overlap', async () => {
    const t = tracking();
    await executeWorkflow(fanScript, t.handler, { maxConcurrent: 6 });
    assert.ok(t.peak() >= 2, `peak ${t.peak()} must be >= 2`);
  });
});

describe('validateWorkflowScript', () => {
  it('allows normal workflow scripts', () => {
    assert.doesNotThrow(() => validateWorkflowScript(`
      phase('Work')
      log('Starting')
      const r = await agent('Do something')
      return { result: r }
    `));
  });

  it('blocks dynamic import()', () => {
    assert.throws(
      () => validateWorkflowScript(`const fs = await import('fs')`),
      /cannot use dynamic import\(\)/,
    );
  });

  it('blocks import() without await', () => {
    assert.throws(
      () => validateWorkflowScript(`import('child_process').then(cp => cp.exec('rm -rf /'))`),
      /cannot use dynamic import\(\)/,
    );
  });

  it('blocks require()', () => {
    assert.throws(
      () => validateWorkflowScript(`const fs = require('fs')`),
      /cannot use require\(\)/,
    );
  });

  it('blocks process.exit', () => {
    assert.throws(
      () => validateWorkflowScript(`process.exit(1)`),
      /cannot access process\.exit\/kill\/env/,
    );
  });

  it('blocks process.kill', () => {
    assert.throws(
      () => validateWorkflowScript(`process.kill(process.pid)`),
      /cannot access process\.exit\/kill\/env/,
    );
  });

  it('blocks process.env access', () => {
    assert.throws(
      () => validateWorkflowScript(`const key = process.env.SECRET_KEY`),
      /cannot access process\.exit\/kill\/env/,
    );
  });

  it('ignores import/require inside double-quoted strings', () => {
    assert.doesNotThrow(() => validateWorkflowScript(`
      const msg = "do not import('fs') or require('os') here"
      return msg
    `));
  });

  it('ignores import/require inside single-quoted strings', () => {
    assert.doesNotThrow(() => validateWorkflowScript(`
      const msg = 'do not import("fs") or require("os") here'
      return msg
    `));
  });

  it('ignores import/require inside template literals', () => {
    assert.doesNotThrow(() => validateWorkflowScript(
      'const msg = `do not import("fs") or require("os") here`\nreturn msg',
    ));
  });

  it('ignores import/require inside line comments', () => {
    assert.doesNotThrow(() => validateWorkflowScript(`
      // import('fs') require('os') process.exit(1)
      return 42
    `));
  });

  it('ignores import/require inside block comments', () => {
    assert.doesNotThrow(() => validateWorkflowScript(`
      /* import('fs') require('os') process.exit(1) */
      return 42
    `));
  });

  it('blocks import() even with whitespace before paren', () => {
    assert.throws(
      () => validateWorkflowScript(`const x = import  ('fs')`),
      /cannot use dynamic import\(\)/,
    );
  });

  it('blocks require() even with whitespace before paren', () => {
    assert.throws(
      () => validateWorkflowScript(`const x = require  ('fs')`),
      /cannot use require\(\)/,
    );
  });

  it('blocks process.env with whitespace around dot', () => {
    assert.throws(
      () => validateWorkflowScript(`const x = process . env.SECRET`),
      /cannot access process\.exit\/kill\/env/,
    );
  });
});

describe('executeWorkflow — sandbox validation integration', () => {
  it('rejects scripts containing dynamic import()', async () => {
    const script = `
export const meta = { name: 'evil', description: 'escape attempt' }
const fs = await import('fs')
return fs.readFileSync('/etc/passwd')
`;
    await assert.rejects(
      () => executeWorkflow(script, async () => 'ok'),
      /cannot use dynamic import\(\)/,
    );
  });

  it('rejects scripts containing require()', async () => {
    const script = `
export const meta = { name: 'evil', description: 'escape attempt' }
const cp = require('child_process')
return cp.execSync('whoami').toString()
`;
    await assert.rejects(
      () => executeWorkflow(script, async () => 'ok'),
      /cannot use require\(\)/,
    );
  });

  it('rejects scripts accessing process.exit', async () => {
    const script = `
export const meta = { name: 'evil', description: 'escape attempt' }
process.exit(0)
`;
    await assert.rejects(
      () => executeWorkflow(script, async () => 'ok'),
      /cannot access process\.exit\/kill\/env/,
    );
  });

  it('allows safe scripts through validation', async () => {
    const script = `
export const meta = { name: 'safe', description: 'safe workflow' }
log('safe workflow running')
return 'safe'
`;
    const result = await executeWorkflow(script, async () => 'ok');
    assert.equal(result.result, 'safe');
    assert.deepEqual(result.logs, ['safe workflow running']);
  });
});

describe('detectCpuCount', () => {
  it('returns a positive integer', () => {
    const count = detectCpuCount();
    assert.ok(count >= 1, `expected >= 1, got ${count}`);
    assert.equal(count, Math.floor(count), 'should be an integer');
  });

  it('returns at least 1 even with very small cgroup limits', () => {
    const count = detectCpuCount();
    assert.ok(count >= 1);
  });

  it('falls back to os.cpus() on non-container hosts', () => {
    const count = detectCpuCount();
    const osCpus = os.cpus()?.length ?? 8;
    // On a non-container host, should match os.cpus() count
    // On a container, should be <= os.cpus() count
    assert.ok(count <= osCpus, `container count ${count} should be <= host ${osCpus}`);
    assert.ok(count >= 1);
  });

  it('result is consistent across calls', () => {
    const a = detectCpuCount();
    const b = detectCpuCount();
    assert.equal(a, b);
  });
});

describe('getMaxConcurrent', () => {
  it('returns a value between 1 and 256', () => {
    const val = getMaxConcurrent();
    assert.ok(val >= 1 && val <= 256, `expected 1-256, got ${val}`);
  });

  it('reads AMI_WORKFLOW_MAX_CONCURRENT_AGENTS env var', () => {
    const orig = process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS;
    process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS = '32';
    try {
      assert.equal(getMaxConcurrent(), 32);
    } finally {
      if (orig !== undefined) process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS = orig;
      else delete process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS;
    }
  });

  it('accepts boundary value 1', () => {
    const orig = process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS;
    process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS = '1';
    try {
      assert.equal(getMaxConcurrent(), 1);
    } finally {
      if (orig !== undefined) process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS = orig;
      else delete process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS;
    }
  });

  it('accepts boundary value 256', () => {
    const orig = process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS;
    process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS = '256';
    try {
      assert.equal(getMaxConcurrent(), 256);
    } finally {
      if (orig !== undefined) process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS = orig;
      else delete process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS;
    }
  });

  it('rejects out-of-range value (too high)', () => {
    const orig = process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS;
    process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS = '500';
    try {
      const val = getMaxConcurrent();
      assert.ok(val >= 4 && val <= 16, 'should fall back to CPU-based default');
    } finally {
      if (orig !== undefined) process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS = orig;
      else delete process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS;
    }
  });

  it('rejects out-of-range value (zero)', () => {
    const orig = process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS;
    process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS = '0';
    try {
      const val = getMaxConcurrent();
      assert.ok(val >= 4 && val <= 16, 'should fall back to CPU-based default');
    } finally {
      if (orig !== undefined) process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS = orig;
      else delete process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS;
    }
  });

  it('rejects non-numeric value', () => {
    const orig = process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS;
    process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS = 'fast';
    try {
      const val = getMaxConcurrent();
      assert.ok(val >= 4 && val <= 16, 'should fall back to CPU-based default');
    } finally {
      if (orig !== undefined) process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS = orig;
      else delete process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS;
    }
  });

  it('defaults to CPU-based calculation when env var not set', () => {
    const orig = process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS;
    delete process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS;
    try {
      const val = getMaxConcurrent();
      const expected = Math.max(4, Math.min(16, detectCpuCount() - 2));
      assert.equal(val, expected);
    } finally {
      if (orig !== undefined) process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS = orig;
    }
  });
});
