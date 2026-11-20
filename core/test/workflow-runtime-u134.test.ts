import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  WorkflowRuntime,
  executeWorkflow,
  parseWorkflowMeta,
  parseDataLiteral,
  workflowAgentCacheKey,
  type AgentOptions,
} from '../src/tools/workflow-runtime';

// U134 — parity unit for core/core/src/tools/workflow-runtime.ts
//
// Gap A (HIGH): parseWorkflowMeta must parse untrusted metadata WITHOUT executing
//   code (parity with the reference's loadSkillsDir, which validates parsed frontmatter
//   as data via a schema and never evals it).
// Gap B (MEDIUM): per-agent maxTurns cap plumbed through AgentOptions + cache key.
// Gap C (LOW): WorkflowRuntime (an EventEmitter subclass) sets maxListeners(0) so
//   deeply-nested workflows adding per-child 'abort' listeners don't trip the
//   MaxListenersExceededWarning.

describe('U134 Gap A — parseWorkflowMeta does NOT execute untrusted code', () => {
  it('rejects a meta literal containing a side-effecting call expression (no eval)', () => {
    const sentinel = '__u134_parse_meta_exec__';
    delete (globalThis as Record<string, unknown>)[sentinel];
    const script = `export const meta = {
      name: ((globalThis['${sentinel}'] = 'pwned'), 'evil'),
      description: 'd',
    }`;
    const meta = parseWorkflowMeta(script);
    // The malicious literal must NOT run: the sentinel stays unset...
    assert.equal((globalThis as Record<string, unknown>)[sentinel], undefined);
    // ...and a literal that is not pure data is rejected.
    assert.equal(meta, null);
  });

  it('rejects a meta literal that references an identifier / call (no eval)', () => {
    assert.equal(parseWorkflowMeta(`export const meta = { name: foo(), description: 'd' }`), null);
    assert.equal(parseWorkflowMeta(`export const meta = { name: someVar, description: 'd' }`), null);
  });

  it('rejects a backtick template literal with interpolation', () => {
    const sentinel = '__u134_tpl_exec__';
    delete (globalThis as Record<string, unknown>)[sentinel];
    const script = 'export const meta = { name: `x${(globalThis[\'' + sentinel + '\'] = 1)}`, description: \'d\' }';
    assert.equal(parseWorkflowMeta(script), null);
    assert.equal((globalThis as Record<string, unknown>)[sentinel], undefined);
  });

  it('still parses a valid pure-data meta block (regression)', () => {
    const script = `export const meta = {
      name: 'wf',
      description: "does things",
      phases: [{ title: 'P1', detail: 'd', model: 'gpt' }, { title: 'P2' }],
      whenToUse: 'when needed',
    }
    const body = 1;`;
    const meta = parseWorkflowMeta(script);
    assert.ok(meta);
    assert.equal(meta.name, 'wf');
    assert.equal(meta.description, 'does things');
    assert.equal(meta.whenToUse, 'when needed');
    assert.ok(Array.isArray(meta.phases));
    assert.equal(meta.phases!.length, 2);
    assert.equal(meta.phases![0]!.title, 'P1');
  });

  it('parses numbers, booleans, null and trailing commas as data', () => {
    const script = `export const meta = {
      name: 'n',
      description: 'd',
      phases: [{ title: 'a', }],
    }`;
    const meta = parseWorkflowMeta(script);
    assert.ok(meta);
    assert.equal(meta.name, 'n');
  });

  it('returns null for a missing meta block', () => {
    assert.equal(parseWorkflowMeta('const x = 1;'), null);
  });

  it('returns null for an unterminated / malformed meta block', () => {
    assert.equal(parseWorkflowMeta('export const meta = {invalid'), null);
  });
});

describe('U134 Gap A — parseDataLiteral accepts pure data, rejects code', () => {
  it('parses every data value type (string, number, bool, null, array, object)', () => {
    assert.equal(parseDataLiteral('"hi"'), 'hi');
    assert.equal(parseDataLiteral("'hi'"), 'hi');
    assert.equal(parseDataLiteral('`hi`'), 'hi');
    assert.equal(parseDataLiteral('42'), 42);
    assert.equal(parseDataLiteral('-3'), -3);
    assert.equal(parseDataLiteral('3.14'), 3.14);
    assert.equal(parseDataLiteral('1e3'), 1000);
    assert.equal(parseDataLiteral('2E-2'), 0.02);
    assert.equal(parseDataLiteral('.5'), 0.5);
    assert.equal(parseDataLiteral('true'), true);
    assert.equal(parseDataLiteral('false'), false);
    assert.equal(parseDataLiteral('null'), null);
    assert.deepEqual(parseDataLiteral('[]'), []);
    assert.deepEqual(parseDataLiteral('{}'), {});
    assert.deepEqual(parseDataLiteral('[1, 2, 3,]'), [1, 2, 3]);
    assert.deepEqual(parseDataLiteral('{ "a": 1, b: [true, null], }'), { a: 1, b: [true, null] });
  });

  it('decodes standard escapes in strings', () => {
    assert.equal(parseDataLiteral('"a\\nb\\tc\\r\\b\\f\\v\\0\\"d"'), 'a\nb\tc\r\b\f\v\0"d');
    assert.equal(parseDataLiteral('"\\q"'), 'q'); // unknown escape -> literal char
  });

  it('throws on every non-data / malformed construct', () => {
    assert.throws(() => parseDataLiteral('foo()'));            // call / identifier
    assert.throws(() => parseDataLiteral('bar'));              // bare identifier
    assert.throws(() => parseDataLiteral(''));                 // unexpected end
    assert.throws(() => parseDataLiteral('  '));               // whitespace only
    assert.throws(() => parseDataLiteral('"unterminated'));    // unterminated string
    assert.throws(() => parseDataLiteral('"bad\\'));           // unterminated escape
    assert.throws(() => parseDataLiteral('`x${y}`'));          // template interpolation
    assert.throws(() => parseDataLiteral('-'));                // invalid number
    assert.throws(() => parseDataLiteral('{ : 1 }'));          // missing key
    assert.throws(() => parseDataLiteral('{ a 1 }'));          // missing colon
    assert.throws(() => parseDataLiteral('{ a: 1 b: 2 }'));    // missing comma in object
    assert.throws(() => parseDataLiteral('[1 2]'));            // missing comma in array
    assert.throws(() => parseDataLiteral('42 trailing'));      // trailing characters
    assert.throws(() => parseDataLiteral('trueX'));            // not a keyword
  });
});

describe('U134 Gap B — per-agent maxTurns cap', () => {
  it('workflowAgentCacheKey distinguishes calls that differ only by maxTurns', () => {
    assert.notEqual(
      workflowAgentCacheKey('p', { maxTurns: 3 } as AgentOptions),
      workflowAgentCacheKey('p', { maxTurns: 5 } as AgentOptions),
    );
    // Stable for identical maxTurns.
    assert.equal(
      workflowAgentCacheKey('p', { maxTurns: 3 } as AgentOptions),
      workflowAgentCacheKey('p', { maxTurns: 3 } as AgentOptions),
    );
    // A maxTurns-bearing call differs from one without it.
    assert.notEqual(
      workflowAgentCacheKey('p', { maxTurns: 3 } as AgentOptions),
      workflowAgentCacheKey('p'),
    );
  });

  it('passes maxTurns through to the agent handler end to end', async () => {
    const seen: Array<number | undefined> = [];
    const handler = async (_prompt: string, opts?: AgentOptions) => {
      seen.push(opts?.maxTurns);
      return 'ok';
    };
    const script = `
      export const meta = { name: 'w', description: 'd' }
      await agent('do it', { maxTurns: 7 });
    `;
    await executeWorkflow(script, handler);
    assert.deepEqual(seen, [7]);
  });
});

describe('U134 Gap C — WorkflowRuntime suppresses MaxListenersExceededWarning', () => {
  it('sets maxListeners to 0 (unlimited) on the runtime', () => {
    const rt = new WorkflowRuntime({ agentHandler: async () => null });
    assert.equal(rt.getMaxListeners(), 0);
  });

  it('a workflow that spawns many nested abort listeners does not cap at 10', async () => {
    // A parent that runs 25 nested workflows registers 25 'abort' listeners on
    // itself (one per child). With the default cap of 10 this emits a warning;
    // with maxListeners(0) it must not, and all children run.
    const warnings: string[] = [];
    const onWarn = (w: Error) => { if (w.name === 'MaxListenersExceededWarning') warnings.push(w.message); };
    process.on('warning', onWarn);
    try {
      const handler = async () => 'x';
      const resolver = () => `export const meta = { name: 'c', description: 'd' }\nawait agent('x');`;
      let body = `export const meta = { name: 'p', description: 'd' }\n`;
      for (let i = 0; i < 25; i++) body += `await workflow('child');\n`;
      const res = await executeWorkflow(body, handler, { workflowResolver: resolver });
      assert.equal(res.agentCount, 25);
    } finally {
      process.off('warning', onWarn);
    }
    assert.deepEqual(warnings, []);
  });
});

describe('U134 Gap D — interactive per-agent retry/skip of a live workflow agent', () => {
  it('skipAgent settles ONE wedged agent as null and lets the workflow continue', async () => {
    // A workflow with two agents run in parallel; the FIRST hangs forever. Without
    // per-agent recovery the only options are "abort everything" or wait forever.
    // skipAgent() must settle just that agent (as null) so the workflow completes.
    let captured: WorkflowRuntime | undefined;
    const skipped: Array<{ agentNumber: number }> = [];
    const handler = async (prompt: string, _opts?: AgentOptions, agentId?: number): Promise<unknown> => {
      if (prompt === 'hang') {
        // The control is registered before produce() is awaited, so we can skip
        // ourselves synchronously to simulate a user skipping a wedged agent.
        captured!.skipAgent(agentId!);
        return new Promise(() => { /* never resolves */ });
      }
      return `done:${prompt}`;
    };
    const script = `
      export const meta = { name: 'w', description: 'd' }
      const [a, b] = await parallel([() => agent('hang'), () => agent('ok')]);
      return { a, b };
    `;
    const res = await executeWorkflow(script, handler, {
      onRuntime: (rt) => {
        captured = rt;
        rt.on('agent-skipped', (e: { agentNumber: number }) => skipped.push(e));
      },
    });
    assert.deepEqual(res.result, { a: null, b: 'done:ok' });
    assert.equal(skipped.length, 1);
    // Both agents were accounted for; skip did not abort the whole run.
    assert.equal(res.agentCount, 2);
  });

  it('retryAgent abandons the stuck attempt and re-invokes the handler for the same agent', async () => {
    let captured: WorkflowRuntime | undefined;
    const retries: Array<{ agentNumber: number }> = [];
    let calls = 0;
    const handler = async (_prompt: string, _opts?: AgentOptions, agentId?: number): Promise<unknown> => {
      calls++;
      if (calls === 1) {
        // First attempt wedges; ask the runtime to retry THIS agent.
        captured!.retryAgent(agentId!);
        return new Promise(() => { /* never resolves */ });
      }
      return 'recovered';
    };
    const script = `
      export const meta = { name: 'w', description: 'd' }
      return await agent('go');
    `;
    const res = await executeWorkflow(script, handler, {
      onRuntime: (rt) => {
        captured = rt;
        rt.on('agent-retry', (e: { agentNumber: number }) => retries.push(e));
      },
    });
    assert.equal(res.result, 'recovered');
    assert.equal(calls, 2); // the handler was re-invoked exactly once
    assert.equal(retries.length, 1);
    assert.equal(res.agentCount, 1); // a retry is the SAME agent, not a new one
  });

  it('liveAgentIds lists in-flight agents; skip/retry of an unknown id return false', async () => {
    let captured: WorkflowRuntime | undefined;
    let liveDuring: number[] = [];
    const handler = async (_prompt: string, _opts?: AgentOptions, agentId?: number): Promise<unknown> => {
      liveDuring = captured!.liveAgentIds();
      // Unknown ids have no live control.
      assert.equal(captured!.skipAgent(9999), false);
      assert.equal(captured!.retryAgent(9999), false);
      // The current agent IS live and skippable.
      captured!.skipAgent(agentId!);
      return new Promise(() => { /* never resolves */ });
    };
    const script = `
      export const meta = { name: 'w', description: 'd' }
      return await agent('x');
    `;
    await executeWorkflow(script, handler, { onRuntime: (rt) => { captured = rt; } });
    assert.deepEqual(liveDuring, [1]);
    // After the run nothing is in-flight.
    assert.deepEqual(captured!.liveAgentIds(), []);
  });

  it('skipAgent/retryAgent return false when no agent is in-flight', () => {
    const rt = new WorkflowRuntime({ agentHandler: async () => null });
    assert.equal(rt.skipAgent(1), false);
    assert.equal(rt.retryAgent(1), false);
    assert.deepEqual(rt.liveAgentIds(), []);
  });

  it('the stable agent id is passed to the handler as its third argument', async () => {
    const seen: Array<number | undefined> = [];
    const handler = async (_p: string, _o: AgentOptions | undefined, agentId?: number) => {
      seen.push(agentId);
      return 'ok';
    };
    const script = `
      export const meta = { name: 'w', description: 'd' }
      await agent('one');
      await agent('two');
    `;
    await executeWorkflow(script, handler);
    assert.deepEqual(seen, [1, 2]);
  });
});
