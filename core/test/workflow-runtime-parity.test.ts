import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  WorkflowRuntime,
  executeWorkflow,
  validateWorkflowScript,
  validateStructuredOutput,
  schemaMaxAttempts,
  type AgentHandler,
} from '../src/tools/workflow-runtime';

/**
 * Parity tests for the workflow runtime vs the reference workflow execution
 * primitives. Each `describe` block targets one functional gap and is written to
 * RED-GREEN discriminate the fix (fails if the corresponding change is reverted).
 */

// ---------------------------------------------------------------------------
// GAP 1: banned non-deterministic globals (Date.now / Math.random)
// Reverting the Date/Math checks in validateWorkflowScript makes these pass the
// validator (no throw), so the assertions below fail → RED.
// ---------------------------------------------------------------------------
describe('parity: banned non-deterministic globals', () => {
  it('rejects Math.random() in a workflow script body', () => {
    assert.throws(
      () => validateWorkflowScript('const x = Math.random();\nreturn x;'),
      /non-deterministic/,
    );
  });

  it('rejects Date.now() in a workflow script body', () => {
    assert.throws(
      () => validateWorkflowScript('const t = Date.now();\nreturn t;'),
      /non-deterministic/,
    );
  });

  it('rejects bracket-notation Date["now"] and Math["random"]', () => {
    assert.throws(() => validateWorkflowScript('return Date["now"]();'), /non-deterministic/);
    assert.throws(() => validateWorkflowScript("return Math['random']();"), /non-deterministic/);
  });

  it('does NOT flag these tokens inside string literals', () => {
    assert.doesNotThrow(() => validateWorkflowScript('log("Math.random is banned");\nreturn 1;'));
    assert.doesNotThrow(() => validateWorkflowScript("log('use Date.now sparingly');\nreturn 1;"));
  });

  it('still allows a legitimate deterministic script', () => {
    assert.doesNotThrow(() => validateWorkflowScript('const r = await agent("hi");\nreturn r;'));
  });

  it('executeWorkflow rejects a script using Math.random()', async () => {
    const handler: AgentHandler = async () => 'ok';
    await assert.rejects(
      () => executeWorkflow('return Math.random();', handler, {}),
      /non-deterministic/,
    );
  });
});

// ---------------------------------------------------------------------------
// GAP 2: schema-forced StructuredOutput — validate + retry
// Reverting the schema branch in createAgent makes agent() return the raw string
// unconditionally, so (a) the structured-parse assertion and (b) the retry-count
// / null-on-failure assertions fail → RED.
// ---------------------------------------------------------------------------
describe('parity: schema-forced StructuredOutput validation + retry', () => {
  const schema = { type: 'object', required: ['answer'] } as Record<string, unknown>;

  it('parses a conforming JSON string into a structured object', async () => {
    const handler: AgentHandler = async () => JSON.stringify({ answer: 42 });
    const rt = new WorkflowRuntime({ agentHandler: handler });
    const ctx = rt.createContext(null);
    const res = await ctx.agent('q', { schema });
    assert.deepEqual(res, { answer: 42 });
    // Discriminator: without the fix the raw string would be returned.
    assert.notEqual(typeof res, 'string');
  });

  it('retries on a non-conforming result then returns the valid one', async () => {
    let calls = 0;
    const handler: AgentHandler = async () => {
      calls++;
      return calls === 1 ? 'not json at all' : JSON.stringify({ answer: 'ok' });
    };
    const rt = new WorkflowRuntime({ agentHandler: handler });
    const retries: number[] = [];
    rt.on('agent-schema-retry', (e: { attempt: number }) => retries.push(e.attempt));
    const ctx = rt.createContext(null);
    const res = await ctx.agent('q', { schema });
    assert.deepEqual(res, { answer: 'ok' });
    // Discriminator: the fix re-invokes the handler; the old code calls it once.
    assert.equal(calls, 2);
    assert.deepEqual(retries, [1]);
  });

  it('returns null (via error path) after exhausting attempts on persistent mismatch', async () => {
    let calls = 0;
    const handler: AgentHandler = async () => { calls++; return 'never valid'; };
    const rt = new WorkflowRuntime({ agentHandler: handler });
    const errors: string[] = [];
    rt.on('agent-error', (e: { error: string }) => errors.push(e.error));
    const ctx = rt.createContext(null);
    const res = await ctx.agent('q', { schema });
    // Discriminator: without validation the raw 'never valid' string is returned.
    assert.equal(res, null);
    assert.equal(calls, schemaMaxAttempts());
    assert.ok(errors.some(e => /schema validation/.test(e)));
  });

  it('a missing-required-key object is treated as non-conforming', async () => {
    let calls = 0;
    const handler: AgentHandler = async () => {
      calls++;
      return calls === 1 ? JSON.stringify({ wrong: 1 }) : JSON.stringify({ answer: 9 });
    };
    const rt = new WorkflowRuntime({ agentHandler: handler });
    const ctx = rt.createContext(null);
    const res = await ctx.agent('q', { schema });
    assert.deepEqual(res, { answer: 9 });
    assert.equal(calls, 2);
  });

  it('respects AMI_WORKFLOW_SCHEMA_MAX_ATTEMPTS', async () => {
    const orig = process.env.AMI_WORKFLOW_SCHEMA_MAX_ATTEMPTS;
    process.env.AMI_WORKFLOW_SCHEMA_MAX_ATTEMPTS = '5';
    try {
      assert.equal(schemaMaxAttempts(), 5);
      let calls = 0;
      const handler: AgentHandler = async () => { calls++; return 'bad'; };
      const rt = new WorkflowRuntime({ agentHandler: handler });
      const ctx = rt.createContext(null);
      const res = await ctx.agent('q', { schema });
      assert.equal(res, null);
      assert.equal(calls, 5);
    } finally {
      if (orig !== undefined) process.env.AMI_WORKFLOW_SCHEMA_MAX_ATTEMPTS = orig;
      else delete process.env.AMI_WORKFLOW_SCHEMA_MAX_ATTEMPTS;
    }
  });

  it('does not touch results when no schema is supplied (raw string passthrough)', async () => {
    const handler: AgentHandler = async () => 'plain text';
    const rt = new WorkflowRuntime({ agentHandler: handler });
    const ctx = rt.createContext(null);
    const res = await ctx.agent('q');
    assert.equal(res, 'plain text');
  });
});

// ---------------------------------------------------------------------------
// validateStructuredOutput unit coverage (the helper behind GAP 2)
// ---------------------------------------------------------------------------
describe('parity: validateStructuredOutput helper', () => {
  it('accepts a conforming object (already parsed)', () => {
    assert.deepEqual(validateStructuredOutput({ a: 1 }, { type: 'object', required: ['a'] }), { a: 1 });
  });
  it('rejects a missing required key', () => {
    assert.equal(validateStructuredOutput({ b: 1 }, { type: 'object', required: ['a'] }), undefined);
  });
  it('rejects non-JSON strings for object schemas', () => {
    assert.equal(validateStructuredOutput('not json', { type: 'object' }), undefined);
  });
  it('infers object type from properties/required when type is absent', () => {
    assert.deepEqual(validateStructuredOutput('{"a":1}', { properties: { a: {} } }), { a: 1 });
    assert.equal(validateStructuredOutput('[1,2]', { required: ['a'] }), undefined);
  });
  it('validates array schemas', () => {
    assert.deepEqual(validateStructuredOutput('[1,2]', { type: 'array' }), [1, 2]);
    assert.equal(validateStructuredOutput('{"a":1}', { type: 'array' }), undefined);
  });
  it('validates string schemas against the raw value', () => {
    assert.equal(validateStructuredOutput('hello', { type: 'string' }), 'hello');
    assert.equal(validateStructuredOutput(42, { type: 'string' }), undefined);
  });
  it('validates number/integer schemas', () => {
    assert.equal(validateStructuredOutput('7', { type: 'number' }), 7);
    assert.equal(validateStructuredOutput('"x"', { type: 'integer' }), undefined);
  });
  it('validates boolean schemas', () => {
    assert.equal(validateStructuredOutput('true', { type: 'boolean' }), true);
    assert.equal(validateStructuredOutput('1', { type: 'boolean' }), undefined);
  });
  it('accepts anything for an unknown/empty descriptor', () => {
    assert.equal(validateStructuredOutput('whatever', {}), 'whatever');
  });
  it('rejects null for an object schema', () => {
    assert.equal(validateStructuredOutput('null', { type: 'object' }), undefined);
  });
});

// ---------------------------------------------------------------------------
// GAP 3 (regression guards for already-correct semantics — parity invariants):
// parallel = barrier + null-on-throw; pipeline = per-item no-barrier flow.
// These do not RED-GREEN a new change but lock the upstream-parity behavior
// so future edits cannot silently break it.
// ---------------------------------------------------------------------------
describe('parity: parallel barrier + null-on-throw (invariant)', () => {
  it('returns null for a throwing thunk but resolves the rest (barrier)', async () => {
    const rt = new WorkflowRuntime({ agentHandler: async () => 'x' });
    const ctx = rt.createContext(null);
    const res = await ctx.parallel<number | string>([
      async () => 1,
      async () => { throw new Error('boom'); },
      async () => 3,
    ]);
    assert.deepEqual(res, [1, null, 3]);
  });
});

describe('parity: pipeline per-item no-barrier flow (invariant)', () => {
  it('flows each item through all stages independently; a throwing stage nulls only that item', async () => {
    const rt = new WorkflowRuntime({ agentHandler: async () => 'x' });
    const ctx = rt.createContext(null);
    const res = await ctx.pipeline(
      [1, 2, 3],
      async (prev: number) => prev * 10,
      async (prev: number, _item: number, idx: number) => { if (idx === 1) throw new Error('bad'); return prev + idx; },
    );
    // item0: 1*10+0=10, item1: throws → null, item2: 3*10+2=32
    assert.deepEqual(res, [10, null, 32]);
  });
});
