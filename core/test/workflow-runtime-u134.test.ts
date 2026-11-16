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
//   code (parity with Claude's loadSkillsDir, which validates parsed frontmatter
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
