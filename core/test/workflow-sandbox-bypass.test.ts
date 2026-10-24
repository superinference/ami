/**
 * Red-green reproducer for the workflow sandbox-escape hardening.
 *
 * BUG: validateWorkflowScript only matched dot-notation `process.exit|kill|env`,
 * so bracket-notation (process['exit']), globalThis/global access, and the
 * Function-constructor escape ([].constructor.constructor('return process')())
 * all passed validation and ran via `new Function(...)` in the global scope.
 *
 * RED: restore the old /\bprocess\s*\.\s*(?:exit|kill|env)\b/ check (and drop
 * the globalThis/global/Function checks) and these cases stop throwing.
 * GREEN: every escape variant is rejected; legitimate scripts still pass.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { validateWorkflowScript } from '../src/tools/workflow-runtime';

describe('validateWorkflowScript blocks sandbox-escape variants', () => {
  const blocked: Array<[string, string]> = [
    ["process['exit'](1)", 'bracket process'],
    ['process["kill"](process["pid"])', 'bracket process kill'],
    ['const k = process["env"].SECRET', 'bracket process env'],
    ['globalThis.process.exit(1)', 'globalThis.process'],
    ["globalThis['process']", 'globalThis bracket'],
    ['global.process.kill(1)', 'global.process'],
    ["Function('return process')()", 'Function constructor'],
    ["const p = ([]).constructor.constructor('return process')()", 'constructor chain'],
  ];

  for (const [script, label] of blocked) {
    it(`blocks ${label}`, () => {
      assert.throws(() => validateWorkflowScript(script), /escape the workflow sandbox/);
    });
  }

  it('still allows a legitimate workflow script', () => {
    assert.doesNotThrow(() => validateWorkflowScript(`
      phase('Work')
      log('go')
      const r = await parallel([() => agent('a'), () => agent('b')])
      return { r }
    `));
  });

  it('still ignores escape tokens inside strings', () => {
    assert.doesNotThrow(() => validateWorkflowScript(
      `const note = "do not use globalThis.process or Function('x') here"\nreturn note`,
    ));
  });
});
