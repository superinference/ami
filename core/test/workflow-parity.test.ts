import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  workflowTool,
  MAX_WORKFLOW_SCRIPT_BYTES,
  sanitizeWorkflowRunId,
  persistWorkflowRun,
  loadWorkflowRun,
} from '../src/tools/workflow';
import type { ToolContext } from '../src/types';

// Workflow tool — the reference parity hardening (additive superset):
//   1. meta must declare BOTH a non-empty name AND description.
//   2. string-typed inputs are validated up front.
//   3. script size is bounded before compilation.
//   4. opt-in resumable runs: runId persists the resolved script; resumeFromRunId
//      replays it (and reuses its agent cache) without re-supplying the script.
//
// Each fix below is RED-GREEN validated: the assertions discriminate the new
// behavior from the pre-fix behavior (see the per-test RED notes).

const dirs: string[] = [];
afterEach(() => { for (const d of dirs.splice(0)) fs.rmSync(d, { recursive: true, force: true }); });
function mkcwd(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wf-parity-'));
  dirs.push(d);
  return d;
}
function ctx(cwd: string): ToolContext {
  return { cwd, abortSignal: new AbortController().signal } as ToolContext;
}

describe('workflowTool parity — meta requires name AND description', () => {
  it('rejects empty meta {} (missing both)', async () => {
    const cwd = mkcwd();
    const script = 'export const meta = {}\nreturn 1';
    const r = await workflowTool.execute!({ script }, ctx(cwd));
    // RED (no guard): parseWorkflowMeta coerces to {name:'',description:''},
    // the workflow runs and returns "Workflow '' completed." (isError false).
    assert.equal(r.isError, true);
    assert.match(r.output, /must define a non-empty `name` and `description`/);
  });

  it('rejects meta with a name but blank description (second clause)', async () => {
    const cwd = mkcwd();
    const script = "export const meta = { name: 'has-name', description: '   ' }\nreturn 1";
    const r = await workflowTool.execute!({ script }, ctx(cwd));
    // RED (no guard): runs and completes as "Workflow 'has-name' completed."
    assert.equal(r.isError, true);
    assert.match(r.output, /non-empty `name` and `description`/);
  });

  it('accepts meta with both fields present (control — green both ways)', async () => {
    const cwd = mkcwd();
    const script = "export const meta = { name: 'ok', description: 'd' }\nreturn 7";
    const r = await workflowTool.execute!({ script }, ctx(cwd));
    assert.equal(r.isError, false);
    assert.match(r.output, /Workflow 'ok' completed/);
    assert.match(r.output, /Result: 7/);
  });
});

describe('workflowTool parity — input type validation', () => {
  it('rejects a non-string scriptPath with a precise message', async () => {
    const cwd = mkcwd();
    const r = await workflowTool.execute!({ scriptPath: 123 as unknown as string }, ctx(cwd));
    // RED (no guard): fs.existsSync(123) is falsy -> "Script file not found: 123".
    // The precise "'scriptPath' must be a string" message discriminates the fix.
    assert.equal(r.isError, true);
    assert.match(r.output, /'scriptPath' must be a string/);
  });

  it('rejects a non-string runId', async () => {
    const cwd = mkcwd();
    const script = "export const meta = { name: 'ok', description: 'd' }\nreturn 1";
    const r = await workflowTool.execute!({ script, runId: 5 as unknown as string }, ctx(cwd));
    assert.equal(r.isError, true);
    assert.match(r.output, /'runId' must be a string/);
  });
});

describe('workflowTool parity — script size guard', () => {
  it('rejects a script larger than the byte cap before compiling it', async () => {
    const cwd = mkcwd();
    // A VALID workflow that would otherwise complete, padded past the cap with a
    // comment. RED (no guard): it compiles and returns "Workflow 'big' completed."
    const pad = '/*' + 'x'.repeat(MAX_WORKFLOW_SCRIPT_BYTES + 1024) + '*/';
    const script = `export const meta = { name: 'big', description: 'd' }\n${pad}\nreturn 1`;
    const r = await workflowTool.execute!({ script }, ctx(cwd));
    assert.equal(r.isError, true);
    assert.match(r.output, /exceeds the maximum size/);
  });

  it('accepts a script at/under the cap (control)', async () => {
    const cwd = mkcwd();
    const script = "export const meta = { name: 'small', description: 'd' }\nreturn 1";
    assert.ok(Buffer.byteLength(script, 'utf-8') <= MAX_WORKFLOW_SCRIPT_BYTES);
    const r = await workflowTool.execute!({ script }, ctx(cwd));
    assert.equal(r.isError, false);
  });
});

describe('workflowTool parity — resumeFromRunId + run persistence', () => {
  it('persists a runId-tagged run and replays it from resumeFromRunId with NO script', async () => {
    const cwd = mkcwd();
    const script = "export const meta = { name: 'resumable', description: 'd' }\nreturn 42";

    const first = await workflowTool.execute!({ script, runId: 'run-A' }, ctx(cwd));
    assert.equal(first.isError, false);
    assert.match(first.output, /Run id: run-A/);
    // The run record was written under .ami/workflows/.runs/.
    assert.ok(fs.existsSync(path.join(cwd, '.ami', 'workflows', '.runs', 'run-A.json')));

    // Resume WITHOUT supplying any script/scriptPath/name.
    const resumed = await workflowTool.execute!({ resumeFromRunId: 'run-A' }, ctx(cwd));
    // RED (no resumeFromRunId support): with no script/scriptPath/name, execute
    // returns "One of script, scriptPath, or name is required" (isError true).
    assert.equal(resumed.isError, false);
    assert.match(resumed.output, /Workflow 'resumable' completed/);
    assert.match(resumed.output, /Result: 42/);
    assert.match(resumed.output, /Run id: run-A/);
  });

  it('persists the SCRIPT TEXT so resume works even after the scriptPath file is deleted', async () => {
    const cwd = mkcwd();
    const file = path.join(cwd, 'wf.js');
    fs.writeFileSync(file, "export const meta = { name: 'from-file', description: 'd' }\nreturn 'loaded'");

    const first = await workflowTool.execute!({ scriptPath: file, runId: 'run-B' }, ctx(cwd));
    assert.equal(first.isError, false);

    // Delete the original source; resume must still reproduce it from the record.
    fs.rmSync(file);
    const resumed = await workflowTool.execute!({ resumeFromRunId: 'run-B' }, ctx(cwd));
    assert.equal(resumed.isError, false);
    assert.match(resumed.output, /Workflow 'from-file' completed/);
    assert.match(resumed.output, /Result: "loaded"/);
  });

  it('errors when resuming an unknown runId', async () => {
    const cwd = mkcwd();
    const r = await workflowTool.execute!({ resumeFromRunId: 'nope' }, ctx(cwd));
    assert.equal(r.isError, true);
    assert.match(r.output, /No saved workflow run found for runId 'nope'/);
  });

  it('treats a malformed run record (no script) as not found', async () => {
    const cwd = mkcwd();
    const runsDir = path.join(cwd, '.ami', 'workflows', '.runs');
    fs.mkdirSync(runsDir, { recursive: true });
    fs.writeFileSync(path.join(runsDir, 'bad.json'), JSON.stringify({ meta: { name: 'x' } }));
    const r = await workflowTool.execute!({ resumeFromRunId: 'bad' }, ctx(cwd));
    assert.equal(r.isError, true);
    assert.match(r.output, /No saved workflow run found/);
  });

  it('run persistence is best-effort: a run still completes when .ami is a file', async () => {
    const cwd = mkcwd();
    // Make `.ami` a FILE so mkdirSync of the runs dir throws ENOTDIR inside
    // persistWorkflowRun — which must be swallowed.
    fs.writeFileSync(path.join(cwd, '.ami'), 'not a dir');
    const script = "export const meta = { name: 'besteffort', description: 'd' }\nreturn 1";
    const r = await workflowTool.execute!({ script, runId: 'run-C' }, ctx(cwd));
    assert.equal(r.isError, false);
    assert.match(r.output, /Workflow 'besteffort' completed/);
  });
});

describe('workflowTool parity — resumeKey cache namespace preserved', () => {
  it('still accepts a resumeKey (no runId) and completes', async () => {
    const cwd = mkcwd();
    const script = "export const meta = { name: 'rk', description: 'd' }\nreturn 1";
    const r = await workflowTool.execute!({ script, resumeKey: 'my/key' }, ctx(cwd));
    assert.equal(r.isError, false);
    assert.match(r.output, /Workflow 'rk' completed/);
    // resumeKey (no runId) does NOT emit a Run id line.
    assert.doesNotMatch(r.output, /Run id:/);
  });
});

describe('workflowTool parity — blank ids are ignored (branch coverage)', () => {
  it('a whitespace-only resumeFromRunId is treated as absent and falls through to script', async () => {
    const cwd = mkcwd();
    const script = "export const meta = { name: 'blank-resume', description: 'd' }\nreturn 1";
    const r = await workflowTool.execute!({ script, resumeFromRunId: '   ' }, ctx(cwd));
    assert.equal(r.isError, false);
    assert.match(r.output, /Workflow 'blank-resume' completed/);
    assert.doesNotMatch(r.output, /Run id:/);
  });

  it('an empty runId does not persist a record or emit a Run id', async () => {
    const cwd = mkcwd();
    const script = "export const meta = { name: 'blank-runid', description: 'd' }\nreturn 1";
    const r = await workflowTool.execute!({ script, runId: '' }, ctx(cwd));
    assert.equal(r.isError, false);
    assert.doesNotMatch(r.output, /Run id:/);
    assert.ok(!fs.existsSync(path.join(cwd, '.ami', 'workflows', '.runs')));
  });

  it('an empty resumeKey disables the cache namespace', async () => {
    const cwd = mkcwd();
    const script = "export const meta = { name: 'blank-rk', description: 'd' }\nreturn 1";
    const r = await workflowTool.execute!({ script, resumeKey: '' }, ctx(cwd));
    assert.equal(r.isError, false);
    assert.ok(!fs.existsSync(path.join(cwd, '.ami', 'workflows', '.cache')));
  });
});

describe('workflowTool parity — args JSON passthrough (capability)', () => {
  it('passes a structured (object) args value through to the script unchanged', async () => {
    const cwd = mkcwd();
    const script = "export const meta = { name: 'argwf', description: 'd' }\nreturn args.answer";
    const r = await workflowTool.execute!({ script, args: { answer: 99 } as unknown as string }, ctx(cwd));
    assert.equal(r.isError, false);
    assert.match(r.output, /Result: 99/);
  });
});

describe('workflow run-record helpers (unit)', () => {
  it('sanitizeWorkflowRunId collapses unsafe characters to underscores', () => {
    assert.equal(sanitizeWorkflowRunId('a/b..c\\d'), 'a_b__c_d');
    assert.equal(sanitizeWorkflowRunId('clean-ID_1'), 'clean-ID_1');
  });

  it('persist + load round-trips a record', () => {
    const cwd = mkcwd();
    const rec = { script: 's', meta: { name: 'n', description: 'd' }, createdAt: 'now' };
    persistWorkflowRun(cwd, 'rt', rec);
    assert.deepEqual(loadWorkflowRun(cwd, 'rt'), rec);
  });

  it('loadWorkflowRun returns null for an absent record', () => {
    const cwd = mkcwd();
    assert.equal(loadWorkflowRun(cwd, 'ghost'), null);
  });
});
