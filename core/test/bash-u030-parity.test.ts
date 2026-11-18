import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { resolveBashTimeout, interpretExitCode, diagnoseError, bashTool } from '../src/tools/bash';

// Unit U030 — parity gaps against Claude Code's BashTool / commandSemantics /
// timeouts behaviour. Each describe block maps to one confirmed gap.

describe('U030 resolveBashTimeout — effective max is never below the configured default', () => {
  const save = { d: process.env.BASH_DEFAULT_TIMEOUT_MS, m: process.env.BASH_MAX_TIMEOUT_MS };
  const restore = () => {
    if (save.d === undefined) delete process.env.BASH_DEFAULT_TIMEOUT_MS; else process.env.BASH_DEFAULT_TIMEOUT_MS = save.d;
    if (save.m === undefined) delete process.env.BASH_MAX_TIMEOUT_MS; else process.env.BASH_MAX_TIMEOUT_MS = save.m;
  };

  it('floors the max up to BASH_DEFAULT_TIMEOUT_MS when max is misconfigured below it', () => {
    process.env.BASH_DEFAULT_TIMEOUT_MS = '99999';
    process.env.BASH_MAX_TIMEOUT_MS = '500';
    // parity (Math.max(max, default)): the 500 ceiling is raised to 99999, so a
    // defaulted request resolves to 99999 — never shrunk below the default.
    try { assert.equal(resolveBashTimeout(undefined), 99999); } finally { restore(); }
  });

  it('keeps a per-call timeout up to the floored max (default used as the floor)', () => {
    process.env.BASH_DEFAULT_TIMEOUT_MS = '99999';
    process.env.BASH_MAX_TIMEOUT_MS = '500';
    try { assert.equal(resolveBashTimeout(80000), 80000); } finally { restore(); }
  });

  it('still clamps a per-call timeout to a max that is >= default', () => {
    process.env.BASH_DEFAULT_TIMEOUT_MS = '100';
    process.env.BASH_MAX_TIMEOUT_MS = '500';
    try { assert.equal(resolveBashTimeout(99999), 500); } finally { restore(); }
  });
});

describe('U030 resolveBashTimeout — numeric-string coercion (semanticNumber parity)', () => {
  const save = { d: process.env.BASH_DEFAULT_TIMEOUT_MS, m: process.env.BASH_MAX_TIMEOUT_MS };
  const restore = () => {
    if (save.d === undefined) delete process.env.BASH_DEFAULT_TIMEOUT_MS; else process.env.BASH_DEFAULT_TIMEOUT_MS = save.d;
    if (save.m === undefined) delete process.env.BASH_MAX_TIMEOUT_MS; else process.env.BASH_MAX_TIMEOUT_MS = save.m;
  };

  it('accepts a numeric string timeout (providers serialize numbers as strings)', () => {
    delete process.env.BASH_DEFAULT_TIMEOUT_MS; delete process.env.BASH_MAX_TIMEOUT_MS;
    try { assert.equal(resolveBashTimeout('5000' as unknown as number), 5000); } finally { restore(); }
  });

  it('falls back to the default for a non-numeric / garbage timeout', () => {
    delete process.env.BASH_DEFAULT_TIMEOUT_MS; delete process.env.BASH_MAX_TIMEOUT_MS;
    try {
      assert.equal(resolveBashTimeout('12px' as unknown as number), 120000);
      assert.equal(resolveBashTimeout('' as unknown as number), 120000);
      assert.equal(resolveBashTimeout({} as unknown as number), 120000);
    } finally { restore(); }
  });
});

describe('U030 interpretExitCode — findstr / ripgrep grep-family exit semantics', () => {
  it('treats findstr exit 1 as "no matches" (not an error)', () => {
    const r = interpretExitCode('findstr /c:"needle" file.txt', 1);
    assert.ok(r?.isNonError, 'findstr exit 1 should be a non-error no-match');
    assert.match(r!.message, /no matches/i);
  });

  it('treats findstr.exe exit 1 as "no matches" (path/extension normalized)', () => {
    const r = interpretExitCode('C:\\Windows\\System32\\findstr.exe needle file.txt', 1);
    assert.ok(r?.isNonError);
  });

  it('treats findstr exit 2 as an error', () => {
    const r = interpretExitCode('findstr /bad', 2);
    assert.ok(r && !r.isNonError, 'findstr exit 2 should be an error');
    assert.match(r!.message, /error/i);
  });

  it('treats ripgrep (rg) exit 2 as an error', () => {
    const r = interpretExitCode('rg "pat(" .', 2);
    assert.ok(r && !r.isNonError, 'rg exit 2 should be an error');
    assert.match(r!.message, /error/i);
  });

  it('treats egrep / fgrep exit 2 as an error', () => {
    for (const cmd of ['egrep "x" f', 'fgrep "x" f']) {
      const r = interpretExitCode(cmd, 2);
      assert.ok(r && !r.isNonError, `${cmd} exit 2 should be an error`);
    }
  });
});

describe('U030 diagnoseError — gh auth-scope / permission remediation hint', () => {
  it('suggests gh auth refresh on a missing-scope error', () => {
    const hint = diagnoseError('error validating token: missing required scopes [workflow]');
    assert.ok(hint, 'expected a hint for a gh scope error');
    assert.match(hint!, /gh auth refresh/);
  });

  it('suggests gh auth refresh on a resource-not-accessible 403', () => {
    const hint = diagnoseError('HTTP 403: Resource not accessible by integration');
    assert.ok(hint);
    assert.match(hint!, /gh auth refresh/);
  });
});

describe('U030 bashTool.getProgressBudgetMs — resolved timeout budget surfaced for live progress', () => {
  const save = { d: process.env.BASH_DEFAULT_TIMEOUT_MS, m: process.env.BASH_MAX_TIMEOUT_MS };
  const restore = () => {
    if (save.d === undefined) delete process.env.BASH_DEFAULT_TIMEOUT_MS; else process.env.BASH_DEFAULT_TIMEOUT_MS = save.d;
    if (save.m === undefined) delete process.env.BASH_MAX_TIMEOUT_MS; else process.env.BASH_MAX_TIMEOUT_MS = save.m;
  };

  it('is exposed as a tool capability', () => {
    assert.equal(typeof bashTool.getProgressBudgetMs, 'function');
  });

  it('returns the same resolved timeout execute() will enforce (per-call timeout wins, clamped)', () => {
    process.env.BASH_DEFAULT_TIMEOUT_MS = '100';
    process.env.BASH_MAX_TIMEOUT_MS = '500';
    try {
      // per-call within bounds → honored
      assert.equal(bashTool.getProgressBudgetMs!({ command: 'x', timeout: 300 }), 300);
      assert.equal(bashTool.getProgressBudgetMs!({ command: 'x', timeout: 300 }), resolveBashTimeout(300));
      // per-call above max → clamped to max
      assert.equal(bashTool.getProgressBudgetMs!({ command: 'x', timeout: 99999 }), 500);
      // no per-call timeout → default
      assert.equal(bashTool.getProgressBudgetMs!({ command: 'x' }), 100);
    } finally { restore(); }
  });

  it('coerces a numeric-string timeout (provider serialization parity)', () => {
    process.env.BASH_DEFAULT_TIMEOUT_MS = '100';
    process.env.BASH_MAX_TIMEOUT_MS = '5000';
    try {
      assert.equal(bashTool.getProgressBudgetMs!({ command: 'x', timeout: '2500' }), 2500);
    } finally { restore(); }
  });

  it('tolerates a missing input object', () => {
    // Called with an empty object: no throw, resolves to the configured default.
    assert.equal(typeof bashTool.getProgressBudgetMs!({}), 'number');
  });
});
