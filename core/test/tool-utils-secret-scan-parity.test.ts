import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as childProcess from 'child_process';
import {
  scanForSecrets,
  scanAddedSecrets,
  redactSecrets,
} from '../src/tools/tool-utils';
import { gitCommitTool } from '../src/tools/git-commit';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// Secret-scanner parity hardening (unit + red-green e2e).
//
// Covers:
//  - scanForSecrets must NEVER return/surface matched secret bytes — only a
//    stable rule id + human label (SecretMatch).
//  - scan + redact share ONE curated rule source (no drift).
//  - AWS detection covers session/temporary key prefixes (ASIA/A3T/ABIA/ACCA).
//  - OpenAI project / service-account / admin keys (sk-proj-/svcacct-/admin-).
//  - Typed { ruleId, label } field on each detection.
//
// Each block names the discriminating value that fails on revert to the old
// string[]-returning scanner.
// ---------------------------------------------------------------------------

describe('scanForSecrets — never surfaces matched secret bytes (returns ruleId/label)', () => {
  it('returns typed SecretMatch objects, not raw secret strings (revert: returned "sk-…...")', () => {
    const secret = 'sk-ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcd';
    const found = scanForSecrets(`key=${secret}`);
    assert.ok(found.length > 0, 'should detect the key');
    for (const m of found) {
      assert.equal(typeof m.ruleId, 'string');
      assert.equal(typeof m.label, 'string');
      // The secret bytes must never appear anywhere in the detection output.
      assert.ok(
        !JSON.stringify(found).includes('ABCDEFGHIJKLMNOPQRSTUVWXYZ'),
        'detection output must not echo the secret bytes',
      );
    }
  });

  it('derives a human-readable label from the rule id', () => {
    const found = scanForSecrets('aws_key=AKIAIOSFODNN7EXAMPLE');
    const aws = found.find(m => m.ruleId === 'aws-access-token');
    assert.ok(aws, 'aws rule should fire');
    assert.equal(aws!.label, 'AWS Access Token');
  });

  it('reports each rule at most once even when it matches twice', () => {
    const line = `a=ghp_${'A'.repeat(36)} b=ghp_${'B'.repeat(36)}`;
    const found = scanForSecrets(line);
    const ids = found.map(m => m.ruleId);
    assert.equal(ids.filter(id => id === 'github-pat').length, 1);
  });
});

describe('scanForSecrets — AWS session/temporary key prefixes (parity add)', () => {
  // ASIA = STS temporary credentials; the old scanner only matched long-lived AKIA.
  const asia = 'ASIA' + 'Z2Z2Z2Z2Z2Z2Z2Z2'; // 16 base32 chars after prefix
  const a3t = 'A3TX' + 'A2A2A2A2A2A2A2A2';

  it('detects an ASIA session token (revert: returned [])', () => {
    const found = scanForSecrets(`AWS_SESSION=${asia}`);
    assert.ok(found.some(m => m.ruleId === 'aws-access-token'));
  });

  it('detects an A3T… prefixed token (revert: returned [])', () => {
    const found = scanForSecrets(`cred ${a3t}`);
    assert.ok(found.some(m => m.ruleId === 'aws-access-token'));
  });

  it('redacts the ASIA session token (shared rule source)', () => {
    const out = redactSecrets(`AWS_SESSION=${asia}`);
    assert.ok(!out.includes(asia), 'session token must be redacted');
    assert.ok(out.includes('[REDACTED-SECRET]'));
  });
});

describe('scanForSecrets — OpenAI project/service-account keys (parity add)', () => {
  // sk-proj-/svcacct-/admin- keys contain hyphens so the generic sk- rule never
  // matched them; the dedicated rule recognises the T3BlbkFJ infix.
  const proj = 'sk-proj-' + 'A'.repeat(24) + 'T3BlbkFJ' + 'B'.repeat(24);
  const svc = 'sk-svcacct-' + 'C'.repeat(24) + 'T3BlbkFJ' + 'D'.repeat(24);

  it('detects a project-scoped key with the dedicated rule id (revert: returned [])', () => {
    const found = scanForSecrets(`OPENAI_API_KEY=${proj}`);
    assert.ok(found.some(m => m.ruleId === 'openai-project-key'));
  });

  it('detects a service-account key (revert: returned [])', () => {
    const found = scanForSecrets(`key ${svc}`);
    assert.ok(found.some(m => m.ruleId === 'openai-project-key'));
  });

  it('redacts the project key fully (shared rule source)', () => {
    const out = redactSecrets(`OPENAI_API_KEY=${proj}`);
    assert.ok(!out.includes(proj), 'project key must not survive');
    assert.ok(out.includes('[REDACTED-SECRET]'));
  });
});

describe('scan + redact share ONE rule source (no drift)', () => {
  // Every representative secret the scanner flags must also be redacted — this
  // only holds because both derive from SECRET_RULES.
  const samples = [
    'sk-ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcd',
    'sk-ant-api03-AbCdEf_gHiJkL-mNoPqRsTuVwXyZ0123456789',
    'ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij',
    'AKIAIOSFODNN7EXAMPLE',
    'ASIA' + 'Z2Z2Z2Z2Z2Z2Z2Z2',
    'glpat-abcdefghijklmnopqrstuvwx',
    'npm_abcdefghijklmnopqrstuvwxyz1234567890ab',
  ];

  for (const s of samples) {
    it(`detects and redacts: ${s.slice(0, 10)}…`, () => {
      const detected = scanForSecrets(`x=${s}`);
      assert.ok(detected.length > 0, `scanner should flag ${s.slice(0, 10)}`);
      const redacted = redactSecrets(`x=${s}`);
      assert.ok(!redacted.includes(s), `redactor should remove ${s.slice(0, 10)}`);
    });
  }
});

describe('scanAddedSecrets — returns SecretMatch[] for introduced secrets', () => {
  it('flags a newly added project key with a typed match', () => {
    const before = 'line1\n';
    const after = 'line1\nOPENAI_API_KEY=sk-proj-' + 'A'.repeat(24) + 'T3BlbkFJ' + 'B'.repeat(24) + '\n';
    const found = scanAddedSecrets(before, after);
    assert.ok(found.some(m => m.ruleId === 'openai-project-key'));
  });

  it('returns [] when the edit only removes lines', () => {
    assert.deepEqual(scanAddedSecrets('a\nAKIAIOSFODNN7EXAMPLE\n', 'a\n'), []);
  });
});

// ---------------------------------------------------------------------------
// Red-green e2e: a real git_commit must NAME the rule, never echo the secret.
// On revert to the old scanner the warning contained the secret bytes verbatim.
// ---------------------------------------------------------------------------

function git(args: string, cwd: string): string {
  return childProcess
    .execSync(`git ${args}`, { cwd, encoding: 'utf-8', stdio: ['pipe', 'pipe', 'pipe'] })
    .trim();
}

const dirs: string[] = [];
after(() => {
  for (const d of dirs) fs.rmSync(d, { recursive: true, force: true });
});

describe('git_commit secret warning — names the rule, never the secret bytes (e2e)', () => {
  it('blocks the commit and the warning contains the label but NOT the secret value', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-secretscan-'));
    dirs.push(dir);
    git('init', dir);
    git('config user.name "Tester"', dir);
    git('config user.email "t@test.local"', dir);
    fs.writeFileSync(path.join(dir, 'f.txt'), 'content\n');

    const secret = 'ASIA' + 'Z2Z2Z2Z2Z2Z2Z2Z2';
    const ctx: ToolContext = { cwd: dir, abortSignal: new AbortController().signal };
    const result = await gitCommitTool.execute(
      { message: `chore: leak ${secret} now`, files: ['f.txt'] },
      ctx,
    );

    assert.equal(result.isError, true);
    assert.match(result.output, /Potential secrets detected/);
    // Names the rule label (gap 0 fix) …
    assert.match(result.output, /AWS Access Token/);
    // … and NEVER echoes the secret bytes (revert: the warning included them).
    assert.ok(!result.output.includes(secret), 'secret bytes must not appear in the warning');
    // The commit must not have been created.
    assert.throws(() => git('rev-parse HEAD', dir));
  });
});
