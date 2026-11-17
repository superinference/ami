import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  scanCodeForSecurityPatterns,
  scanAddedLinesForPatterns,
  getPatternById,
  SECURITY_PATTERNS,
  type SecurityFinding,
} from '../src/code-security-patterns';

// ---------------------------------------------------------------------------
// U238 parity (security-review.ts:69-74, 112-123):
//   (1) Hardcoded secrets / credential detection as a first-class HIGH category.
//   (2) Structured per-finding fields: `category` + separate `remediation`.
// Observable-behavior assertions only (call the real scanner).
// ---------------------------------------------------------------------------

function findings(content: string, file: string): SecurityFinding[] {
  return scanCodeForSecurityPatterns(content, file);
}
function ids(content: string, file: string): string[] {
  return findings(content, file).map(f => f.patternId);
}

describe('U238 :: hardcoded_secret — known key formats (high)', () => {
  it('flags an AWS access key id', () => {
    const f = findings('const k = "AKIAIOSFODNN7EXAMPLE";', 'config.ts');
    const hit = f.find(x => x.patternId === 'hardcoded_secret');
    assert.ok(hit, 'AWS access key must be flagged');
    assert.equal(hit!.severity, 'high');
  });

  it('flags a GitHub personal access token (ghp_)', () => {
    assert.ok(ids('token = "ghp_' + 'a'.repeat(36) + '"', 'a.ts').includes('hardcoded_secret'));
  });

  it('flags an OpenAI-style sk- key', () => {
    assert.ok(ids('const key = "sk-' + 'A1b2C3d4'.repeat(3) + '"', 'a.ts').includes('hardcoded_secret'));
  });

  it('flags a Google API key (AIza...)', () => {
    assert.ok(ids('const g = "AIza' + 'b'.repeat(35) + '"', 'a.js').includes('hardcoded_secret'));
  });

  it('flags a Slack token (xoxb-)', () => {
    assert.ok(ids('const s = "xoxb-123456789012-abcDEF"', 'a.ts').includes('hardcoded_secret'));
  });

  it('flags a PEM private key header', () => {
    assert.ok(ids('-----BEGIN RSA PRIVATE KEY-----', 'id_rsa.ts').includes('hardcoded_secret'));
    assert.ok(ids('-----BEGIN PRIVATE KEY-----', 'key.ts').includes('hardcoded_secret'));
  });

  it('does NOT flag ordinary identifiers / short strings', () => {
    assert.ok(!ids('const name = "AKIA";', 'a.ts').includes('hardcoded_secret'));
    assert.ok(!ids('const region = "us-east-1";', 'a.ts').includes('hardcoded_secret'));
  });

  it('is excluded in documentation files', () => {
    assert.ok(!ids('Set key to "AKIAIOSFODNN7EXAMPLE".', 'README.md').includes('hardcoded_secret'));
  });
});

describe('U238 :: hardcoded_credential — credential-shaped assignments (high)', () => {
  it('flags password assigned an inline literal', () => {
    const hit = findings('password = "hunter2pass"', 'login.py').find(f => f.patternId === 'hardcoded_credential');
    assert.ok(hit, 'inline password literal must be flagged');
    assert.equal(hit!.severity, 'high');
  });

  it('flags api_key / apiKey (case- and separator-insensitive) with : or =', () => {
    assert.ok(ids('api_key = "sometokenvalue123"', 'a.py').includes('hardcoded_credential'));
    assert.ok(ids('apiKey: "sometokenvalue123",', 'a.ts').includes('hardcoded_credential'));
    assert.ok(ids('CLIENT_SECRET="abcdefgh12345678"', 'a.ts').includes('hardcoded_credential'));
  });

  it('does NOT flag a comparison (==) or an env lookup', () => {
    assert.ok(!ids('if (password == "x") {}', 'a.ts').includes('hardcoded_credential'));
    assert.ok(!ids('apiKey = process.env.API_KEY', 'a.ts').includes('hardcoded_credential'));
    assert.ok(!ids('password = os.environ["PW"]', 'a.py').includes('hardcoded_credential'));
  });

  it('does NOT flag a short value below the min length', () => {
    assert.ok(!ids('password = "short"', 'a.py').includes('hardcoded_credential'));
  });

  it('is detected in the added-lines diff scanner too', () => {
    const before = 'const a = 1;\n';
    const after = 'const a = 1;\nconst token = "deadbeefcafef00d";\n';
    const got = scanAddedLinesForPatterns(before, after, 'app.ts');
    const hit = got.find(f => f.patternId === 'hardcoded_credential');
    assert.ok(hit);
    assert.equal(hit!.line, 2);
  });
});

describe('U238 :: structured finding fields (category + remediation)', () => {
  it('every pattern declares a non-empty category and remediation', () => {
    for (const p of SECURITY_PATTERNS) {
      assert.ok(p.category && p.category.length > 0, `${p.id}: missing category`);
      assert.ok(p.remediation && p.remediation.length > 0, `${p.id}: missing remediation`);
    }
  });

  it('findings carry category + remediation verbatim from the pattern', () => {
    const f = findings('const x = eval(userInput);', 'a.ts').find(x => x.patternId === 'eval_injection')!;
    const p = getPatternById('eval_injection')!;
    assert.equal(f.category, p.category);
    assert.equal(f.remediation, p.remediation);
    // remediation is distinct from the description message (not collapsed)
    assert.notEqual(f.remediation, f.message);
  });

  it('secret findings surface the hardcoded_secret category', () => {
    const f = findings('const k = "AKIAIOSFODNN7EXAMPLE";', 'a.ts').find(x => x.patternId === 'hardcoded_secret')!;
    assert.equal(f.category, 'hardcoded_secret');
    assert.ok(/rotate|secrets? manager|environment variable/i.test(f.remediation));
  });

  it('diff scanner also carries category + remediation', () => {
    const got = scanAddedLinesForPatterns('', 'os.system(f"ls {p}")', 'a.py');
    const hit = got.find(f => f.patternId === 'os_system_injection')!;
    assert.equal(hit.category, 'command_injection');
    assert.ok(hit.remediation.length > 0);
  });
});
