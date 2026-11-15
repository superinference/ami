import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  scanCodeForSecurityPatterns,
  getPatternById,
  SECURITY_PATTERNS,
  type SecurityFinding,
} from '../src/code-security-patterns';

// ---------------------------------------------------------------------------
// Parity: additional insecure-pattern detectors + the new 'low' severity tier.
// Observable-behavior assertions only (call the real scanner).
// ---------------------------------------------------------------------------

function findings(content: string, file: string): SecurityFinding[] {
  return scanCodeForSecurityPatterns(content, file);
}
function ids(content: string, file: string): string[] {
  return findings(content, file).map(f => f.patternId);
}
function sev(content: string, file: string, id: string): string | undefined {
  return findings(content, file).find(f => f.patternId === id)?.severity;
}

describe('code-security-patterns parity :: new severity tier', () => {
  it('exposes at least one low-severity pattern', () => {
    // Discriminator for the re-applied fix: before it, no pattern was 'low'.
    assert.ok(SECURITY_PATTERNS.some(p => p.severity === 'low'));
  });

  it('every pattern severity is one of high|medium|low', () => {
    for (const p of SECURITY_PATTERNS) {
      assert.ok(['high', 'medium', 'low'].includes(p.severity), `${p.id}: ${p.severity}`);
    }
  });

  it('findings carry through the declared severity verbatim', () => {
    for (const p of SECURITY_PATTERNS) {
      const f = getPatternById(p.id);
      assert.ok(f);
      assert.equal(f!.severity, p.severity);
    }
  });
});

describe('code-security-patterns parity :: python exec() injection (high)', () => {
  it('flags the exec() builtin in Python', () => {
    assert.ok(ids('exec(user_code)', 'a.py').includes('python_exec_injection'));
    assert.equal(sev('exec(user_code)', 'a.py', 'python_exec_injection'), 'high');
  });

  it('does not flag cursor.execute() or os.execvp (not the exec builtin)', () => {
    assert.ok(!ids('cursor.execute(sql)', 'a.py').includes('python_exec_injection'));
    assert.ok(!ids('os.execvp("ls", args)', 'a.py').includes('python_exec_injection'));
  });

  it('does not flag member access obj.exec(', () => {
    assert.ok(!ids('obj.exec(x)', 'a.py').includes('python_exec_injection'));
  });

  it('does not fire in a JS file (Python-only)', () => {
    assert.ok(!ids('exec(x)', 'a.js').includes('python_exec_injection'));
  });
});

describe('code-security-patterns parity :: node vm code execution (high)', () => {
  it('flags vm.runInNewContext / runInThisContext in JS', () => {
    assert.ok(ids('vm.runInNewContext(code)', 'a.js').includes('vm_code_execution'));
    assert.ok(ids('vm.runInThisContext(src)', 'a.ts').includes('vm_code_execution'));
    assert.equal(sev('vm.runInNewContext(code)', 'a.js', 'vm_code_execution'), 'high');
  });

  it('does not fire in a Python file (JS-only)', () => {
    assert.ok(!ids('vm.runInNewContext(code)', 'a.py').includes('vm_code_execution'));
  });
});

describe('code-security-patterns parity :: weak hash md5/sha1 (medium)', () => {
  it('flags hashlib.md5 and hashlib.sha1 in Python', () => {
    assert.ok(ids('h = hashlib.md5(data)', 'a.py').includes('weak_hash'));
    assert.ok(ids('h = hashlib.sha1(data)', 'a.py').includes('weak_hash'));
    assert.equal(sev('h = hashlib.md5(data)', 'a.py', 'weak_hash'), 'medium');
  });

  it('flags crypto.createHash("md5") in JS', () => {
    assert.ok(ids("crypto.createHash('md5').update(x)", 'a.js').includes('weak_hash'));
  });

  it('does not flag sha256', () => {
    assert.ok(!ids('h = hashlib.sha256(data)', 'a.py').includes('weak_hash'));
    assert.ok(!ids("crypto.createHash('sha256')", 'a.js').includes('weak_hash'));
  });
});

describe('code-security-patterns parity :: insecure randomness (low)', () => {
  it('flags Math.random() in JS as low severity', () => {
    assert.ok(ids('const t = Math.random();', 'a.js').includes('insecure_random_js'));
    assert.equal(sev('const t = Math.random();', 'a.js', 'insecure_random_js'), 'low');
  });

  it('flags stdlib random.* in Python as low severity', () => {
    assert.ok(ids('x = random.randint(0, 9)', 'a.py').includes('insecure_random_py'));
    assert.equal(sev('x = random.randint(0, 9)', 'a.py', 'insecure_random_py'), 'low');
  });

  it('does not flag np.random.randint (member access, not stdlib random)', () => {
    assert.ok(!ids('x = np.random.randint(0, 9)', 'a.py').includes('insecure_random_py'));
  });

  it('Math.random is JS-only, stdlib random is Python-only', () => {
    assert.ok(!ids('Math.random()', 'a.py').includes('insecure_random_js'));
    assert.ok(!ids('random.random()', 'a.js').includes('insecure_random_py'));
  });
});

describe('code-security-patterns parity :: cleartext http url (low)', () => {
  it('flags a non-local http:// URL as low severity', () => {
    assert.ok(ids('fetch("http://api.example.com/x")', 'a.js').includes('insecure_http_url'));
    assert.equal(sev('fetch("http://api.example.com/x")', 'a.js', 'insecure_http_url'), 'low');
  });

  it('does not flag https:// URLs', () => {
    assert.ok(!ids('fetch("https://api.example.com/x")', 'a.js').includes('insecure_http_url'));
  });

  it('does not flag localhost / loopback http URLs', () => {
    assert.ok(!ids('fetch("http://localhost:3000")', 'a.js').includes('insecure_http_url'));
    assert.ok(!ids('fetch("http://127.0.0.1:8080")', 'a.js').includes('insecure_http_url'));
  });

  it('is excluded in documentation files', () => {
    assert.ok(!ids('See http://example.com for details.', 'README.md').includes('insecure_http_url'));
  });
});
