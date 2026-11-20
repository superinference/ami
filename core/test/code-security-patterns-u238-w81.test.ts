import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  scanCodeForSecurityPatterns,
  scanAddedLinesForPatterns,
  computeAddedLineIndices,
  isTestFile,
  getPatternById,
  SECURITY_PATTERNS,
  type SecurityFinding,
} from '../src/code-security-patterns';

// ---------------------------------------------------------------------------
// U238 / w81 — parity with the reference's security-review detection algorithm:
//   * SQL/NoSQL injection + path-traversal detectors (security-review.ts:62-67)
//   * multi-line cross-boundary pattern matching (security-review.ts:140-160)
//   * sound positional added-lines diff (LCS, not Set-membership)
//   * context-aware false-positive suppression (comments, test files, React xss)
//   * per-finding exploit narrative (security-review.ts:112-123)
// Observable-behavior assertions only (call the real scanner).
// ---------------------------------------------------------------------------

function findings(content: string, file: string): SecurityFinding[] {
  return scanCodeForSecurityPatterns(content, file);
}
function ids(content: string, file: string): string[] {
  return findings(content, file).map(f => f.patternId);
}

describe('w81 :: SQL/NoSQL injection detector (high)', () => {
  it('flags an f-string SQL passed to cursor.execute (Python)', () => {
    assert.ok(
      ids('cursor.execute(f"SELECT * FROM users WHERE id = {uid}")', 'db.py').includes('sql_injection'),
    );
    assert.equal(getPatternById('sql_injection')!.severity, 'high');
  });

  it('flags concatenated SQL passed to execute()', () => {
    assert.ok(
      ids('cursor.execute("SELECT * FROM t WHERE id = " + user_id)', 'db.py').includes('sql_injection'),
    );
  });

  it('flags a %-formatted SQL string', () => {
    assert.ok(
      ids('cursor.execute("SELECT * FROM t WHERE id = %s" % uid)', 'db.py').includes('sql_injection'),
    );
  });

  it('flags .format()-built SQL', () => {
    assert.ok(
      ids('db.execute("SELECT * FROM t WHERE id = {}".format(uid))', 'db.py').includes('sql_injection'),
    );
  });

  it('flags a template-literal query with interpolation (JS)', () => {
    assert.ok(
      ids('db.query(`SELECT * FROM t WHERE id = ${userId}`)', 'db.ts').includes('sql_injection'),
    );
  });

  it('does NOT flag a parameterized query (safe)', () => {
    assert.ok(!ids('cursor.execute(sql, params)', 'db.py').includes('sql_injection'));
    assert.ok(!ids('cursor.execute("SELECT * FROM t WHERE id = ?", [uid])', 'db.py').includes('sql_injection'));
  });

  it('does NOT flag a bare constant query', () => {
    assert.ok(!ids('cursor.execute("SELECT 1")', 'db.py').includes('sql_injection'));
  });
});

describe('w81 :: multi-line cross-boundary matching', () => {
  it('catches an SQL concatenation split across multiple lines', () => {
    const code = [
      'cursor.execute(',
      '    "SELECT * FROM users WHERE id = " + user_id',
      ')',
    ].join('\n');
    const got = findings(code, 'db.py');
    const hit = got.find(f => f.patternId === 'sql_injection');
    assert.ok(hit, 'multi-line SQL concatenation must be flagged');
    // match is anchored at the `execute(` on the first line
    assert.equal(hit!.line, 1);
  });

  it('a multi-line pattern marks its declaring pattern as multiline', () => {
    assert.equal(getPatternById('sql_injection')!.multiline, true);
  });
});

describe('w81 :: path-traversal detector (high)', () => {
  it('flags open() with a ../ traversal segment (Python)', () => {
    assert.ok(ids('f = open("../" + name)', 'io.py').includes('path_traversal'));
    assert.ok(ids('f = open(f"/data/../{name}")', 'io.py').includes('path_traversal'));
  });

  it('flags readFileSync with a traversal segment (JS)', () => {
    assert.ok(ids('const d = fs.readFileSync(base + "/../" + p)', 'io.ts').includes('path_traversal'));
  });

  it('does NOT flag a plain, confined open()', () => {
    assert.ok(!ids('f = open("config.json")', 'io.py').includes('path_traversal'));
  });
});

describe('w81 :: context-aware false-positive suppression', () => {
  it('skips a comment-only line (JS //)', () => {
    assert.ok(!ids('// eval(userInput) should be avoided', 'a.ts').includes('eval_injection'));
  });

  it('skips a comment-only line (Python #)', () => {
    assert.ok(!ids('# os.system("rm -rf /") is dangerous', 'a.py').includes('os_system_injection'));
  });

  it('skips an inline-trailing-comment match (JS)', () => {
    assert.ok(!ids('const x = 1; // eval(danger)', 'a.ts').includes('eval_injection'));
  });

  it('skips an inline-trailing-comment match (Python)', () => {
    assert.ok(!ids('x = 1  # os.system("ls")', 'a.py').includes('os_system_injection'));
  });

  it('still flags a real match that only shares a line with a later comment', () => {
    assert.ok(ids('os.system(cmd)  # run it', 'a.py').includes('os_system_injection'));
  });

  it('does NOT misread a // inside a string literal as a comment (http URL still flagged)', () => {
    assert.ok(ids('const u = "http://api.example.com/x";', 'a.ts').includes('insecure_http_url'));
  });

  it('returns nothing for a test file (whole-file suppression)', () => {
    assert.equal(findings('eval(x); os.system("ls")', 'foo.test.ts').length, 0);
    assert.equal(scanAddedLinesForPatterns('', 'eval(x)', 'module.spec.js').length, 0);
    assert.equal(findings('eval(x)', '__tests__/thing.ts').length, 0);
    assert.equal(findings('exec(x)', 'test_module.py').length, 0);
  });

  it('isTestFile classifies conventions but not a bare "test.ts" source', () => {
    assert.equal(isTestFile('a.test.ts'), true);
    assert.equal(isTestFile('pkg/__tests__/x.js'), true);
    assert.equal(isTestFile('test_app.py'), true);
    assert.equal(isTestFile('conftest.py'), true);
    assert.equal(isTestFile('src/test.ts'), false);
    assert.equal(isTestFile('src/app.ts'), false);
  });
});

describe('w81 :: React/tsx XSS precedent suppression', () => {
  it('suppresses a non-unsafe-method xss (javascript: URI) in .tsx', () => {
    assert.ok(!ids('<a href="javascript:alert(1)">x</a>', 'page.tsx').includes('javascript_uri_xss'));
    assert.ok(!ids('<a href="javascript:alert(1)">x</a>', 'page.jsx').includes('javascript_uri_xss'));
  });

  it('still flags a javascript: URI in plain JS/HTML-ish files', () => {
    assert.ok(ids('link.href = "javascript:alert(1)"', 'legacy.js').includes('javascript_uri_xss'));
  });

  it('still flags unsafe-method xss (innerHTML) in .tsx (precedent exempts unsafe methods)', () => {
    assert.ok(ids('el.innerHTML = userHtml;', 'ui.tsx').includes('innerHTML_xss'));
    assert.ok(ids('<div dangerouslySetInnerHTML={{__html: d}} />', 'ui.tsx').includes('dangerously_set_html'));
  });
});

describe('w81 :: per-finding exploit narrative', () => {
  it('carries the exploit scenario from pattern to finding', () => {
    const f = findings('const x = eval(userInput);', 'a.ts').find(x => x.patternId === 'eval_injection')!;
    const p = getPatternById('eval_injection')!;
    assert.ok(p.exploit && p.exploit.length > 0);
    assert.equal(f.exploit, p.exploit);
  });

  it('omits exploit for patterns that declare none', () => {
    const f = findings('new Function("x")', 'a.ts').find(x => x.patternId === 'new_function_injection')!;
    assert.equal(f.exploit, undefined);
    assert.equal('exploit' in f, false);
  });

  it('sql_injection and path_traversal declare exploit scenarios', () => {
    assert.ok(getPatternById('sql_injection')!.exploit);
    assert.ok(getPatternById('path_traversal')!.exploit);
  });
});

describe('w81 :: sound positional added-lines diff (LCS)', () => {
  it('flags a newly-added dangerous line even when identical text already exists', () => {
    // The old Set-membership heuristic treated this as "not added" because the
    // text already appeared on line 1. The LCS diff identifies it positionally.
    const before = 'eval(x);\nfoo();';
    const after = 'eval(x);\nfoo();\neval(x);';
    const got = scanAddedLinesForPatterns(before, after, 'app.ts');
    const hit = got.find(f => f.patternId === 'eval_injection');
    assert.ok(hit, 'the newly-inserted duplicate dangerous line must be flagged');
    assert.equal(hit!.line, 3);
  });

  it('computeAddedLineIndices returns positional (0-based) indices', () => {
    assert.deepEqual(computeAddedLineIndices('a\nb', 'a\nX\nb'), [1]);
    assert.deepEqual(computeAddedLineIndices('a\nb', 'a\nb'), []);
    assert.deepEqual(computeAddedLineIndices('', 'x'), [0]);
    // trailing duplicate insertion
    assert.deepEqual(computeAddedLineIndices('a\nb', 'a\nb\na'), [2]);
  });

  it('still returns empty when nothing dangerous is added', () => {
    assert.equal(scanAddedLinesForPatterns('const a = 1;', 'const a = 1;\nconst b = 2;', 'app.ts').length, 0);
  });
});

describe('w81 :: dedup keyed on pattern:line', () => {
  it('reports one finding per (pattern, line) even if the regex could match twice', () => {
    const code = 'eval(a);\nconst x = 1;\neval(b);';
    const got = findings(code, 'app.ts');
    const evalHits = got.filter(f => f.patternId === 'eval_injection');
    assert.equal(evalHits.length, 2);
    assert.deepEqual(evalHits.map(f => f.line).sort((x, y) => x - y), [1, 3]);
  });
});

describe('w81 :: new patterns have structured fields', () => {
  it('sql_injection, path_traversal, javascript_uri_xss declare category + remediation', () => {
    for (const id of ['sql_injection', 'path_traversal', 'javascript_uri_xss']) {
      const p = getPatternById(id)!;
      assert.ok(p, `${id} must exist`);
      assert.ok(p.category && p.category.length > 0);
      assert.ok(p.remediation && p.remediation.length > 0);
    }
  });

  it('keeps all pattern ids unique after additions', () => {
    const allIds = SECURITY_PATTERNS.map(p => p.id);
    assert.equal(allIds.length, new Set(allIds).size);
  });
});
