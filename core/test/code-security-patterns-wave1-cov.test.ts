import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  scanCodeForSecurityPatterns,
  scanAddedLinesForPatterns,
} from '../src/code-security-patterns';

// Wave-1 coverage tests: exercise the branch paths that the baseline suite
// leaves uncovered (excludeExtensions skip in the diff scanner, the dedup
// short-circuit in the full scanner) plus bypass variants that pin the
// security regexes against evasion.

describe('scanAddedLinesForPatterns — excludeExtensions branch', () => {
  it('skips eval() when the added line lives in an excluded doc extension', () => {
    // eval_injection carries excludeExtensions: DOC_EXTS. Adding an eval line
    // to a .md file must take the `excludeExtensions.includes(ext)` === true
    // skip path, so NO finding is produced even though the regex would match.
    const before = '# Title\n';
    const after = '# Title\nCall eval(payload) to run it.\n';
    const findings = scanAddedLinesForPatterns(before, after, 'docs.md');
    assert.deepEqual(
      findings.filter(f => f.patternId === 'eval_injection'),
      [],
      'eval in markdown diff must be excluded',
    );
  });

  it('still flags eval() added to a non-excluded source extension', () => {
    // Discriminator: same regex, non-doc extension => excludeExtensions.includes
    // is false, so the finding IS produced. If the skip branch were taken
    // unconditionally this would wrongly return empty.
    const before = 'const a = 1;\n';
    const after = 'const a = 1;\nconst b = eval(userInput);\n';
    const findings = scanAddedLinesForPatterns(before, after, 'app.ts');
    const hit = findings.find(f => f.patternId === 'eval_injection');
    assert.ok(hit, 'eval added to .ts must be flagged');
    assert.equal(hit!.line, 2);
  });

  it('flags an extension-less pattern (new Function) added to any file type', () => {
    // new_function_injection has neither extensions nor excludeExtensions, so
    // both guard conditions short-circuit on their left operand (falsy) and the
    // finding is emitted regardless of file type — including a .md doc.
    const before = '';
    const after = 'const f = new Function("return 1");';
    const mdFindings = scanAddedLinesForPatterns(before, after, 'notes.md');
    assert.ok(
      mdFindings.some(f => f.patternId === 'new_function_injection'),
      'new Function must be flagged even in a doc file',
    );
  });

  it('skips a JS-only pattern when the diff is a Python file', () => {
    // child_process_exec has extensions: JS_EXTS. A .py diff takes the
    // `!extensions.includes(ext)` === true continue path.
    const before = '';
    const after = 'child_process.exec(cmd)';
    const findings = scanAddedLinesForPatterns(before, after, 'worker.py');
    assert.ok(!findings.some(f => f.patternId === 'child_process_exec'));
  });
});

describe('scanCodeForSecurityPatterns — dedup / repeated-match branch', () => {
  it('reports the same pattern once per line when it matches on multiple lines', () => {
    // Drives the `findings.some(...)` dedup callback with a non-empty findings
    // array whose entries share the patternId: it must compare line numbers
    // (second operand of the &&) and still push the distinct line.
    const code = 'eval(a);\nconst x = 1;\neval(b);';
    const findings = scanCodeForSecurityPatterns(code, 'app.ts');
    const evalHits = findings.filter(f => f.patternId === 'eval_injection');
    assert.equal(evalHits.length, 2, 'both eval lines must be reported');
    assert.deepEqual(evalHits.map(f => f.line).sort((x, y) => x - y), [1, 3]);
  });

  it('does not duplicate a finding for a single matching line', () => {
    const findings = scanCodeForSecurityPatterns('eval(a);', 'app.ts');
    assert.equal(findings.filter(f => f.patternId === 'eval_injection').length, 1);
  });
});

describe('security regex bypass / evasion variants', () => {
  it('flags eval reached via trailing whitespace before the paren', () => {
    // The regex anchors on `eval(` with no space; ensure a direct call with a
    // leading identifier boundary is still caught and a near-miss is not.
    const hit = scanCodeForSecurityPatterns('  eval(danger);', 'a.ts');
    assert.ok(hit.some(f => f.patternId === 'eval_injection'));
    const miss = scanCodeForSecurityPatterns('const evaluate = 1;', 'a.ts');
    assert.ok(!miss.some(f => f.patternId === 'eval_injection'));
  });

  it('does not flag property access like obj._eval( (word-boundary guard)', () => {
    const findings = scanCodeForSecurityPatterns('obj._eval(x)', 'a.ts');
    assert.ok(!findings.some(f => f.patternId === 'eval_injection'));
  });

  it('flags torch.load even with weights_only set to False explicitly', () => {
    const findings = scanCodeForSecurityPatterns(
      'm = torch.load("x.pt", weights_only=False)',
      'load.py',
    );
    assert.ok(findings.some(f => f.patternId === 'torch_unsafe_load'));
  });

  it('flags subprocess shell=True despite extra spaces around equals', () => {
    const findings = scanCodeForSecurityPatterns(
      'subprocess.Popen(cmd, shell = True)',
      'run.py',
    );
    assert.ok(findings.some(f => f.patternId === 'subprocess_shell'));
  });

  it('flags InsecureSkipVerify:true in Go-style config regardless of extension', () => {
    const findings = scanCodeForSecurityPatterns(
      'tls.Config{InsecureSkipVerify: true}',
      'tls.go',
    );
    assert.ok(findings.some(f => f.patternId === 'tls_verify_disabled'));
  });

  it('flags pickle.Unpickler constructor, not just loads()', () => {
    const findings = scanCodeForSecurityPatterns(
      'up = pickle.Unpickler(fp)',
      'de.py',
    );
    assert.ok(findings.some(f => f.patternId === 'pickle_load'));
  });

  it('does not flag safe_pickle.loads via the preceding-char guard', () => {
    const findings = scanCodeForSecurityPatterns('safepickle.loads(x)', 'de.py');
    // `safepickle` -> char before `pickle` is `e`, excluded by lookbehind.
    assert.ok(!findings.some(f => f.patternId === 'pickle_load'));
  });
});
