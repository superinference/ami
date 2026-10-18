import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  scanCodeForSecurityPatterns,
  scanAddedLinesForPatterns,
  getPatternById,
  SECURITY_PATTERNS,
} from '../src/code-security-patterns';

describe('SECURITY_PATTERNS', () => {
  it('has unique IDs', () => {
    const ids = SECURITY_PATTERNS.map(p => p.id);
    assert.equal(ids.length, new Set(ids).size, 'duplicate pattern IDs');
  });

  it('all have required fields', () => {
    for (const p of SECURITY_PATTERNS) {
      assert.ok(p.id, 'missing id');
      assert.ok(p.regex, 'missing regex');
      assert.ok(p.severity === 'high' || p.severity === 'medium', `invalid severity: ${p.severity}`);
      assert.ok(p.message, 'missing message');
    }
  });
});

describe('getPatternById', () => {
  it('finds known pattern', () => {
    const p = getPatternById('eval_injection');
    assert.ok(p);
    assert.equal(p.id, 'eval_injection');
  });
  it('returns undefined for unknown', () => {
    assert.equal(getPatternById('nonexistent'), undefined);
  });
});

describe('scanCodeForSecurityPatterns — JS/TS patterns', () => {
  it('detects eval()', () => {
    const findings = scanCodeForSecurityPatterns('const x = eval(userInput);', 'app.ts');
    assert.ok(findings.some(f => f.patternId === 'eval_injection'));
  });

  it('does NOT flag eval() in markdown', () => {
    const findings = scanCodeForSecurityPatterns('Use eval() to run code.', 'README.md');
    assert.ok(!findings.some(f => f.patternId === 'eval_injection'));
  });

  it('does NOT flag model.eval() (method call)', () => {
    const findings = scanCodeForSecurityPatterns('model.eval()', 'train.py');
    assert.ok(!findings.some(f => f.patternId === 'eval_injection'));
  });

  it('detects child_process.exec()', () => {
    const findings = scanCodeForSecurityPatterns(
      'child_process.exec(`ls ${dir}`);',
      'server.ts',
    );
    assert.ok(findings.some(f => f.patternId === 'child_process_exec'));
  });

  it('detects execSync()', () => {
    const findings = scanCodeForSecurityPatterns(
      'const out = execSync("npm install");',
      'build.ts',
    );
    assert.ok(findings.some(f => f.patternId === 'child_process_exec'));
  });

  it('does NOT detect child_process.exec in Python files', () => {
    const findings = scanCodeForSecurityPatterns('child_process.exec(cmd)', 'script.py');
    assert.ok(!findings.some(f => f.patternId === 'child_process_exec'));
  });

  it('detects new Function()', () => {
    const findings = scanCodeForSecurityPatterns('const fn = new Function("return " + str);', 'app.js');
    assert.ok(findings.some(f => f.patternId === 'new_function_injection'));
  });

  it('detects innerHTML assignment', () => {
    const findings = scanCodeForSecurityPatterns('element.innerHTML = userHtml;', 'ui.tsx');
    assert.ok(findings.some(f => f.patternId === 'innerHTML_xss'));
  });

  it('detects outerHTML assignment', () => {
    const findings = scanCodeForSecurityPatterns('el.outerHTML = html;', 'component.jsx');
    assert.ok(findings.some(f => f.patternId === 'outerHTML_xss'));
  });

  it('detects dangerouslySetInnerHTML', () => {
    const findings = scanCodeForSecurityPatterns(
      '<div dangerouslySetInnerHTML={{ __html: data }} />',
      'page.tsx',
    );
    assert.ok(findings.some(f => f.patternId === 'dangerously_set_html'));
  });

  it('detects document.write()', () => {
    const findings = scanCodeForSecurityPatterns('document.write(html);', 'legacy.js');
    assert.ok(findings.some(f => f.patternId === 'document_write'));
  });

  it('detects insertAdjacentHTML', () => {
    const findings = scanCodeForSecurityPatterns('el.insertAdjacentHTML("beforeend", html);', 'ui.ts');
    assert.ok(findings.some(f => f.patternId === 'insertAdjacentHTML_xss'));
  });

  it('detects deprecated crypto.createCipher', () => {
    const findings = scanCodeForSecurityPatterns('const cipher = crypto.createCipher("aes-256-cbc", key);', 'crypto.ts');
    assert.ok(findings.some(f => f.patternId === 'deprecated_crypto'));
  });

  it('does NOT flag deprecated crypto in Python', () => {
    const findings = scanCodeForSecurityPatterns('crypto.createCipher()', 'app.py');
    assert.ok(!findings.some(f => f.patternId === 'deprecated_crypto'));
  });

  it('detects AES ECB mode', () => {
    const findings = scanCodeForSecurityPatterns("const cipher = 'aes-256-ecb';", 'crypto.ts');
    assert.ok(findings.some(f => f.patternId === 'aes_ecb_mode'));
  });

  it('detects TLS verification disabled', () => {
    const findings = scanCodeForSecurityPatterns('rejectUnauthorized: false', 'client.ts');
    assert.ok(findings.some(f => f.patternId === 'tls_verify_disabled'));
  });

  it('detects NODE_TLS_REJECT_UNAUTHORIZED=0', () => {
    const findings = scanCodeForSecurityPatterns("process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';", 'config.ts');
    assert.ok(findings.some(f => f.patternId === 'tls_verify_disabled'));
  });
});

describe('scanCodeForSecurityPatterns — Python patterns', () => {
  it('detects os.system()', () => {
    const findings = scanCodeForSecurityPatterns('os.system(f"ls {path}")', 'deploy.py');
    assert.ok(findings.some(f => f.patternId === 'os_system_injection'));
  });

  it('does NOT detect os.system in JS files', () => {
    const findings = scanCodeForSecurityPatterns('os.system(cmd)', 'app.js');
    assert.ok(!findings.some(f => f.patternId === 'os_system_injection'));
  });

  it('detects subprocess with shell=True', () => {
    const findings = scanCodeForSecurityPatterns('subprocess.run(cmd, shell=True)', 'script.py');
    assert.ok(findings.some(f => f.patternId === 'subprocess_shell'));
  });

  it('detects pickle.load()', () => {
    const findings = scanCodeForSecurityPatterns('data = pickle.load(f)', 'model.py');
    assert.ok(findings.some(f => f.patternId === 'pickle_load'));
  });

  it('detects cloudpickle.loads()', () => {
    const findings = scanCodeForSecurityPatterns('obj = cloudpickle.loads(data)', 'worker.py');
    assert.ok(findings.some(f => f.patternId === 'pickle_load'));
  });

  it('detects yaml.load()', () => {
    const findings = scanCodeForSecurityPatterns('config = yaml.load(stream)', 'config.py');
    assert.ok(findings.some(f => f.patternId === 'unsafe_yaml_load'));
  });

  it('detects yaml.unsafe_load()', () => {
    const findings = scanCodeForSecurityPatterns('data = yaml.unsafe_load(f)', 'parse.py');
    assert.ok(findings.some(f => f.patternId === 'unsafe_yaml_load'));
  });

  it('detects torch.load() without weights_only=True', () => {
    const findings = scanCodeForSecurityPatterns('model = torch.load("model.pt")', 'train.py');
    assert.ok(findings.some(f => f.patternId === 'torch_unsafe_load'));
  });

  it('does NOT flag torch.load(path, weights_only=True)', () => {
    const findings = scanCodeForSecurityPatterns('model = torch.load("model.pt", weights_only=True)', 'train.py');
    assert.ok(!findings.some(f => f.patternId === 'torch_unsafe_load'));
  });

  it('detects unsafe XML parsing', () => {
    const findings = scanCodeForSecurityPatterns('tree = ET.parse(xml_file)', 'parse.py');
    assert.ok(findings.some(f => f.patternId === 'xml_unsafe_parse'));
  });

  it('detects verify=False', () => {
    const findings = scanCodeForSecurityPatterns('requests.get(url, verify=False)', 'client.py');
    assert.ok(findings.some(f => f.patternId === 'tls_verify_disabled'));
  });
});

describe('scanCodeForSecurityPatterns — Go patterns', () => {
  it('detects exec.Command with shell', () => {
    const findings = scanCodeForSecurityPatterns('cmd := exec.Command("bash", "-c", userCmd)', 'main.go');
    assert.ok(findings.some(f => f.patternId === 'go_exec_shell'));
  });

  it('does NOT flag exec.Command("bash") in JS files', () => {
    const findings = scanCodeForSecurityPatterns('exec.Command("bash", "-c", cmd)', 'app.ts');
    assert.ok(!findings.some(f => f.patternId === 'go_exec_shell'));
  });
});

describe('scanCodeForSecurityPatterns — line numbers', () => {
  it('reports correct line numbers', () => {
    const code = 'const a = 1;\nconst b = eval(x);\nconst c = 3;';
    const findings = scanCodeForSecurityPatterns(code, 'test.ts');
    assert.equal(findings.length, 1);
    assert.equal(findings[0].line, 2);
  });

  it('reports multiple findings on different lines', () => {
    const code = 'eval(a);\nconst x = 1;\nel.innerHTML = b;';
    const findings = scanCodeForSecurityPatterns(code, 'test.tsx');
    assert.ok(findings.length >= 2);
    const evalF = findings.find(f => f.patternId === 'eval_injection');
    const htmlF = findings.find(f => f.patternId === 'innerHTML_xss');
    assert.ok(evalF);
    assert.ok(htmlF);
    assert.equal(evalF!.line, 1);
    assert.equal(htmlF!.line, 3);
  });
});

describe('scanAddedLinesForPatterns', () => {
  it('only flags newly added dangerous lines', () => {
    const before = 'const a = 1;\nconst b = eval(old);';
    const after = 'const a = 1;\nconst b = eval(old);\nconst c = eval(new_input);';
    const findings = scanAddedLinesForPatterns(before, after, 'app.ts');
    assert.equal(findings.length, 1);
    assert.equal(findings[0].line, 3);
  });

  it('returns empty when no new dangerous lines', () => {
    const before = 'const a = eval(x);';
    const after = 'const a = eval(x);';
    const findings = scanAddedLinesForPatterns(before, after, 'app.ts');
    assert.equal(findings.length, 0);
  });

  it('returns empty when safe lines are added', () => {
    const before = 'const a = 1;';
    const after = 'const a = 1;\nconst b = 2;';
    const findings = scanAddedLinesForPatterns(before, after, 'app.ts');
    assert.equal(findings.length, 0);
  });

  it('respects file extension filtering', () => {
    const before = '';
    const after = 'os.system("ls")';
    const jsFindings = scanAddedLinesForPatterns(before, after, 'app.js');
    const pyFindings = scanAddedLinesForPatterns(before, after, 'app.py');
    assert.ok(!jsFindings.some(f => f.patternId === 'os_system_injection'));
    assert.ok(pyFindings.some(f => f.patternId === 'os_system_injection'));
  });
});

describe('scanCodeForSecurityPatterns — no false positives', () => {
  it('does not flag textContent assignment', () => {
    const findings = scanCodeForSecurityPatterns('el.textContent = text;', 'safe.ts');
    assert.ok(!findings.some(f => f.patternId === 'innerHTML_xss'));
  });

  it('does not flag subprocess.run without shell=True', () => {
    const findings = scanCodeForSecurityPatterns('subprocess.run(["ls", "-la"])', 'safe.py');
    assert.ok(!findings.some(f => f.patternId === 'subprocess_shell'));
  });

  it('does not flag yaml.safe_load', () => {
    const findings = scanCodeForSecurityPatterns('config = yaml.safe_load(stream)', 'config.py');
    assert.ok(!findings.some(f => f.patternId === 'unsafe_yaml_load'));
  });

  it('does not flag execFile (safe alternative)', () => {
    const findings = scanCodeForSecurityPatterns('execFile("command", [arg])', 'server.ts');
    assert.ok(!findings.some(f => f.patternId === 'child_process_exec'));
  });

  it('does not flag exec.Command without shell in Go', () => {
    const findings = scanCodeForSecurityPatterns('cmd := exec.Command("ls", "-la")', 'main.go');
    assert.ok(!findings.some(f => f.patternId === 'go_exec_shell'));
  });
});
