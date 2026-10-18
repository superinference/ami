import * as path from 'path';

export interface SecurityPattern {
  id: string;
  regex: RegExp;
  extensions?: string[];
  excludeExtensions?: string[];
  severity: 'high' | 'medium';
  message: string;
}

const JS_EXTS = ['.js', '.jsx', '.ts', '.tsx', '.mjs', '.cjs', '.mts', '.cts', '.vue', '.svelte'];
const PY_EXTS = ['.py', '.pyi'];
const DOC_EXTS = ['.md', '.mdx', '.txt', '.rst'];
const GO_EXTS = ['.go'];

export const SECURITY_PATTERNS: SecurityPattern[] = [
  {
    id: 'eval_injection',
    regex: /(?<![a-zA-Z0-9_.])eval\(/,
    excludeExtensions: DOC_EXTS,
    severity: 'high',
    message: 'eval() executes arbitrary code — use JSON.parse() for data or ast.literal_eval() for Python literals',
  },
  {
    id: 'child_process_exec',
    regex: /(?:child_process\.exec|execSync)\(/,
    extensions: JS_EXTS,
    severity: 'high',
    message: 'child_process.exec() enables command injection — use execFile() or spawn() with argument arrays',
  },
  {
    id: 'new_function_injection',
    regex: /new\s+Function\s*\(/,
    severity: 'high',
    message: 'new Function() with string interpolation is a code injection vulnerability',
  },
  {
    id: 'innerHTML_xss',
    regex: /\.innerHTML\s*=/,
    severity: 'high',
    message: 'innerHTML assignment with untrusted content enables XSS — use textContent or sanitize with DOMPurify',
  },
  {
    id: 'outerHTML_xss',
    regex: /\.outerHTML\s*=/,
    severity: 'high',
    message: 'outerHTML assignment is an XSS sink — use textContent or sanitize with DOMPurify',
  },
  {
    id: 'dangerously_set_html',
    regex: /dangerouslySetInnerHTML/,
    severity: 'medium',
    message: 'dangerouslySetInnerHTML can cause XSS if content is not sanitized',
  },
  {
    id: 'document_write',
    regex: /document\.write\(/,
    severity: 'medium',
    message: 'document.write() enables XSS — use DOM manipulation methods instead',
  },
  {
    id: 'insertAdjacentHTML_xss',
    regex: /\.insertAdjacentHTML\(/,
    severity: 'medium',
    message: 'insertAdjacentHTML is an XSS sink — use insertAdjacentText() or sanitize with DOMPurify',
  },
  {
    id: 'os_system_injection',
    regex: /\bos\.system\s*\(/,
    extensions: PY_EXTS,
    severity: 'high',
    message: 'os.system() runs a shell — use subprocess.run([...]) with argument arrays',
  },
  {
    id: 'subprocess_shell',
    regex: /subprocess\.(?:run|call|Popen|check_output|check_call)\(.*shell\s*=\s*True/,
    extensions: PY_EXTS,
    severity: 'high',
    message: 'subprocess with shell=True enables command injection — pass arguments as a list',
  },
  {
    id: 'pickle_load',
    regex: /(?<![a-zA-Z0-9_])(?:pickle|cPickle|cloudpickle|dill)\.(?:loads?|Unpickler)\s*\(/,
    extensions: PY_EXTS,
    severity: 'high',
    message: 'Pickle deserialization from untrusted sources allows arbitrary code execution — use JSON or msgspec',
  },
  {
    id: 'unsafe_yaml_load',
    regex: /\byaml\.(?:load|unsafe_load)\s*\(/,
    extensions: PY_EXTS,
    severity: 'high',
    message: 'yaml.load()/unsafe_load() execute arbitrary Python — use yaml.safe_load()',
  },
  {
    id: 'torch_unsafe_load',
    regex: /(?:\btorch\.load|\.torch_load)\s*\((?![^)\n]{0,200}weights_only\s*=\s*True)/,
    extensions: PY_EXTS,
    severity: 'high',
    message: 'torch.load() defaults to weights_only=False which unpickles — pass weights_only=True',
  },
  {
    id: 'go_exec_shell',
    regex: /exec\.Command\(\s*"(?:sh|bash|\/bin\/sh|\/bin\/bash)"/,
    extensions: GO_EXTS,
    severity: 'high',
    message: 'exec.Command with shell interpreter enables command injection — pass arguments directly',
  },
  {
    id: 'tls_verify_disabled',
    regex: /\bverify\s*=\s*False\b|rejectUnauthorized\s*:\s*false|InsecureSkipVerify\s*:\s*true|NODE_TLS_REJECT_UNAUTHORIZED\s*=\s*['"]?0/,
    severity: 'medium',
    message: 'Disabling TLS verification allows MITM attacks — add CA to trust store instead',
  },
  {
    id: 'xml_unsafe_parse',
    regex: /\b(?:xml\.etree\.ElementTree|ElementTree|ET)\.(?:parse|fromstring|XML)\s*\(|\bminidom\.(?:parse|parseString)\s*\(/,
    extensions: PY_EXTS,
    severity: 'medium',
    message: 'stdlib XML parsers are vulnerable to XXE attacks — use defusedxml',
  },
  {
    id: 'deprecated_crypto',
    regex: /\bcrypto\.(?:createCipher|createDecipher)\b/,
    extensions: JS_EXTS,
    severity: 'medium',
    message: 'createCipher/createDecipher are deprecated and insecure — use createCipheriv/createDecipheriv',
  },
  {
    id: 'aes_ecb_mode',
    regex: /\bAES\.MODE_ECB\b|\bmodes\.ECB\s*\(|['"]aes-\d+-ecb['"]/,
    severity: 'medium',
    message: 'AES-ECB leaks plaintext structure — use AES-GCM or AES-CBC with HMAC',
  },
];

export interface SecurityFinding {
  patternId: string;
  severity: 'high' | 'medium';
  message: string;
  line: number;
}

export function scanCodeForSecurityPatterns(
  content: string,
  filePath: string,
): SecurityFinding[] {
  const ext = path.extname(filePath).toLowerCase();
  const findings: SecurityFinding[] = [];

  for (const pattern of SECURITY_PATTERNS) {
    if (pattern.extensions && !pattern.extensions.includes(ext)) continue;
    if (pattern.excludeExtensions && pattern.excludeExtensions.includes(ext)) continue;

    const lines = content.split('\n');
    for (let i = 0; i < lines.length; i++) {
      if (pattern.regex.test(lines[i])) {
        if (!findings.some(f => f.patternId === pattern.id && f.line === i + 1)) {
          findings.push({
            patternId: pattern.id,
            severity: pattern.severity,
            message: pattern.message,
            line: i + 1,
          });
        }
      }
    }
  }

  return findings;
}

export function scanAddedLinesForPatterns(
  before: string,
  after: string,
  filePath: string,
): SecurityFinding[] {
  const beforeLines = new Set(before.split('\n'));
  const afterLines = after.split('\n');
  const addedContent: string[] = [];
  const lineMap: number[] = [];

  for (let i = 0; i < afterLines.length; i++) {
    if (!beforeLines.has(afterLines[i])) {
      addedContent.push(afterLines[i]);
      lineMap.push(i + 1);
    }
  }

  if (addedContent.length === 0) return [];

  const ext = path.extname(filePath).toLowerCase();
  const findings: SecurityFinding[] = [];

  for (const pattern of SECURITY_PATTERNS) {
    if (pattern.extensions && !pattern.extensions.includes(ext)) continue;
    if (pattern.excludeExtensions && pattern.excludeExtensions.includes(ext)) continue;

    for (let i = 0; i < addedContent.length; i++) {
      if (pattern.regex.test(addedContent[i])) {
        findings.push({
          patternId: pattern.id,
          severity: pattern.severity,
          message: pattern.message,
          line: lineMap[i],
        });
      }
    }
  }

  return findings;
}

export function getPatternById(id: string): SecurityPattern | undefined {
  return SECURITY_PATTERNS.find(p => p.id === id);
}
