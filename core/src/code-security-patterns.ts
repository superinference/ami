import * as path from 'path';

export interface SecurityPattern {
  id: string;
  regex: RegExp;
  extensions?: string[];
  excludeExtensions?: string[];
  severity: 'high' | 'medium' | 'low';
  message: string;
  /** Coarse vulnerability class (e.g. 'xss', 'command-injection', 'secrets'). */
  category?: string;
  /** Explicit fix recommendation, kept separate from the human-readable message. */
  remediation?: string;
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
    category: 'code-injection',
    message: 'eval() executes arbitrary code — use JSON.parse() for data or ast.literal_eval() for Python literals',
    remediation: 'Parse data with JSON.parse() (JS) or ast.literal_eval() (Python) instead of eval().',
  },
  {
    id: 'child_process_exec',
    regex: /(?:child_process\.exec|execSync)\(/,
    extensions: JS_EXTS,
    severity: 'high',
    category: 'command-injection',
    message: 'child_process.exec() enables command injection — use execFile() or spawn() with argument arrays',
    remediation: 'Use execFile()/spawn() with an argument array so no shell interprets the input.',
  },
  {
    id: 'new_function_injection',
    regex: /new\s+Function\s*\(/,
    severity: 'high',
    category: 'code-injection',
    message: 'new Function() with string interpolation is a code injection vulnerability',
    remediation: 'Replace new Function() with a static function or a safe data parser.',
  },
  {
    id: 'innerHTML_xss',
    regex: /\.innerHTML\s*=/,
    severity: 'high',
    category: 'xss',
    message: 'innerHTML assignment with untrusted content enables XSS — use textContent or sanitize with DOMPurify',
    remediation: 'Assign via textContent, or sanitize the HTML with DOMPurify before insertion.',
  },
  {
    id: 'outerHTML_xss',
    regex: /\.outerHTML\s*=/,
    severity: 'high',
    category: 'xss',
    message: 'outerHTML assignment is an XSS sink — use textContent or sanitize with DOMPurify',
    remediation: 'Rebuild the node with safe DOM APIs or sanitize with DOMPurify before assigning.',
  },
  {
    id: 'dangerously_set_html',
    regex: /dangerouslySetInnerHTML/,
    severity: 'medium',
    category: 'xss',
    message: 'dangerouslySetInnerHTML can cause XSS if content is not sanitized',
    remediation: 'Sanitize the value with DOMPurify before passing it to dangerouslySetInnerHTML.',
  },
  {
    id: 'document_write',
    regex: /document\.write\(/,
    severity: 'medium',
    category: 'xss',
    message: 'document.write() enables XSS — use DOM manipulation methods instead',
    remediation: 'Create and append nodes with document.createElement()/appendChild() instead.',
  },
  {
    id: 'insertAdjacentHTML_xss',
    regex: /\.insertAdjacentHTML\(/,
    severity: 'medium',
    category: 'xss',
    message: 'insertAdjacentHTML is an XSS sink — use insertAdjacentText() or sanitize with DOMPurify',
    remediation: 'Use insertAdjacentText() for text, or sanitize with DOMPurify for markup.',
  },
  {
    id: 'os_system_injection',
    regex: /\bos\.system\s*\(/,
    extensions: PY_EXTS,
    severity: 'high',
    category: 'command-injection',
    message: 'os.system() runs a shell — use subprocess.run([...]) with argument arrays',
    remediation: 'Call subprocess.run([...]) with an argument list and shell=False.',
  },
  {
    id: 'subprocess_shell',
    regex: /subprocess\.(?:run|call|Popen|check_output|check_call)\(.*shell\s*=\s*True/,
    extensions: PY_EXTS,
    severity: 'high',
    category: 'command-injection',
    message: 'subprocess with shell=True enables command injection — pass arguments as a list',
    remediation: 'Drop shell=True and pass the command as an argument list.',
  },
  {
    id: 'pickle_load',
    regex: /(?<![a-zA-Z0-9_])(?:pickle|cPickle|cloudpickle|dill)\.(?:loads?|Unpickler)\s*\(/,
    extensions: PY_EXTS,
    severity: 'high',
    category: 'insecure-deserialization',
    message: 'Pickle deserialization from untrusted sources allows arbitrary code execution — use JSON or msgspec',
    remediation: 'Deserialize untrusted data with JSON or msgspec instead of pickle.',
  },
  {
    id: 'unsafe_yaml_load',
    regex: /\byaml\.(?:load|unsafe_load)\s*\(/,
    extensions: PY_EXTS,
    severity: 'high',
    category: 'insecure-deserialization',
    message: 'yaml.load()/unsafe_load() execute arbitrary Python — use yaml.safe_load()',
    remediation: 'Use yaml.safe_load() for untrusted YAML.',
  },
  {
    id: 'torch_unsafe_load',
    regex: /(?:\btorch\.load|\.torch_load)\s*\((?![^)\n]{0,200}weights_only\s*=\s*True)/,
    extensions: PY_EXTS,
    severity: 'high',
    category: 'insecure-deserialization',
    message: 'torch.load() defaults to weights_only=False which unpickles — pass weights_only=True',
    remediation: 'Pass weights_only=True to torch.load() when loading untrusted checkpoints.',
  },
  {
    id: 'go_exec_shell',
    regex: /exec\.Command\(\s*"(?:sh|bash|\/bin\/sh|\/bin\/bash)"/,
    extensions: GO_EXTS,
    severity: 'high',
    category: 'command-injection',
    message: 'exec.Command with shell interpreter enables command injection — pass arguments directly',
    remediation: 'Invoke the target binary directly with exec.Command(bin, args...) rather than a shell.',
  },
  {
    id: 'tls_verify_disabled',
    regex: /\bverify\s*=\s*False\b|rejectUnauthorized\s*:\s*false|InsecureSkipVerify\s*:\s*true|NODE_TLS_REJECT_UNAUTHORIZED\s*=\s*['"]?0/,
    severity: 'medium',
    category: 'insecure-transport',
    message: 'Disabling TLS verification allows MITM attacks — add CA to trust store instead',
    remediation: 'Keep certificate verification on and add the CA certificate to the trust store.',
  },
  {
    id: 'xml_unsafe_parse',
    regex: /\b(?:xml\.etree\.ElementTree|ElementTree|ET)\.(?:parse|fromstring|XML)\s*\(|\bminidom\.(?:parse|parseString)\s*\(/,
    extensions: PY_EXTS,
    severity: 'medium',
    category: 'xxe',
    message: 'stdlib XML parsers are vulnerable to XXE attacks — use defusedxml',
    remediation: 'Parse untrusted XML with defusedxml instead of the stdlib parsers.',
  },
  {
    id: 'deprecated_crypto',
    regex: /\bcrypto\.(?:createCipher|createDecipher)\b/,
    extensions: JS_EXTS,
    severity: 'medium',
    category: 'weak-crypto',
    message: 'createCipher/createDecipher are deprecated and insecure — use createCipheriv/createDecipheriv',
    remediation: 'Switch to createCipheriv()/createDecipheriv() with an explicit IV.',
  },
  {
    id: 'aes_ecb_mode',
    regex: /\bAES\.MODE_ECB\b|\bmodes\.ECB\s*\(|['"]aes-\d+-ecb['"]/,
    severity: 'medium',
    category: 'weak-crypto',
    message: 'AES-ECB leaks plaintext structure — use AES-GCM or AES-CBC with HMAC',
    remediation: 'Use an authenticated mode such as AES-GCM (or AES-CBC with a separate HMAC).',
  },
  {
    id: 'hardcoded_credential',
    regex: /AKIA[0-9A-Z]{16}|\bsk-[A-Za-z0-9]{20,}|\bgh[pousr]_[A-Za-z0-9]{36}|\bxox[baprs]-[A-Za-z0-9-]{10,}|\bAIza[0-9A-Za-z_-]{35}|\bsk_live_[0-9a-zA-Z]{24,}|-----BEGIN (?:RSA |EC |OPENSSH |DSA |PGP )?PRIVATE KEY-----/,
    severity: 'high',
    category: 'secrets',
    message: 'Hardcoded credential detected (recognized API key / token / private-key prefix) — secrets in source are directly exploitable',
    remediation: 'Move the secret to an environment variable or a secrets manager and rotate the exposed value.',
  },
  {
    id: 'hardcoded_secret',
    regex: /(?:api[_-]?key|secret|token|password|passwd|pwd|access[_-]?key|client[_-]?secret|auth[_-]?token|private[_-]?key)\s*[:=]\s*['"][^'"\s$]{8,}['"]/i,
    excludeExtensions: DOC_EXTS,
    severity: 'high',
    category: 'secrets',
    message: 'Hardcoded secret assigned to a credential-like variable — API keys, passwords and tokens must not be committed in source',
    remediation: 'Read the value from an environment variable or secrets manager instead of a literal, and rotate anything already committed.',
  },
  {
    id: 'weak_hash_algorithm',
    regex: /\bhashlib\.(?:md5|sha1)\s*\(|createHash\(\s*['"](?:md5|sha1)['"]\s*\)/,
    severity: 'low',
    category: 'weak-crypto',
    message: 'MD5/SHA-1 are collision-prone and unsuitable for security use — defense-in-depth finding',
    remediation: 'Use SHA-256+ for integrity and a password hash (bcrypt/scrypt/argon2) for credentials.',
  },
];

export interface SecurityFinding {
  patternId: string;
  severity: 'high' | 'medium' | 'low';
  message: string;
  line: number;
  /** Coarse vulnerability class copied from the matched pattern, when set. */
  category?: string;
  /** Explicit fix recommendation copied from the matched pattern, when set. */
  remediation?: string;
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
            category: pattern.category,
            remediation: pattern.remediation,
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
          category: pattern.category,
          remediation: pattern.remediation,
        });
      }
    }
  }

  return findings;
}

export function getPatternById(id: string): SecurityPattern | undefined {
  return SECURITY_PATTERNS.find(p => p.id === id);
}
