import * as path from 'path';

// PARITY: severity now includes a 'low' tier for weak-but-context-dependent
// findings (insecure randomness, cleartext HTTP, weak hashing). 'low' findings
// are advisory — they flag patterns that are frequently benign but can be
// security-relevant depending on usage.
export type SecuritySeverity = 'high' | 'medium' | 'low';

export interface SecurityPattern {
  id: string;
  regex: RegExp;
  extensions?: string[];
  excludeExtensions?: string[];
  severity: SecuritySeverity;
  message: string;
  // PARITY (security-review.ts:112-123): Claude's finding format is structured —
  // file, line, severity, CATEGORY, description, exploit scenario, and a separate
  // fix RECOMMENDATION. AMI previously collapsed description+fix into `message`
  // only. `category` is a stable machine-readable class slug (e.g. 'xss',
  // 'command_injection', 'hardcoded_secret') and `remediation` is the concrete
  // fix recommendation as its own field.
  category: string;
  remediation: string;
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
    category: 'code_injection',
    message: 'eval() executes arbitrary code — use JSON.parse() for data or ast.literal_eval() for Python literals',
    remediation: 'Use JSON.parse() for data or ast.literal_eval() for Python literals instead of eval().',
  },
  {
    id: 'child_process_exec',
    regex: /(?:child_process\.exec|execSync)\(/,
    extensions: JS_EXTS,
    severity: 'high',
    category: 'command_injection',
    message: 'child_process.exec() enables command injection — use execFile() or spawn() with argument arrays',
    remediation: 'Use execFile() or spawn() with an argument array so the shell never parses interpolated input.',
  },
  {
    id: 'new_function_injection',
    regex: /new\s+Function\s*\(/,
    severity: 'high',
    category: 'code_injection',
    message: 'new Function() with string interpolation is a code injection vulnerability',
    remediation: 'Avoid new Function(); parse data explicitly or use a fixed dispatch table.',
  },
  {
    id: 'innerHTML_xss',
    regex: /\.innerHTML\s*=/,
    severity: 'high',
    category: 'xss',
    message: 'innerHTML assignment with untrusted content enables XSS — use textContent or sanitize with DOMPurify',
    remediation: 'Assign textContent, or sanitize the HTML with DOMPurify before insertion.',
  },
  {
    id: 'outerHTML_xss',
    regex: /\.outerHTML\s*=/,
    severity: 'high',
    category: 'xss',
    message: 'outerHTML assignment is an XSS sink — use textContent or sanitize with DOMPurify',
    remediation: 'Assign textContent, or sanitize the HTML with DOMPurify before insertion.',
  },
  {
    id: 'dangerously_set_html',
    regex: /dangerouslySetInnerHTML/,
    severity: 'medium',
    category: 'xss',
    message: 'dangerouslySetInnerHTML can cause XSS if content is not sanitized',
    remediation: 'Sanitize the HTML with DOMPurify before passing it to dangerouslySetInnerHTML.',
  },
  {
    id: 'document_write',
    regex: /document\.write\(/,
    severity: 'medium',
    category: 'xss',
    message: 'document.write() enables XSS — use DOM manipulation methods instead',
    remediation: 'Build nodes with DOM APIs (createElement/append) instead of document.write().',
  },
  {
    id: 'insertAdjacentHTML_xss',
    regex: /\.insertAdjacentHTML\(/,
    severity: 'medium',
    category: 'xss',
    message: 'insertAdjacentHTML is an XSS sink — use insertAdjacentText() or sanitize with DOMPurify',
    remediation: 'Use insertAdjacentText(), or sanitize the HTML with DOMPurify first.',
  },
  {
    id: 'os_system_injection',
    regex: /\bos\.system\s*\(/,
    extensions: PY_EXTS,
    severity: 'high',
    category: 'command_injection',
    message: 'os.system() runs a shell — use subprocess.run([...]) with argument arrays',
    remediation: 'Use subprocess.run([...]) with an argument list instead of os.system().',
  },
  {
    id: 'subprocess_shell',
    regex: /subprocess\.(?:run|call|Popen|check_output|check_call)\(.*shell\s*=\s*True/,
    extensions: PY_EXTS,
    severity: 'high',
    category: 'command_injection',
    message: 'subprocess with shell=True enables command injection — pass arguments as a list',
    remediation: 'Drop shell=True and pass the command as an argument list.',
  },
  {
    id: 'pickle_load',
    regex: /(?<![a-zA-Z0-9_])(?:pickle|cPickle|cloudpickle|dill)\.(?:loads?|Unpickler)\s*\(/,
    extensions: PY_EXTS,
    severity: 'high',
    category: 'insecure_deserialization',
    message: 'Pickle deserialization from untrusted sources allows arbitrary code execution — use JSON or msgspec',
    remediation: 'Use JSON or msgspec for untrusted data; never unpickle attacker-controlled bytes.',
  },
  {
    id: 'unsafe_yaml_load',
    regex: /\byaml\.(?:load|unsafe_load)\s*\(/,
    extensions: PY_EXTS,
    severity: 'high',
    category: 'insecure_deserialization',
    message: 'yaml.load()/unsafe_load() execute arbitrary Python — use yaml.safe_load()',
    remediation: 'Use yaml.safe_load() instead of yaml.load()/unsafe_load().',
  },
  {
    id: 'torch_unsafe_load',
    regex: /(?:\btorch\.load|\.torch_load)\s*\((?![^)\n]{0,200}weights_only\s*=\s*True)/,
    extensions: PY_EXTS,
    severity: 'high',
    category: 'insecure_deserialization',
    message: 'torch.load() defaults to weights_only=False which unpickles — pass weights_only=True',
    remediation: 'Pass weights_only=True to torch.load() so it never unpickles arbitrary objects.',
  },
  {
    id: 'go_exec_shell',
    regex: /exec\.Command\(\s*"(?:sh|bash|\/bin\/sh|\/bin\/bash)"/,
    extensions: GO_EXTS,
    severity: 'high',
    category: 'command_injection',
    message: 'exec.Command with shell interpreter enables command injection — pass arguments directly',
    remediation: 'Pass the program and its arguments directly to exec.Command, not through a shell.',
  },
  {
    id: 'tls_verify_disabled',
    regex: /\bverify\s*=\s*False\b|rejectUnauthorized\s*:\s*false|InsecureSkipVerify\s*:\s*true|NODE_TLS_REJECT_UNAUTHORIZED\s*=\s*['"]?0/,
    severity: 'medium',
    category: 'tls_validation',
    message: 'Disabling TLS verification allows MITM attacks — add CA to trust store instead',
    remediation: 'Add the CA certificate to the trust store instead of disabling TLS verification.',
  },
  {
    id: 'xml_unsafe_parse',
    regex: /\b(?:xml\.etree\.ElementTree|ElementTree|ET)\.(?:parse|fromstring|XML)\s*\(|\bminidom\.(?:parse|parseString)\s*\(/,
    extensions: PY_EXTS,
    severity: 'medium',
    category: 'xxe',
    message: 'stdlib XML parsers are vulnerable to XXE attacks — use defusedxml',
    remediation: 'Parse XML with the defusedxml package to block XXE/entity-expansion attacks.',
  },
  {
    id: 'deprecated_crypto',
    regex: /\bcrypto\.(?:createCipher|createDecipher)\b/,
    extensions: JS_EXTS,
    severity: 'medium',
    category: 'weak_crypto',
    message: 'createCipher/createDecipher are deprecated and insecure — use createCipheriv/createDecipheriv',
    remediation: 'Use createCipheriv/createDecipheriv with an explicit IV.',
  },
  {
    id: 'aes_ecb_mode',
    regex: /\bAES\.MODE_ECB\b|\bmodes\.ECB\s*\(|['"]aes-\d+-ecb['"]/,
    severity: 'medium',
    category: 'weak_crypto',
    message: 'AES-ECB leaks plaintext structure — use AES-GCM or AES-CBC with HMAC',
    remediation: 'Use AES-GCM, or AES-CBC with a separate HMAC, instead of ECB mode.',
  },
  // PARITY: additional insecure-pattern detectors (additive).
  {
    id: 'python_exec_injection',
    // Python exec() builtin runs arbitrary code; exclude member access (.exec)
    // and longer identifiers (execute/execvp) via the preceding-char lookbehind
    // and the mandatory `(` directly after `exec`.
    regex: /(?<![a-zA-Z0-9_.])exec\s*\(/,
    extensions: PY_EXTS,
    severity: 'high',
    category: 'code_injection',
    message: 'exec() executes arbitrary Python — avoid it, or restrict globals/locals and never pass untrusted input',
    remediation: 'Avoid exec(); if unavoidable, restrict globals/locals and never pass untrusted input.',
  },
  {
    id: 'vm_code_execution',
    regex: /\bvm\.(?:runInThisContext|runInNewContext|runInContext|compileFunction)\s*\(/,
    extensions: JS_EXTS,
    severity: 'high',
    category: 'code_injection',
    message: 'node:vm executes code strings and is not a security sandbox — never run untrusted input through it',
    remediation: 'Never run untrusted input through node:vm; use a real sandbox (separate process/container) for untrusted code.',
  },
  {
    id: 'weak_hash',
    regex: /\bhashlib\.(?:md5|sha1)\s*\(|\bcrypto\.createHash\(\s*['"](?:md5|sha1)['"]/,
    severity: 'medium',
    category: 'weak_crypto',
    message: 'MD5/SHA-1 are broken for security use — use SHA-256+ (or bcrypt/scrypt/argon2 for passwords)',
    remediation: 'Use SHA-256 or stronger (bcrypt/scrypt/argon2 for passwords) instead of MD5/SHA-1.',
  },
  {
    id: 'insecure_random_js',
    regex: /\bMath\.random\s*\(/,
    extensions: JS_EXTS,
    severity: 'low',
    category: 'weak_randomness',
    message: 'Math.random() is not cryptographically secure — use crypto.getRandomValues()/crypto.randomBytes() for tokens or secrets',
    remediation: 'Use crypto.getRandomValues() or crypto.randomBytes() for security-sensitive randomness.',
  },
  {
    id: 'insecure_random_py',
    regex: /(?<![a-zA-Z0-9_.])random\.(?:random|randint|randrange|choice|choices|sample|uniform|getrandbits|shuffle)\s*\(/,
    extensions: PY_EXTS,
    severity: 'low',
    category: 'weak_randomness',
    message: 'the random module is not cryptographically secure — use the secrets module for tokens or secrets',
    remediation: 'Use the secrets module for tokens, passwords, or other security-sensitive randomness.',
  },
  {
    id: 'insecure_http_url',
    regex: /http:\/\/(?!localhost|127\.0\.0\.1|0\.0\.0\.0)/,
    excludeExtensions: DOC_EXTS,
    severity: 'low',
    category: 'cleartext_transport',
    message: 'cleartext http:// URL is subject to interception/tampering — use https://',
    remediation: 'Use an https:// URL so the traffic is encrypted and tamper-evident.',
  },
  // PARITY (security-review.ts:69-74): Claude's security review treats "Hardcoded
  // API keys, passwords, or tokens" as a first-class HIGH-severity category.
  // These two detectors cover (a) well-known provider key *formats* and (b)
  // credential-shaped *assignments* to inline string literals.
  {
    id: 'hardcoded_secret',
    // Known, high-confidence secret token formats: AWS access keys, GitHub PATs,
    // OpenAI/Stripe `sk-`/`sk_live_` keys, Google API keys, Slack tokens, and PEM
    // private-key headers. Each is distinctive enough to flag with high confidence.
    regex: /\bAKIA[0-9A-Z]{16}\b|\bgh[pousr]_[A-Za-z0-9]{36}\b|\b(?:sk|rk)[-_](?:live_|proj-)?[A-Za-z0-9]{20,}\b|\bAIza[0-9A-Za-z_-]{35}\b|\bxox[baprs]-[0-9A-Za-z-]{10,}\b|-----BEGIN (?:RSA |EC |DSA |OPENSSH |PGP )?PRIVATE KEY-----/,
    excludeExtensions: DOC_EXTS,
    severity: 'high',
    category: 'hardcoded_secret',
    message: 'hardcoded secret / API key detected — committed credentials can be extracted from history and abused',
    remediation: 'Move the secret to an environment variable or secrets manager and rotate the exposed credential immediately.',
  },
  {
    id: 'hardcoded_credential',
    // Credential-shaped assignment: a variable named like a password/secret/token/
    // api_key assigned (`=` or `:`) a quoted literal of >=8 chars. Comparisons
    // (`==`), env lookups (`process.env.X`, `os.environ["X"]` — short quoted keys),
    // and unquoted values do not match.
    regex: /(?:password|passwd|pwd|secret|api[_-]?key|apikey|access[_-]?token|auth[_-]?token|token|client[_-]?secret|private[_-]?key)["']?\s*[:=]\s*['"][^'"\n]{8,}['"]/i,
    excludeExtensions: DOC_EXTS,
    severity: 'high',
    category: 'hardcoded_secret',
    message: 'hardcoded credential assignment detected — inline passwords/tokens are exposed in source and version history',
    remediation: 'Load credentials from environment variables or a secrets manager; never inline them as string literals.',
  },
];

export interface SecurityFinding {
  patternId: string;
  severity: SecuritySeverity;
  message: string;
  line: number;
  // PARITY (security-review.ts:112-123): structured per-finding fields carried
  // through from the matched pattern so consumers can render category + fix
  // recommendation as distinct columns (not a single blob).
  category: string;
  remediation: string;
}

function toFinding(pattern: SecurityPattern, line: number): SecurityFinding {
  return {
    patternId: pattern.id,
    severity: pattern.severity,
    message: pattern.message,
    line,
    category: pattern.category,
    remediation: pattern.remediation,
  };
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
          findings.push(toFinding(pattern, i + 1));
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
        findings.push(toFinding(pattern, lineMap[i]));
      }
    }
  }

  return findings;
}

export function getPatternById(id: string): SecurityPattern | undefined {
  return SECURITY_PATTERNS.find(p => p.id === id);
}
