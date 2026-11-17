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
  // PARITY (security-review.ts:112-123 "Exploit Scenario"): an optional concrete
  // attack narrative describing how the pattern is abused. Surfaced per-finding
  // and through the `/security-patterns` + `--introspect` surfaces so the model
  // and the user see WHY a match matters, not just that it matched.
  exploit?: string;
  // PARITY (security-review.ts:140-160 "multi-line cross-boundary"): when true the
  // pattern is tested against the whole (comment-stripped) file buffer with a
  // newline-spanning regex instead of line-by-line, so a construct split across
  // lines (e.g. an SQL string concatenated over several lines then passed to
  // execute()) is still caught. The matched offset is mapped back to a line.
  multiline?: boolean;
  // PARITY (security-review.ts PRECEDENTS #6): React/Angular/tsx are considered
  // XSS-safe unless an explicitly unsafe method is used. XSS-category patterns
  // that are NOT an unsafe-method form (e.g. a `javascript:` URI in an attribute)
  // are suppressed in `.jsx`/`.tsx` files. Unsafe DOM/React sinks (innerHTML,
  // dangerouslySetInnerHTML, document.write, …) set `unsafeMethod: true` so they
  // still fire there.
  unsafeMethod?: boolean;
}

const JS_EXTS = ['.js', '.jsx', '.ts', '.tsx', '.mjs', '.cjs', '.mts', '.cts', '.vue', '.svelte'];
const PY_EXTS = ['.py', '.pyi'];
const DOC_EXTS = ['.md', '.mdx', '.txt', '.rst'];
const GO_EXTS = ['.go'];

// Extensions whose line comments start with `#` (vs the `//` default). Used by
// the comment-stripping pre-filter so an in-comment match is never reported.
const HASH_COMMENT_EXTS = new Set([
  ...PY_EXTS, '.sh', '.bash', '.zsh', '.rb', '.yml', '.yaml', '.toml',
  '.ini', '.cfg', '.conf', '.pl', '.r', '.tf',
]);

export const SECURITY_PATTERNS: SecurityPattern[] = [
  {
    id: 'eval_injection',
    regex: /(?<![a-zA-Z0-9_.])eval\(/,
    excludeExtensions: DOC_EXTS,
    severity: 'high',
    category: 'code_injection',
    message: 'eval() executes arbitrary code — use JSON.parse() for data or ast.literal_eval() for Python literals',
    remediation: 'Use JSON.parse() for data or ast.literal_eval() for Python literals instead of eval().',
    exploit: 'An attacker who controls the string passed to eval() runs arbitrary code in the process, gaining full RCE.',
  },
  {
    id: 'child_process_exec',
    regex: /(?:child_process\.exec|execSync)\(/,
    extensions: JS_EXTS,
    severity: 'high',
    category: 'command_injection',
    message: 'child_process.exec() enables command injection — use execFile() or spawn() with argument arrays',
    remediation: 'Use execFile() or spawn() with an argument array so the shell never parses interpolated input.',
    exploit: 'User input interpolated into the command string lets an attacker inject shell metacharacters (e.g. `; rm -rf /`) and run arbitrary commands.',
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
    unsafeMethod: true,
  },
  {
    id: 'outerHTML_xss',
    regex: /\.outerHTML\s*=/,
    severity: 'high',
    category: 'xss',
    message: 'outerHTML assignment is an XSS sink — use textContent or sanitize with DOMPurify',
    remediation: 'Assign textContent, or sanitize the HTML with DOMPurify before insertion.',
    unsafeMethod: true,
  },
  {
    id: 'dangerously_set_html',
    regex: /dangerouslySetInnerHTML/,
    severity: 'medium',
    category: 'xss',
    message: 'dangerouslySetInnerHTML can cause XSS if content is not sanitized',
    remediation: 'Sanitize the HTML with DOMPurify before passing it to dangerouslySetInnerHTML.',
    unsafeMethod: true,
  },
  {
    id: 'document_write',
    regex: /document\.write\(/,
    severity: 'medium',
    category: 'xss',
    message: 'document.write() enables XSS — use DOM manipulation methods instead',
    remediation: 'Build nodes with DOM APIs (createElement/append) instead of document.write().',
    unsafeMethod: true,
  },
  {
    id: 'insertAdjacentHTML_xss',
    regex: /\.insertAdjacentHTML\(/,
    severity: 'medium',
    category: 'xss',
    message: 'insertAdjacentHTML is an XSS sink — use insertAdjacentText() or sanitize with DOMPurify',
    remediation: 'Use insertAdjacentText(), or sanitize the HTML with DOMPurify first.',
    unsafeMethod: true,
  },
  // PARITY (security-review.ts PRECEDENTS #6): a `javascript:` URI in an href/src/
  // action attribute is an XSS vector, but it is NOT an unsafe-method form, so it
  // is suppressed in React (.jsx/.tsx) per Claude's precedent while still firing
  // in plain HTML/JS/TS.
  {
    id: 'javascript_uri_xss',
    regex: /\b(?:href|src|xlink:href|formaction|action)\s*[:=]\s*["'`]?\s*javascript:/i,
    severity: 'medium',
    category: 'xss',
    message: 'javascript: URI in a link/attribute executes script when activated — use a safe scheme or sanitize the URL',
    remediation: 'Reject or sanitize URLs so only http(s)/mailto/relative schemes are allowed; never emit a javascript: URI.',
    unsafeMethod: false,
    exploit: 'An attacker supplies a `javascript:` URL that runs script in the victim\'s session when the link is clicked.',
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
  // PARITY (security-review.ts:62-67 "SQL injection via unsanitized user input"):
  // an SQL/NoSQL string built by f-string, template-literal interpolation, or
  // string concatenation and handed to execute()/query()/raw(). Multi-line so a
  // statement concatenated across several lines is still caught.
  {
    id: 'sql_injection',
    regex: /\b(?:execute|executemany|executescript|query|raw)\s*\(\s*(?:f["']|`[^`]*\$\{|["'][\s\S]*?["']\s*\.\s*format\s*\(|["'][\s\S]*?["']\s*[%+]|[A-Za-z_][\w.]*\s*[%+]\s*["'`])/,
    multiline: true,
    severity: 'high',
    category: 'sql_injection',
    message: 'SQL/NoSQL query built via string interpolation or concatenation enables injection — use parameterized queries',
    remediation: 'Use parameterized queries / bound parameters (e.g. execute(sql, params)); never interpolate user input into the query string.',
    exploit: 'An attacker injects SQL syntax through the interpolated value (e.g. `\' OR 1=1 --`) to read, modify, or destroy database rows.',
  },
  // PARITY (security-review.ts:62-67 "Path traversal in file operations"): a file
  // operation whose argument contains a `../` (or `..\`) relative-traversal
  // segment — the concrete, high-confidence traversal smell.
  {
    id: 'path_traversal',
    regex: /\b(?:open|readFile|readFileSync|createReadStream|createWriteStream|writeFile|writeFileSync|appendFile|sendFile|unlink|readlink|realpath)\s*\([^)\n]*\.\.[\\/]/,
    excludeExtensions: DOC_EXTS,
    severity: 'high',
    category: 'path_traversal',
    message: 'file operation with a ../ traversal segment enables path traversal — resolve and confine the path to a base directory',
    remediation: 'Resolve the path and verify it stays within an allow-listed base directory (e.g. path.resolve + startsWith check) before opening it.',
    exploit: 'An attacker supplies `../../etc/passwd`-style input so the file operation escapes the intended directory and reads or writes arbitrary files.',
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
    exploit: 'Anyone with read access to the source (or its git history) extracts the live credential and impersonates the service.',
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
    exploit: 'The inline credential leaks to anyone reading the repository or its history, who can then authenticate as the owner.',
  },
];

export interface SecurityFinding {
  patternId: string;
  severity: SecuritySeverity;
  message: string;
  line: number;
  // PARITY (security-review.ts:112-123): structured per-finding fields carried
  // through from the matched pattern so consumers can render category + fix
  // recommendation (+ the optional exploit narrative) as distinct columns.
  category: string;
  remediation: string;
  exploit?: string;
}

function toFinding(pattern: SecurityPattern, line: number): SecurityFinding {
  const finding: SecurityFinding = {
    patternId: pattern.id,
    severity: pattern.severity,
    message: pattern.message,
    line,
    category: pattern.category,
    remediation: pattern.remediation,
  };
  if (pattern.exploit) finding.exploit = pattern.exploit;
  return finding;
}

/**
 * PARITY (security-review.ts FALSE POSITIVE FILTERING #11): findings in files
 * that are only unit/integration tests are not reported. Matches `*.test.*` /
 * `*.spec.*` suffixes, `__tests__`/`test`/`tests` directories, and Python
 * `test_*.py`/`conftest.py` conventions.
 */
export function isTestFile(filePath: string): boolean {
  const normalized = filePath.replace(/\\/g, '/');
  return (
    /\.(test|spec)\.(?:js|jsx|ts|tsx|mjs|cjs|mts|cts|py|go|rb|rs|java|php)$/i.test(normalized) ||
    /(?:^|\/)(?:__tests__|__mocks__|tests?|spec)\//.test(normalized) ||
    /(?:^|\/)test_[^/]+\.py$/.test(normalized) ||
    /(?:^|\/)conftest\.py$/.test(normalized)
  );
}

/**
 * PARITY (security-review.ts PRECEDENTS: "Do not report findings in comments"):
 * returns the index at which a line comment starts, or the line length if there
 * is none. String literals are tracked so a comment marker inside a quoted string
 * (e.g. the `//` in `"http://x"`) is not treated as a comment.
 */
function commentStart(line: string, ext: string): number {
  const hash = HASH_COMMENT_EXTS.has(ext);
  let quote: string | null = null;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (quote) {
      if (c === '\\') { i++; continue; }
      if (c === quote) quote = null;
      continue;
    }
    if (c === '"' || c === "'" || c === '`') { quote = c; continue; }
    if (hash) {
      if (c === '#') return i;
    } else if (c === '/' && line[i + 1] === '/') {
      return i;
    }
  }
  return line.length;
}

// Drop the (line-)comment tail so an in-comment match is never scanned. The code
// portion length is preserved via the original slice, keeping line structure
// intact when the per-line results are rejoined for multi-line matching.
function stripComment(line: string, ext: string): string {
  const idx = commentStart(line, ext);
  return idx === line.length ? line : line.slice(0, idx);
}

function isJsxSuppressed(pattern: SecurityPattern, ext: string): boolean {
  return (
    (ext === '.jsx' || ext === '.tsx') &&
    pattern.category === 'xss' &&
    pattern.unsafeMethod !== true
  );
}

function globalRegex(re: RegExp): RegExp {
  return re.global ? re : new RegExp(re.source, re.flags + 'g');
}

// 0-based line index of a character offset within `content`.
function lineIndexOfOffset(content: string, offset: number): number {
  let line = 0;
  for (let i = 0; i < offset && i < content.length; i++) {
    if (content[i] === '\n') line++;
  }
  return line;
}

function pushFinding(
  findings: SecurityFinding[],
  seen: Set<string>,
  pattern: SecurityPattern,
  line: number,
): void {
  const key = `${pattern.id}:${line}`;
  if (seen.has(key)) return;
  seen.add(key);
  findings.push(toFinding(pattern, line));
}

// Run one pattern over a prepared (comment-stripped) code view and collect the
// matching lines. `candidateLines` is the 0-based set of line indices eligible
// for single-line patterns (all lines for a full scan, only added lines for a
// diff scan); multi-line patterns are scoped by `multilineLineFilter`.
function collectMatches(
  pattern: SecurityPattern,
  codeLines: string[],
  codeContent: string,
  candidateLines: Iterable<number>,
  multilineLineFilter: (lineIdx0: number) => boolean,
  findings: SecurityFinding[],
  seen: Set<string>,
): void {
  if (pattern.multiline) {
    const re = globalRegex(pattern.regex);
    let m: RegExpExecArray | null;
    while ((m = re.exec(codeContent)) !== null) {
      const idx0 = lineIndexOfOffset(codeContent, m.index);
      if (multilineLineFilter(idx0)) pushFinding(findings, seen, pattern, idx0 + 1);
      if (m.index === re.lastIndex) re.lastIndex++;
    }
    return;
  }
  for (const idx0 of candidateLines) {
    if (pattern.regex.test(codeLines[idx0])) {
      pushFinding(findings, seen, pattern, idx0 + 1);
    }
  }
}

function patternAppliesTo(pattern: SecurityPattern, ext: string): boolean {
  if (pattern.extensions && !pattern.extensions.includes(ext)) return false;
  if (pattern.excludeExtensions && pattern.excludeExtensions.includes(ext)) return false;
  if (isJsxSuppressed(pattern, ext)) return false;
  return true;
}

export function scanCodeForSecurityPatterns(
  content: string,
  filePath: string,
): SecurityFinding[] {
  if (isTestFile(filePath)) return [];
  const ext = path.extname(filePath).toLowerCase();
  const findings: SecurityFinding[] = [];
  const seen = new Set<string>();

  // Hoisted once (not per pattern): split + comment-strip the whole file.
  const codeLines = content.split('\n').map(l => stripComment(l, ext));
  const codeContent = codeLines.join('\n');
  const allLineIndices: number[] = codeLines.map((_, i) => i);

  for (const pattern of SECURITY_PATTERNS) {
    if (!patternAppliesTo(pattern, ext)) continue;
    collectMatches(pattern, codeLines, codeContent, allLineIndices, () => true, findings, seen);
  }

  return findings;
}

/**
 * PARITY (security-review.ts "added lines diff"): identifies the lines newly
 * introduced by a change using a positional LCS line diff — not a Set-membership
 * heuristic — so a dangerous line is flagged even when identical text already
 * existed elsewhere (duplicate/boilerplate), and so line numbers are exact.
 * Returns the 0-based indices (into `after`) of added lines, in ascending order.
 */
export function computeAddedLineIndices(before: string, after: string): number[] {
  const a = before.split('\n');
  const b = after.split('\n');
  const n = a.length;
  const m = b.length;
  // dp[i][j] = length of the LCS of a[i:] and b[j:].
  const dp: number[][] = Array.from({ length: n + 1 }, () => new Array<number>(m + 1).fill(0));
  for (let i = n - 1; i >= 0; i--) {
    for (let j = m - 1; j >= 0; j--) {
      dp[i][j] = a[i] === b[j] ? dp[i + 1][j + 1] + 1 : Math.max(dp[i + 1][j], dp[i][j + 1]);
    }
  }
  const added: number[] = [];
  let i = 0;
  let j = 0;
  while (i < n && j < m) {
    if (a[i] === b[j]) {
      i++;
      j++;
    } else if (dp[i + 1][j] >= dp[i][j + 1]) {
      i++; // line removed from `before`
    } else {
      added.push(j); // line present only in `after`
      j++;
    }
  }
  while (j < m) {
    added.push(j);
    j++;
  }
  return added;
}

export function scanAddedLinesForPatterns(
  before: string,
  after: string,
  filePath: string,
): SecurityFinding[] {
  if (isTestFile(filePath)) return [];
  const added = computeAddedLineIndices(before, after);
  if (added.length === 0) return [];
  const addedSet = new Set(added);

  const ext = path.extname(filePath).toLowerCase();
  const findings: SecurityFinding[] = [];
  const seen = new Set<string>();

  const codeLines = after.split('\n').map(l => stripComment(l, ext));
  const codeContent = codeLines.join('\n');

  for (const pattern of SECURITY_PATTERNS) {
    if (!patternAppliesTo(pattern, ext)) continue;
    collectMatches(
      pattern,
      codeLines,
      codeContent,
      added,
      idx0 => addedSet.has(idx0),
      findings,
      seen,
    );
  }

  return findings;
}

export function getPatternById(id: string): SecurityPattern | undefined {
  return SECURITY_PATTERNS.find(p => p.id === id);
}
