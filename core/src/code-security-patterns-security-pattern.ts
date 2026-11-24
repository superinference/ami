// Split from code-security-patterns.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).

// PARITY: severity now includes a 'low' tier for weak-but-context-dependent
// findings (insecure randomness, cleartext HTTP, weak hashing). 'low' findings
// are advisory — they flag patterns that are frequently benign but can be
// security-relevant depending on usage.
export const SECURITY_SEVERITYS = ['high', 'medium', 'low'] as const;

export type SecuritySeverity = (typeof SECURITY_SEVERITYS)[number];

export interface SecurityPattern {
  id: string;
  regex: RegExp;
  extensions?: string[];
  excludeExtensions?: string[];
  severity: SecuritySeverity;
  message: string;
  // PARITY (security-review.ts:112-123): the reference's finding format is structured —
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

export const JS_EXTS = ['.js', '.jsx', '.ts', '.tsx', '.mjs', '.cjs', '.mts', '.cts', '.vue', '.svelte'];

export const PY_EXTS = ['.py', '.pyi'];

export const DOC_EXTS = ['.md', '.mdx', '.txt', '.rst'];

export const GO_EXTS = ['.go'];

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

export function toFinding(pattern: SecurityPattern, line: number): SecurityFinding {
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

export function isJsxSuppressed(pattern: SecurityPattern, ext: string): boolean {
  return (
    (ext === '.jsx' || ext === '.tsx') &&
    pattern.category === 'xss' &&
    pattern.unsafeMethod !== true
  );
}

export function globalRegex(re: RegExp): RegExp {
  return re.global ? re : new RegExp(re.source, re.flags + 'g');
}

// 0-based line index of a character offset within `content`.
export function lineIndexOfOffset(content: string, offset: number): number {
  let line = 0;
  for (let i = 0; i < offset && i < content.length; i++) {
    if (content[i] === '\n') line++;
  }
  return line;
}

export function pushFinding(
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
export function collectMatches(
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
