/**
 * Test-runner logs are mostly passing cases. Keeping them in the tool result
 * fills the prompt, and spilling them under the repo makes them part of the
 * evaluation diff. Callers compact before any spill to disk.
 */

// [parity] Expanded runner set so compaction kicks in for the JS/TS, Python,
// Go, Rust, Elixir, Swift and Dart ecosystems Claude Code recognizes — not just
// the original handful. Word-boundary anchored to avoid matching substrings
// (e.g. `ava` never matches `java`).
const TEST_RUNNER_RE =
  /\b(go\s+test|gotestsum|cargo\s+test|mvn\s+test|gradlew\s+test|pytest|npm\s+test|yarn\s+test|pnpm\s+test|bun\s+test|deno\s+test|dotnet\s+test|swift\s+test|flutter\s+test|mix\s+test|rspec|phpunit|ctest|jest|vitest|mocha|ava|jasmine|ginkgo|bats|tox|nox|behave|cypress|playwright\s+test|make\s+(?:test|tests|check))\b/;

// [parity] Broadened failure markers so TAP (`not ok`), Python tracebacks,
// generic exceptions and the ✕/✗ failure glyphs used by jest/vitest are kept
// when surfacing failing output. `Error` (case-insensitive) already covers
// `AssertionError`/`ValueError`, and `FAIL` already covers `FAILED`.
const FAIL_TEXT_RE =
  /panic|FAIL|Error|Exception|Traceback|not ok|syntax error|no such file|undefined|✕|✗/i;
const SUITE_CAP = 12_000;

// [parity] Recognize the human-readable summary lines emitted by pytest
// (`=== 3 failed, 5 passed, 2 skipped in 1.2s ===`), jest/vitest
// (`Tests: 1 failed, 4 passed, 5 total`), mocha (`5 passing`, `2 failing`,
// `1 pending`) and cargo (`3 passed; 1 failed; 2 ignored`). Maps the vocabulary
// of each runner onto passed/failed/skipped.
const SUMMARY_TOKEN_RE =
  /(\d+)\s+(passed|passing|failed|failing|skipped|pending|ignored|errors?|errored)\b/gi;

export function isTestRunnerCommand(command: string): boolean {
  return TEST_RUNNER_RE.test(command);
}

/**
 * A full-suite log. This matches `go test -json` output even when the
 * command was only `cat reports/go-test-results.json`.
 */
export function looksLikeSuiteLog(output: string): boolean {
  if (!output || output.includes('Showing failing output only')) return false;
  const actions = output.match(/"Action"\s*:\s*"/g);
  if (actions && actions.length >= 8 && /"(?:Test|Package|Elapsed)"/.test(output)) return true;
  const verbose = output.match(/^--- (?:PASS|FAIL):/gm);
  return verbose !== null && verbose.length >= 8;
}

function expandSuiteLines(output: string): string[] {
  const lines: string[] = [];
  for (const line of output.split('\n')) {
    const hits = line.match(/"Action"\s*:\s*"/g);
    if (hits && hits.length > 1) lines.push(...line.split(/(?<=\})\s*(?=\{)/));
    else lines.push(line);
  }
  return lines;
}

function actionName(line: string): string | null {
  const match = line.match(/"Action"\s*:\s*"([^"]*)"/);
  return match ? match[1] : null;
}

function capSuite(text: string): string {
  if (text.length > SUITE_CAP) return text.slice(0, SUITE_CAP) + '\n...[failing output truncated]';
  return text;
}

/**
 * [parity] Parse a test runner's human-readable summary into structured counts.
 * Tallies every recognized token across the output — single-line summaries
 * (pytest, jest/vitest), multi-line tallies (mocha's `passing`/`failing`/
 * `pending`) and per-binary lines (cargo's repeated `test result:`) all sum
 * correctly, since these runners report each count once. Returns null when no
 * summary token is present. `passing`→passed, `failing`/`error`→failed,
 * `pending`/`ignored`→skipped.
 */
export function parseTestSummary(
  output: string,
): { passed: number; failed: number; skipped: number } | null {
  if (!output) return null;
  let passed = 0;
  let failed = 0;
  let skipped = 0;
  let found = false;
  for (const match of output.matchAll(SUMMARY_TOKEN_RE)) {
    const count = parseInt(match[1], 10);
    const kind = match[2].toLowerCase();
    if (kind === 'passed' || kind === 'passing') {
      passed += count;
      found = true;
    } else if (kind === 'failed' || kind === 'failing' || kind.startsWith('error')) {
      failed += count;
      found = true;
    } else if (kind === 'skipped' || kind === 'pending' || kind === 'ignored') {
      skipped += count;
      found = true;
    }
  }
  return found ? { passed, failed, skipped } : null;
}

function formatJsonSuite(lines: string[]): string {
  const passCount = lines.filter((l) => actionName(l) === 'pass' && /"Test"\s*:/.test(l)).length;
  const failCount = lines.filter((l) => actionName(l) === 'fail' && /"Test"\s*:/.test(l)).length;
  // [parity] `go test -json` emits `"Action":"skip"` for skipped tests. Count
  // them so the summary reflects the full pass/fail/skip breakdown rather than
  // silently omitting skips.
  const skipCount = lines.filter((l) => actionName(l) === 'skip' && /"Test"\s*:/.test(l)).length;
  const body = lines.filter((l) => {
    const action = actionName(l);
    if (action === null) return l.startsWith('[') || FAIL_TEXT_RE.test(l);
    if (action === 'fail') return true;
    if (action === 'output' && FAIL_TEXT_RE.test(l)) return true;
    return false;
  });
  const summary =
    skipCount > 0
      ? `Tests: ${passCount} passed, ${failCount} failed, ${skipCount} skipped. Showing failing output only.`
      : `Tests: ${passCount} passed, ${failCount} failed. Showing failing output only.`;
  return [summary, ...body].join('\n');
}

/** Drop passing tests. Keep the failing test text. Never return the suite JSON. */
export function compactTestLog(output: string): string {
  if (!output || output.includes('Showing failing output only')) return output;
  const lines = expandSuiteLines(output);
  const jsonEvents = lines.filter((l) => actionName(l) !== null);
  if (jsonEvents.length >= 8) return capSuite(formatJsonSuite(lines));
  if (looksLikeSuiteLog(output)) {
    const interesting = lines.filter((l) => l.startsWith('[') || FAIL_TEXT_RE.test(l));
    const passCount = lines.filter((l) => /^--- PASS:/.test(l)).length;
    // [parity] Also report the failing count from `--- FAIL:` markers, so the
    // verbose (non-JSON) go-test summary carries a pass/fail breakdown instead
    // of only a pass tally.
    const failCount = lines.filter((l) => /^--- FAIL:/.test(l)).length;
    const body = interesting.length > 0 ? interesting.slice(0, 120).join('\n') : '';
    return capSuite(
      `Passed ${passCount}, Failed ${failCount}. Showing failing output only.\n${body}`.trimEnd(),
    );
  }
  if (output.length < 8_000) return output;
  const interesting = lines.filter((l) => l.startsWith('[') || FAIL_TEXT_RE.test(l));
  const passCount = lines.filter((l) => /--- PASS:/.test(l)).length;
  if (interesting.length < 3) return output.slice(0, 8_000);
  // [parity] Prefer an accurate runner-reported summary (pytest/jest/vitest/
  // mocha/cargo) when one is present; fall back to the `--- PASS:` tally for
  // raw go-test output that has no summary line.
  const summary = parseTestSummary(output);
  const header = summary
    ? `Tests: ${summary.passed} passed, ${summary.failed} failed${summary.skipped ? `, ${summary.skipped} skipped` : ''}. Showing failing output only.`
    : `Passed ${passCount}. Showing failing output only.`;
  return capSuite(`${header}\n` + interesting.slice(0, 120).join('\n'));
}

/** Text that may be stored for the next prompt. A suite log never is. */
export function foldToolOutputForPrompt(output: string): string {
  if (!looksLikeSuiteLog(output)) return output;
  return compactTestLog(output);
}
