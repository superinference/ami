/**
 * Test-runner logs are mostly passing cases. Keeping them in the tool result
 * fills the prompt, and spilling them under the repo makes them part of the
 * evaluation diff. Callers compact before any spill to disk.
 */

import { countCharInString } from '../utils/string';
import { validateBoundedIntEnvVar } from '../config';

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
// [parity] Compaction thresholds are operator-tunable env knobs (defaults match
// the former hardcoded magic numbers) so large CI suites — or a debugging run that
// wants more failing context — can widen them without a rebuild. Each is parsed
// through the single canonical `validateBoundedIntEnvVar`, so an absent/mistyped/
// negative value falls back to the default and an over-limit value is capped,
// mirroring Claude's `getMaxOutputLength`/`validateBoundedIntEnvVar`
// (utils/shell/outputLimits.ts) rather than flowing through verbatim.
const SUITE_CAP_DEFAULT = 12_000;
const SIZE_GATE_DEFAULT = 8_000;
const BODY_LINES_DEFAULT = 120;
// Upper bounds: the char caps share Claude's bash output ceiling (150_000); the
// body-line cap is bounded well above any realistic failing-test list.
const CHAR_CAP_UPPER_LIMIT = 150_000;
const BODY_LINES_UPPER_LIMIT = 100_000;

/** Suite-log char cap before a truncation marker is appended (`AMI_TEST_LOG_SUITE_CAP`). */
function suiteCap(): number {
  return validateBoundedIntEnvVar('AMI_TEST_LOG_SUITE_CAP', process.env.AMI_TEST_LOG_SUITE_CAP, {
    default: SUITE_CAP_DEFAULT,
    min: 1,
    max: CHAR_CAP_UPPER_LIMIT,
  }).effective;
}

/** Char threshold above which a non-suite log is compacted (`AMI_TEST_LOG_SIZE_GATE`). */
function sizeGate(): number {
  return validateBoundedIntEnvVar('AMI_TEST_LOG_SIZE_GATE', process.env.AMI_TEST_LOG_SIZE_GATE, {
    default: SIZE_GATE_DEFAULT,
    min: 1,
    max: CHAR_CAP_UPPER_LIMIT,
  }).effective;
}

/** Max failing-output lines kept in the compacted body (`AMI_TEST_LOG_BODY_LINES`). */
function bodyLines(): number {
  return validateBoundedIntEnvVar('AMI_TEST_LOG_BODY_LINES', process.env.AMI_TEST_LOG_BODY_LINES, {
    default: BODY_LINES_DEFAULT,
    min: 1,
    max: BODY_LINES_UPPER_LIMIT,
  }).effective;
}

/**
 * Count the lines elided when `text` is head-sliced at `cap`, +1 (the partial
 * final line), without allocating the dropped tail. Shared by every truncation
 * path so the quantitative marker can never drift between sites. Parity: Claude's
 * `remainingLines = countCharInString(content, '\n', maxOutputLength) + 1`.
 */
function droppedLineCount(text: string, cap: number): number {
  return countCharInString(text, '\n', cap) + 1;
}

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
  const cap = suiteCap();
  if (text.length > cap) {
    // Quantify how much failing output was dropped so the model/caller can decide
    // whether to re-read the full log from disk, instead of an opaque marker.
    // Parity: Claude's formatOutput computes
    // `remainingLines = countCharInString(content, '\n', maxOutputLength) + 1`
    // and emits `... [N lines truncated] ...`. countCharInString's `start`
    // argument counts only the elided tail without allocating a slice.
    return text.slice(0, cap) + `\n...[${droppedLineCount(text, cap)} lines of failing output truncated]`;
  }
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
    const body = interesting.length > 0 ? interesting.slice(0, bodyLines()).join('\n') : '';
    return capSuite(
      `Passed ${passCount}, Failed ${failCount}. Showing failing output only.\n${body}`.trimEnd(),
    );
  }
  const gate = sizeGate();
  if (output.length < gate) return output;
  const interesting = lines.filter((l) => l.startsWith('[') || FAIL_TEXT_RE.test(l));
  const passCount = lines.filter((l) => /--- PASS:/.test(l)).length;
  if (interesting.length < 3) {
    // [parity] Too few interesting lines to build a failing-only view, but the log
    // is over the size gate — head-slice it AND append a quantitative truncation
    // marker so the model knows content was cut and roughly how much, instead of a
    // silent hard slice. Claude's formatOutput never returns a silent cut. When the
    // log is exactly at the gate nothing is actually dropped, so no marker is added.
    return output.length > gate
      ? output.slice(0, gate) + `\n...[${droppedLineCount(output, gate)} lines truncated]`
      : output;
  }
  // [parity] Prefer an accurate runner-reported summary (pytest/jest/vitest/
  // mocha/cargo) when one is present; fall back to the `--- PASS:` tally for
  // raw go-test output that has no summary line.
  const summary = parseTestSummary(output);
  const header = summary
    ? `Tests: ${summary.passed} passed, ${summary.failed} failed${summary.skipped ? `, ${summary.skipped} skipped` : ''}. Showing failing output only.`
    : `Passed ${passCount}. Showing failing output only.`;
  return capSuite(`${header}\n` + interesting.slice(0, bodyLines()).join('\n'));
}

/** Text that may be stored for the next prompt. A suite log never is. */
export function foldToolOutputForPrompt(output: string): string {
  if (!looksLikeSuiteLog(output)) return output;
  return compactTestLog(output);
}
