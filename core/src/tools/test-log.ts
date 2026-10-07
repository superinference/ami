/**
 * Test-runner logs are mostly passing cases. Keeping them in the tool result
 * fills the prompt, and spilling them under the repo makes them part of the
 * evaluation diff. Callers compact before any spill to disk.
 */

const TEST_RUNNER_RE =
  /\b(go\s+test|gotestsum|cargo\s+test|mvn\s+test|gradlew\s+test|pytest|npm\s+test|yarn\s+test|pnpm\s+test|dotnet\s+test|rspec|phpunit|ctest|make\s+(?:test|tests|check))\b/;

const FAIL_TEXT_RE = /panic|FAIL|Error|syntax error|no such file|undefined/i;
const SUITE_CAP = 12_000;

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

function formatJsonSuite(lines: string[]): string {
  const passCount = lines.filter((l) => actionName(l) === 'pass' && /"Test"\s*:/.test(l)).length;
  const failCount = lines.filter((l) => actionName(l) === 'fail' && /"Test"\s*:/.test(l)).length;
  const body = lines.filter((l) => {
    const action = actionName(l);
    if (action === null) return l.startsWith('[') || FAIL_TEXT_RE.test(l);
    if (action === 'fail') return true;
    if (action === 'output' && FAIL_TEXT_RE.test(l)) return true;
    return false;
  });
  return [`Tests: ${passCount} passed, ${failCount} failed. Showing failing output only.`, ...body].join('\n');
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
    const body = interesting.length > 0 ? interesting.slice(0, 120).join('\n') : '';
    return capSuite(`Passed ${passCount}. Showing failing output only.\n${body}`.trimEnd());
  }
  if (output.length < 8_000) return output;
  const interesting = lines.filter((l) => l.startsWith('[') || FAIL_TEXT_RE.test(l));
  const passCount = lines.filter((l) => /--- PASS:/.test(l)).length;
  if (interesting.length < 3) return output.slice(0, 8_000);
  return capSuite(`Passed ${passCount}. Showing failing output only.\n` + interesting.slice(0, 120).join('\n'));
}

/** Text that may be stored for the next prompt. A suite log never is. */
export function foldToolOutputForPrompt(output: string): string {
  if (!looksLikeSuiteLog(output)) return output;
  return compactTestLog(output);
}
