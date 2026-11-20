/**
 * Dangerous code-execution allow-rule detection.
 *
 * An allow rule like `Bash(python:*)`, `Bash(node:*)`, or `Bash(npx:*)` grants
 * the model arbitrary code execution through that interpreter / package-runner,
 * silently bypassing the command safety classifier — a `python:*` allow rule
 * auto-approves `python -c '<anything>'` without ever flagging it as a
 * code-execution escape hatch.
 *
 * AMI's command classifier labels such interpreters merely `unsafe` (not
 * `destructive`), so the existing `isDestructiveBash` allow-rule downgrade
 * (permissions.ts) never catches them. This module supplies the missing piece:
 * a single canonical list of code-execution entry points plus a rule-shape
 * matcher that recognizes an over-broad code-exec allow-rule prefix, so the
 * permission manager can DOWNGRADE such a matched allow rule to an explicit
 * prompt in the rule-evaluated modes (`ask` / `acceptEdits`) instead of silently
 * auto-approving arbitrary code.
 *
 * Mirrors the reference's `CROSS_PLATFORM_CODE_EXEC` / `DANGEROUS_BASH_PATTERNS`
 * (utils/permissions/dangerousPatterns.ts) and the `isDangerousBashPermission`
 * rule-shape matcher (utils/permissions/permissionSetup.ts).
 *
 * Intentional AMI scoping divergence: the tool-wide `Bash(*)` / empty-content
 * grant is NOT treated as a code-exec prefix here — AMI deliberately honors an
 * explicit full-bash allow rule as a user-chosen autonomy grant (and the blanket
 * `auto-allow` / `bypassPermissions` modes already express "run everything").
 * Only NAMED interpreter / package-runner / shell prefixes are downgraded, which
 * is where a rule silently widens into arbitrary code execution.
 */

/**
 * Cross-platform code-execution entry points present on both Unix and Windows.
 * A single shared source so interpreter additions can't drift between lists.
 */
export const CROSS_PLATFORM_CODE_EXEC: readonly string[] = [
  // Interpreters
  'python',
  'python3',
  'python2',
  'node',
  'deno',
  'tsx',
  'ruby',
  'perl',
  'php',
  'lua',
  // Package runners — each fetches/executes arbitrary package code. `npm run`
  // (etc.) is a SEPARATE entry from bare `npm`: the matcher is exact-shape, not
  // prefix, so `npm` alone must not flag a rule and bare `npm:*` stays allowed
  // while `npm run:*` (runs arbitrary package.json scripts) is caught.
  'npx',
  'bunx',
  'npm run',
  'yarn run',
  'pnpm run',
  'bun run',
  // Shells reachable cross-platform (Git Bash / WSL on Windows, native on Unix)
  'bash',
  'sh',
  // Remote arbitrary-command wrapper
  'ssh',
];

/**
 * Full danger list for Bash allow rules: the cross-platform code-exec entry
 * points plus Unix shells and arg-executing wrappers that run arbitrary code
 * via `-c` / by wrapping any following command.
 *
 * (the reference additionally gates a set of ant-internal tools behind
 * `USER_TYPE === 'ant'`; those — `coo`, `fa run`, and over-allowlisted general
 * tools like `curl`/`git`/`gh`/`aws` — are Anthropic-sandbox-specific empirical
 * risk calls, not universal code-exec, and are intentionally omitted for AMI.)
 */
export const DANGEROUS_BASH_PATTERNS: readonly string[] = [
  ...CROSS_PLATFORM_CODE_EXEC,
  'zsh',
  'fish',
  'eval',
  'exec',
  'env',
  'xargs',
  'sudo',
];

/**
 * Does a Bash allow-rule's content denote an over-broad code-execution prefix?
 *
 * Recognizes the rule-shape variants AMI/the reference produce for a prefix grant
 * (exact, `:*`, trailing `*`, ` *`, ` -…*`) against every {@link
 * DANGEROUS_BASH_PATTERNS} entry. Returns `false` for a NARROW, specific rule
 * (e.g. `python manage.py migrate`, `node server.js`) — only a broad prefix
 * that would auto-approve arbitrary code is flagged.
 *
 * The tool-wide grant (`undefined` / empty / standalone `*`) returns `false`
 * by design (see the module header's scoping note); it is handled by AMI's
 * autonomy modes, not by this per-rule downgrade.
 */
export function isDangerousCodeExecPrefix(ruleContent: string | undefined): boolean {
  if (ruleContent === undefined) return false;
  const content = ruleContent.trim().toLowerCase();
  // Tool-wide / empty grant is intentionally NOT flagged here.
  if (content === '' || content === '*') return false;

  for (const pattern of DANGEROUS_BASH_PATTERNS) {
    const p = pattern.toLowerCase();
    // Exact prefix as a bare rule: `python`
    if (content === p) return true;
    // Prefix syntax: `python:*`
    if (content === `${p}:*`) return true;
    // Trailing wildcard: `python*` (matches python, python3, …)
    if (content === `${p}*`) return true;
    // Wildcard with space: `python *` (matches `python script.py`)
    if (content === `${p} *`) return true;
    // Flagged form: `python -*` / `python -c*` (matches `python -c '<code>'`)
    if (content.startsWith(`${p} -`) && content.endsWith('*')) return true;
  }
  return false;
}
