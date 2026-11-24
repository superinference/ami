// Split from tool-utils.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as path from 'path';
import * as fs from 'fs';
import type { ToolCallContent } from '../types';
import { _toolUtilsHooks, validateFilePath } from './tool-utils-resolve-deepest-existing-ancestor';
import type { ToolDescriptionContext } from './tool-utils-resolve-deepest-existing-ancestor';
import { VCS_MARKERS } from './tool-utils-resolve-file-path';

/**
 * Detect the version-control system(s) governing `dir` (default cwd). Git is
 * found by walking ancestors synchronously (so being inside any subdirectory of a
 * repo still reports `git`, mirroring the reference's `findGitRoot`); Mercurial/
 * SVN/Perforce/Jujutsu/Sapling are detected by their marker in `dir`; Perforce is
 * additionally inferred from the `P4PORT` env knob. Returns the detected VCS
 * names (possibly several, e.g. a git + perforce hybrid). Mirrors the reference's
 * `detectVcs(dir?)`.
 */
export function detectVcs(dir: string = process.cwd()): string[] {
  const detected = new Set<string>();
  if (process.env.P4PORT) detected.add('perforce');
  // Git: walk ancestors until a `.git` dir/file is found (worktrees/submodules
  // use a `.git` FILE), stopping at the filesystem root.
  let current = path.resolve(dir);
  for (;;) {
    try {
      const st = fs.statSync(path.join(current, '.git'));
      if (st.isDirectory() || st.isFile()) { detected.add('git'); break; }
    } catch { /* no .git at this level — keep walking up */ }
    const parent = path.dirname(current);
    if (parent === current) break;
    current = parent;
  }
  for (const [marker, name] of VCS_MARKERS) {
    try { if (fs.existsSync(path.join(dir, marker))) detected.add(name); } catch { /* unreadable */ }
  }
  return [...detected];
}

export function buildToolDescriptionContext(cwd: string): ToolDescriptionContext {
  const ctx: ToolDescriptionContext = { cwd, projectName: path.basename(cwd) };
  // Walk ancestors for git so a subdirectory of a repo still reports hasGit (the
  // old cwd-only existsSync check missed that). detectVcs swallows its own fs
  // errors, so no extra guard is needed here.
  ctx.hasGit = detectVcs(cwd).includes('git');
  try { ctx.hasPackageJson = _toolUtilsHooks.existsSync(path.join(cwd, 'package.json')); } catch { ctx.hasPackageJson = false; }
  return ctx;
}

export function renderToolDescription(
  template: string,
  context: ToolDescriptionContext,
): string {
  return template
    .replace(/\{\{cwd\}\}/g, context.cwd)
    .replace(/\{\{projectName\}\}/g, context.projectName ?? '')
    .replace(/\{\{#hasGit\}\}([\s\S]*?)\{\{\/hasGit\}\}/g, context.hasGit ? '$1' : '')
    .replace(/\{\{#hasPackageJson\}\}([\s\S]*?)\{\{\/hasPackageJson\}\}/g, context.hasPackageJson ? '$1' : '');
}

// ---------------------------------------------------------------------------
// Secret scanner — checks content for leaked credentials before writes.
//
// Detection (scanForSecrets) and redaction (redactSecrets) derive from ONE
// curated rule source (SECRET_RULES) so they can never drift: a pattern added
// for detection is automatically redacted, and vice-versa. Detection results
// (SecretMatch) carry only a stable rule id + human label and NEVER the matched
// bytes, so warnings and logs can name what leaked without echoing the secret.
// ---------------------------------------------------------------------------

/**
 * A curated secret-detection rule. Capture group 1 is the secret token itself;
 * any surrounding boundary characters stay OUTSIDE the group so redaction can
 * replace the secret while preserving the surrounding text.
 */
export interface SecretRule {
  /** Stable kebab-case id, surfaced in warnings/labels (never the secret bytes). */
  id: string;
  /** Regex source; capture group 1 = the secret token to redact. */
  source: string;
  /** Optional JS regex flags (defaults to case-sensitive). */
  flags?: string;
}

/**
 * A secret detection result. Intentionally carries only a rule id + human label
 * and NEVER the matched bytes, so callers can name what leaked without echoing
 * the credential itself.
 */
export interface SecretMatch {
  /** The rule that fired, e.g. `aws-access-token`. */
  ruleId: string;
  /** Human-readable label, e.g. `AWS Access Token`. */
  label: string;
}

export const SECRET_RULES: SecretRule[] = [
  // OpenAI project / service-account / admin keys carry a distinctive `T3BlbkFJ`
  // infix — match BEFORE the generic `sk-` rule so they label precisely.
  // PARITY ADD: sk-proj-/sk-svcacct-/sk-admin- scoped keys.
  { id: 'openai-project-key', source: '(?:^|[^a-zA-Z0-9])(sk-(?:proj|svcacct|admin)-[A-Za-z0-9_-]{20,}T3BlbkFJ[A-Za-z0-9_-]{20,})' },
  // OpenAI standard (non-scoped) user API key — the `sk-<20>T3BlbkFJ<20>` form
  // whose mandatory `T3BlbkFJ` infix is the structural anchor the reference's
  // secretScanner uses for near-zero-false-positive OpenAI detection (the second
  // alternative of its `openai-api-key` rule). PARITY ADD: matched BEFORE the
  // generic `sk-` rule so a modern user key is identified by its structure, not
  // only by the broad prefix heuristic. The generic rule below is kept as AMI's
  // wider defensive net (superset) for legacy/opaque `sk-` tokens.
  { id: 'openai-user-key', source: '(?:^|[^a-zA-Z0-9])(sk-[a-zA-Z0-9]{20}T3BlbkFJ[a-zA-Z0-9]{20})' },
  // Anthropic `sk-ant-…` — body contains `-`/`_`, so must precede the generic sk- rule.
  { id: 'anthropic-api-key', source: '(?:^|[^a-zA-Z0-9])(sk-ant-[a-zA-Z0-9_-]{20,})' },
  { id: 'openai-api-key', source: '(?:^|[^a-zA-Z0-9])(sk-[a-zA-Z0-9]{20,})' },
  { id: 'github-pat', source: '(?:^|[^a-zA-Z0-9])(ghp_[a-zA-Z0-9]{36})' },
  // GitHub fine-grained PAT (82-char underscore-bearing body) — before the generic gh* rule.
  { id: 'github-fine-grained-pat', source: '(?:^|[^a-zA-Z0-9])(github_pat_[A-Za-z0-9_]{70,})' },
  { id: 'github-app-token', source: '(?:^|[^a-zA-Z0-9])(gh[osur]_[a-zA-Z0-9]{36,})' },
  { id: 'groq-api-key', source: '(?:^|[^a-zA-Z0-9])(gsk_[a-zA-Z0-9]{20,})' },
  // AWS access key IDs — PARITY ADD: cover session/temporary creds (ASIA = STS
  // temp credentials) and the A3T…/ABIA/ACCA prefixes, not just long-lived AKIA.
  { id: 'aws-access-token', source: '(?:^|[^a-zA-Z0-9])((?:A3T[A-Z0-9]|AKIA|ASIA|ABIA|ACCA)[A-Z2-7]{16})' },
  { id: 'gcp-api-key', source: '(?:^|[^a-zA-Z0-9])(AIza[a-zA-Z0-9_-]{35})' },
  // Hugging Face user access tokens (`hf_` + 34 letters). PARITY ADD — matches
  // the reference's huggingface-access-token rule; AI-model credentials leak here too.
  { id: 'huggingface-access-token', source: '(?:^|[^a-zA-Z0-9])(hf_[a-zA-Z]{34})' },
  // DigitalOcean personal-access / OAuth tokens (`dop_v1_`/`doo_v1_` + 64 hex).
  // PARITY ADD — curated-provider breadth (the reference SECRET_RULES).
  { id: 'digitalocean-pat', source: '(?:^|[^a-zA-Z0-9])(dop_v1_[a-f0-9]{64})' },
  { id: 'digitalocean-access-token', source: '(?:^|[^a-zA-Z0-9])(doo_v1_[a-f0-9]{64})' },
  // GCP / Vertex AI service-account identities (`<name>@<project>.iam.gserviceaccount.com`).
  // These name a privileged service identity; redacting + detecting them keeps a
  // leaked service-account email out of shared transcripts (the reference parity).
  { id: 'gcp-service-account', source: '(?:^|[^a-zA-Z0-9])([a-z0-9-]+@[a-z0-9-]+\\.iam\\.gserviceaccount\\.com)' },
  // Google OAuth access/refresh tokens (`ya29.<token>`).
  { id: 'google-oauth-token', source: '(?:^|[^a-zA-Z0-9])(ya29\\.[0-9A-Za-z_-]{20,})' },
  // Stripe secret / restricted keys (`sk_live_`, `rk_test_`, …) — underscore-delimited.
  { id: 'stripe-access-token', source: '(?:^|[^a-zA-Z0-9])([rs]k_(?:live|test)_[0-9a-zA-Z]{20,})' },
  // SendGrid API keys (`SG.<22>.<43>`).
  { id: 'sendgrid-api-token', source: '(?:^|[^a-zA-Z0-9])(SG\\.[A-Za-z0-9_-]{22}\\.[A-Za-z0-9_-]{43})' },
  { id: 'gitlab-token', source: '(?:^|[^a-zA-Z0-9])(gl(?:pat|dt|rt|oas|ptt|agent|imt|soat|cbt|ft|ffct)-[a-zA-Z0-9_-]{20,})' },
  { id: 'slack-token', source: '(?:^|[^a-zA-Z0-9])(xox[bpsar]-[a-zA-Z0-9-]{10,})' },
  // JWT — the signature (3rd) segment is optional so both scan + redact catch it.
  { id: 'jwt', source: '(?:^|[^a-zA-Z0-9])(eyJ[a-zA-Z0-9_-]{20,}\\.eyJ[a-zA-Z0-9_-]{20,}(?:\\.[a-zA-Z0-9_-]+)?)' },
  { id: 'npm-access-token', source: '(?:^|[^a-zA-Z0-9])(npm_[a-zA-Z0-9]{36,})' },
  { id: 'pypi-upload-token', source: '(?:^|[^a-zA-Z0-9])(pypi-[a-zA-Z0-9]{50,})' },
  // PARITY ADD (curated-provider breadth, the reference SECRET_RULES). Each new rule
  // keeps the secret token in capture group 1 (so redactSecrets replaces ONLY the
  // token, preserving boundary chars) and adopts the reference's trailing boundary
  // alternation `(?:[\x60'"\s;]|\\[nr]|$)` — which treats a literal escaped newline
  // (the two-char `\n`/`\r` inside a serialized JSON/quoted string) as a valid
  // terminator, catching secrets embedded in minified/escaped content.
  // Azure AD client secret — distinctive `\dQ~` infix (very low false-positive).
  { id: 'azure-ad-client-secret', source: '(?:^|[\\\\\'"\\x60\\s>=:(,)])([a-zA-Z0-9_~.]{3}\\dQ~[a-zA-Z0-9_~.-]{31,34})(?:$|[\\\\\'"\\x60\\s<),])' },
  // Twilio API key — `SK` + 32 hex; negative lookahead bounds a longer hex run.
  { id: 'twilio-api-key', source: '(?:^|[^a-zA-Z0-9])(SK[0-9a-fA-F]{32})(?![0-9a-fA-F])' },
  // Databricks personal access token — `dapi` + 32 hex (optional `-N` shard).
  { id: 'databricks-api-token', source: '(?:^|[^a-zA-Z0-9])(dapi[a-f0-9]{32}(?:-[0-9])?)(?:[\\x60\'"\\s;]|\\\\[nr]|$)' },
  // HashiCorp Terraform Cloud API token — `<14>.atlasv1.<60-70>`.
  { id: 'hashicorp-tf-api-token', source: '(?:^|[^a-zA-Z0-9])([a-zA-Z0-9]{14}\\.atlasv1\\.[a-zA-Z0-9\\-_=]{60,70})' },
  // Pulumi access token — `pul-` + 40 hex.
  { id: 'pulumi-api-token', source: '(?:^|[^a-zA-Z0-9])(pul-[a-f0-9]{40})(?:[\\x60\'"\\s;]|\\\\[nr]|$)' },
  // Postman API key — `PMAK-<24hex>-<34hex>`.
  { id: 'postman-api-token', source: '(?:^|[^a-zA-Z0-9])(PMAK-[a-fA-F0-9]{24}-[a-fA-F0-9]{34})(?:[\\x60\'"\\s;]|\\\\[nr]|$)' },
  // Grafana Cloud API token — `glc_` + base64-ish body.
  { id: 'grafana-cloud-api-token', source: '(?:^|[^a-zA-Z0-9])(glc_[A-Za-z0-9+/]{32,400}={0,3})(?:[\\x60\'"\\s;]|\\\\[nr]|$)' },
  // Grafana API key — legacy `eyJrIjoi…` base64 blob. A single-segment base64
  // run (no `.eyJ`), so it never collides with the two-segment `jwt` rule.
  { id: 'grafana-api-key', source: '(?:^|[^a-zA-Z0-9])(eyJrIjoi[A-Za-z0-9+/]{70,400}={0,3})(?:[\\x60\'"\\s;]|\\\\[nr]|$)' },
  // Grafana service-account token — `glsa_<32>_<8 hex>`.
  { id: 'grafana-service-account-token', source: '(?:^|[^a-zA-Z0-9])(glsa_[A-Za-z0-9]{32}_[A-Fa-f0-9]{8})(?:[\\x60\'"\\s;]|\\\\[nr]|$)' },
  // Sentry user auth token — `sntryu_` + 64 hex.
  { id: 'sentry-user-token', source: '(?:^|[^a-zA-Z0-9])(sntryu_[a-f0-9]{64})(?:[\\x60\'"\\s;]|\\\\[nr]|$)' },
  // Sentry org auth token — `sntrys_` + an embedded base64 JSON header/region claim.
  { id: 'sentry-org-token', source: '(?:^|[^a-zA-Z0-9])(sntrys_eyJpYXQiO[a-zA-Z0-9+/]{10,200}(?:LCJyZWdpb25fdXJs|InJlZ2lvbl91cmwi|cmVnaW9uX3VybCI6)[a-zA-Z0-9+/]{10,200}={0,2}_[a-zA-Z0-9+/]{43})' },
  // Slack app-level token — `xapp-<ver>-<id>-<ts>-<secret>` (case-insensitive).
  { id: 'slack-app-token', source: '(?:^|[^a-zA-Z0-9])(xapp-\\d-[A-Za-z0-9]+-\\d+-[a-z0-9]+)', flags: 'i' },
  // Shopify access token — `shpat_` + 32 hex.
  { id: 'shopify-access-token', source: '(?:^|[^a-zA-Z0-9])(shpat_[a-fA-F0-9]{32})(?:[\\x60\'"\\s;]|\\\\[nr]|$)' },
  // Shopify shared secret — `shpss_` + 32 hex.
  { id: 'shopify-shared-secret', source: '(?:^|[^a-zA-Z0-9])(shpss_[a-fA-F0-9]{32})(?:[\\x60\'"\\s;]|\\\\[nr]|$)' },
  // Quoted credential assignment: password/secret/token/api_key : = "value".
  { id: 'credential-assignment', source: '(?:password|secret|token|api_key)\\s*[:=]\\s*[\'"]([^\'"]{8,})[\'"]', flags: 'i' },
];

export const MAX_SCAN_LINE_LENGTH = 500;

// eslint-disable-next-line security/detect-bidi-characters -- matches invisible chars to strip
export const INVISIBLE_CHARS_RE = /[­͏؜᠎​-‏‪-‮⁠-⁤⁦-⁯﻿￹-￻]/g;

// Rule-id words whose canonical capitalization differs from plain title case.
export const RULE_LABEL_SPECIAL: Record<string, string> = {
  aws: 'AWS', gcp: 'GCP', api: 'API', pat: 'PAT', jwt: 'JWT', npm: 'NPM',
  pypi: 'PyPI', oauth: 'OAuth', github: 'GitHub', gitlab: 'GitLab',
  openai: 'OpenAI', sendgrid: 'SendGrid', groq: 'Groq',
  huggingface: 'HuggingFace', digitalocean: 'DigitalOcean',
  ad: 'AD', tf: 'TF', hashicorp: 'HashiCorp',
};

/** Convert a kebab-case rule id to a human-readable label (never a secret). */
export function ruleIdToLabel(ruleId: string): string {
  return ruleId
    .split('-')
    .map(part => RULE_LABEL_SPECIAL[part] ?? (part.length === 0 ? part : part[0].toUpperCase() + part.slice(1)))
    .join(' ');
}

/**
 * Build the canonical write-block message for detected secrets — ONE source of
 * truth for every edit mode (single replace, replace_all, multi-edit, full
 * overwrite) so the wording never drifts between paths. Parity with the reference's
 * secret guard, which both NAMES the matched secret labels AND states the
 * sharing-risk rationale (a committed secret is exposed to everyone with
 * repository access), not just the type list. `where` distinguishes a fresh
 * write ("content") from an in-place edit ("edited content"). Labels only — the
 * matched secret bytes are never echoed.
 */
export function secretWarning(
  secrets: SecretMatch[],
  where: 'content' | 'edited content',
  opts: { teamMemory?: boolean } = {},
): string {
  const labels = secrets.map(s => s.label).join(', ');
  // Team-memory scope: a file under the shared team-memory tree is replicated to
  // every repository collaborator, so writing a secret there is strictly worse
  // than committing one to this checkout. Surface a distinct, collaborator-aware
  // block message for that path (parity with the reference's teamMemSecretGuard,
  // which hard-blocks with "Team memory is shared with all repository
  // collaborators"). Any other path keeps the generic repository-scoped warning.
  if (opts.teamMemory) {
    return `Error: Potential secrets detected in ${where}: ${labels}. ` +
      'This file is in the shared TEAM MEMORY, which is synced to and readable by ALL repository collaborators — ' +
      'never write a secret there. Remove the secret (use an env var or secret manager) before writing.';
  }
  return `Warning: Potential secrets detected in ${where}: ${labels}. ` +
    'A committed secret is exposed to everyone with access to this repository and its history — ' +
    'remove the secret (use an env var or secret manager) before writing.';
}

// ---------------------------------------------------------------------------
// CRLF-aware line ending helpers (shared by file-edit)
// ---------------------------------------------------------------------------

export function detectLineEnding(content: string): '\r\n' | '\n' {
  const crlf = (content.match(/\r\n/g) || []).length;
  const lf = (content.match(/(?<!\r)\n/g) || []).length;
  return crlf > lf ? '\r\n' : '\n';
}

export function toolCallFingerprint(toolName: string, input: Record<string, unknown>): string {
  const sortedKeys = Object.keys(input).sort();
  const parts = sortedKeys.map(k => `${k}=${JSON.stringify(input[k])}`);
  return `${toolName}:${parts.join('&')}`;
}

export function deduplicateToolCalls(
  calls: ToolCallContent[],
): { deduplicated: ToolCallContent[]; removedCount: number; removedIds: string[] } {
  const seen = new Set<string>();
  const deduplicated: ToolCallContent[] = [];
  const removedIds: string[] = [];

  for (const call of calls) {
    let parsed: Record<string, unknown>;
    try {
      parsed = JSON.parse(call.function.arguments);
    } catch {
      parsed = { _raw: call.function.arguments };
    }
    const fp = toolCallFingerprint(call.function.name, parsed);
    if (seen.has(fp)) {
      removedIds.push(call.id);
    } else {
      seen.add(fp);
      deduplicated.push(call);
    }
  }

  return { deduplicated, removedCount: removedIds.length, removedIds };
}

/**
 * True when a tool name is an MCP tool — the canonical `mcp__<server>__<tool>`
 * prefix every MCP-backed tool is registered under. Single source for the
 * built-in-vs-MCP partition so no call site re-derives the prefix test.
 */
export function isMcpToolName(name: string): boolean {
  return name.startsWith('mcp__');
}

/**
 * Deterministically order a tool list for PROMPT-CACHE STABILITY: partition into
 * built-ins vs MCP tools (by {@link isMcpToolName}), sort EACH group
 * alphabetically by name, and return `builtIn.concat(mcp)`. Built-ins stay a
 * contiguous, stably-ordered prefix and MCP tools follow in a stable order, so
 * the provider's tool-schema cache prefix does not shift between turns/runs as
 * MCP servers connect on demand. De-duplicates by name (first occurrence wins),
 * mirroring the reference's `mergeAndFilterTools` partition-sort. Pure — returns a
 * new array and never mutates the input. Generic over any `{ name }` shape so it
 * can order either `ToolDefinition`s or already-serialized tool schemas.
 */
export function partitionAndSortTools<T extends { name: string }>(tools: readonly T[]): T[] {
  const seen = new Set<string>();
  const builtIn: T[] = [];
  const mcp: T[] = [];
  for (const tool of tools) {
    if (seen.has(tool.name)) continue;
    seen.add(tool.name);
    (isMcpToolName(tool.name) ? mcp : builtIn).push(tool);
  }
  const byName = (a: T, b: T): number => a.name.localeCompare(b.name);
  builtIn.sort(byName);
  mcp.sort(byName);
  return builtIn.concat(mcp);
}

/**
 * Validate a file path before writing: rejects NUL bytes and symlinks.
 * Symlinks at the target path could redirect writes to arbitrary files.
 * If the file doesn't exist yet, that's fine — allow writes to new files.
 */
/** Glob metacharacters that must never appear in a NEW write/create target.
 *  A create path like `src/*.ts` or `out/{a,b}` would pass base-directory
 *  validation but resolve to an unintended literal path (the write tool never
 *  expands globs), so a non-existent target containing `* ? { }` is hard-denied
 *  (the reference `GLOB_PATTERN_REGEX`). SUPERSET over the reference: brackets `[ ]` are
 *  DELIBERATELY NOT rejected — framework dynamic-route filenames (e.g. a Next.js
 *  `[id].tsx` or SvelteKit `[slug]/+page.svelte`) are legitimate literal paths,
 *  and the write tool uses the path verbatim (no shell glob expansion), so there
 *  is no base-dir bypass risk from brackets. */
export const GLOB_PATTERN_REGEX = /[*?{}]/;

export function validateFileWritePath(filePath: string): { valid: boolean; error?: string } {
  const nullCheck = validateFilePath(filePath);
  if (nullCheck) return { valid: false, error: nullCheck.output };

  try {
    const stat = fs.lstatSync(filePath);
    if (stat.isSymbolicLink()) {
      return { valid: false, error: `Error: "${filePath}" is a symlink — write blocked to prevent symlink redirect attack.` };
    }
  } catch {
    // File doesn't exist yet — a create. A glob metacharacter in a create target
    // is a base-dir-validation bypass, so reject it (write tools use the path
    // literally and never expand globs).
    if (GLOB_PATTERN_REGEX.test(filePath)) {
      return {
        valid: false,
        error: `Error: "${filePath}" contains glob metacharacters (*?{}). Provide a single literal file path for write/create operations.`,
      };
    }
  }

  return { valid: true };
}

/**
 * Environment overrides that make `git` fail fast instead of blocking on an
 * interactive credential/askpass prompt. A non-TTY stdin alone is not enough:
 * git can still try an askpass helper or a terminal prompt and hang until the
 * caller's timeout. Spread this over `process.env` for any `git` subprocess
 * (clone/fetch/etc.) spawned by a tool so a private/unauthenticated repo errors
 * out immediately with a clear auth failure. Shared by every git-spawning tool.
 */
export const GIT_NO_PROMPT_ENV: Readonly<Record<string, string>> = {
  GIT_TERMINAL_PROMPT: '0',
  GIT_ASKPASS: '',
  SSH_ASKPASS: '',
  GCM_INTERACTIVE: 'never',
};

/**
 * Crash-safe, flushed, permission-preserving synchronous file write — the single
 * canonical durable-write primitive for the file tools. It:
 *  1. captures the existing file's mode (so the executable bit / permissions survive),
 *  2. writes to a sibling `${target}.tmp.${pid}.${now}` temp file with `flush: true`
 *     (fsync to disk — a crash/ENOSPC mid-write never leaves a truncated target),
 *  3. chmods the temp back to the captured mode,
 *  4. atomically `renameSync`s temp→target (the target is never partially written),
 *  5. on ANY atomic-path failure, unlinks the temp and falls back to a direct
 *     `writeFileSync` so the write still lands on filesystems where rename across
 *     the same dir is unavailable.
 * `data` may be a string or Buffer (callers that re-encode pass a Buffer).
 */
/**
 * Parse an explicit file-permission-bits value from tool input into a numeric
 * mode. Accepts a number (used as-is) or an octal string (`'600'`, `'0600'`,
 * `'0o755'`). Returns undefined for absent/invalid input so the caller falls back
 * to the default (umask / preserved) permissions instead of applying a bogus
 * mode. Bounded to the standard permission range (<= 0o7777) so a wild value can
 * never request an unexpected setuid/sticky combination. Parity with the reference
 * Code's `{ mode }` option on its fs write helpers (create-with-exact-bits).
 */
export function parseFileMode(raw: unknown): number | undefined {
  if (typeof raw === 'number') {
    return Number.isInteger(raw) && raw >= 0 && raw <= 0o7777 ? raw : undefined;
  }
  if (typeof raw === 'string') {
    const s = raw.trim().replace(/^0o/i, '');
    if (!/^[0-7]{3,4}$/.test(s)) return undefined;
    const n = parseInt(s, 8);
    return n >= 0 && n <= 0o7777 ? n : undefined;
  }
  return undefined;
}
