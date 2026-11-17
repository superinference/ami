import * as child_process from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { execCommand } from '../utils/shell';
import { generateTempFilePath } from '../utils/tempfile';
import { detectGitTransientState, classifyRepoRemote } from '../utils/git';
import { parseEnvBool } from '../utils/env';
import { scanForSecrets } from './tool-utils';
import { loadProjectConfig, loadLocalConfig, loadGlobalConfig, loadManagedConfig, mergeConfigs } from '../config';

const CO_AUTHOR_TRAILER = 'Co-Authored-By: AMI <ami@superinference.org>';
const TRAILER_REGEX = /Co-Authored-By:\s*AMI\s*<ami@superinference\.org>/i;

/**
 * Secret-bearing FILE guard. Mirrors the sensitive-name/ext/pattern list that
 * permissions.ts applies to reads so the sanctioned commit path can never stage
 * and push a credential file (`.env`, `credentials.json`, `id_rsa`, `*.pem`, …).
 * These constants are local because the permissions.ts equivalents are
 * function-scoped and not exported; keep the two lists in sync if either moves.
 */
const SECRET_FILE_NAMES = new Set([
  'id_rsa', 'id_ed25519', 'id_ecdsa', 'id_dsa', '.netrc',
  'credentials.json',
]);
const SECRET_FILE_EXTS = ['.pem', '.key', '.p12', '.pfx', '.keystore'];
/** Suffixes that make an `.env`-prefixed file a safe template, not a real secret. */
const SAFE_ENV_SUFFIX_REGEX = /\.(example|sample|template|dist|defaults?)$/i;
const SECRET_FILE_PATTERNS = [/^secrets\.ya?ml$/i];

/** True when a staged file's basename matches a known secret-file shape. */
function isSecretFile(basename: string): boolean {
  if (/^\.env($|\.)/i.test(basename) && !SAFE_ENV_SUFFIX_REGEX.test(basename)) return true;
  if (SECRET_FILE_NAMES.has(basename)) return true;
  if (SECRET_FILE_EXTS.some(ext => basename.toLowerCase().endsWith(ext))) return true;
  return SECRET_FILE_PATTERNS.some(p => p.test(basename));
}

/**
 * Strip foreign / disallowed attribution from a commit message before the AMI
 * trailer is applied. Removes any `Co-Authored-By:` trailer that is NOT AMI's
 * own and any AI-tool provenance footer (lines carrying the 🤖 marker), so an
 * external assistant's byline can never land in a commit — matching the repo's
 * attribution policy regardless of what the drafted message contained.
 */
function stripDisallowedAttribution(message: string): string {
  const kept = message.split('\n').filter(line => {
    if (/^\s*Co-Authored-By:/i.test(line) && !TRAILER_REGEX.test(line)) return false;
    if (line.includes('🤖')) return false;
    return true;
  });
  return kept.join('\n').replace(/\n{3,}/g, '\n\n').trimEnd();
}

/**
 * Dependency seam for tests (mirrors the fsHooks pattern in config.ts). The
 * temp-file write/cleanup routes through this object so tests can simulate an
 * unlink failure (e.g. the temp file already removed) via
 * `mock.method(_gitCommitFs, 'unlinkSync', ...)`. Defaults are the real fs calls,
 * so production behaviour is unchanged.
 */
export const _gitCommitFs = {
  writeFileSync: fs.writeFileSync as typeof fs.writeFileSync,
  unlinkSync: fs.unlinkSync as typeof fs.unlinkSync,
  /**
   * Async, cancellable staging seam. Parity with Claude Code, which routes every
   * git invocation through an async child that always forwards `signal` (and
   * `timeout`): the former blocking `execFileSync('git', ['add', …])` could not
   * honor `context.abortSignal` and pinned the event loop for the whole `git add`
   * over a large tree. This argv-form (no shell → injection-safe) async wrapper
   * forwards the abort signal so staging is interruptible and non-blocking, like
   * the diff/commit/log steps already are. Mockable in tests via
   * `mock.method(_gitCommitFs, 'execFileAsync', …)`.
   */
  execFileAsync(
    file: string,
    args: readonly string[],
    options: child_process.ExecFileOptions,
  ): Promise<{ stdout: string; stderr: string }> {
    return new Promise((resolve, reject) => {
      child_process.execFile(file, args as string[], options, (err, stdout, stderr) => {
        if (err) reject(err);
        else resolve({ stdout: String(stdout), stderr: String(stderr) });
      });
    });
  },
};

/**
 * Resolve the client-surface identifier that originated this commit, so git
 * hooks / downstream tooling can see which frontend drove it. Parity with Claude
 * Code's `getClientSurface()` (`CLAUDE_CODE_ENTRYPOINT ?? 'cli'`): AMI prefers its
 * own `AMI_ENTRYPOINT`, then honors `CLAUDE_CODE_ENTRYPOINT` for cross-tool
 * compatibility, and defaults to `'cli'`. A blank/whitespace value falls back to
 * the default rather than stamping an empty surface.
 */
export function resolveClientSurface(env: NodeJS.ProcessEnv = process.env): string {
  const raw = env.AMI_ENTRYPOINT ?? env.CLAUDE_CODE_ENTRYPOINT;
  const trimmed = raw?.trim();
  return trimmed && trimmed.length > 0 ? trimmed : 'cli';
}

/**
 * Collapse a raw model identifier to a stable, public short name for the commit
 * surface. Parity with Claude Code's `sanitizeModelName` (buildSurfaceKey +
 * sanitizeModelName): strip a provider/vendor prefix (`anthropic/claude-…`,
 * `openai/…`, a bedrock `….anthropic.*` ARN segment) and a trailing
 * `-YYYYMMDD` / `@date` snapshot suffix so `claude-opus-4-8-20260115` and
 * `anthropic/claude-opus-4-8` both normalize to `opus-4-8`. A `claude-` family
 * prefix is dropped too (the AMI trailer already carries the AMI identity). An
 * unrecognizable/blank value yields `undefined` rather than stamping noise.
 */
export function sanitizeModelName(raw: string | undefined): string | undefined {
  if (!raw) return undefined;
  let name = raw.trim().toLowerCase();
  if (!name) return undefined;
  // Drop a provider prefix segment (last path/colon component wins).
  name = name.split(/[/:]/).pop() ?? name;
  // Bedrock/ARN style "us.anthropic.claude-…": keep the trailing model token.
  name = name.split('.').pop() ?? name;
  // Strip a trailing date snapshot suffix (-YYYYMMDD or @anything).
  name = name.replace(/@.*$/, '').replace(/-\d{8}$/, '');
  // Drop a leading vendor-family prefix.
  name = name.replace(/^(claude|gpt|gemini)-/, '');
  name = name.trim();
  return name.length > 0 ? name : undefined;
}

/**
 * Resolve the model name to stamp onto the commit (consumed by git hooks /
 * downstream tooling, parallel to {@link resolveClientSurface}'s AI_SURFACE).
 * Honors `AMI_COMMIT_MODEL` first, then the generic `ANTHROPIC_MODEL` /
 * `AMI_MODEL` the engine already sets, passing the raw value through
 * {@link sanitizeModelName}. Returns `undefined` when none is set.
 */
export function resolveCommitModel(env: NodeJS.ProcessEnv = process.env): string | undefined {
  return sanitizeModelName(env.AMI_COMMIT_MODEL ?? env.AMI_MODEL ?? env.ANTHROPIC_MODEL);
}

/**
 * Whether to PRESERVE the repo's own commit hooks / GPG signing even in detached
 * mode. AMI bypasses them by default in detached runs (anti-hang: a blocking
 * pre-commit hook or a passphrase prompt would wedge an unattended run), but
 * Claude's protocol is "NEVER skip hooks unless explicitly requested". Setting
 * `AMI_DETACHED_PRESERVE_HOOKS=1` is that explicit request — the repo's
 * pre-commit guards (secret scanners, linters, policy hooks) and signing stay
 * active in detached mode too. Default/interactive behavior is unchanged.
 */
export function shouldPreserveCommitHooks(env: NodeJS.ProcessEnv = process.env): boolean {
  return env.AMI_DETACHED_PRESERVE_HOOKS === '1';
}

/** Git notes ref the per-commit attribution introspection record is written to. */
export const ATTRIBUTION_NOTES_REF = 'ami-attribution';

/**
 * Opt-in gate for the per-commit AI-contribution introspection surface. Parity
 * with Claude Code's per-commit attribution record (`calculateCommitAttribution`
 * → structured git-notes JSON), but OFF by default so a commit never silently
 * mutates a repo's notes refs: set `AMI_COMMIT_ATTRIBUTION_NOTES=1`/`true`/`yes`/
 * `on` to have `git_commit` record a structured attribution note for each commit
 * it creates. Uses the shared {@link parseEnvBool} truthiness convention.
 */
export function attributionNotesEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return parseEnvBool(env.AMI_COMMIT_ATTRIBUTION_NOTES);
}

/** Structured per-commit attribution introspection record. */
export interface CommitAttributionNote {
  /** Always 'ami' — the sanctioned commit tool only creates agent-authored commits. */
  authoredBy: 'ami';
  /** Client surface that drove the commit ({@link resolveClientSurface}). */
  surface: string;
  /** Normalized model name ({@link resolveCommitModel}), or null when unknown. */
  model: string | null;
  /** Whether a Co-Authored-By attribution trailer was applied to the message. */
  trailerApplied: boolean;
  /** The files committed (staged paths), for a per-file contribution view. */
  files: string[];
  /** Count of files in the commit. */
  fileCount: number;
  /** ISO timestamp the attribution record was produced. */
  committedAt: string;
}

/**
 * Build the structured attribution introspection record for a commit. This is
 * the data AMI can compute truthfully at commit time (surface / model / authored
 * files / trailer). It deliberately does NOT fabricate Claude Code's
 * character-level `claudePercent` — accurate per-character authorship needs a
 * cross-cutting Edit/Write char tracker and is a separate concern; a made-up
 * percentage would be worse than none. `now` is injectable for deterministic
 * tests.
 */
export function buildAttributionNote(
  params: {
    surface: string;
    model: string | undefined;
    trailerApplied: boolean;
    files: string[];
  },
  now: Date = new Date(),
): CommitAttributionNote {
  return {
    authoredBy: 'ami',
    surface: params.surface,
    model: params.model ?? null,
    trailerApplied: params.trailerApplied,
    files: params.files,
    fileCount: params.files.length,
    committedAt: now.toISOString(),
  };
}

/**
 * Build the normalized one-line commit affordance from `git log --oneline -1`
 * output ("<shortsha> <subject>") → "Committed <shortsha>: <subject>", so a
 * frontend can render a clean commit line. Parity with Claude's structured
 * commit-result summary (sha + kind). Returns '' for empty input; a sha with no
 * subject yields "Committed <shortsha>".
 */
export function formatCommitSummary(onelineOutput: string): string {
  const line = onelineOutput.trim();
  if (!line) return '';
  const m = line.match(/^(\S+)\s+(.*)$/);
  if (m && m[2].length > 0) return `Committed ${m[1]}: ${m[2]}`;
  return `Committed ${line}`;
}

/**
 * Write the structured attribution introspection record as a git note on the
 * just-created commit (ref `refs/notes/ami-attribution`). Best-effort by design —
 * the caller invokes this only when {@link attributionNotesEnabled} and inside a
 * try/catch, so a notes failure (e.g. a notes.rewriteRef hook, a read-only object
 * store) can never fail an otherwise-successful commit.
 *
 * The note body is staged through a temp file whose path is derived in
 * content-hash mode ({@link generateTempFilePath}): identical attribution content
 * resolves to the same stable path, exercising the deterministic temp-path branch
 * with a real consumer. `-f` overwrites any pre-existing note on the same commit.
 */
export async function writeCommitAttributionNote(
  sha: string,
  note: CommitAttributionNote,
  execOpts: Parameters<typeof execCommand>[1],
): Promise<void> {
  const json = JSON.stringify(note, null, 2);
  const noteFile = generateTempFilePath('ami-attr-note', '.json', { contentHash: json });
  try {
    _gitCommitFs.writeFileSync(noteFile, json, { encoding: 'utf-8' });
    await execCommand(
      `git notes --ref=${ATTRIBUTION_NOTES_REF} add -f -F "${noteFile}" ${sha}`,
      execOpts,
    );
  } finally {
    try { _gitCommitFs.unlinkSync(noteFile); } catch { /* already cleaned or never written */ }
  }
}

export function buildCommitEnv(): NodeJS.ProcessEnv {
  const env = { ...process.env };
  env.GIT_EDITOR = 'false';
  env.GIT_PAGER = 'cat';
  env.GIT_TERMINAL_PROMPT = '0';
  env.AI_AGENT = 'superinference';
  env.AI_SURFACE = resolveClientSurface(env);
  const model = resolveCommitModel(env);
  if (model) env.AI_MODEL = model;
  return env;
}

/**
 * Resolve the commit trailer to append for a repo at `cwd`, honoring (in order):
 *  1. The merged config's `attribution.commit` (parity: Claude Code's
 *     `attribution.commit` setting): a non-empty string REPLACES the default AMI
 *     trailer; the empty string `""` suppresses it (→ `null`). When explicitly set
 *     it takes PRECEDENCE over the deprecated binary env opt-out below — matching
 *     Claude Code, whose `attribution` setting overrides the legacy
 *     `includeCoAuthoredBy` toggle.
 *  2. `SUPERINFERENCE_DISABLE_CO_AUTHOR=1` — the deprecated binary opt-out:
 *     suppress the trailer (→ `null`) only when no explicit config override exists.
 *  3. Absent → the default AMI trailer.
 * All config layers participate via {@link mergeConfigs} (managed > cli > … > global),
 * so an enterprise/global/project policy can set the custom trailer. A broken config
 * never blocks a commit — it falls back to the env opt-out / default AMI trailer.
 */
export function resolveCommitAttribution(cwd: string): string | null {
  let attribution: { commit?: string; auto?: boolean; ownRemotes?: string[] } | undefined;
  try {
    const merged = mergeConfigs(
      {}, {},
      loadProjectConfig(cwd),
      loadGlobalConfig(),
      loadLocalConfig(cwd),
      loadManagedConfig(),
    );
    attribution = merged.attribution;
  } catch {
    attribution = undefined;
  }
  // Explicit config attribution takes precedence over the deprecated env opt-out.
  const custom = attribution?.commit;
  if (custom !== undefined) {
    const trimmed = custom.trim();
    return trimmed.length === 0 ? null : trimmed;
  }
  // FUNCTIONAL CHANGE (additive): tolerant parse so `=true`/`yes`/`on` (not only
  // the strict `'1'`) suppress the co-author trailer, matching the shared convention.
  if (parseEnvBool(process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR)) return null;
  // Opt-in repo-class gate (parity: Claude Code's undercover auto-default). When
  // `attribution.auto` is enabled, keep the trailer ONLY when the origin remote
  // is positively classified as owned; external / unknown / no-remote all
  // suppress it (fail-safe-ON — the strict side wins unless ownership is
  // confirmed). Explicit `attribution.commit` and the env opt-out above retain
  // precedence; this gate only decides the otherwise-default case.
  if (attribution?.auto === true) {
    if (classifyRepoRemote(cwd, attribution.ownRemotes ?? []) !== 'owned') return null;
  }
  return CO_AUTHOR_TRAILER;
}

function buildFinalMessage(message: string, trailer: string | null = CO_AUTHOR_TRAILER): string {
  // Always remove foreign/disallowed attribution first — a drafted message must
  // never carry another assistant's byline, even when the trailer is opted out.
  const trimmed = stripDisallowedAttribution(message.trim());
  // Attribution opt-out (env / config `attribution.commit: ""`) → no trailer.
  if (trailer === null) {
    return trimmed;
  }
  // Avoid a duplicate trailer when the drafted message already carries it.
  const alreadyPresent = trailer === CO_AUTHOR_TRAILER
    ? TRAILER_REGEX.test(trimmed)
    : trimmed.includes(trailer);
  if (alreadyPresent) {
    return trimmed;
  }
  return trimmed + '\n\n' + trailer;
}

export const gitCommitTool: ToolDefinition = {
  name: 'git_commit',
  description:
    'Create a git commit with automatic Co-Authored-By attribution for AMI. ' +
    'Use this instead of running git commit via bash. ' +
    'Optionally stage files before committing. The Co-Authored-By trailer is appended automatically unless SUPERINFERENCE_DISABLE_CO_AUTHOR=1 is set or the config `attribution.commit` overrides/suppresses it. ' +
    'Writing the message: look at the recent commits (`git log --oneline -10`) and follow this repository\'s existing style; pick the verb for the nature of the change — "add" for a wholly new feature, "update" for an enhancement to an existing one, "fix" for a bug fix (also refactor/test/docs/chore); keep it concise (1-2 sentences) and focused on the WHY, not a restatement of the what. ' +
    'Do not stage files that likely contain secrets (.env, credentials.json, *.pem, id_rsa); this tool refuses them and refuses a staged diff containing an embedded credential. ' +
    'When AMI_COMMIT_ATTRIBUTION_NOTES is set, a structured per-commit attribution record (surface, model, committed files) is written as a git note on ref ami-attribution for introspection.',
  inputSchema: {
    type: 'object',
    properties: {
      message: {
        type: 'string',
        description:
          'The commit message. Follow the repo\'s recent-commit style, use the right verb ' +
          '(add = new feature, update = enhancement, fix = bug fix), and keep it a concise ' +
          '1-2 sentences focused on the "why" rather than the "what". ' +
          'The Co-Authored-By trailer is appended automatically (unless SUPERINFERENCE_DISABLE_CO_AUTHOR=1) — do not include it.',
      },
      files: {
        type: 'array',
        items: { type: 'string' },
        description:
          'Files to stage before committing. If omitted, commits whatever is already staged. ' +
          'Pass ["."] to stage all tracked modified files. ' +
          'Do NOT stage files that likely contain secrets (.env, credentials.json, *.pem, id_rsa, ' +
          '.git-credentials); such files are refused, and a staged diff whose added lines contain ' +
          'an API key / token / private key is also refused.',
      },
    },
    required: ['message'],
  },
  isReadOnly: false,

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    const message = input.message as string;
    const files = input.files as string[] | undefined;
    const env = buildCommitEnv();
    const execOpts = { cwd: context.cwd, timeout: 30_000, abortSignal: context.abortSignal, env };

    if (!message || message.trim().length === 0) {
      return { output: 'Error: commit message must not be empty.', isError: true };
    }

    // Transient git-state guard: refuse to create a fresh commit while the repo
    // is mid-way through a rebase / merge / cherry-pick / revert / bisect. The
    // git_commit tool is for authoring new agent commits, not for continuing a
    // multi-step operation (which needs the original message/author). Committing
    // here would hijack the in-progress operation with the drafted message. The
    // user should finish or abort it with git, or use bash `git commit` to
    // deliberately continue the operation.
    const transient = detectGitTransientState(context.cwd);
    if (transient) {
      return {
        output:
          `Error: a git ${transient} is in progress; refusing to create a new commit. ` +
          `Finish or abort the ${transient} with git first, ` +
          'or use bash `git commit` if you intend to continue that operation.',
        isError: true,
      };
    }

    // Guard against flag injection via files array
    if (files) {
      const flagEntry = files.find(f => typeof f === 'string' && f.startsWith('-') && f !== '.' && f !== '..');
      if (flagEntry) {
        return {
          output: `Error: files entries must be file paths, not flags ("${flagEntry}").`,
          isError: true,
        };
      }
    }

    // Stage files if specified. Runs through the async, cancellable execFileAsync
    // seam (carries context.abortSignal) so a large `git add` is interruptible and
    // never blocks the event loop — same cancellation boundary as the diff/commit
    // steps below.
    if (files && files.length > 0) {
      const safeFiles = files.filter(f => typeof f === 'string');
      const addArgs = safeFiles.length === 1 && safeFiles[0] === '.'
        ? ['add', '-u']
        : ['add', '--', ...safeFiles];
      try {
        await _gitCommitFs.execFileAsync('git', addArgs, {
          cwd: context.cwd, timeout: 30_000, env, signal: context.abortSignal,
        });
      } catch (e) {
        const err = e instanceof Error ? e.message : String(e);
        return { output: `Error staging files: ${err}`, isError: true };
      }
    }

    // Check that there are staged changes
    const diffResult = await execCommand('git diff --cached --quiet', execOpts);
    if (diffResult.exitCode === 0) {
      return {
        output: 'Error: nothing staged for commit. Stage files with the "files" parameter, or use bash to run git add first.',
        isError: true,
      };
    }

    // Secret-bearing FILE guard: refuse to commit staged paths that look like
    // credential files (.env, credentials.json, id_rsa, *.pem, …). git_commit
    // stages/commits directly via git, bypassing the bash permission layer, so
    // this is the only place that can stop a credential file reaching a remote.
    const stagedResult = await execCommand('git diff --cached --name-only', execOpts);
    const stagedPaths = stagedResult.stdout
      .split('\n')
      .map(p => p.trim())
      .filter(Boolean);
    const secretFiles = stagedPaths.filter(p => isSecretFile(path.basename(p)));
    if (secretFiles.length > 0) {
      return {
        output:
          `Error: refusing to commit files that likely contain secrets: ${secretFiles.join(', ')}. ` +
          'Unstage them (git restore --staged <file>) or add them to .gitignore before committing.',
        isError: true,
      };
    }

    // Secret-bearing CONTENT guard: scan the ADDED lines of the staged diff (the
    // surface where credentials actually leak — an API key / token / private key
    // pasted into a committed file, not into the subject line). Parity with
    // Claude's "do not commit files that likely contain secrets" protocol, which
    // AMI previously enforced only over file NAMES and the message text. Only `+`
    // (added) lines are scanned so removing a secret is never flagged, reusing the
    // same shared scanForSecrets ruleset as the message scan.
    const stagedDiff = await execCommand('git diff --cached --no-color', execOpts);
    const addedLines = stagedDiff.stdout
      .split('\n')
      .filter(l => l.startsWith('+') && !l.startsWith('+++'))
      .map(l => l.slice(1))
      .join('\n');
    const contentSecrets = scanForSecrets(addedLines);
    if (contentSecrets.length > 0) {
      const labels = [...new Set(contentSecrets.map(s => s.label))].join(', ');
      return {
        output:
          `Error: refusing to commit — potential secrets detected in the staged changes: ${labels}. ` +
          'Remove the secret(s) from the staged diff (or unstage the offending hunk) before committing.',
        isError: true,
      };
    }

    const secrets = scanForSecrets(message);
    if (secrets.length > 0) {
      return { output: `Warning: Potential secrets detected in commit message: ${secrets.map(s => s.label).join(', ')}. Remove secrets before committing.`, isError: true };
    }

    // Build the final commit message with trailer (config/env-aware attribution).
    const trailer = resolveCommitAttribution(context.cwd);
    const finalMessage = buildFinalMessage(message, trailer);

    // Write message to temp file to avoid shell escaping issues. The shared
    // generateTempFilePath helper derives an unpredictable randomUUID name under
    // the AMI temp base (honoring AMI_TMPDIR / CLAUDE_CODE_TMPDIR); combined with
    // the 'wx' (O_EXCL) flag a pre-planted symlink or predictable-path attacker
    // can't hijack the write target.
    const tmpFile = generateTempFilePath('ami-commit', '.txt');

    try {
      try {
        _gitCommitFs.writeFileSync(tmpFile, finalMessage, { encoding: 'utf-8', flag: 'wx' });
      } catch (e) {
        const err = e instanceof Error ? e.message : String(e);
        return { output: `Error writing commit message file: ${err}`, isError: true };
      }

      // In detached/unattended mode, bypass user hooks and GPG signing so a
      // pre-commit hook or a passphrase prompt can't hang the run. In the
      // interactive TUI we deliberately keep the user's hooks/signing active.
      // Parity with Claude's "NEVER skip hooks unless explicitly requested": the
      // bypass can be opted OUT of (preserving the repo's own pre-commit guards /
      // GPG signing even in detached runs) via AMI_DETACHED_PRESERVE_HOOKS=1.
      const unattendedFlags = context.detachedMode && !shouldPreserveCommitHooks(env)
        ? ' -c core.hooksPath=/dev/null -c commit.gpgsign=false'
        : '';
      const commitResult = await execCommand(`git${unattendedFlags} commit -F "${tmpFile}"`, execOpts);
      if (commitResult.exitCode !== 0) {
        const err = (commitResult.stderr || commitResult.stdout).trim();
        return { output: `Error: git commit failed:\n${err}`, isError: true };
      }

      // Get the commit summary
      const logResult = await execCommand('git log --oneline -1', execOpts);
      const commitLine = logResult.stdout.trim();

      // Per-commit AI-contribution introspection surface (opt-in via
      // AMI_COMMIT_ATTRIBUTION_NOTES). Records a structured attribution note —
      // surface / model / authored files / trailer — on the new commit so
      // downstream tooling can inspect agent authorship via
      // `git notes --ref=ami-attribution show <sha>`. Best-effort: a notes
      // failure must never fail a successful commit.
      if (attributionNotesEnabled(env)) {
        try {
          // rev-parse HEAD always resolves right after a successful commit.
          const sha = (await execCommand('git rev-parse HEAD', execOpts)).stdout.trim();
          const note = buildAttributionNote({
            surface: resolveClientSurface(env),
            model: resolveCommitModel(env),
            trailerApplied: trailer !== null,
            files: stagedPaths,
          });
          await writeCommitAttributionNote(sha, note, execOpts);
        } catch { /* best-effort introspection — never fail the commit */ }
      }

      const parts: string[] = [];
      // Normalized affordance line the frontend can render cleanly ("Committed
      // <shortsha>: <subject>"), parity with Claude's structured commit summary.
      const summary = formatCommitSummary(commitLine);
      if (summary) {
        parts.push(summary);
      }
      if (commitResult.stdout.trim()) {
        parts.push(commitResult.stdout.trim());
      }

      return { output: parts.join('\n'), isError: false };
    } finally {
      try { _gitCommitFs.unlinkSync(tmpFile); } catch { /* already cleaned or never written */ }
    }
  },
};

export { buildFinalMessage, CO_AUTHOR_TRAILER, TRAILER_REGEX, isSecretFile, stripDisallowedAttribution };
