import * as child_process from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { execCommand } from '../utils/shell';
import { generateTempFilePath } from '../utils/tempfile';
import { detectGitTransientState, getStagedDiffSize, isFileStagedForDeletion } from '../utils/git';
import '../utils/env';
import { scanForSecrets } from './tool-utils';
import { getSessionFileAttribution, computeCommitEffort, advanceEffortBaselines, setStartingHeadSha, buildAttributionData, normalizeAttributionPath } from './attribution-store';
import '../version';
import { agentContributionRatio } from '../session';
import '../memory';
import '../reference-journal';

import '../config';
import { ATTRIBUTION_NOTES_REF, CO_AUTHOR_TRAILER, SAFE_ENV_SUFFIX_REGEX, SECRET_FILE_EXTS, SECRET_FILE_PATTERNS, TRAILER_REGEX, aggregateAgentContributionPercent, attributionNotesEnabled, buildAttributionNote, buildCommitEnv, countMemoriesRecalled, formatCommitSummary, resolveClientSurface, resolveCommitAttribution, resolveCommitModel, shouldPreserveCommitHooks, stripDisallowedAttribution } from './git-commit-build-attribution-note';
import type { CommitAttributionNote, CommitFileContribution } from './git-commit-build-attribution-note';
export { ATTRIBUTION_NOTES_REF, aggregateAgentContributionPercent, attributionNotesEnabled, buildAttributionNote, buildCommitEnv, countMemoriesRecalled, formatCommitSummary, formatContributionByline, resolveClientSurface, resolveCommitAttribution, resolveCommitModel, sanitizeModelName, shouldPreserveCommitHooks } from './git-commit-build-attribution-note';
export type { CommitAttributionNote, CommitFileContribution, ContributionStats } from './git-commit-build-attribution-note';

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

/** True when a staged file's basename matches a known secret-file shape. */
function isSecretFile(basename: string): boolean {
  if (/^\.env($|\.)/i.test(basename) && !SAFE_ENV_SUFFIX_REGEX.test(basename)) return true;
  if (SECRET_FILE_NAMES.has(basename)) return true;
  if (SECRET_FILE_EXTS.some(ext => basename.toLowerCase().endsWith(ext))) return true;
  return SECRET_FILE_PATTERNS.some(p => p.test(basename));
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
  readFileSync: fs.readFileSync as typeof fs.readFileSync,
  /**
   * Async, cancellable staging seam. Parity with the reference, which routes every
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
 * Build the per-file agent character-contribution list for the committed paths
 * from the session-scoped attribution store (populated by the edit/write tools).
 * Only files the agent actually authored this session appear; each carries the
 * running character count plus the fraction of the file's CURRENT size that
 * represents ({@link agentContributionRatio}, reading the committed file to size
 * it — best-effort, a read failure just yields a 0 ratio). Returns `[]` when no
 * tracked file is part of this commit.
 */
export function collectCommitFileContributions(
  sessionId: string | undefined,
  cwd: string,
  stagedPaths: string[],
): CommitFileContribution[] {
  const attribution = getSessionFileAttribution(sessionId);
  const out: CommitFileContribution[] = [];
  for (const rel of stagedPaths) {
    const abs = path.resolve(cwd, rel);
    // Look up by the SAME realpath-normalized repo-relative key the edit/write
    // tools store under (parity: the reference's normalizeFilePath diff-key matching).
    const state = attribution[normalizeAttributionPath(abs, cwd)];
    if (!state) continue;
    let totalChars = 0;
    try { totalChars = _gitCommitFs.readFileSync(abs, { encoding: 'utf-8' }).length; } catch { totalChars = 0; }
    const entry: CommitFileContribution = {
      path: rel,
      agentContribution: state.agentContribution,
      totalChars,
      ratio: agentContributionRatio(state, totalChars),
    };
    // Honestly flag a file staged for deletion (its content is gone on disk, so
    // the 0 totalChars/ratio above are expected, not an unreadable-file error).
    if (isFileStagedForDeletion(cwd, rel)) entry.deleted = true;
    out.push(entry);
  }
  return out;
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
    'When AMI_COMMIT_ATTRIBUTION_NOTES is set, a structured per-commit attribution record (surface, model, committed files, and per-file agent character contribution for files edited this session) is written as a git note on ref ami-attribution for introspection.',
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
    // the reference's "do not commit files that likely contain secrets" protocol, which
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

    // Gather the staged-state-dependent attribution signals BEFORE committing —
    // once the commit lands the index matches HEAD, so `git diff --cached` is
    // empty (staged change volume reads 0) and a staged deletion is no longer
    // reported by `--name-status`. These MEASURED figures can only be read now.
    // Computed solely for the opt-in attribution-note introspection surface, so a
    // commit without it enabled pays no extra git calls.
    const notesEnabled = attributionNotesEnabled(env);
    const stagedChangeChars = notesEnabled ? getStagedDiffSize(context.cwd) : 0;
    const fileContributions = notesEnabled
      ? collectCommitFileContributions(context.sessionId, context.cwd, stagedPaths)
      : [];
    // Capture the session's starting HEAD (idempotent — only the first commit of a
    // session sets it) so later tooling can detect commits made OUTSIDE this
    // session by comparing against it (parity: the reference's startingHeadSha). Best
    // effort and gated on the opt-in note so a plain commit pays no extra git call;
    // a repo with no commits yet yields an empty SHA and is a no-op.
    if (notesEnabled) {
      const preHead = (await execCommand('git rev-parse HEAD', execOpts)).stdout.trim();
      setStartingHeadSha(context.sessionId, preHead);
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
      // Parity with the reference's "NEVER skip hooks unless explicitly requested": the
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
      if (notesEnabled) {
        try {
          // rev-parse HEAD always resolves right after a successful commit.
          const sha = (await execCommand('git rev-parse HEAD', execOpts)).stdout.trim();
          const clientSurface = resolveClientSurface(env);
          // Versioned, structured authorship payload (per-file agent/human chars,
          // surface breakdown, excluded generated files) — attached only when
          // something was tracked this session. Parity: the reference's AttributionData.
          // Uses the PRE-COMMIT fileContributions captured above (post-commit the
          // index matches HEAD, so staged-state signals would read empty).
          const attributionData =
            fileContributions.length > 0
              ? buildAttributionData({
                  surface: clientSurface,
                  sessions: context.sessionId ? [context.sessionId] : [],
                  stagedFiles: stagedPaths,
                  contributions: fileContributions,
                })
              : undefined;
          const note = buildAttributionNote({
            surface: clientSurface,
            model: resolveCommitModel(env),
            trailerApplied: trailer !== null,
            files: stagedPaths,
            fileContributions,
            attributionData,
            // Computed contribution-introspection signals (all MEASURED): overall
            // agent-authored percent across the committed tracked files, the
            // session's user-prompt ("N-shotted") count, and the number of memory
            // files recalled this session. buildAttributionNote renders these into
            // a `byline` and omits any zero signal.
            contributionPercent: aggregateAgentContributionPercent(fileContributions),
            // PER-COMMIT steer count (prompts since the previous commit), not a
            // cumulative session total — parity with the reference's per-commit
            // "N-shotted" via the promptCountAtLastCommit baseline.
            promptCount: computeCommitEffort(context.sessionId).steers,
            memoriesRecalled: countMemoriesRecalled(context.cwd, context.sessionId),
            // MEASURED staged change volume (incl. human/untracked-staged edits the
            // per-file agent store misses), captured pre-commit via the
            // chars-per-line heuristic.
            stagedChangeChars,
          });
          await writeCommitAttributionNote(sha, note, execOpts);
        } catch { /* best-effort introspection — never fail the commit */ }
      }

      // A commit landed: snapshot the running effort counters into their per-commit
      // baselines so the NEXT commit's steer/permission/escape figures measure only
      // the effort AFTER this commit (parity: the reference advancing the effort
      // baselines on commit). Done AFTER the note read above (which consumes the
      // current deltas) and unconditionally so the baselines stay correct even if
      // attribution notes are toggled between commits.
      advanceEffortBaselines(context.sessionId);

      const parts: string[] = [];
      // Normalized affordance line the frontend can render cleanly ("Committed
      // <shortsha>: <subject>"), parity with the reference's structured commit summary.
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
