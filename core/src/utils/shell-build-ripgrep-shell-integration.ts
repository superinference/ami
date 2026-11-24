// Split from shell.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as os from 'os';
import * as path from 'path';
import { resolveRipgrepConfig, type RipgrepConfig } from '../tools/ripgrep-config';
import { tmuxIsolationEnabled, commandUsesTmux, prepareBashTmuxOverrides } from './tmux-socket';
import { resolveTmpBase, shQuote } from './shell-compute-progress-slices';

// ---------------------------------------------------------------------------
// Shell-environment snapshot (user aliases / functions / options / PATH)
//
// Parity with the reference's ShellSnapshot.ts + bashProvider.ts snapshot sourcing.
// A command run through `bash -c '<cmd>'` does NOT see the user's interactive
// aliases/functions (those live in ~/.bashrc / ~/.zshrc, which a non-login,
// non-interactive shell never sources). the reference captures them ONCE per session
// into a snapshot file and `source`s it before each command, so model-run
// commands behave like the user's interactive shell WITHOUT paying a full
// login-shell startup per command. AMI mirrors this as an OPT-IN capability
// (env knob {@link SHELL_SNAPSHOT_ENV}) so default behavior is unchanged:
// - enabled → create+source the snapshot, spawn WITHOUT `-l`;
// - enabled but snapshot creation failed → gracefully fall back to a login
//   shell (`-l`) so the profile/rc files are at least sourced;
// - disabled → current behavior (login only when `AMI_LOGIN_SHELL` is set).
// Aliases are re-applied via `eval` (a second parse pass) because the shell
// parses a whole command line before executing, so an alias sourced on the same
// line would not expand — the exact reason the reference wraps the user command in
// `eval` after sourcing the snapshot.
// ---------------------------------------------------------------------------

/**
 * Env knob (opt-in) that turns on per-command shell-environment snapshots. When
 * truthy (`1`/`true`/`on`/`yes`), {@link execCommand}'s bash path captures the
 * user's shell config (aliases, functions, options, PATH) once per session and
 * sources it before every command so model-run commands see the user's
 * interactive environment. Unset/blank/falsey leaves the current behavior
 * unchanged. Mirrors the reference's always-on snapshot (ShellSnapshot.ts); AMI
 * makes it opt-in to keep the default command environment deterministic.
 */
export const SHELL_SNAPSHOT_ENV = 'AMI_SHELL_SNAPSHOT';

/** True when {@link SHELL_SNAPSHOT_ENV} names an enabled value. */
export function shellSnapshotEnabled(): boolean {
  const raw = (process.env[SHELL_SNAPSHOT_ENV] || '').trim().toLowerCase();
  return raw === '1' || raw === 'true' || raw === 'on' || raw === 'yes';
}

/** How long the one-shot snapshot-capture shell may run before giving up (ms). */
export const SNAPSHOT_CREATION_TIMEOUT_MS = 10_000;

/**
 * The user shell config file a snapshot sources, chosen from the shell family:
 * zsh → `~/.zshrc`, bash → `~/.bashrc`, anything else → `~/.profile`. Parity
 * with the reference's `getConfigFile` (ShellSnapshot.ts).
 */
export function snapshotConfigFile(shellPath: string): string {
  const fileName = shellPath.includes('zsh')
    ? '.zshrc'
    : shellPath.includes('bash')
      ? '.bashrc'
      : '.profile';
  return path.join(os.homedir(), fileName);
}

/**
 * Build a shell function that invokes `binaryPath` with an explicit `argv[0]`
 * (`argv0`), prepending `prependArgs` before the caller's `"$@"`. This is the
 * ARGV0-dispatch trick for multi-call / busybox-style binaries (one executable
 * that dispatches on `argv[0]`): to behave as ripgrep, such a binary must see
 * `argv[0] === 'rg'` even when spawned from a wrapper path. The four ordered
 * branches mirror the reference's `createArgv0ShellFunction` (ShellSnapshot.ts):
 *   1. zsh sets `argv[0]` via the `ARGV0` env var;
 *   2. Windows/msys/cygwin bash where `exec -a` is unavailable → `ARGV0` env var;
 *   3. a subshell (`$BASHPID != $$`) can `exec -a` directly;
 *   4. otherwise wrap `exec -a` in its own subshell so the parent shell survives.
 * `binaryPath` is POSIX-quoted; each `prependArgs` element must already be a safe
 * shell word (no spaces/metacharacters) — it is injected literally.
 */
export function createArgv0ShellFunction(
  funcName: string,
  argv0: string,
  binaryPath: string,
  prependArgs: string[] = [],
): string {
  const quotedPath = shQuote(binaryPath);
  const argSuffix = prependArgs.length > 0 ? `${prependArgs.join(' ')} "$@"` : '"$@"';
  return [
    `function ${funcName} {`,
    '  if [[ -n $ZSH_VERSION ]]; then',
    `    ARGV0=${argv0} ${quotedPath} ${argSuffix}`,
    '  elif [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "cygwin" ]] || [[ "$OSTYPE" == "win32" ]]; then',
    `    ARGV0=${argv0} ${quotedPath} ${argSuffix}`,
    '  elif [[ $BASHPID != $$ ]]; then',
    `    exec -a ${argv0} ${quotedPath} ${argSuffix}`,
    '  else',
    `    (exec -a ${argv0} ${quotedPath} ${argSuffix})`,
    '  fi',
    '}',
  ].join('\n');
}

/**
 * Build the shell snippet that makes a model-typed `rg` in a bash/zsh command
 * dispatch to the SAME ripgrep binary AMI's grep/glob tools use, so interactive
 * `rg` and tool `rg` behave identically. Parity with the reference's
 * `createRipgrepShellIntegration` (ShellSnapshot.ts), adapted to AMI:
 *
 * - **system mode** (bare PATH `rg`, the default) → `null`: the model's typed
 *   `rg` already resolves the same PATH binary, so there is nothing to dispatch
 *   and no snippet is emitted (the reference likewise only integrates when it has its
 *   own binary / ARGV0 dispatch).
 * - **config/builtin mode with `argv0`** (a multi-call ripgrep binary) → an
 *   {@link createArgv0ShellFunction} ARGV0-dispatch function.
 * - **config/builtin mode without `argv0`** → a plain function that execs the
 *   configured path (plus any config-level `args`), each component POSIX-quoted.
 *
 * The definition is guarded by a `command -v rg` probe (run in a subshell after
 * `unalias rg` so a user alias cannot shadow the probe — parity with the reference's
 * `(unalias rg 2>/dev/null; command -v rg)`): a real system `rg` on PATH wins,
 * matching the user's interactive shell. AMI emits a function (not an alias) for
 * both cases so dispatch works without relying on `expand_aliases` ordering —
 * an intentional, more-robust superset of the reference's alias form.
 *
 * Returns `null` when no integration is needed (system mode), so callers can
 * skip embedding it. Consumed by {@link buildSnapshotScript}, which appends the
 * snippet to the sourceable snapshot file.
 */
export function buildRipgrepShellIntegration(config: RipgrepConfig = resolveRipgrepConfig()): string | null {
  if (config.mode === 'system') return null;
  const definition = config.argv0
    ? createArgv0ShellFunction('rg', config.argv0, config.command, config.args ?? [])
    : (() => {
        const target = config.args && config.args.length
          ? `${shQuote(config.command)} ${config.args.map(shQuote).join(' ')}`
          : shQuote(config.command);
        return `function rg {\n  ${target} "$@"\n}`;
      })();
  const indented = definition.split('\n').map((line) => (line ? `  ${line}` : line)).join('\n');
  return [
    '# AMI ripgrep shell integration',
    'if ! (unalias rg 2>/dev/null; command -v rg) >/dev/null 2>&1; then',
    indented,
    'fi',
  ].join('\n');
}

/**
 * The capture script that, when run by the user's shell as a login shell, writes
 * a sourceable snapshot of their functions, shell options, aliases and PATH to
 * `$SNAPSHOT_FILE`. Faithful to the reference's `getSnapshotScript` /
 * `getUserSnapshotContent`: zsh and bash use their respective introspection
 * (`typeset` vs `declare`, `setopt` vs `shopt`), completion functions
 * (single-underscore prefix) are filtered out while `__`-prefixed helpers are
 * kept, each capture is head-capped at 1000 lines, and `expand_aliases` is
 * force-enabled so a sourced alias expands in the subsequent `eval` pass.
 */
export function buildSnapshotScript(shellPath: string, configFileExists: boolean): string {
  const isZsh = shellPath.includes('zsh');
  // zsh function bodies are written verbatim; bash function bodies are base64-encoded
  // and wrapped in `eval "$(… base64 -d)"` so special characters survive the capture
  // intact — parity with the reference's `getUserSnapshotContent` (ShellSnapshot.ts:L203-232).
  // `declare -f`/`typeset -f >/dev/null` runs first to force-autoload lazy functions;
  // single-underscore completion funcs are filtered (`grep -vE '^_[^_]'`) while
  // double-underscore helpers (e.g. __pyenv_init) are kept.
  const functions = isZsh
    ? `typeset -f > /dev/null 2>&1
typeset +f | grep -vE '^_[^_]' | while read func; do typeset -f "$func" >> "$SNAPSHOT_FILE"; done`
    : `declare -f > /dev/null 2>&1
declare -F | cut -d' ' -f3 | grep -vE '^_[^_]' | while read func; do
  encoded_func=$(declare -f "$func" | base64)
  echo "eval \\"\\$(echo '$encoded_func' | base64 -d)\\" > /dev/null 2>&1" >> "$SNAPSHOT_FILE"
done`;
  const options = isZsh
    ? `setopt | sed 's/^/setopt /' | head -n 1000 >> "$SNAPSHOT_FILE"
echo "setopt aliases" >> "$SNAPSHOT_FILE"`
    : `shopt -p | head -n 1000 >> "$SNAPSHOT_FILE"
echo "shopt -s expand_aliases" >> "$SNAPSHOT_FILE"`;
  const userContent = configFileExists
    ? `echo "# Functions" >> "$SNAPSHOT_FILE"
${functions}
echo "# Shell Options" >> "$SNAPSHOT_FILE"
${options}
echo "# Aliases" >> "$SNAPSHOT_FILE"
alias | sed 's/^alias //g' | sed 's/^/alias -- /' | head -n 1000 >> "$SNAPSHOT_FILE"`
    : isZsh
      ? '# No user config file to source'
      : 'echo "shopt -s expand_aliases" >> "$SNAPSHOT_FILE"';
  const configFile = snapshotConfigFile(shellPath);
  // Append the ripgrep shell integration (when AMI has a non-system ripgrep
  // configured) so a model-typed `rg` in the sourced snapshot dispatches to the
  // same binary the grep/glob tools use. A quoted heredoc delimiter keeps the
  // snippet (which contains `$@`, `$$`, `$BASHPID`) literal in the capture pass.
  const ripgrepIntegration = buildRipgrepShellIntegration();
  const ripgrepBlock = ripgrepIntegration
    ? `cat >> "$SNAPSHOT_FILE" <<'AMI_RG_SNIPPET_EOF'
${ripgrepIntegration}
AMI_RG_SNIPPET_EOF`
    : '# No ripgrep shell integration (system ripgrep)';
  return `SNAPSHOT_FILE="$AMI_SNAPSHOT_FILE"
${configFileExists ? `source ${shQuote(configFile)} < /dev/null` : '# No user config file to source'}
echo "# Snapshot file" >| "$SNAPSHOT_FILE"
echo "unalias -a 2>/dev/null || true" >> "$SNAPSHOT_FILE"
${userContent}
echo "export PATH=${shQuote(process.env.PATH || '')}" >> "$SNAPSHOT_FILE"
${ripgrepBlock}
if [ ! -f "$SNAPSHOT_FILE" ]; then echo "snapshot not created" >&2; exit 1; fi`;
}

// ---------------------------------------------------------------------------
// Cross-shell provider abstraction (PowerShell as a first-class shell type)
//
// Mirrors the reference's `src/utils/shell/*` provider split (shellProvider.ts
// SHELL_TYPES/ShellType, powershellProvider.ts invocation flags + exit-code
// normalization, powershellDetection.ts pwsh-over-powershell preference + snap
// avoidance + memoized cache, resolveDefaultShell.ts safe default). AMI keeps
// bash/zsh as the POSIX default and treats PowerShell as an OPT-IN shell family
// selected per call ({@link ExecCommandOptions.shellType}) or globally via the
// `AMI_DEFAULT_SHELL` / `CLAUDE_CODE_SHELL_TYPE` env knob — never auto-flipping
// Windows to PowerShell (parity with the reference's resolveDefaultShell, which does
// not auto-flip either).
// ---------------------------------------------------------------------------

/** The shell families AMI can spawn a command through. */
export const SHELL_TYPES = ['bash', 'powershell'] as const;

export type ShellType = (typeof SHELL_TYPES)[number];

/** Platform-independent default shell family (parity: the reference never auto-flips to PowerShell). */
export const DEFAULT_SHELL_TYPE: ShellType = 'bash';

/**
 * Resolve the default shell family for commands that do not name one, from the
 * `AMI_DEFAULT_SHELL` (or `CLAUDE_CODE_SHELL_TYPE` for parity) env knob. An
 * unset / blank / unrecognized value safely falls back to {@link DEFAULT_SHELL_TYPE}
 * (`'bash'`) — mirrors the reference's `resolveDefaultShell()` safe fallback.
 */
export function resolveDefaultShell(): ShellType {
  const raw = (process.env.AMI_DEFAULT_SHELL || process.env.CLAUDE_CODE_SHELL_TYPE || '').trim().toLowerCase();
  return (SHELL_TYPES as readonly string[]).includes(raw) ? (raw as ShellType) : DEFAULT_SHELL_TYPE;
}

// ---------------------------------------------------------------------------
// Per-shell async environment overrides (getEnvironmentOverrides)
//
// Parity with the reference's `ShellProvider.getEnvironmentOverrides(command)`
// (shellProvider.ts:L28-32): an async hook that computes extra env vars for the
// resolved shell family just before spawn and MAY perform async initialization.
// The reference cites tmux socket setup for bash — deferring the cost until a
// command actually uses tmux, then pointing tmux at an isolated socket so AMI-run
// tmux servers do not collide with the user's default socket.
//
// AMI adaptation:
//  - The seam is OPT-IN (env knob {@link TMUX_ISOLATION_ENV}) so default behavior
//    is unchanged; a command that doesn't use tmux (or has isolation off) gets an
//    empty override and the executor's deterministic same-tick spawn is preserved.
//  - Isolation is achieved via `TMUX_TMPDIR` (the dir tmux uses for its default
//    server socket), NOT by overriding `TMUX` — so tmux still auto-starts/connects
//    its server on demand instead of mistaking the value for a nested session.
//  - /env session vars are applied at the bash-tool env-build layer
//    (buildSubprocessEnv → options.env), NOT here, so there is no double-apply —
//    this seam only adds the orthogonal per-shell async init the reference does.
// ---------------------------------------------------------------------------

/**
 * Per-shell environment overrides merged into the spawn env by {@link execCommand}
 * just before launch. Parity with the reference's
 * `ShellProvider.getEnvironmentOverrides(command)`.
 *
 * Returns a plain record synchronously for the common case (so the executor's
 * deterministic same-tick spawn is preserved), and a `Promise` only when a shell
 * genuinely needs async init — currently the bash tmux-socket isolation path
 * ({@link TMUX_ISOLATION_ENV} enabled AND the command {@link commandUsesTmux}).
 * The tmux-socket state machine (cached availability probe, concurrent-init
 * dedup, graceful degradation, on-exit teardown) lives in {@link ./tmux-socket}.
 * PowerShell and the non-tmux bash path return an empty record.
 */
export function getEnvironmentOverrides(
  shellType: ShellType,
  command: string,
): Record<string, string> | Promise<Record<string, string>> {
  if (shellType === 'bash' && tmuxIsolationEnabled() && commandUsesTmux(command)) {
    return prepareBashTmuxOverrides(resolveTmpBase());
  }
  return {};
}

/**
 * The hardened, non-interactive PowerShell invocation flags + command. Shared so
 * the flag set lives in exactly one place (parity with the reference's
 * `buildPowerShellArgs` in powershellProvider.ts):
 * - `-NoProfile` skips the user/system profile scripts (deterministic env, no
 *   slow or side-effecting startup), and
 * - `-NonInteractive` makes PowerShell fail fast instead of prompting, so a
 *   command that would block on a `Read-Host` returns rather than hanging.
 */
export function buildPowerShellArgs(command: string): string[] {
  return ['-NoProfile', '-NonInteractive', '-Command', command];
}

/**
 * Append PowerShell exit-code normalization to a command so the spawned process
 * exits with the correct code (parity with the reference's powershellProvider.ts
 * cwd-tracking `exit $_ec`).
 *
 * On Windows PowerShell 5.1 a native exe that merely writes to a PS-redirected
 * stderr (e.g. `git push 2>&1`) sets `$?` to `$false` even when the exe returned
 * exit 0, so a naive `if (!$?)` reports a false-positive failure. `$LASTEXITCODE`
 * holds the real native exit code and is `$null` only when no native exe has run
 * yet — so we prefer it and fall back to `$?` for cmdlet-only pipelines.
 */
export function withPowerShellExitCode(command: string): string {
  return `${command}\n$_ec = if ($null -ne $LASTEXITCODE) { $LASTEXITCODE } elseif ($?) { 0 } else { 1 }\nexit $_ec`;
}

/** Env knob naming an explicit PowerShell executable path (overrides detection). */
export const POWERSHELL_PATH_ENV = ['AMI_POWERSHELL_PATH', 'CLAUDE_CODE_POWERSHELL_PATH'] as const;

/**
 * Known non-snap PowerShell install locations probed when PATH resolves `pwsh`
 * to a snap launcher (which can hang in subprocesses while snapd initializes
 * confinement). The underlying apt/rpm binary is reliable.
 */
export const POWERSHELL_PROBE_PATHS = ['/opt/microsoft/powershell/7/pwsh', '/usr/bin/pwsh', '/usr/local/bin/pwsh'];

// ---------------------------------------------------------------------------
// Windows git-bash resolution (trusted bash.exe discovery)
//
// On Windows AMI runs user bash commands through git-bash's `bash.exe` — the
// same POSIX semantics the bash security guards assume — NOT `cmd`. Parity with
// the reference's windowsPaths.ts (findExecutable + findGitBashPath): an explicit
// AMI_GIT_BASH_PATH / CLAUDE_CODE_GIT_BASH_PATH override wins, else git's default
// install locations are probed, else `where.exe` resolves `git` with a
// cwd-shadow security filter and `bash.exe` is derived from it. Unlike the reference
// (which `process.exit(1)`s when none is found), AMI returns `null` so
// {@link execCommand} surfaces an actionable launch failure instead of killing
// the whole process — a library-safe divergence.
// ---------------------------------------------------------------------------

/** Env knobs naming an explicit git-bash `bash.exe` path (overrides discovery). */
export const GIT_BASH_PATH_ENV = ['AMI_GIT_BASH_PATH', 'CLAUDE_CODE_GIT_BASH_PATH'] as const;

/**
 * Default git-for-Windows install locations probed before `where.exe` (64-bit
 * before 32-bit). The `cmd\git.exe` entry is used — not `mingw64\bin\git.exe`,
 * whose directory is the "raw" tools without the environment setup git-bash
 * expects. Parity with the reference's `findExecutable` default locations.
 */
export const GIT_DEFAULT_LOCATIONS = [
  'C:\\Program Files\\Git\\cmd\\git.exe',
  'C:\\Program Files (x86)\\Git\\cmd\\git.exe',
];

/**
 * Build the user-facing launch-failure message when git-bash cannot be resolved
 * on Windows. When the user set an explicit git-bash path env var
 * ({@link GIT_BASH_PATH_ENV}) the bad value is echoed back so a typo is obvious —
 * a message distinct from the "no git-bash installed at all" case. Parity with
 * the reference distinguishing the explicitly-configured-but-missing path
 * (windowsPaths.ts:L104-106) from git-bash-not-found (L120-122). Used by
 * {@link execCommand} to surface an actionable exit-126 launch failure instead of
 * exiting the process.
 */
export function describeGitBashResolutionError(): string {
  const override = GIT_BASH_PATH_ENV.map(k => (process.env[k] || '').trim()).find(Boolean);
  if (override) {
    return `The configured git-bash path "${override}" does not exist. AMI runs commands through bash on Windows: fix AMI_GIT_BASH_PATH to point at your bash.exe, or install git-bash (https://git-scm.com/downloads/win).`;
  }
  return 'No git-bash found. AMI runs commands through bash on Windows: install git-bash (https://git-scm.com/downloads/win), or set AMI_GIT_BASH_PATH to its bash.exe.';
}
