// Split from powershell-security.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { COMMON_ALIASES } from './powershell-parser';

/**
 * Normalize a type name as it appears in a PowerShell cast literal.
 * Handles: case folding, array suffix (`String[]` → `string`), and generic
 * brackets (`List[int]` → `list`, conservative — the outer generic wrapper may
 * be unsafe even when the type argument is safe, so we check the outer type).
 */
export function normalizeTypeName(name: string): string {
  return name
    .toLowerCase()
    .replace(/\[\]$/, '')
    .replace(/\[.*\]$/, '')
    .trim();
}

// A PowerShell type literal applied as a cast or static-member access: `[Type]`
// immediately followed (modulo whitespace) by a value token (`'`, `"`, `$`, `@`,
// `(`) or a static-member accessor (`::`). The type name starts with a letter,
// may contain dots (FQ names) and a trailing `[]` (array). Requiring the literal
// to be *applied* (a value/`::` follows) means bash array/glob/test-bracket
// syntax (`[0]`, `[a-z]*`, `[ -f x ]`, `${a[i]}`) does not match.
export const PS_TYPE_CAST_RE = /\[([A-Za-z_][A-Za-z0-9_.]*(?:\[\])?)\]\s*(?=['"$@(]|::)/g;

// PowerShell shells / process spawners / code loaders that must never be
// offered as a bare wildcard prefix. The bare shell names (pwsh/powershell/cmd/
// bash/…) are already covered by permissions.ts's BARE_SHELL_PREFIXES, so this
// list holds only the PowerShell-cmdlet spawners/loaders not represented there.
export const PS_SPAWNERS_AND_LOADERS: readonly string[] = [
  'start-process',
  'add-type',
  'new-object',
];

/**
 * Alias forms (from the shared {@link COMMON_ALIASES} map) whose canonical
 * cmdlet is in `canonical` — so `invoke-expression` in the set automatically
 * pulls in `iex`. Mirrors the reference's `aliasesOf()` derivation: add a cmdlet
 * to one validator list and its aliases follow, no separate maintenance.
 */
export function aliasesOf(canonical: ReadonlySet<string>): string[] {
  return Object.entries(COMMON_ALIASES)
    .filter(([, target]) => canonical.has(target.toLowerCase()))
    .map(([alias]) => alias.toLowerCase());
}

export type PsDetection = { blocked: boolean; reason?: string };

export const PS_PASS: PsDetection = { blocked: false };

/**
 * Split a command into the whitespace-separated tokens that follow the first
 * command-position match of `cmdletRe`, up to the next statement/pipeline
 * separator. Quoted runs are preserved as single tokens (quotes stripped).
 * Used by the New-Object / Start-Process argument scanners below.
 */
export function argTokensAfter(command: string, cmdletRe: RegExp): string[] | undefined {
  const m = cmdletRe.exec(command);
  if (!m) {
    return undefined;
  }
  const rest = command.slice(m.index + m[0].length);
  // Stop at the next top-level statement or pipeline boundary.
  const segment = rest.split(/[;|\n\r]/)[0] ?? '';
  const raw = segment.match(/'[^']*'|"[^"]*"|\S+/g) ?? [];
  return raw.map((t) => t.replace(/^['"]|['"]$/g, ''));
}

/**
 * Stop-parsing token (`--%`) detection. PowerShell's `--%` tells the parser to
 * stop interpreting everything that follows and pass the remainder verbatim to
 * the target command — defeating any structural/token analysis of the tail
 * (a documented analysis-evasion / obfuscation technique). Like
 * `-EncodedCommand`, the remainder is opaque to the security checks, so its
 * presence is surfaced for approval (fail-safe: an un-analyzable command is
 * never silently allowed).
 *
 * PowerShell's tokenizer (SpecialCharacters.IsDash) treats en-dash (U+2013),
 * em-dash (U+2014) and horizontal-bar (U+2015) as dash-equivalents, so those
 * variants are normalized to ASCII `-` before matching — mirroring the reference's
 * parser token check `($tok.Text -replace '[–—―]','-') -eq '--%'`.
 * The token must stand alone (bounded by start/whitespace/statement separators)
 * so substrings like `git log --%H` or `sed 's/--%/x/'` do not match.
 */
export function detectStopParsingToken(command: string): PsDetection {
  const normalized = command.replace(/[–—―]/g, '-');
  if (/(?:^|[\s;|&(){}])--%(?=$|[\s;|&(){}])/.test(normalized)) {
    return {
      blocked: true,
      reason:
        'PowerShell stop-parsing token (--%) passes everything after it verbatim to the command — opaque to security analysis',
    };
  }
  return PS_PASS;
}

// ---------------------------------------------------------------------------
// PowerShell common parameters.
//
// Mirrors the reference's PowerShellTool/commonParameters.ts: the switches and
// value-taking parameters that [CmdletBinding()] adds to EVERY advanced cmdlet.
// A value-param consumes the token after it, so a positional-argument scanner
// must skip those values — otherwise `Start-Job -ErrorAction Stop` would
// mistake `Stop` for a positional -FilePath. Stored lowercase with a leading
// dash; callers lowercase their input. Source: about_CommonParameters.
// ---------------------------------------------------------------------------

export const COMMON_SWITCHES: readonly string[] = ['-verbose', '-debug'];

export const COMMON_VALUE_PARAMS: readonly string[] = [
  '-erroraction',
  '-warningaction',
  '-informationaction',
  '-progressaction',
  '-errorvariable',
  '-warningvariable',
  '-informationvariable',
  '-outvariable',
  '-outbuffer',
  '-pipelinevariable',
];

// Alt parameter-prefix characters PowerShell's tokenizer (SpecialCharacters.
// IsDash) accepts as equivalent to ASCII '-': '/', en-dash, em-dash, horizontal
// bar. Normalized to '-' before any parameter-abbreviation test so
// `Start-Job –FilePath x` (en-dash) cannot bypass the check.
export const PS_ALT_PARAM_PREFIX_RE = /^[/–—―]/;

export function normalizeParamToken(token: string): string {
  return token.replace(/`/g, '').replace(PS_ALT_PARAM_PREFIX_RE, '-').toLowerCase();
}

/**
 * True if `arg` is an abbreviation of `fullParam` at least as long as
 * `minPrefix` (e.g. matches `-f`, `-fi`, … `-filepath` for fullParam
 * `-filepath`, minPrefix `-f`). Tolerant of alt dash prefixes and colon-bound
 * values (`-FilePath:x`). Mirrors the reference's psExeHasParamAbbreviation.
 */
export function argMatchesParam(arg: string, fullParam: string, minPrefix: string): boolean {
  const name = normalizeParamToken(arg).split(':')[0]!;
  return (
    name.startsWith('-') &&
    name.length >= minPrefix.length &&
    fullParam.startsWith(name)
  );
}

/**
 * True if the token list contains a non-dash positional string argument, after
 * accounting for common value-parameters (which consume the following token).
 * Over-flagging any positional string is fail-safe — the reference does the same
 * for -FilePath / -MemberName positional binding.
 */
export function hasPositionalStringArg(args: string[]): boolean {
  for (let i = 0; i < args.length; i++) {
    const norm = normalizeParamToken(args[i]!);
    // A script-block argument (`{ … }`) selects the ScriptBlock parameter-set,
    // not -FilePath/-MemberName, so a positional path/member cannot bind. Stop
    // scanning — the script-block hazard is covered by detectScriptBlockInjection.
    if (args[i]!.startsWith('{')) {
      return false;
    }
    if (norm.startsWith('-')) {
      const name = norm.split(':')[0]!;
      if (
        !norm.includes(':') &&
        COMMON_VALUE_PARAMS.some((p) => name.length >= 3 && p.startsWith(name))
      ) {
        i++; // value-param consumes its following value
      }
      continue;
    }
    return true;
  }
  return false;
}

/**
 * Dynamic command-name invocation (mirrors the reference's checkDynamicCommandName):
 * the call operator `&` applied to a runtime expression rather than a literal
 * command name — `& ('i'+'ex')`, `& ('iex','x')[0]`, `& ${function:Invoke-Expression}`,
 * `& @(...)`. The command name is computed at runtime and cannot be statically
 * validated, so it is surfaced for approval. Only the PowerShell-distinctive
 * forms (`& (` quoted/concat expr, `& ${…}`, `& @(`) are matched so the check
 * cannot misfire on a POSIX background `cmd &` + subshell.
 */
export function detectDynamicCommandName(command: string): PsDetection {
  if (/&\s*(?:\$\{|\(\s*['"]|@\()/.test(command)) {
    return {
      blocked: true,
      reason:
        'PowerShell invokes a command whose name is a dynamic expression (call operator &) which cannot be statically validated',
    };
  }
  return PS_PASS;
}

// ---------------------------------------------------------------------------
// PowerShell read-only cmdlet allowlist (auto-allow).
//
// Mirrors the reference's PowerShellTool/readOnlyValidation.ts CMDLET_ALLOWLIST /
// isReadOnlyCommand: the Windows counterpart of the POSIX read-only SAFE_COMMANDS
// set in permissions.ts. A command composed solely of these side-effect-free
// cmdlets (Get-ChildItem, Get-Content, Select-String, …) is auto-allowed in
// `ask` mode so a user is not prompted for a plain read on Windows (where
// PowerShell is AMI's default shell).
//
// Only the hyphenated Verb-Noun cmdlet names are listed — never a bare alias
// (`cat`, `ls`, `sort`, `echo`, `cd`): those collide with POSIX commands already
// classified by SAFE_COMMANDS, and resolving them here could auto-allow a bash
// command that the bash classifier would gate. Hyphenated cmdlet names cannot
// collide with a POSIX command, so this set is PowerShell-only by construction.
//
// Every listed cmdlet is a pure reader/formatter/transformer with no filesystem
// or state write: Get-* (read), Format-*/Out-*/ConvertTo-*/ConvertFrom-*/
// Join-*/Split-*/Measure-*/Group-*/Sort-*/Select-*/Compare-* (pipeline shaping,
// output only), Test-Path/Resolve-Path/Convert-Path (path probes), and the cwd
// movers (Set/Push/Pop-Location — a bare cwd change is inert; the cd+write and
// multi-cd desync guards in the classifier run BEFORE this check). Writers
// (Set-/Add-/Out-File/Export-/Tee-Object/New-/Remove-Item, Invoke-*) are
// deliberately absent.
// ---------------------------------------------------------------------------

/**
 * Per-cmdlet read-only flag configuration. Mirrors the reference's
 * readOnlyValidation.ts CMDLET_ALLOWLIST `CommandConfig`:
 *   - `safeFlags` — the ONLY flags the cmdlet may carry and still be a pure read
 *     (lowercase, leading dash — the runtime comparison form). A flag outside
 *     this set ∪ {@link COMMON_PARAMETERS} makes the command non-read-only (→ a
 *     prompt). Side-effecting / remote / external-entity flags (-ComputerName,
 *     -CimSession, Get-WinEvent -FilterXml/-FilterHashtable) are deliberately
 *     omitted so they are NOT auto-allowed.
 *   - `allowAllFlags` — the cmdlet's entire flag surface is display/selection
 *     only (Format-*, Select-Object, Get-ComputerInfo, …); every flag is
 *     accepted. The dangerous-arg VALUE vectors those cmdlets have in the reference
 *     (calculated-property hashtables, `$env:` leaks, script blocks) are already
 *     rejected wholesale by {@link PS_READONLY_DISQUALIFY_RE} — strictly more
 *     conservative than the reference's per-arg `argLeaksValue` element-type whitelist.
 *
 * COMMON_PARAMETERS (-Verbose/-ErrorAction/-ErrorVariable/…) are merged in at
 * check time, so they are never restated here.
 */
export type PowerShellReadOnlyConfig = {
  /** Flags this read-only cmdlet accepts (lowercase, leading dash). */
  safeFlags?: readonly string[];
  /** When true, every flag is accepted (whole flag surface is read-only). */
  allowAllFlags?: boolean;
};
