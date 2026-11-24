/**
 * PowerShell AST-based command structural analysis — the reference parity.
 *
 * On Windows AMI tells the model to use PowerShell, yet the only command
 * security it had was the bash/zsh/cmd-oriented regex layer in
 * `bash-security.ts` (plus the text heuristics in `detectPowerShellExecution`).
 * PowerShell syntax — cmdlets, pipelines, subexpressions `$( )`, `@()` array
 * evaluation, script blocks, splatting, nested control-flow commands, and
 * hashtable-embedded redirections — was never *structurally* analysed, so
 * PowerShell-specific permission bypasses went undetected.
 *
 * This module mirrors the reference's `utils/powershell/parser.ts`. It parses a
 * PowerShell command with the native .NET AST parser (spawning `pwsh`/
 * `powershell` with `Parser::ParseInput` via `-EncodedCommand`) into a
 * structured {@link ParsedPowerShellCommand}, and provides the pure-TS analysis
 * layer that reasons about that structure instead of raw text:
 *
 *  - alias → canonical cmdlet resolution ({@link COMMON_ALIASES},
 *    {@link resolveToCanonical}) with a prototype-pollution-safe table and the
 *    deliberate PS-Core native-.exe-collision omissions (sort/curl/wget/sc);
 *  - deep redirection hoisting + `$null` sink exemption
 *    ({@link transformStatement}, {@link getFileRedirections},
 *    {@link isNullRedirectionTarget});
 *  - security-flag derivation from the AST element types with a
 *    belt-and-suspenders `securityPatterns` cross-check ({@link deriveSecurityFlags});
 *  - non-ASCII cmdlet homoglyph defense, module-prefix stripping, parameter /
 *    abbreviation detection, and a platform-gated UTF-8 byte-budget length gate.
 *
 * The spawn runtime is injectable ({@link __setPwshRuntimeForTest}) so the full
 * parse pipeline is exercisable without a real `pwsh` on the host.
 *
 * [FUNCTIONAL CHANGE] New module — structural PowerShell command analysis
 * (the reference parity, parser.ts). Additive: the existing regex-based
 * `detectPowerShellExecution` text heuristics are retained alongside it.
 */

import 'node:child_process';
import { log } from '../logger';
import '../utils/which';
import { memoizeWithLRU } from '../utils/windows-paths';
import { FIXED_ARGV_OVERHEAD, PARSE_SCRIPT_BODY, SAFETY_MARGIN, UNIX_MAX_COMMAND_LENGTH, WINDOWS_ARGV_CAP, buildParseScript, getParseTimeoutMs, makeInvalidResult, toUtf16LeBase64 } from './powershell-parser-parse-script-body';
import type { CommandElementType, ParsedCommandElement, ParsedPowerShellCommand, RawParsedOutput } from './powershell-parser-parse-script-body';
import { WINDOWS_PATHEXT, defaultResolvePwshPath, defaultSpawn, getAllCommandNames, transformRawOutput } from './powershell-parser-transform-command-ast';
import type { PwshRuntime, PwshSpawnResult } from './powershell-parser-transform-command-ast';
export { COMMAND_ELEMENT_TYPES, PARSE_SCRIPT_BODY, PIPELINE_ELEMENT_TYPES, REDIRECTION_OPERATORS, STATEMENT_TYPES, buildParseScript, getParseTimeoutMs, toUtf16LeBase64 } from './powershell-parser-parse-script-body';
export type { CommandElementChild, CommandElementType, ParseError, ParsedCommandElement, ParsedPowerShellCommand, ParsedRedirection, ParsedStatement, ParsedVariable, PipelineElementType, RawCommandElement, RawParsedOutput, RawPipelineElement, RawRedirection, RawStatement, RedirectionOperator, StatementType } from './powershell-parser-parse-script-body';
export { classifyCommandName, getAllCommandNames, mapElementType, mapStatementType, stripModulePrefix, transformCommandAst, transformExpressionElement, transformRawOutput, transformRedirection, transformStatement } from './powershell-parser-transform-command-ast';
export type { PwshRuntime, PwshSpawnResult } from './powershell-parser-transform-command-ast';
export { commandHasArg, commandHasArgAbbreviation, getAllCommands, getAllRedirections, getFileRedirections, getPipelineSegments, getVariablesByScope, isNullRedirectionTarget, isSingleCommand } from './powershell-parser-command-has-arg-abbreviation';
const ENCODED_CMD_WRAPPER = `$EncodedCommand = ''\n`.length;
const SCRIPT_CHARS_BUDGET = ((WINDOWS_ARGV_CAP - FIXED_ARGV_OVERHEAD) * 3) / 8;
const CMD_B64_BUDGET =
  SCRIPT_CHARS_BUDGET - PARSE_SCRIPT_BODY.length - ENCODED_CMD_WRAPPER;
/** Unit: UTF-8 BYTES. Compare against Buffer.byteLength, not .length. */
export const WINDOWS_MAX_COMMAND_LENGTH = Math.max(
  0,
  Math.floor((CMD_B64_BUDGET * 3) / 4) - SAFETY_MARGIN,
);
/** Unit: UTF-8 BYTES (see note above). Platform-gated. */
export const MAX_COMMAND_LENGTH =
  process.platform === 'win32'
    ? WINDOWS_MAX_COMMAND_LENGTH
    : UNIX_MAX_COMMAND_LENGTH;

const DEFAULT_PWSH_RUNTIME: PwshRuntime = {
  resolvePwshPath: defaultResolvePwshPath,
  spawn: defaultSpawn,
};

let pwshRuntime: PwshRuntime = DEFAULT_PWSH_RUNTIME;

/** Test seam: override the pwsh spawn runtime. */
export function __setPwshRuntimeForTest(rt: Partial<PwshRuntime>): void {
  pwshRuntime = { ...DEFAULT_PWSH_RUNTIME, ...rt };
}

/** Test seam: restore the default pwsh spawn runtime and clear the cache. */
export function __resetPwshRuntimeForTest(): void {
  pwshRuntime = DEFAULT_PWSH_RUNTIME;
  parsePowerShellCommandCached.cache.clear();
}

async function parsePowerShellCommandImpl(
  command: string,
): Promise<ParsedPowerShellCommand> {
  // SECURITY: MAX_COMMAND_LENGTH is a UTF-8 BYTE budget (CJK chars are 3 bytes
  // but 1 code unit). Measure Buffer.byteLength, not command.length.
  const commandBytes = Buffer.byteLength(command, 'utf8');
  if (commandBytes > MAX_COMMAND_LENGTH) {
    log('powershell-parser', 'command too long for parsing', {
      bytes: commandBytes,
      max: MAX_COMMAND_LENGTH,
    });
    return makeInvalidResult(
      command,
      `Command too long for parsing (${commandBytes} bytes). Maximum supported length is ${MAX_COMMAND_LENGTH} bytes.`,
      'CommandTooLong',
    );
  }

  const pwshPath = pwshRuntime.resolvePwshPath();
  if (!pwshPath) {
    return makeInvalidResult(
      command,
      'PowerShell is not available',
      'NoPowerShell',
    );
  }

  const script = buildParseScript(command);
  const encodedScript = toUtf16LeBase64(script);
  const args = [
    '-NoProfile',
    '-NonInteractive',
    '-NoLogo',
    '-EncodedCommand',
    encodedScript,
  ];

  // Spawn with one retry on timeout — loaded CI runners occasionally exceed the
  // timeout on the first .NET JIT + ParseInput even after warmup.
  const parseTimeoutMs = getParseTimeoutMs();
  let result: PwshSpawnResult | undefined;
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      result = await pwshRuntime.spawn(pwshPath, args, parseTimeoutMs);
    } catch (e: unknown) {
      log('powershell-parser', 'failed to spawn pwsh', {
        error: e instanceof Error ? e.message : String(e),
      });
      return makeInvalidResult(
        command,
        `Failed to spawn PowerShell: ${e instanceof Error ? e.message : e}`,
        'PwshSpawnError',
      );
    }
    if (!result.timedOut) break;
    log('powershell-parser', 'pwsh timed out', {
      timeoutMs: parseTimeoutMs,
      attempt: attempt + 1,
    });
  }

  if (!result || result.timedOut) {
    return makeInvalidResult(
      command,
      `pwsh timed out after ${parseTimeoutMs}ms (2 attempts)`,
      'PwshTimeout',
    );
  }

  if (result.exitCode !== 0) {
    log('powershell-parser', 'pwsh exited non-zero', {
      code: result.exitCode,
      stderr: result.stderr,
    });
    return makeInvalidResult(
      command,
      `pwsh exited with code ${result.exitCode}: ${result.stderr}`,
      'PwshError',
    );
  }

  const trimmed = result.stdout.trim();
  if (!trimmed) {
    log('powershell-parser', 'empty stdout from pwsh');
    return makeInvalidResult(
      command,
      'No output from PowerShell parser',
      'EmptyOutput',
    );
  }

  try {
    const raw = JSON.parse(trimmed) as RawParsedOutput;
    return transformRawOutput(raw);
  } catch {
    log('powershell-parser', 'invalid JSON from pwsh', {
      preview: trimmed.slice(0, 200),
    });
    return makeInvalidResult(
      command,
      'Invalid JSON from PowerShell parser',
      'InvalidJson',
    );
  }
}

// Error IDs representing transient process failures — evicted from the cache so
// subsequent calls retry. Deterministic failures (CommandTooLong, syntax errors
// from a successful parse) stay cached.
const TRANSIENT_ERROR_IDS = new Set([
  'PwshSpawnError',
  'PwshError',
  'PwshTimeout',
  'EmptyOutput',
  'InvalidJson',
]);

const parsePowerShellCommandCached = memoizeWithLRU<
  Promise<ParsedPowerShellCommand>
>((command: string) => {
  const promise = parsePowerShellCommandImpl(command);
  // Evict transient failures after resolution so they can be retried. The
  // current caller still receives this promise, so concurrent callers share it.
  void promise.then((r) => {
    if (!r.valid && TRANSIENT_ERROR_IDS.has(r.errors[0]?.errorId ?? '')) {
      parsePowerShellCommandCached.cache.delete(command);
    }
  });
  return promise;
}, 256);

/**
 * Parse a PowerShell command using the native AST parser. Spawns pwsh to parse
 * the command and returns the structured result (or valid=false on failure).
 * Results are memoized by command string.
 */
export function parsePowerShellCommand(
  command: string,
): Promise<ParsedPowerShellCommand> {
  return parsePowerShellCommandCached(command);
}

// ---------------------------------------------------------------------------
// Alias canonicalisation.
// ---------------------------------------------------------------------------

/**
 * Common PowerShell aliases mapped to their canonical cmdlet names. Uses
 * Object.create(null) so attacker-controlled command names like 'constructor'
 * or '__proto__' return undefined, not inherited Object.prototype properties.
 *
 * SECURITY: sort/curl/wget/sc are DELIBERATELY omitted — on PS Core 6+ those
 * resolve to native .exe, so mapping them would auto-allow the wrong program.
 */
export const COMMON_ALIASES: Record<string, string> = Object.assign(
  Object.create(null) as Record<string, string>,
  {
    ls: 'Get-ChildItem',
    dir: 'Get-ChildItem',
    gci: 'Get-ChildItem',
    cat: 'Get-Content',
    type: 'Get-Content',
    gc: 'Get-Content',
    cd: 'Set-Location',
    sl: 'Set-Location',
    chdir: 'Set-Location',
    pushd: 'Push-Location',
    popd: 'Pop-Location',
    pwd: 'Get-Location',
    gl: 'Get-Location',
    gi: 'Get-Item',
    gp: 'Get-ItemProperty',
    ni: 'New-Item',
    mkdir: 'New-Item',
    // `md` is PowerShell's built-in alias for `mkdir`. resolveToCanonical is
    // single-hop (no md→mkdir→New-Item chaining), so it needs its own entry.
    md: 'New-Item',
    ri: 'Remove-Item',
    del: 'Remove-Item',
    rd: 'Remove-Item',
    rmdir: 'Remove-Item',
    rm: 'Remove-Item',
    erase: 'Remove-Item',
    mi: 'Move-Item',
    mv: 'Move-Item',
    move: 'Move-Item',
    ci: 'Copy-Item',
    cp: 'Copy-Item',
    copy: 'Copy-Item',
    cpi: 'Copy-Item',
    si: 'Set-Item',
    rni: 'Rename-Item',
    ren: 'Rename-Item',
    ps: 'Get-Process',
    gps: 'Get-Process',
    kill: 'Stop-Process',
    spps: 'Stop-Process',
    start: 'Start-Process',
    saps: 'Start-Process',
    sajb: 'Start-Job',
    ipmo: 'Import-Module',
    echo: 'Write-Output',
    write: 'Write-Output',
    sleep: 'Start-Sleep',
    help: 'Get-Help',
    man: 'Get-Help',
    gcm: 'Get-Command',
    gsv: 'Get-Service',
    gv: 'Get-Variable',
    sv: 'Set-Variable',
    h: 'Get-History',
    history: 'Get-History',
    iex: 'Invoke-Expression',
    iwr: 'Invoke-WebRequest',
    irm: 'Invoke-RestMethod',
    icm: 'Invoke-Command',
    ii: 'Invoke-Item',
    nsn: 'New-PSSession',
    etsn: 'Enter-PSSession',
    exsn: 'Exit-PSSession',
    gsn: 'Get-PSSession',
    rsn: 'Remove-PSSession',
    cls: 'Clear-Host',
    clear: 'Clear-Host',
    select: 'Select-Object',
    where: 'Where-Object',
    foreach: 'ForEach-Object',
    '%': 'ForEach-Object',
    '?': 'Where-Object',
    measure: 'Measure-Object',
    ft: 'Format-Table',
    fl: 'Format-List',
    fw: 'Format-Wide',
    oh: 'Out-Host',
    ogv: 'Out-GridView',
    ac: 'Add-Content',
    clc: 'Clear-Content',
    tee: 'Tee-Object',
    epcsv: 'Export-Csv',
    sp: 'Set-ItemProperty',
    rp: 'Remove-ItemProperty',
    cli: 'Clear-Item',
    epal: 'Export-Alias',
    sls: 'Select-String',
  },
);

/**
 * Resolve a command name to its canonical cmdlet name using COMMON_ALIASES.
 * Strips Windows executable extensions from path-free names (so `git.exe` →
 * `git`); SECURITY: only when the name has no path separator — `scripts\git.exe`
 * is a relative path and must NOT canonicalise to `git`. Returns lowercase.
 */
export function resolveToCanonical(name: string): string {
  let lower = name.toLowerCase();
  if (!lower.includes('\\') && !lower.includes('/')) {
    lower = lower.replace(WINDOWS_PATHEXT, '');
  }
  const alias = COMMON_ALIASES[lower];
  if (alias) return alias.toLowerCase();
  return lower;
}

const DIRECTORY_CHANGE_CMDLETS = new Set([
  'set-location',
  'push-location',
  'pop-location',
]);
const DIRECTORY_CHANGE_ALIASES = new Set([
  'cd',
  'sl',
  'chdir',
  'pushd',
  'popd',
]);

/** Case-insensitive check for a command by name, handling common aliases. */
export function hasCommandNamed(
  parsed: ParsedPowerShellCommand,
  name: string,
): boolean {
  const lowerName = name.toLowerCase();
  const canonicalFromAlias = COMMON_ALIASES[lowerName]?.toLowerCase();
  for (const cmdName of getAllCommandNames(parsed)) {
    if (cmdName === lowerName) return true;
    const canonical = COMMON_ALIASES[cmdName]?.toLowerCase();
    if (canonical === lowerName) return true;
    if (canonicalFromAlias && cmdName === canonicalFromAlias) return true;
    if (canonical && canonicalFromAlias && canonical === canonicalFromAlias)
      return true;
  }
  return false;
}

/** Whether the command contains any directory-changing command. */
export function hasDirectoryChange(parsed: ParsedPowerShellCommand): boolean {
  for (const cmdName of getAllCommandNames(parsed)) {
    if (
      DIRECTORY_CHANGE_CMDLETS.has(cmdName) ||
      DIRECTORY_CHANGE_ALIASES.has(cmdName)
    )
      return true;
  }
  return false;
}

/**
 * Tokenizer-level dash characters PowerShell accepts as parameter prefixes
 * (SpecialCharacters.IsDash): ASCII hyphen, en-dash, em-dash, horizontal bar.
 */
export const PS_TOKENIZER_DASH_CHARS = new Set([
  '-',
  '\u2013',
  '\u2014',
  '\u2015',
]);

/**
 * Whether an argument is a PowerShell parameter (flag). The AST element type is
 * authoritative when available ('Parameter' covers `-Path`/`–Path`/`—Path`);
 * otherwise falls back to a char check against PS_TOKENIZER_DASH_CHARS.
 */
export function isPowerShellParameter(
  arg: string,
  elementType?: CommandElementType,
): boolean {
  if (elementType !== undefined) return elementType === 'Parameter';
  return arg.length > 0 && PS_TOKENIZER_DASH_CHARS.has(arg[0]!);
}

/** Security-relevant flags derived from the parsed AST. */
export type SecurityFlags = {
  /** Contains $(...) / @() / (...) subexpression. */
  hasSubExpressions: boolean;
  /** Contains { ... } script block expressions. */
  hasScriptBlocks: boolean;
  /** Contains @variable splatting. */
  hasSplatting: boolean;
  /** Contains expandable strings with embedded expressions ("...$()..."). */
  hasExpandableStrings: boolean;
  /** Contains .NET method invocations ([Type]::Method or $obj.Method()). */
  hasMemberInvocations: boolean;
  /** Contains variable assignments ($x = ...). */
  hasAssignments: boolean;
  /** Uses stop-parsing token (--%). */
  hasStopParsing: boolean;
};

/**
 * Derive security-relevant flags from the parsed command structure. Walks the
 * per-element AST node types, with a belt-and-suspenders cross-check against the
 * per-statement `securityPatterns` (FindAll) to catch patterns elementTypes
 * miss (member invocations inside assignments, subexpressions in non-pipeline
 * statements).
 */
export function deriveSecurityFlags(
  parsed: ParsedPowerShellCommand,
): SecurityFlags {
  const flags: SecurityFlags = {
    hasSubExpressions: false,
    hasScriptBlocks: false,
    hasSplatting: false,
    hasExpandableStrings: false,
    hasMemberInvocations: false,
    hasAssignments: false,
    hasStopParsing: parsed.hasStopParsing,
  };

  function checkElements(cmd: ParsedCommandElement): void {
    if (!cmd.elementTypes) return;
    for (const et of cmd.elementTypes) {
      switch (et) {
        case 'ScriptBlock':
          flags.hasScriptBlocks = true;
          break;
        case 'SubExpression':
          flags.hasSubExpressions = true;
          break;
        case 'ExpandableString':
          flags.hasExpandableStrings = true;
          break;
        case 'MemberInvocation':
          flags.hasMemberInvocations = true;
          break;
      }
    }
  }

  for (const stmt of parsed.statements) {
    if (stmt.statementType === 'AssignmentStatementAst') {
      flags.hasAssignments = true;
    }
    for (const cmd of stmt.commands) checkElements(cmd);
    if (stmt.nestedCommands) {
      for (const cmd of stmt.nestedCommands) checkElements(cmd);
    }
    if (stmt.securityPatterns) {
      if (stmt.securityPatterns.hasMemberInvocations)
        flags.hasMemberInvocations = true;
      if (stmt.securityPatterns.hasSubExpressions)
        flags.hasSubExpressions = true;
      if (stmt.securityPatterns.hasExpandableStrings)
        flags.hasExpandableStrings = true;
      if (stmt.securityPatterns.hasScriptBlocks) flags.hasScriptBlocks = true;
    }
  }

  for (const v of parsed.variables) {
    if (v.isSplatted) {
      flags.hasSplatting = true;
      break;
    }
  }

  return flags;
}
