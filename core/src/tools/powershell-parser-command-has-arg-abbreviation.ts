// Split from powershell-parser.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import type { ParsedCommandElement, ParsedPowerShellCommand, ParsedRedirection, ParsedStatement, ParsedVariable } from './powershell-parser-parse-script-body';

/** All pipeline segments flattened to a single command list. */
export function getAllCommands(
  parsed: ParsedPowerShellCommand,
): ParsedCommandElement[] {
  const commands: ParsedCommandElement[] = [];
  for (const statement of parsed.statements) {
    for (const cmd of statement.commands) commands.push(cmd);
    if (statement.nestedCommands) {
      for (const cmd of statement.nestedCommands) commands.push(cmd);
    }
  }
  return commands;
}

/** All redirections across all statements (including nested commands). */
export function getAllRedirections(
  parsed: ParsedPowerShellCommand,
): ParsedRedirection[] {
  const redirections: ParsedRedirection[] = [];
  for (const statement of parsed.statements) {
    for (const redir of statement.redirections) redirections.push(redir);
    if (statement.nestedCommands) {
      for (const cmd of statement.nestedCommands) {
        if (cmd.redirections) {
          for (const redir of cmd.redirections) redirections.push(redir);
        }
      }
    }
  }
  return redirections;
}

/** All variables, optionally filtered by scope (e.g., 'env'). */
export function getVariablesByScope(
  parsed: ParsedPowerShellCommand,
  scope: string,
): ParsedVariable[] {
  const prefix = scope.toLowerCase() + ':';
  return parsed.variables.filter((v) =>
    v.path.toLowerCase().startsWith(prefix),
  );
}

/** Whether the command is a single simple command (no pipes/semicolons/ops). */
export function isSingleCommand(parsed: ParsedPowerShellCommand): boolean {
  const stmt = parsed.statements[0];
  return (
    parsed.statements.length === 1 &&
    stmt !== undefined &&
    stmt.commands.length === 1 &&
    (!stmt.nestedCommands || stmt.nestedCommands.length === 0)
  );
}

/** Case-insensitive check whether a command has a given argument/flag. */
export function commandHasArg(
  command: ParsedCommandElement,
  arg: string,
): boolean {
  const lowerArg = arg.toLowerCase();
  return command.args.some((a) => a.toLowerCase() === lowerArg);
}

/**
 * Whether any argument is an unambiguous abbreviation of a PowerShell parameter.
 * PowerShell allows parameter abbreviation as long as the prefix is unambiguous
 * (`-en`/`-enc`/`-enco` all match `-EncodedCommand`). Strips colon-bound values
 * (`-en:base64` → `-en`) and backtick escapes before comparison.
 */
export function commandHasArgAbbreviation(
  command: ParsedCommandElement,
  fullParam: string,
  minPrefix: string,
): boolean {
  const lowerFull = fullParam.toLowerCase();
  const lowerMin = minPrefix.toLowerCase();
  return command.args.some((a) => {
    const colonIndex = a.indexOf(':', 1);
    const paramPart = colonIndex > 0 ? a.slice(0, colonIndex) : a;
    const lower = paramPart.replace(/`/g, '').toLowerCase();
    return (
      lower.startsWith(lowerMin) &&
      lowerFull.startsWith(lower) &&
      lower.length <= lowerFull.length
    );
  });
}

/** Split a parsed command into its pipeline segments (one per statement). */
export function getPipelineSegments(
  parsed: ParsedPowerShellCommand,
): ParsedStatement[] {
  return parsed.statements;
}

/**
 * True if a redirection target is PowerShell's `$null` automatic variable.
 * `> $null` discards output (like /dev/null) — not a filesystem write. `${null}`
 * is the same variable via curly-brace syntax.
 */
export function isNullRedirectionTarget(target: string): boolean {
  const t = target.trim().toLowerCase();
  return t === '$null' || t === '${null}';
}

/** File redirections (writes), excluding merging redirections and `$null` sinks. */
export function getFileRedirections(
  parsed: ParsedPowerShellCommand,
): ParsedRedirection[] {
  return getAllRedirections(parsed).filter(
    (r) => !r.isMerging && !isNullRedirectionTarget(r.target),
  );
}
