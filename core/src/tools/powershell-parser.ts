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

import { execFile } from 'node:child_process';
import { log } from '../logger';
import { whichSync } from '../utils/which';
import { memoizeWithLRU } from '../utils/windows-paths';

// ---------------------------------------------------------------------------
// Public types describing the parsed output. These map to
// System.Management.Automation.Language AST classes.
// ---------------------------------------------------------------------------

/** AST element type for pipeline elements (CommandBaseAst derivatives). */
export const PIPELINE_ELEMENT_TYPES = ['CommandAst', 'CommandExpressionAst', 'ParenExpressionAst'] as const;
export type PipelineElementType = (typeof PIPELINE_ELEMENT_TYPES)[number];

/** AST node type for individual command elements (arguments, expressions). */
export const COMMAND_ELEMENT_TYPES = ['ScriptBlock', 'SubExpression', 'ExpandableString', 'MemberInvocation', 'Variable', 'StringConstant', 'Parameter', 'Other'] as const;
export type CommandElementType = (typeof COMMAND_ELEMENT_TYPES)[number];

/** A child node of a command element (one level deep; colon-bound params). */
export type CommandElementChild = {
  type: CommandElementType;
  text: string;
};

/** AST statement type (StatementAst derivatives). */
export const STATEMENT_TYPES = ['PipelineAst', 'PipelineChainAst', 'AssignmentStatementAst', 'IfStatementAst', 'ForStatementAst', 'ForEachStatementAst', 'WhileStatementAst', 'DoWhileStatementAst', 'DoUntilStatementAst', 'SwitchStatementAst', 'TryStatementAst', 'TrapStatementAst', 'FunctionDefinitionAst', 'DataStatementAst', 'UnknownStatementAst'] as const;
export type StatementType = (typeof STATEMENT_TYPES)[number];

/** A redirection operator found in the command. */
export const REDIRECTION_OPERATORS = ['>', '>>', '2>', '2>>', '*>', '*>>', '2>&1'] as const;
export type RedirectionOperator = (typeof REDIRECTION_OPERATORS)[number];

/** A command invocation within a pipeline segment. */
export type ParsedCommandElement = {
  /** The command/cmdlet name (e.g., "Get-ChildItem", "git"). */
  name: string;
  /** The command name type: cmdlet, application (exe), or unknown. */
  nameType: 'cmdlet' | 'application' | 'unknown';
  /** The AST element type from PowerShell's parser. */
  elementType: PipelineElementType;
  /** All arguments as strings (includes flags like "-Recurse"). */
  args: string[];
  /** The full text of this command element. */
  text: string;
  /** AST node types for each element in this command. */
  elementTypes?: CommandElementType[];
  /** Child nodes of each argument, aligned with `args[]`. */
  children?: (CommandElementChild[] | undefined)[];
  /** Redirections on this command element (from nested commands in && / ||). */
  redirections?: ParsedRedirection[];
};

/** A redirection found in the command. */
export type ParsedRedirection = {
  operator: RedirectionOperator;
  /** The target (file path or stream number). */
  target: string;
  /** Whether this is a merging redirection like 2>&1. */
  isMerging: boolean;
};

/** A parsed statement (pipeline, assignment, control flow, etc.). */
export type ParsedStatement = {
  statementType: StatementType;
  commands: ParsedCommandElement[];
  redirections: ParsedRedirection[];
  text: string;
  /** Commands found recursively inside control-flow body blocks (FindAll). */
  nestedCommands?: ParsedCommandElement[];
  /** Security-relevant AST patterns found via FindAll on the whole statement. */
  securityPatterns?: {
    hasMemberInvocations?: boolean;
    hasSubExpressions?: boolean;
    hasExpandableStrings?: boolean;
    hasScriptBlocks?: boolean;
  };
};

/** A variable reference found in the command. */
export type ParsedVariable = {
  /** The variable path (e.g., "HOME", "env:PATH", "global:x"). */
  path: string;
  /** Whether this variable uses splatting (@var instead of $var). */
  isSplatted: boolean;
};

/** A parse error from PowerShell's parser. */
export type ParseError = {
  message: string;
  errorId: string;
};

/** The complete parsed result from the PowerShell AST parser. */
export type ParsedPowerShellCommand = {
  valid: boolean;
  errors: ParseError[];
  statements: ParsedStatement[];
  variables: ParsedVariable[];
  /** Whether the token stream contains a stop-parsing (--%) token. */
  hasStopParsing: boolean;
  originalCommand: string;
  /** All .NET type literals found anywhere in the AST. */
  typeLiterals?: string[];
  /** Whether the command contains `using module`/`using assembly` statements. */
  hasUsingStatements?: boolean;
  /** Whether the command contains `#Requires` directives. */
  hasScriptRequirements?: boolean;
};

// ---------------------------------------------------------------------------
// Raw types describing the PS1 script's JSON output (exported for testing).
// ---------------------------------------------------------------------------

export type RawCommandElement = {
  type: string;
  text: string;
  value?: string;
  expressionType?: string;
  children?: { type: string; text: string }[];
};

export type RawRedirection = {
  type: string;
  append?: boolean;
  fromStream?: string;
  locationText?: string;
};

export type RawPipelineElement = {
  type: string;
  text: string;
  commandElements?: RawCommandElement[];
  redirections?: RawRedirection[];
  expressionType?: string;
};

export type RawStatement = {
  type: string;
  text: string;
  elements?: RawPipelineElement[];
  nestedCommands?: RawPipelineElement[];
  redirections?: RawRedirection[];
  securityPatterns?: {
    hasMemberInvocations?: boolean;
    hasSubExpressions?: boolean;
    hasExpandableStrings?: boolean;
    hasScriptBlocks?: boolean;
  };
};

export type RawParsedOutput = {
  valid: boolean;
  errors: { message: string; errorId: string }[];
  statements: RawStatement[];
  variables: { path: string; isSplatted: boolean }[];
  hasStopParsing: boolean;
  originalCommand: string;
  typeLiterals?: string[];
  hasUsingStatements?: boolean;
  hasScriptRequirements?: boolean;
};

// ---------------------------------------------------------------------------
// Parse timeout knob — read inside the impl, not top-level, so env ordering is
// respected. Default 5s mirrors the reference's DEFAULT_PARSE_TIMEOUT_MS.
// ---------------------------------------------------------------------------

const DEFAULT_PARSE_TIMEOUT_MS = 5_000;

export function getParseTimeoutMs(): number {
  const env = process.env.AMI_PWSH_PARSE_TIMEOUT_MS;
  if (env) {
    const parsed = parseInt(env, 10);
    if (!isNaN(parsed) && parsed > 0) return parsed;
  }
  return DEFAULT_PARSE_TIMEOUT_MS;
}

/**
 * The PowerShell parse script. The command is passed via a Base64-encoded
 * `$EncodedCommand` variable (UTF-8) to avoid here-string injection, parsed
 * with the native AST parser, and emitted as compact JSON. Comments are kept
 * terse — every char here eats into the Windows argv budget.
 */
export const PARSE_SCRIPT_BODY = `
if (-not $EncodedCommand) {
    Write-Output '{"valid":false,"errors":[{"message":"No command provided","errorId":"NoInput"}],"statements":[],"variables":[],"hasStopParsing":false,"originalCommand":""}'
    exit 0
}
$Command = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($EncodedCommand))
$tokens = $null
$parseErrors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseInput($Command, [ref]$tokens, [ref]$parseErrors)
$allVariables = [System.Collections.ArrayList]::new()
function Get-RawCommandElements {
    param([System.Management.Automation.Language.CommandAst]$CmdAst)
    $elems = [System.Collections.ArrayList]::new()
    foreach ($ce in $CmdAst.CommandElements) {
        $ceData = @{ type = $ce.GetType().Name; text = $ce.Extent.Text }
        if ($ce.PSObject.Properties['Value'] -and $null -ne $ce.Value -and $ce.Value -is [string]) { $ceData.value = $ce.Value }
        if ($ce -is [System.Management.Automation.Language.CommandExpressionAst]) { $ceData.expressionType = $ce.Expression.GetType().Name }
        $a=$ce.Argument;if($a){$ceData.children=@(@{type=$a.GetType().Name;text=$a.Extent.Text})}
        [void]$elems.Add($ceData)
    }
    return $elems
}
function Get-RawRedirections {
    param($Redirections)
    $result = [System.Collections.ArrayList]::new()
    foreach ($redir in $Redirections) {
        $redirData = @{ type = $redir.GetType().Name }
        if ($redir -is [System.Management.Automation.Language.FileRedirectionAst]) {
            $redirData.append = [bool]$redir.Append
            $redirData.fromStream = $redir.FromStream.ToString()
            $redirData.locationText = $redir.Location.Extent.Text
        }
        [void]$result.Add($redirData)
    }
    return $result
}
function Get-SecurityPatterns($A) {
    $p = @{}
    foreach ($n in $A.FindAll({ param($x)
        $x -is [System.Management.Automation.Language.MemberExpressionAst] -or
        $x -is [System.Management.Automation.Language.SubExpressionAst] -or
        $x -is [System.Management.Automation.Language.ArrayExpressionAst] -or
        $x -is [System.Management.Automation.Language.ExpandableStringExpressionAst] -or
        $x -is [System.Management.Automation.Language.ScriptBlockExpressionAst] -or
        $x -is [System.Management.Automation.Language.ParenExpressionAst]
    }, $true)) { switch ($n.GetType().Name) {
        'InvokeMemberExpressionAst' { $p.hasMemberInvocations = $true }
        'MemberExpressionAst' { $p.hasMemberInvocations = $true }
        'SubExpressionAst' { $p.hasSubExpressions = $true }
        'ArrayExpressionAst' { $p.hasSubExpressions = $true }
        'ParenExpressionAst' { $p.hasSubExpressions = $true }
        'ExpandableStringExpressionAst' { $p.hasExpandableStrings = $true }
        'ScriptBlockExpressionAst' { $p.hasScriptBlocks = $true }
    }}
    if ($p.Count -gt 0) { return $p }
    return $null
}
foreach ($v in $ast.FindAll({ param($node) $node -is [System.Management.Automation.Language.VariableExpressionAst] }, $true)) {
    [void]$allVariables.Add(@{ path = $v.VariablePath.ToString(); isSplatted = [bool]$v.Splatted })
}
$typeLiterals = [System.Collections.ArrayList]::new()
foreach ($t in $ast.FindAll({ param($n)
    $n -is [System.Management.Automation.Language.TypeExpressionAst] -or
    $n -is [System.Management.Automation.Language.TypeConstraintAst]
}, $true)) { [void]$typeLiterals.Add($t.TypeName.FullName) }
$hasStopParsing = $false
$tk = [System.Management.Automation.Language.TokenKind]
foreach ($tok in $tokens) {
    if ($tok.Kind -eq $tk::MinusMinus) { $hasStopParsing = $true; break }
    if ($tok.Kind -eq $tk::Generic -and ($tok.Text -replace '[\u2013\u2014\u2015]','-') -eq '--%') { $hasStopParsing = $true; break }
}
$statements = [System.Collections.ArrayList]::new()
function Process-BlockStatements {
    param($Block)
    if (-not $Block) { return }
    foreach ($stmt in $Block.Statements) {
        $statement = @{ type = $stmt.GetType().Name; text = $stmt.Extent.Text }
        if ($stmt -is [System.Management.Automation.Language.PipelineAst]) {
            $elements = [System.Collections.ArrayList]::new()
            foreach ($element in $stmt.PipelineElements) {
                $elemData = @{ type = $element.GetType().Name; text = $element.Extent.Text }
                if ($element -is [System.Management.Automation.Language.CommandAst]) {
                    $elemData.commandElements = @(Get-RawCommandElements -CmdAst $element)
                    $elemData.redirections = @(Get-RawRedirections -Redirections $element.Redirections)
                } elseif ($element -is [System.Management.Automation.Language.CommandExpressionAst]) {
                    $elemData.expressionType = $element.Expression.GetType().Name
                    $elemData.redirections = @(Get-RawRedirections -Redirections $element.Redirections)
                }
                [void]$elements.Add($elemData)
            }
            $statement.elements = @($elements)
            $nestedCmds = [System.Collections.ArrayList]::new()
            foreach ($cmd in $stmt.FindAll({ param($node) $node -is [System.Management.Automation.Language.CommandAst] }, $true)) {
                if ($cmd.Parent -eq $stmt) { continue }
                [void]$nestedCmds.Add(@{ type = $cmd.GetType().Name; text = $cmd.Extent.Text; commandElements = @(Get-RawCommandElements -CmdAst $cmd); redirections = @(Get-RawRedirections -Redirections $cmd.Redirections) })
            }
            if ($nestedCmds.Count -gt 0) { $statement.nestedCommands = @($nestedCmds) }
            $r = $stmt.FindAll({param($n) $n -is [System.Management.Automation.Language.FileRedirectionAst]}, $true)
            if ($r.Count -gt 0) { $rr = @(Get-RawRedirections -Redirections $r); $statement.redirections = if ($statement.redirections) { @($statement.redirections) + $rr } else { $rr } }
        } else {
            $nested = [System.Collections.ArrayList]::new()
            foreach ($cmd in $stmt.FindAll({ param($node) $node -is [System.Management.Automation.Language.CommandAst] }, $true)) {
                [void]$nested.Add(@{ type = 'CommandAst'; text = $cmd.Extent.Text; commandElements = @(Get-RawCommandElements -CmdAst $cmd); redirections = @(Get-RawRedirections -Redirections $cmd.Redirections) })
            }
            if ($nested.Count -gt 0) { $statement.nestedCommands = @($nested) }
            $r = $stmt.FindAll({param($n) $n -is [System.Management.Automation.Language.FileRedirectionAst]}, $true)
            if ($r.Count -gt 0) { $statement.redirections = @(Get-RawRedirections -Redirections $r) }
        }
        $sp = Get-SecurityPatterns $stmt
        if ($sp) { $statement.securityPatterns = $sp }
        [void]$statements.Add($statement)
    }
}
Process-BlockStatements -Block $ast.BeginBlock
Process-BlockStatements -Block $ast.ProcessBlock
Process-BlockStatements -Block $ast.EndBlock
Process-BlockStatements -Block $ast.CleanBlock
Process-BlockStatements -Block $ast.DynamicParamBlock
if ($ast.ParamBlock) {
  $pb = $ast.ParamBlock
  $pn = [System.Collections.ArrayList]::new()
  foreach ($c in $pb.FindAll({param($n) $n -is [System.Management.Automation.Language.CommandAst]}, $true)) {
    [void]$pn.Add(@{type='CommandAst';text=$c.Extent.Text;commandElements=@(Get-RawCommandElements -CmdAst $c);redirections=@(Get-RawRedirections -Redirections $c.Redirections)})
  }
  $pr = $pb.FindAll({param($n) $n -is [System.Management.Automation.Language.FileRedirectionAst]}, $true)
  $ps = Get-SecurityPatterns $pb
  if ($pn.Count -gt 0 -or $pr.Count -gt 0 -or $ps) {
    $st = @{type='ParamBlockAst';text=$pb.Extent.Text}
    if ($pn.Count -gt 0) { $st.nestedCommands = @($pn) }
    if ($pr.Count -gt 0) { $st.redirections = @(Get-RawRedirections -Redirections $pr) }
    if ($ps) { $st.securityPatterns = $ps }
    [void]$statements.Add($st)
  }
}
$output = @{
    valid = ($parseErrors.Count -eq 0)
    errors = @($parseErrors | ForEach-Object { @{ message = $_.Message; errorId = $_.ErrorId } })
    statements = @($statements)
    variables = @($allVariables)
    hasStopParsing = $hasStopParsing
    originalCommand = $Command
    typeLiterals = @($typeLiterals)
    hasUsingStatements = [bool]($ast.UsingStatements -and $ast.UsingStatements.Count -gt 0)
    hasScriptRequirements = [bool]($ast.ScriptRequirements -ne $null)
}
$output | ConvertTo-Json -Depth 10 -Compress
`;

// ---------------------------------------------------------------------------
// Command-length gate — a UTF-8 BYTE budget (NOT code-unit count). A BMP char
// in U+0800–U+FFFF is 1 UTF-16 code unit but 3 UTF-8 bytes; comparing against
// `.length` would let a pure-CJK command overflow Windows CreateProcess's
// 32,767-char argv, fail the spawn, and silently downgrade deny→ask. The
// Windows ceiling is COMPUTED from PARSE_SCRIPT_BODY.length so it cannot drift.
// Unix uses a looser pre-existing 4,500 (its ARG_MAX is 2MB+); applying the
// Windows limit on Unix would regress long compound commands.
// ---------------------------------------------------------------------------

const WINDOWS_ARGV_CAP = 32_767;
const FIXED_ARGV_OVERHEAD = 200;
const ENCODED_CMD_WRAPPER = `$EncodedCommand = ''\n`.length;
const SAFETY_MARGIN = 100;
const SCRIPT_CHARS_BUDGET = ((WINDOWS_ARGV_CAP - FIXED_ARGV_OVERHEAD) * 3) / 8;
const CMD_B64_BUDGET =
  SCRIPT_CHARS_BUDGET - PARSE_SCRIPT_BODY.length - ENCODED_CMD_WRAPPER;
/** Unit: UTF-8 BYTES. Compare against Buffer.byteLength, not .length. */
export const WINDOWS_MAX_COMMAND_LENGTH = Math.max(
  0,
  Math.floor((CMD_B64_BUDGET * 3) / 4) - SAFETY_MARGIN,
);
const UNIX_MAX_COMMAND_LENGTH = 4_500;
/** Unit: UTF-8 BYTES (see note above). Platform-gated. */
export const MAX_COMMAND_LENGTH =
  process.platform === 'win32'
    ? WINDOWS_MAX_COMMAND_LENGTH
    : UNIX_MAX_COMMAND_LENGTH;

const INVALID_RESULT_BASE: Omit<
  ParsedPowerShellCommand,
  'errors' | 'originalCommand'
> = {
  valid: false,
  statements: [],
  variables: [],
  hasStopParsing: false,
};

function makeInvalidResult(
  command: string,
  message: string,
  errorId: string,
): ParsedPowerShellCommand {
  return {
    ...INVALID_RESULT_BASE,
    errors: [{ message, errorId }],
    originalCommand: command,
  };
}

/** Base64-encode a string as UTF-16LE (required by pwsh -EncodedCommand). */
export function toUtf16LeBase64(text: string): string {
  return Buffer.from(text, 'utf16le').toString('base64');
}

/** Build the full pwsh parse script with the command base64-embedded (UTF-8). */
export function buildParseScript(command: string): string {
  const encoded = Buffer.from(command, 'utf8').toString('base64');
  return `$EncodedCommand = '${encoded}'\n${PARSE_SCRIPT_BODY}`;
}

/** Ensure a value is an array (PS 5.1 ConvertTo-Json unwraps single elements). */
function ensureArray<T>(value: T | T[] | undefined | null): T[] {
  if (value === undefined || value === null) return [];
  return Array.isArray(value) ? value : [value];
}

/** Map raw .NET AST type name to our StatementType union. */
export function mapStatementType(rawType: string): StatementType {
  switch (rawType) {
    case 'PipelineAst':
      return 'PipelineAst';
    case 'PipelineChainAst':
      return 'PipelineChainAst';
    case 'AssignmentStatementAst':
      return 'AssignmentStatementAst';
    case 'IfStatementAst':
      return 'IfStatementAst';
    case 'ForStatementAst':
      return 'ForStatementAst';
    case 'ForEachStatementAst':
      return 'ForEachStatementAst';
    case 'WhileStatementAst':
      return 'WhileStatementAst';
    case 'DoWhileStatementAst':
      return 'DoWhileStatementAst';
    case 'DoUntilStatementAst':
      return 'DoUntilStatementAst';
    case 'SwitchStatementAst':
      return 'SwitchStatementAst';
    case 'TryStatementAst':
      return 'TryStatementAst';
    case 'TrapStatementAst':
      return 'TrapStatementAst';
    case 'FunctionDefinitionAst':
      return 'FunctionDefinitionAst';
    case 'DataStatementAst':
      return 'DataStatementAst';
    default:
      return 'UnknownStatementAst';
  }
}

/** Map raw .NET AST type name to our CommandElementType union. */
export function mapElementType(
  rawType: string,
  expressionType?: string,
): CommandElementType {
  switch (rawType) {
    case 'ScriptBlockExpressionAst':
      return 'ScriptBlock';
    case 'SubExpressionAst':
    case 'ArrayExpressionAst':
      // SECURITY: @() (ArrayExpressionAst) is a sibling of $() not a subclass,
      // but both evaluate arbitrary pipelines with side effects, e.g.
      // `Get-ChildItem @(Remove-Item ./data)`. Map both to SubExpression so
      // hasSubExpressions fires.
      return 'SubExpression';
    case 'ExpandableStringExpressionAst':
      return 'ExpandableString';
    case 'InvokeMemberExpressionAst':
    case 'MemberExpressionAst':
      return 'MemberInvocation';
    case 'VariableExpressionAst':
      return 'Variable';
    case 'StringConstantExpressionAst':
    case 'ConstantExpressionAst':
      // Numeric literals (ConstantExpressionAst) are inert values, not code —
      // treat like string constants so `-Seconds:5` is not a false positive.
      return 'StringConstant';
    case 'CommandParameterAst':
      return 'Parameter';
    case 'ParenExpressionAst':
      return 'SubExpression';
    case 'CommandExpressionAst':
      // Delegate to the wrapped expression type so nested SubExpression /
      // ExpandableString / ScriptBlock are caught without a manual list.
      if (expressionType) return mapElementType(expressionType);
      return 'Other';
    default:
      return 'Other';
  }
}

/** Classify a command name as cmdlet, application, or unknown. */
export function classifyCommandName(
  name: string,
): 'cmdlet' | 'application' | 'unknown' {
  if (/^[A-Za-z]+-[A-Za-z][A-Za-z0-9_]*$/.test(name)) return 'cmdlet';
  if (/[.\\/]/.test(name)) return 'application';
  return 'unknown';
}

/**
 * Strip a module prefix (`Module\Cmdlet` → `Cmdlet`) but NOT drive letters
 * (`C:\`), UNC (`\\server`), or relative (`.\`, `..\`) paths, so file-path
 * commands stay classified as applications.
 */
export function stripModulePrefix(name: string): string {
  const idx = name.lastIndexOf('\\');
  if (idx < 0) return name;
  if (
    /^[A-Za-z]:/.test(name) ||
    name.startsWith('\\\\') ||
    name.startsWith('.\\') ||
    name.startsWith('..\\')
  )
    return name;
  return name.substring(idx + 1);
}

/** Transform a raw CommandAst pipeline element into ParsedCommandElement. */
export function transformCommandAst(
  raw: RawPipelineElement,
): ParsedCommandElement {
  const cmdElements = ensureArray(raw.commandElements);
  let name = '';
  const args: string[] = [];
  const elementTypes: CommandElementType[] = [];
  const children: (CommandElementChild[] | undefined)[] = [];
  let hasChildren = false;

  let nameType: 'cmdlet' | 'application' | 'unknown' = 'unknown';
  if (cmdElements.length > 0) {
    const first = cmdElements[0]!;
    const isFirstStringLiteral =
      first.type === 'StringConstantExpressionAst' ||
      first.type === 'ExpandableStringExpressionAst';
    const rawNameUnstripped =
      isFirstStringLiteral && typeof first.value === 'string'
        ? first.value
        : first.text;
    // Strip surrounding quotes from the command name (when .value is absent,
    // .text preserves quotes, e.g. `& 'Invoke-Expression'`).
    const rawName = rawNameUnstripped.replace(/^['"]|['"]$/g, '');
    // SECURITY: PowerShell built-in cmdlet names are ASCII-only. .NET
    // OrdinalIgnoreCase folds U+017F (ſ)→S and U+0131 (ı)→I, which JS
    // .toLowerCase() does NOT fold — so `ſtart-proceſſ` would otherwise slip
    // past name comparisons as a cmdlet. Force 'application' to gate auto-allow.
    if (/[\u0080-\uFFFF]/.test(rawName)) {
      nameType = 'application';
    } else {
      nameType = classifyCommandName(rawName);
    }
    name = stripModulePrefix(rawName);
    elementTypes.push(mapElementType(first.type, first.expressionType));

    for (let i = 1; i < cmdElements.length; i++) {
      const ce = cmdElements[i]!;
      const isStringLiteral =
        ce.type === 'StringConstantExpressionAst' ||
        ce.type === 'ExpandableStringExpressionAst';
      args.push(isStringLiteral && ce.value != null ? ce.value : ce.text);
      elementTypes.push(mapElementType(ce.type, ce.expressionType));
      const rawChildren = ensureArray(ce.children);
      if (rawChildren.length > 0) {
        hasChildren = true;
        children.push(
          rawChildren.map((c) => ({
            type: mapElementType(c.type),
            text: c.text,
          })),
        );
      } else {
        children.push(undefined);
      }
    }
  }

  const result: ParsedCommandElement = {
    name,
    nameType,
    elementType: 'CommandAst',
    args,
    text: raw.text,
    elementTypes,
    ...(hasChildren ? { children } : {}),
  };

  const rawRedirs = ensureArray(raw.redirections);
  if (rawRedirs.length > 0) {
    result.redirections = rawRedirs.map(transformRedirection);
  }

  return result;
}

/** Transform a non-CommandAst pipeline element into ParsedCommandElement. */
export function transformExpressionElement(
  raw: RawPipelineElement,
): ParsedCommandElement {
  const elementType: PipelineElementType =
    raw.type === 'ParenExpressionAst'
      ? 'ParenExpressionAst'
      : 'CommandExpressionAst';
  return {
    name: raw.text,
    nameType: 'unknown',
    elementType,
    args: [],
    text: raw.text,
    elementTypes: [mapElementType(raw.type, raw.expressionType)],
  };
}

/** Map a raw redirection to ParsedRedirection. */
export function transformRedirection(raw: RawRedirection): ParsedRedirection {
  if (raw.type === 'MergingRedirectionAst') {
    return { operator: '2>&1', target: '', isMerging: true };
  }
  const append = raw.append ?? false;
  const fromStream = raw.fromStream ?? 'Output';
  let operator: RedirectionOperator;
  if (append) {
    switch (fromStream) {
      case 'Error':
        operator = '2>>';
        break;
      case 'All':
        operator = '*>>';
        break;
      default:
        operator = '>>';
        break;
    }
  } else {
    switch (fromStream) {
      case 'Error':
        operator = '2>';
        break;
      case 'All':
        operator = '*>';
        break;
      default:
        operator = '>';
        break;
    }
  }
  return { operator, target: raw.locationText ?? '', isMerging: false };
}

/** Transform a raw statement into ParsedStatement. */
export function transformStatement(raw: RawStatement): ParsedStatement {
  const statementType = mapStatementType(raw.type);
  const commands: ParsedCommandElement[] = [];
  const redirections: ParsedRedirection[] = [];

  if (raw.elements) {
    for (const elem of ensureArray(raw.elements)) {
      if (elem.type === 'CommandAst') {
        commands.push(transformCommandAst(elem));
        for (const redir of ensureArray(elem.redirections)) {
          redirections.push(transformRedirection(redir));
        }
      } else {
        commands.push(transformExpressionElement(elem));
        // SECURITY: CommandExpressionAst inherits .Redirections. `1 > /tmp/x`
        // is a CommandExpressionAst with a FileRedirectionAst — extract here so
        // compound `Get-ChildItem; 1 > /tmp/x` is not auto-allowed.
        for (const redir of ensureArray(elem.redirections)) {
          redirections.push(transformRedirection(redir));
        }
      }
    }
    // SECURITY: deep FindAll for FileRedirectionAst catches redirections hidden
    // inside colon-bound ParenExpression args (`-Name:('x' > file)`) and
    // hashtable value statements (`@{k='x' > ~/.bashrc}`). Dedupe by
    // (operator, target) so direct-element redirections found twice are not
    // double-reported.
    const seen = new Set(redirections.map((r) => `${r.operator}\0${r.target}`));
    for (const redir of ensureArray(raw.redirections)) {
      const r = transformRedirection(redir);
      const key = `${r.operator}\0${r.target}`;
      if (!seen.has(key)) {
        seen.add(key);
        redirections.push(r);
      }
    }
  } else {
    commands.push({
      name: raw.text,
      nameType: 'unknown',
      elementType: 'CommandExpressionAst',
      args: [],
      text: raw.text,
    });
    // SECURITY: redirections inside control flow (`if ($x) { 1 > /tmp/evil }`)
    // attach to a CommandExpressionAst — a sibling of CommandAst, invisible to
    // the nested-CommandAst walk. Hoist them to the statement level.
    for (const redir of ensureArray(raw.redirections)) {
      redirections.push(transformRedirection(redir));
    }
  }

  let nestedCommands: ParsedCommandElement[] | undefined;
  const rawNested = ensureArray(raw.nestedCommands);
  if (rawNested.length > 0) {
    nestedCommands = rawNested.map(transformCommandAst);
  }

  const result: ParsedStatement = {
    statementType,
    commands,
    redirections,
    text: raw.text,
    nestedCommands,
  };

  if (raw.securityPatterns) {
    result.securityPatterns = raw.securityPatterns;
  }

  return result;
}

/** Transform the complete raw PS output into ParsedPowerShellCommand. */
export function transformRawOutput(
  raw: RawParsedOutput,
): ParsedPowerShellCommand {
  const result: ParsedPowerShellCommand = {
    valid: raw.valid,
    errors: ensureArray(raw.errors),
    statements: ensureArray(raw.statements).map(transformStatement),
    variables: ensureArray(raw.variables),
    hasStopParsing: raw.hasStopParsing,
    originalCommand: raw.originalCommand,
  };
  const tl = ensureArray(raw.typeLiterals);
  if (tl.length > 0) result.typeLiterals = tl;
  if (raw.hasUsingStatements) result.hasUsingStatements = true;
  if (raw.hasScriptRequirements) result.hasScriptRequirements = true;
  return result;
}

// ---------------------------------------------------------------------------
// Spawn runtime — injectable so the full parse pipeline is testable without a
// real pwsh. Default resolves pwsh/powershell via whichSync and runs it with
// execFile under a timeout.
// ---------------------------------------------------------------------------

export type PwshSpawnResult = {
  stdout: string;
  stderr: string;
  timedOut: boolean;
  failed: boolean;
  exitCode: number | null;
};

export type PwshRuntime = {
  /** Resolve the pwsh/powershell executable path, or null if unavailable. */
  resolvePwshPath: () => string | null;
  /** Spawn pwsh with the given args under a timeout. Never rejects. */
  spawn: (
    pwshPath: string,
    args: string[],
    timeoutMs: number,
  ) => Promise<PwshSpawnResult>;
};

function defaultResolvePwshPath(): string | null {
  return whichSync('pwsh') ?? whichSync('powershell');
}

function defaultSpawn(
  pwshPath: string,
  args: string[],
  timeoutMs: number,
): Promise<PwshSpawnResult> {
  return new Promise<PwshSpawnResult>((resolve) => {
    execFile(
      pwshPath,
      args,
      { timeout: timeoutMs, maxBuffer: 16 * 1024 * 1024 },
      (error, stdout, stderr) => {
        const timedOut =
          !!error &&
          (error as NodeJS.ErrnoException & { killed?: boolean }).killed ===
            true &&
          (error as NodeJS.ErrnoException & { signal?: string }).signal ===
            'SIGTERM';
        const exitCode =
          error && typeof (error as { code?: unknown }).code === 'number'
            ? ((error as { code: number }).code)
            : error
              ? 1
              : 0;
        resolve({
          stdout: stdout ?? '',
          stderr: stderr ?? '',
          timedOut,
          failed: !!error,
          exitCode: error ? exitCode : 0,
        });
      },
    );
  });
}

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

/** Windows executable extensions stripped from bare (path-free) names. */
const WINDOWS_PATHEXT = /\.(exe|cmd|bat|com)$/;

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

// ---------------------------------------------------------------------------
// Analysis helpers — derived from the parsed AST structure.
// ---------------------------------------------------------------------------

/** All command names across statements/pipelines/nested commands (lowercased). */
export function getAllCommandNames(parsed: ParsedPowerShellCommand): string[] {
  const names: string[] = [];
  for (const statement of parsed.statements) {
    for (const cmd of statement.commands) names.push(cmd.name.toLowerCase());
    if (statement.nestedCommands) {
      for (const cmd of statement.nestedCommands)
        names.push(cmd.name.toLowerCase());
    }
  }
  return names;
}

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
