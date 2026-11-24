// Split from powershell-parser.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).

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

export const DEFAULT_PARSE_TIMEOUT_MS = 5_000;

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

export const WINDOWS_ARGV_CAP = 32_767;

export const FIXED_ARGV_OVERHEAD = 200;

export const SAFETY_MARGIN = 100;

export const UNIX_MAX_COMMAND_LENGTH = 4_500;

export const INVALID_RESULT_BASE: Omit<
  ParsedPowerShellCommand,
  'errors' | 'originalCommand'
> = {
  valid: false,
  statements: [],
  variables: [],
  hasStopParsing: false,
};

export function makeInvalidResult(
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
export function ensureArray<T>(value: T | T[] | undefined | null): T[] {
  if (value === undefined || value === null) return [];
  return Array.isArray(value) ? value : [value];
}
