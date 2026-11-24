// Split from powershell-parser.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { execFile } from 'node:child_process';
import { whichSync } from '../utils/which';
import { ensureArray } from './powershell-parser-parse-script-body';
import type { CommandElementChild, CommandElementType, ParsedCommandElement, ParsedPowerShellCommand, ParsedRedirection, ParsedStatement, PipelineElementType, RawParsedOutput, RawPipelineElement, RawRedirection, RawStatement, RedirectionOperator, StatementType } from './powershell-parser-parse-script-body';

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

export function defaultResolvePwshPath(): string | null {
  return whichSync('pwsh') ?? whichSync('powershell');
}

export function defaultSpawn(
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

/** Windows executable extensions stripped from bare (path-free) names. */
export const WINDOWS_PATHEXT = /\.(exe|cmd|bat|com)$/;

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
