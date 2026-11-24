// Split from powershell-security.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { commandHasArgAbbreviation } from './powershell-parser';
import type { ParsedCommandElement } from './powershell-parser';
import { PS_ALT_PARAM_PREFIX_RE } from './powershell-security-power-shell-read-only-config';
import type { CmdletPathConfig } from './powershell-security-powershell-readonly-config';

/**
 * Per-cmdlet path-parameter table. Keyed by the canonical (alias-resolved,
 * lowercase) cmdlet name — look up with {@link resolveToCanonical}.
 */
export const CMDLET_PATH_CONFIG: Record<string, CmdletPathConfig> = {
  // ─── Write/create operations ──────────────────────────────────────────────
  'set-content': {
    operationType: 'write',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-passthru', '-force', '-whatif', '-confirm', '-usetransaction', '-nonewline', '-asbytestream'],
    knownValueParams: ['-value', '-filter', '-include', '-exclude', '-credential', '-encoding', '-stream'],
  },
  'add-content': {
    operationType: 'write',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-passthru', '-force', '-whatif', '-confirm', '-usetransaction', '-nonewline', '-asbytestream'],
    knownValueParams: ['-value', '-filter', '-include', '-exclude', '-credential', '-encoding', '-stream'],
  },
  'remove-item': {
    operationType: 'write',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-recurse', '-force', '-whatif', '-confirm', '-usetransaction'],
    knownValueParams: ['-filter', '-include', '-exclude', '-credential', '-stream'],
  },
  'clear-content': {
    operationType: 'write',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-force', '-whatif', '-confirm', '-usetransaction'],
    knownValueParams: ['-filter', '-include', '-exclude', '-credential', '-stream'],
  },
  'out-file': {
    operationType: 'write',
    // -Path is PowerShell's documented ALIAS for -FilePath.
    pathParams: ['-filepath', '-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-append', '-force', '-noclobber', '-nonewline', '-whatif', '-confirm'],
    knownValueParams: ['-inputobject', '-encoding', '-width'],
  },
  'tee-object': {
    operationType: 'write',
    pathParams: ['-filepath', '-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-append'],
    knownValueParams: ['-inputobject', '-variable', '-encoding'],
  },
  'export-csv': {
    operationType: 'write',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-append', '-force', '-noclobber', '-notypeinformation', '-includetypeinformation', '-useculture', '-noheader', '-whatif', '-confirm'],
    knownValueParams: ['-inputobject', '-delimiter', '-encoding', '-quotefields', '-usequotes'],
  },
  'export-clixml': {
    operationType: 'write',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-force', '-noclobber', '-whatif', '-confirm'],
    knownValueParams: ['-inputobject', '-depth', '-encoding'],
  },
  'new-item': {
    operationType: 'write',
    // -Path is position 0. -Name (leaf-only) is resolved RELATIVE TO -Path, so a
    // non-leaf value forces an ask (it would resolve against the wrong base).
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    leafOnlyPathParams: ['-name'],
    knownSwitches: ['-force', '-whatif', '-confirm', '-usetransaction'],
    knownValueParams: ['-itemtype', '-value', '-credential', '-type'],
  },
  'copy-item': {
    operationType: 'write',
    // -Path (source) + -Destination (dest) both extracted + validated as write.
    pathParams: ['-path', '-literalpath', '-pspath', '-lp', '-destination'],
    knownSwitches: ['-container', '-force', '-passthru', '-recurse', '-whatif', '-confirm', '-usetransaction'],
    knownValueParams: ['-filter', '-include', '-exclude', '-credential', '-fromsession', '-tosession'],
  },
  'move-item': {
    operationType: 'write',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp', '-destination'],
    knownSwitches: ['-force', '-passthru', '-whatif', '-confirm', '-usetransaction'],
    knownValueParams: ['-filter', '-include', '-exclude', '-credential'],
  },
  'rename-item': {
    operationType: 'write',
    // -NewName is leaf-only per docs and Rename-Item rejects `..`, so it is a
    // knownValueParam (not leafOnly) — unlike New-Item -Name which accepts it.
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-force', '-passthru', '-whatif', '-confirm', '-usetransaction'],
    knownValueParams: ['-newname', '-credential', '-filter', '-include', '-exclude'],
  },
  'set-item': {
    operationType: 'write',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-force', '-passthru', '-whatif', '-confirm', '-usetransaction'],
    knownValueParams: ['-value', '-credential', '-filter', '-include', '-exclude'],
  },
  'invoke-webrequest': {
    operationType: 'write',
    pathParams: ['-outfile', '-infile'],
    positionalSkip: 1, // positional-0 is -Uri (URL), not a filesystem path
    optionalWrite: true, // only writes with -OutFile; bare iwr is pipeline-only
    knownSwitches: ['-allowinsecureredirect', '-allowunencryptedauthentication', '-disablekeepalive', '-nobodyprogress', '-passthru', '-preservefileauthorizationmetadata', '-resume', '-skipcertificatecheck', '-skipheadervalidation', '-skiphttperrorcheck', '-usebasicparsing', '-usedefaultcredentials'],
    knownValueParams: ['-uri', '-method', '-body', '-contenttype', '-headers', '-maximumredirection', '-maximumretrycount', '-proxy', '-proxycredential', '-retryintervalsec', '-sessionvariable', '-timeoutsec', '-token', '-transferencoding', '-useragent', '-websession', '-credential', '-authentication', '-certificate', '-certificatethumbprint', '-form', '-httpversion'],
  },
  'invoke-restmethod': {
    operationType: 'write',
    pathParams: ['-outfile', '-infile'],
    positionalSkip: 1,
    optionalWrite: true,
    knownSwitches: ['-allowinsecureredirect', '-allowunencryptedauthentication', '-disablekeepalive', '-followrellink', '-nobodyprogress', '-passthru', '-preservefileauthorizationmetadata', '-resume', '-skipcertificatecheck', '-skipheadervalidation', '-skiphttperrorcheck', '-usebasicparsing', '-usedefaultcredentials'],
    knownValueParams: ['-uri', '-method', '-body', '-contenttype', '-headers', '-maximumfollowrellink', '-maximumredirection', '-maximumretrycount', '-proxy', '-proxycredential', '-responseheaderstvariable', '-retryintervalsec', '-sessionvariable', '-statuscodevariable', '-timeoutsec', '-token', '-transferencoding', '-useragent', '-websession', '-credential', '-authentication', '-certificate', '-certificatethumbprint', '-form', '-httpversion'],
  },
  'expand-archive': {
    operationType: 'write',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp', '-destinationpath'],
    knownSwitches: ['-force', '-passthru', '-whatif', '-confirm'],
    knownValueParams: [],
  },
  'compress-archive': {
    operationType: 'write',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp', '-destinationpath'],
    knownSwitches: ['-force', '-update', '-passthru', '-whatif', '-confirm'],
    knownValueParams: ['-compressionlevel'],
  },
  'set-itemproperty': {
    operationType: 'write',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-passthru', '-force', '-whatif', '-confirm', '-usetransaction'],
    knownValueParams: ['-name', '-value', '-type', '-filter', '-include', '-exclude', '-credential', '-inputobject'],
  },
  'new-itemproperty': {
    operationType: 'write',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-force', '-whatif', '-confirm', '-usetransaction'],
    knownValueParams: ['-name', '-value', '-propertytype', '-type', '-filter', '-include', '-exclude', '-credential'],
  },
  'remove-itemproperty': {
    operationType: 'write',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-force', '-whatif', '-confirm', '-usetransaction'],
    knownValueParams: ['-name', '-filter', '-include', '-exclude', '-credential'],
  },
  'clear-item': {
    operationType: 'write',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-force', '-whatif', '-confirm', '-usetransaction'],
    knownValueParams: ['-filter', '-include', '-exclude', '-credential'],
  },
  'export-alias': {
    operationType: 'write',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-append', '-force', '-noclobber', '-passthru', '-whatif', '-confirm'],
    knownValueParams: ['-name', '-description', '-scope', '-as'],
  },
  // ─── Read operations ──────────────────────────────────────────────────────
  'get-content': {
    operationType: 'read',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-force', '-usetransaction', '-wait', '-raw', '-asbytestream'],
    knownValueParams: ['-readcount', '-totalcount', '-tail', '-first', '-head', '-last', '-filter', '-include', '-exclude', '-credential', '-delimiter', '-encoding', '-stream'],
  },
  'get-childitem': {
    operationType: 'read',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-recurse', '-force', '-name', '-usetransaction', '-followsymlink', '-directory', '-file', '-hidden', '-readonly', '-system'],
    knownValueParams: ['-filter', '-include', '-exclude', '-depth', '-attributes', '-credential'],
  },
  'get-item': {
    operationType: 'read',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-force', '-usetransaction'],
    knownValueParams: ['-filter', '-include', '-exclude', '-credential', '-stream'],
  },
  'get-filehash': {
    operationType: 'read',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: [],
    knownValueParams: ['-algorithm', '-inputstream'],
  },
  'test-path': {
    operationType: 'read',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-isvalid', '-usetransaction'],
    knownValueParams: ['-filter', '-include', '-exclude', '-pathtype', '-credential', '-olderthan', '-newerthan'],
  },
  'select-string': {
    operationType: 'read',
    pathParams: ['-path', '-literalpath', '-pspath', '-lp'],
    knownSwitches: ['-simplematch', '-casesensitive', '-quiet', '-list', '-notmatch', '-allmatches', '-noemphasis', '-raw'],
    knownValueParams: ['-inputobject', '-pattern', '-include', '-exclude', '-encoding', '-context', '-culture'],
  },
};

/**
 * True if a lowercase parameter name (leading dash) matches any entry in
 * `paramList`, honoring PowerShell's unambiguous-prefix matching (`-Lit` matches
 * `-LiteralPath`). Mirrors the reference's `matchesParam`.
 */
export function matchesCmdletParam(paramLower: string, paramList: readonly string[]): boolean {
  for (const p of paramList) {
    if (p === paramLower || (paramLower.length > 1 && p.startsWith(paramLower))) {
      return true;
    }
  }
  return false;
}

/**
 * True if a colon-syntax value (`-Path:<value>`) contains expression constructs
 * (arrays, subexpressions, variables, backtick escapes) that mask the real
 * runtime path. The outer CommandParameterAst hides these from the AST walk, so
 * they are detected textually. Mirrors the reference's `hasComplexColonValue`.
 */
export function hasComplexColonValue(rawValue: string): boolean {
  return (
    rawValue.includes(',') ||
    rawValue.startsWith('(') ||
    rawValue.startsWith('[') ||
    rawValue.includes('`') ||
    rawValue.includes('@(') ||
    rawValue.startsWith('@{') ||
    rawValue.includes('$')
  );
}

// ---------------------------------------------------------------------------
// AST-based PowerShell security validator.
//
// Mirrors the reference's PowerShellTool/powershellSecurity.ts
// `powershellCommandIsSafe(command, parsed)`: the ordered validator chain that
// consumes a native-parsed `ParsedPowerShellCommand` AST (from
// powershell-parser.ts's `parsePowerShellCommand`) and, crucially, FAILS SAFE —
// a command whose AST could not be parsed (`valid === false`) returns `ask`,
// never `allow`.
//
// This complements the textual `detectDangerousPowerShellCommand` /
// `detectPowerShellExecution` guards used in the synchronous bash-security path:
// those run over EVERY shell command (no pwsh spawn) and therefore must keep the
// broad AST-only signals OFF to avoid over-flagging POSIX. When a REAL pwsh AST
// is available, this function adds the structural checks the textual path cannot
// safely do (dynamic command name via the StringConstant allowlist; subexpression
// / expandable-string / splatting / member-invocation signals; positional
// -FilePath/-MemberName binding; script-block injection triage; CLM type-literal
// allowlist; env: scope manipulation; AST-alias-aware Invoke-Expression) and
// DELEGATES the cmdlet/type-name hazards to the one canonical textual detector so
// there is no second, parallel implementation of them.
// ---------------------------------------------------------------------------

/** Result of the AST-based validator (parity with the reference's shape). */
export type PowerShellSecurityResult = {
  behavior: 'passthrough' | 'ask';
  message?: string;
};

export const PS_SAFE_RESULT: PowerShellSecurityResult = { behavior: 'passthrough' };

export function askResult(message: string): PowerShellSecurityResult {
  return { behavior: 'ask', message };
}

/**
 * Alt-dash-aware wrapper around {@link commandHasArgAbbreviation}. PowerShell's
 * tokenizer treats `/`, en-dash, em-dash and horizontal-bar as `-` equivalents,
 * so `Start-Job –FilePath x` (en-dash) must match `-filepath`. Reuses the shared
 * abbreviation helper (no re-implementation), retrying once with the alternate
 * prefixes normalized to `-`. Mirrors the reference's psExeHasParamAbbreviation.
 */
export function psParamAbbrevAltAware(
  cmd: ParsedCommandElement,
  fullParam: string,
  minPrefix: string,
): boolean {
  if (commandHasArgAbbreviation(cmd, fullParam, minPrefix)) {
    return true;
  }
  const normalized: ParsedCommandElement = {
    ...cmd,
    args: cmd.args.map((a) =>
      a.length > 0 && PS_ALT_PARAM_PREFIX_RE.test(a[0]!) ? '-' + a.slice(1) : a,
    ),
  };
  return commandHasArgAbbreviation(normalized, fullParam, minPrefix);
}

/**
 * True if a command binds a non-dash positional StringConstant argument (which
 * parameter-set resolution would route to -FilePath / -MemberName). A script
 * block arg selects the ScriptBlock parameter set instead, so it does not count.
 * Uses the AST `elementTypes` when present; over-flagging is fail-safe. Mirrors
 * the positional-binding scan in the reference's checkDangerousFilePathExecution /
 * checkForEachMemberName.
 */
/**
 * True if a command binds `-Verb RunAs` through a colon-bound parameter whose
 * value, after stripping quotes/backticks/whitespace, normalizes to `runas`
 * (`-Verb:RunAs`, `-Verb:'RunAs'`, `-Verb:" RunAs "`, `-V`+backtick+`erb:…`).
 * Reads the parser's structural `children[]` (the CommandParameterAst.Argument of
 * each colon-bound parameter) so an elevation verb obscured by quoting/padding
 * that the whitespace-tokenizing textual regex cannot anticipate is still
 * surfaced. Fail-open when `children` is unavailable (the textual regex layer
 * still runs). Mirrors the reference's checkStartProcess children[] layer.
 */
export function startProcessColonVerbRunAs(cmd: ParsedCommandElement): boolean {
  if (cmd.children === undefined) {
    return false;
  }
  for (let i = 0; i < cmd.args.length; i++) {
    // Strip backticks before matching the parameter name (`-V`+backtick+`erb:`).
    const argName = cmd.args[i]!.replace(/`/g, '');
    if (!/^[-–—―/]v[a-z]*:/i.test(argName)) {
      continue;
    }
    const kids = cmd.children[i];
    if (kids === undefined) {
      continue;
    }
    for (const child of kids) {
      if (child.text.replace(/['"`\s]/g, '').toLowerCase() === 'runas') {
        return true;
      }
    }
  }
  return false;
}

export function hasPositionalStringArgAst(cmd: ParsedCommandElement): boolean {
  for (let i = 0; i < cmd.args.length; i++) {
    const arg = cmd.args[i];
    const argType = cmd.elementTypes?.[i + 1];
    if (argType === 'StringConstant' && arg !== undefined && !arg.startsWith('-')) {
      return true;
    }
  }
  return false;
}
