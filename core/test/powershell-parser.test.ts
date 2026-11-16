/**
 * Tests for the PowerShell AST-based structural command analyser
 * (src/tools/powershell-parser.ts), mirroring Claude Code's
 * references/claude/claude-code-main/src/utils/powershell/parser.ts.
 *
 * Covers the five confirmed parity gaps for this file:
 *  - U141/structural analysis: the raw-JSON → ParsedPowerShellCommand transform
 *    pipeline (statements, pipeline segments, nested commands, name
 *    classification, children, variables, typeLiterals, using/#Requires).
 *  - U141/native AST parse: parsePowerShellCommand driving the spawn runtime end
 *    to end (success, no-pwsh, timeout+retry, non-zero exit, empty, invalid
 *    JSON, command-too-long) via an injected PwshRuntime, plus LRU memoization
 *    and transient-failure cache eviction.
 *  - U141/alias table: COMMON_ALIASES prototype-pollution safety + deliberate
 *    native-exe omissions, and single-hop resolveToCanonical (incl. `md`).
 *  - U141/redirection hoisting: transformStatement deep-redirection merge with
 *    (operator,target) dedupe, stream operators, isNullRedirectionTarget sink
 *    exemption, getFileRedirections.
 *  - U141/security flags: deriveSecurityFlags over elementTypes + the
 *    belt-and-suspenders securityPatterns cross-check + splatting, and
 *    mapElementType ($()/@()/scriptblock/member/expandable).
 */

import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  parsePowerShellCommand,
  getParseTimeoutMs,
  buildParseScript,
  toUtf16LeBase64,
  PARSE_SCRIPT_BODY,
  WINDOWS_MAX_COMMAND_LENGTH,
  MAX_COMMAND_LENGTH,
  mapStatementType,
  mapElementType,
  classifyCommandName,
  stripModulePrefix,
  transformCommandAst,
  transformExpressionElement,
  transformRedirection,
  transformStatement,
  transformRawOutput,
  COMMON_ALIASES,
  resolveToCanonical,
  getAllCommandNames,
  getAllCommands,
  getAllRedirections,
  getVariablesByScope,
  hasCommandNamed,
  hasDirectoryChange,
  isSingleCommand,
  commandHasArg,
  PS_TOKENIZER_DASH_CHARS,
  isPowerShellParameter,
  commandHasArgAbbreviation,
  getPipelineSegments,
  isNullRedirectionTarget,
  getFileRedirections,
  deriveSecurityFlags,
  __setPwshRuntimeForTest,
  __resetPwshRuntimeForTest,
  type RawParsedOutput,
  type RawStatement,
  type PwshSpawnResult,
} from '../src/tools/powershell-parser';
import { memoizeWithLRU } from '../src/utils/windows-paths';

function okSpawn(json: unknown): PwshSpawnResult {
  return {
    stdout: JSON.stringify(json),
    stderr: '',
    timedOut: false,
    failed: false,
    exitCode: 0,
  };
}

function minimalRaw(partial: Partial<RawParsedOutput>): RawParsedOutput {
  return {
    valid: true,
    errors: [],
    statements: [],
    variables: [],
    hasStopParsing: false,
    originalCommand: '',
    ...partial,
  };
}

// A simple pipeline statement with one CommandAst element.
function pipelineStmt(
  name: string,
  rest: Partial<RawStatement> = {},
): RawStatement {
  return {
    type: 'PipelineAst',
    text: name,
    elements: [
      {
        type: 'CommandAst',
        text: name,
        commandElements: [
          { type: 'StringConstantExpressionAst', text: name, value: name },
        ],
        redirections: [],
      },
    ],
    ...rest,
  };
}

describe('mapStatementType', () => {
  it('maps known AST statement types', () => {
    assert.equal(mapStatementType('PipelineAst'), 'PipelineAst');
    assert.equal(mapStatementType('IfStatementAst'), 'IfStatementAst');
    assert.equal(mapStatementType('TrapStatementAst'), 'TrapStatementAst');
    assert.equal(
      mapStatementType('FunctionDefinitionAst'),
      'FunctionDefinitionAst',
    );
  });
  it('falls back to UnknownStatementAst', () => {
    assert.equal(mapStatementType('SomethingNewAst'), 'UnknownStatementAst');
  });
});

describe('mapElementType', () => {
  it('maps @() ArrayExpression to SubExpression (side-effect parity)', () => {
    assert.equal(mapElementType('ArrayExpressionAst'), 'SubExpression');
    assert.equal(mapElementType('SubExpressionAst'), 'SubExpression');
    assert.equal(mapElementType('ParenExpressionAst'), 'SubExpression');
  });
  it('maps scriptblocks, members, expandable strings', () => {
    assert.equal(mapElementType('ScriptBlockExpressionAst'), 'ScriptBlock');
    assert.equal(mapElementType('InvokeMemberExpressionAst'), 'MemberInvocation');
    assert.equal(mapElementType('MemberExpressionAst'), 'MemberInvocation');
    assert.equal(
      mapElementType('ExpandableStringExpressionAst'),
      'ExpandableString',
    );
  });
  it('treats numeric ConstantExpressionAst as a StringConstant (inert)', () => {
    assert.equal(mapElementType('ConstantExpressionAst'), 'StringConstant');
    assert.equal(mapElementType('StringConstantExpressionAst'), 'StringConstant');
  });
  it('maps parameters and variables; unknown → Other', () => {
    assert.equal(mapElementType('CommandParameterAst'), 'Parameter');
    assert.equal(mapElementType('VariableExpressionAst'), 'Variable');
    assert.equal(mapElementType('WeirdAst'), 'Other');
  });
  it('delegates CommandExpressionAst to its wrapped expression type', () => {
    assert.equal(
      mapElementType('CommandExpressionAst', 'SubExpressionAst'),
      'SubExpression',
    );
    assert.equal(mapElementType('CommandExpressionAst'), 'Other');
  });
});

describe('classifyCommandName', () => {
  it('recognises Verb-Noun cmdlets', () => {
    assert.equal(classifyCommandName('Get-ChildItem'), 'cmdlet');
    assert.equal(classifyCommandName('Remove-Item'), 'cmdlet');
  });
  it('recognises path-bearing applications', () => {
    assert.equal(classifyCommandName('./run.sh'), 'application');
    assert.equal(classifyCommandName('scripts\\x'), 'application');
    assert.equal(classifyCommandName('a.exe'), 'application');
  });
  it('defaults to unknown', () => {
    assert.equal(classifyCommandName('git'), 'unknown');
  });
});

describe('stripModulePrefix', () => {
  it('strips a module prefix', () => {
    assert.equal(
      stripModulePrefix('Microsoft.PowerShell.Utility\\Invoke-Expression'),
      'Invoke-Expression',
    );
  });
  it('does NOT strip drive letters, UNC, or relative paths', () => {
    assert.equal(stripModulePrefix('C:\\Windows\\x.exe'), 'C:\\Windows\\x.exe');
    assert.equal(stripModulePrefix('\\\\server\\share'), '\\\\server\\share');
    assert.equal(stripModulePrefix('.\\local.ps1'), '.\\local.ps1');
    assert.equal(stripModulePrefix('..\\up.ps1'), '..\\up.ps1');
  });
  it('returns name unchanged when there is no backslash', () => {
    assert.equal(stripModulePrefix('Get-ChildItem'), 'Get-ChildItem');
  });
});

describe('transformCommandAst — name classification', () => {
  it('classifies a cmdlet and strips module prefix after classifying', () => {
    const el = transformCommandAst({
      type: 'CommandAst',
      text: 'Get-Process -Name x',
      commandElements: [
        {
          type: 'StringConstantExpressionAst',
          text: 'Get-Process',
          value: 'Get-Process',
        },
        { type: 'CommandParameterAst', text: '-Name' },
        { type: 'StringConstantExpressionAst', text: 'x', value: 'x' },
      ],
    });
    assert.equal(el.name, 'Get-Process');
    assert.equal(el.nameType, 'cmdlet');
    assert.deepEqual(el.args, ['-Name', 'x']);
    assert.deepEqual(el.elementTypes, [
      'StringConstant',
      'Parameter',
      'StringConstant',
    ]);
  });

  it('forces application nameType for non-ASCII homoglyph cmdlet names', () => {
    // U+017F (ſ) folds to S under .NET OrdinalIgnoreCase: `ſtart-proceſſ`.
    const el = transformCommandAst({
      type: 'CommandAst',
      text: '\u017ftart-proce\u017f\u017f',
      commandElements: [
        {
          type: 'StringConstantExpressionAst',
          text: '\u017ftart-proce\u017f\u017f',
          value: '\u017ftart-proce\u017f\u017f',
        },
      ],
    });
    assert.equal(el.nameType, 'application');
  });

  it('path-bearing name classifies as application before stripping', () => {
    const el = transformCommandAst({
      type: 'CommandAst',
      text: 'scripts\\Get-Process',
      commandElements: [
        {
          type: 'StringConstantExpressionAst',
          text: 'scripts\\Get-Process',
          value: 'scripts\\Get-Process',
        },
      ],
    });
    assert.equal(el.nameType, 'application');
    assert.equal(el.name, 'Get-Process');
  });

  it('strips surrounding quotes from .text when .value is absent', () => {
    const el = transformCommandAst({
      type: 'CommandAst',
      text: "& 'Invoke-Expression'",
      commandElements: [
        { type: 'StringConstantExpressionAst', text: "'Invoke-Expression'" },
      ],
    });
    assert.equal(el.name, 'Invoke-Expression');
  });

  it('captures colon-bound parameter children mapped through mapElementType', () => {
    const el = transformCommandAst({
      type: 'CommandAst',
      text: 'Get-Thing -In:$env:SECRET',
      commandElements: [
        { type: 'StringConstantExpressionAst', text: 'Get-Thing', value: 'Get-Thing' },
        {
          type: 'CommandParameterAst',
          text: '-In:$env:SECRET',
          children: [{ type: 'VariableExpressionAst', text: '$env:SECRET' }],
        },
      ],
    });
    assert.ok(el.children);
    assert.deepEqual(el.children![0], [
      { type: 'Variable', text: '$env:SECRET' },
    ]);
  });

  it('returns empty name for an element-less command', () => {
    const el = transformCommandAst({ type: 'CommandAst', text: '' });
    assert.equal(el.name, '');
    assert.equal(el.nameType, 'unknown');
  });

  it('preserves nested-command redirections', () => {
    const el = transformCommandAst({
      type: 'CommandAst',
      text: 'rm x > out.txt',
      commandElements: [
        { type: 'StringConstantExpressionAst', text: 'rm', value: 'rm' },
      ],
      redirections: [{ type: 'FileRedirectionAst', locationText: 'out.txt' }],
    });
    assert.deepEqual(el.redirections, [
      { operator: '>', target: 'out.txt', isMerging: false },
    ]);
  });
});

describe('transformExpressionElement', () => {
  it('maps a ParenExpression element', () => {
    const el = transformExpressionElement({
      type: 'ParenExpressionAst',
      text: '(1+1)',
    });
    assert.equal(el.elementType, 'ParenExpressionAst');
    assert.deepEqual(el.elementTypes, ['SubExpression']);
  });
  it('maps a non-paren expression to CommandExpressionAst', () => {
    const el = transformExpressionElement({
      type: 'CommandExpressionAst',
      text: '1',
      expressionType: 'ConstantExpressionAst',
    });
    assert.equal(el.elementType, 'CommandExpressionAst');
    assert.deepEqual(el.elementTypes, ['StringConstant']);
  });
});

describe('transformRedirection', () => {
  it('maps merging redirection 2>&1', () => {
    assert.deepEqual(transformRedirection({ type: 'MergingRedirectionAst' }), {
      operator: '2>&1',
      target: '',
      isMerging: true,
    });
  });
  it('maps file redirections across streams and append', () => {
    assert.equal(
      transformRedirection({
        type: 'FileRedirectionAst',
        locationText: 'f',
      }).operator,
      '>',
    );
    assert.equal(
      transformRedirection({
        type: 'FileRedirectionAst',
        append: true,
        locationText: 'f',
      }).operator,
      '>>',
    );
    assert.equal(
      transformRedirection({
        type: 'FileRedirectionAst',
        fromStream: 'Error',
        locationText: 'f',
      }).operator,
      '2>',
    );
    assert.equal(
      transformRedirection({
        type: 'FileRedirectionAst',
        fromStream: 'Error',
        append: true,
        locationText: 'f',
      }).operator,
      '2>>',
    );
    assert.equal(
      transformRedirection({
        type: 'FileRedirectionAst',
        fromStream: 'All',
        locationText: 'f',
      }).operator,
      '*>',
    );
    assert.equal(
      transformRedirection({
        type: 'FileRedirectionAst',
        fromStream: 'All',
        append: true,
        locationText: 'f',
      }).operator,
      '*>>',
    );
  });
});

describe('transformStatement — deep redirection hoisting + dedupe', () => {
  it('dedupes a direct-element redirection re-discovered by the deep FindAll', () => {
    const stmt = transformStatement({
      type: 'PipelineAst',
      text: 'rm x > out.txt',
      elements: [
        {
          type: 'CommandAst',
          text: 'rm x > out.txt',
          commandElements: [
            { type: 'StringConstantExpressionAst', text: 'rm', value: 'rm' },
          ],
          redirections: [
            { type: 'FileRedirectionAst', locationText: 'out.txt' },
          ],
        },
      ],
      // The PS1 deep FindAll re-reports the same redirection at statement level.
      redirections: [{ type: 'FileRedirectionAst', locationText: 'out.txt' }],
    });
    assert.equal(stmt.redirections.length, 1);
  });

  it('hoists a hidden redirection found only by the deep FindAll', () => {
    const stmt = transformStatement({
      type: 'PipelineAst',
      text: "Get-Thing -Name:('x' > ~/.bashrc)",
      elements: [
        {
          type: 'CommandAst',
          text: "Get-Thing -Name:('x' > ~/.bashrc)",
          commandElements: [
            {
              type: 'StringConstantExpressionAst',
              text: 'Get-Thing',
              value: 'Get-Thing',
            },
          ],
          redirections: [],
        },
      ],
      redirections: [{ type: 'FileRedirectionAst', locationText: '~/.bashrc' }],
    });
    assert.deepEqual(stmt.redirections, [
      { operator: '>', target: '~/.bashrc', isMerging: false },
    ]);
  });

  it('extracts redirections from a CommandExpressionAst element (1 > /tmp/x)', () => {
    const stmt = transformStatement({
      type: 'PipelineAst',
      text: '1 > /tmp/x',
      elements: [
        {
          type: 'CommandExpressionAst',
          text: '1 > /tmp/x',
          expressionType: 'ConstantExpressionAst',
          redirections: [{ type: 'FileRedirectionAst', locationText: '/tmp/x' }],
        },
      ],
    });
    assert.deepEqual(stmt.commands[0]!.elementType, 'CommandExpressionAst');
    assert.deepEqual(stmt.redirections, [
      { operator: '>', target: '/tmp/x', isMerging: false },
    ]);
  });

  it('hoists control-flow redirections for non-pipeline statements', () => {
    const stmt = transformStatement({
      type: 'IfStatementAst',
      text: 'if ($x) { 1 > /tmp/evil }',
      nestedCommands: [],
      redirections: [{ type: 'FileRedirectionAst', locationText: '/tmp/evil' }],
    });
    assert.equal(stmt.statementType, 'IfStatementAst');
    assert.equal(stmt.commands.length, 1);
    assert.equal(stmt.commands[0]!.elementType, 'CommandExpressionAst');
    assert.deepEqual(stmt.redirections, [
      { operator: '>', target: '/tmp/evil', isMerging: false },
    ]);
  });

  it('carries securityPatterns and nested commands through', () => {
    const stmt = transformStatement({
      type: 'IfStatementAst',
      text: 'if ($x) { Remove-Item / }',
      nestedCommands: [
        {
          type: 'CommandAst',
          text: 'Remove-Item /',
          commandElements: [
            {
              type: 'StringConstantExpressionAst',
              text: 'Remove-Item',
              value: 'Remove-Item',
            },
          ],
        },
      ],
      securityPatterns: { hasScriptBlocks: true },
    });
    assert.equal(stmt.nestedCommands?.[0]!.name, 'Remove-Item');
    assert.deepEqual(stmt.securityPatterns, { hasScriptBlocks: true });
  });
});

describe('isNullRedirectionTarget / getFileRedirections', () => {
  it('treats $null and ${null} as a sink (not a write)', () => {
    assert.ok(isNullRedirectionTarget('$null'));
    assert.ok(isNullRedirectionTarget('  ${NULL} '));
    assert.ok(!isNullRedirectionTarget('$nullish'));
    assert.ok(!isNullRedirectionTarget('out.txt'));
  });

  it('excludes merging + $null redirections from file redirections', () => {
    const parsed = transformRawOutput(
      minimalRaw({
        statements: [
          {
            type: 'PipelineAst',
            text: 'x',
            elements: [
              {
                type: 'CommandExpressionAst',
                text: 'x',
                redirections: [
                  { type: 'FileRedirectionAst', locationText: 'out.txt' },
                  { type: 'FileRedirectionAst', locationText: '$null' },
                  { type: 'MergingRedirectionAst' },
                ],
              },
            ],
          },
        ],
      }),
    );
    const files = getFileRedirections(parsed);
    assert.equal(files.length, 1);
    assert.equal(files[0]!.target, 'out.txt');
  });

  it('getAllRedirections includes nested-command redirections', () => {
    const parsed = transformRawOutput(
      minimalRaw({
        statements: [
          {
            type: 'IfStatementAst',
            text: 'if ($x) { rm y > z }',
            nestedCommands: [
              {
                type: 'CommandAst',
                text: 'rm y > z',
                commandElements: [
                  { type: 'StringConstantExpressionAst', text: 'rm', value: 'rm' },
                ],
                redirections: [
                  { type: 'FileRedirectionAst', locationText: 'z' },
                ],
              },
            ],
          },
        ],
      }),
    );
    const all = getAllRedirections(parsed);
    assert.ok(all.some((r) => r.target === 'z'));
  });
});

describe('COMMON_ALIASES — prototype safety + deliberate omissions', () => {
  it('returns undefined for prototype-pollution keys', () => {
    assert.equal(COMMON_ALIASES['constructor'], undefined);
    assert.equal(COMMON_ALIASES['__proto__'], undefined);
    assert.equal(COMMON_ALIASES['toString'], undefined);
    assert.equal(Object.getPrototypeOf(COMMON_ALIASES), null);
  });
  it('omits native-exe-colliding aliases on PS Core', () => {
    for (const omitted of ['sort', 'curl', 'wget', 'sc']) {
      assert.equal(COMMON_ALIASES[omitted], undefined);
    }
  });
  it('maps destructive and invoke aliases', () => {
    assert.equal(COMMON_ALIASES['rm'], 'Remove-Item');
    assert.equal(COMMON_ALIASES['iex'], 'Invoke-Expression');
    assert.equal(COMMON_ALIASES['kill'], 'Stop-Process');
  });
});

describe('resolveToCanonical — single-hop + PATHEXT', () => {
  it('resolves md directly (single-hop, no md→mkdir chaining)', () => {
    assert.equal(resolveToCanonical('md'), 'new-item');
    assert.equal(resolveToCanonical('mkdir'), 'new-item');
  });
  it('strips .exe/.cmd/.bat/.com on bare names only', () => {
    assert.equal(resolveToCanonical('git.exe'), 'git');
    assert.equal(resolveToCanonical('tool.CMD'), 'tool');
    // path-bearing names are NOT stripped/canonicalised to a PATH executable
    assert.equal(resolveToCanonical('scripts\\git.exe'), 'scripts\\git.exe');
    assert.equal(resolveToCanonical('./git.exe'), './git.exe');
  });
  it('lowercases and falls through for unknown names', () => {
    assert.equal(resolveToCanonical('Get-Process'), 'get-process');
    assert.equal(resolveToCanonical('RM'), 'remove-item');
  });
});

describe('name/command accessors', () => {
  const parsed = transformRawOutput(
    minimalRaw({
      statements: [
        {
          type: 'PipelineAst',
          text: 'cd x | rm y',
          elements: [
            {
              type: 'CommandAst',
              text: 'cd x',
              commandElements: [
                { type: 'StringConstantExpressionAst', text: 'cd', value: 'cd' },
              ],
            },
          ],
          nestedCommands: [
            {
              type: 'CommandAst',
              text: 'rm y',
              commandElements: [
                { type: 'StringConstantExpressionAst', text: 'rm', value: 'rm' },
              ],
            },
          ],
        },
      ],
      variables: [
        { path: 'env:SECRET', isSplatted: false },
        { path: 'HOME', isSplatted: true },
      ],
    }),
  );

  it('getAllCommandNames + getAllCommands include nested commands', () => {
    assert.deepEqual(getAllCommandNames(parsed), ['cd', 'rm']);
    assert.equal(getAllCommands(parsed).length, 2);
  });
  it('hasDirectoryChange detects cd alias', () => {
    assert.ok(hasDirectoryChange(parsed));
  });
  it('getVariablesByScope filters by scope prefix', () => {
    const env = getVariablesByScope(parsed, 'env');
    assert.equal(env.length, 1);
    assert.equal(env[0]!.path, 'env:SECRET');
  });
  it('getPipelineSegments returns the statements', () => {
    assert.equal(getPipelineSegments(parsed).length, 1);
  });
  it('isSingleCommand is false for compound/nested commands', () => {
    assert.ok(!isSingleCommand(parsed));
    const single = transformRawOutput(
      minimalRaw({ statements: [pipelineStmt('Get-Process')] }),
    );
    assert.ok(isSingleCommand(single));
  });
  it('hasCommandNamed resolves aliases both directions', () => {
    assert.ok(hasCommandNamed(parsed, 'Remove-Item')); // rm → Remove-Item
    assert.ok(hasCommandNamed(parsed, 'rm')); // direct alias
    assert.ok(!hasCommandNamed(parsed, 'Get-Content'));
  });
  it('hasCommandNamed matches canonical form of a requested alias', () => {
    const p = transformRawOutput(
      minimalRaw({ statements: [pipelineStmt('Remove-Item')] }),
    );
    assert.ok(hasCommandNamed(p, 'rm'));
  });
  it('hasCommandNamed matches alias-to-alias (same canonical)', () => {
    const p = transformRawOutput(
      minimalRaw({ statements: [pipelineStmt('del')] }),
    );
    assert.ok(hasCommandNamed(p, 'rm'));
  });
});

describe('commandHasArg / parameter detection', () => {
  const cmd = transformCommandAst({
    type: 'CommandAst',
    text: 'x -Recurse',
    commandElements: [
      { type: 'StringConstantExpressionAst', text: 'x', value: 'x' },
      { type: 'CommandParameterAst', text: '-Recurse' },
    ],
  });

  it('commandHasArg is case-insensitive', () => {
    assert.ok(commandHasArg(cmd, '-recurse'));
    assert.ok(!commandHasArg(cmd, '-force'));
  });

  it('PS_TOKENIZER_DASH_CHARS covers ASCII + en/em-dash + horizontal bar', () => {
    assert.ok(PS_TOKENIZER_DASH_CHARS.has('-'));
    assert.ok(PS_TOKENIZER_DASH_CHARS.has('\u2013'));
    assert.ok(PS_TOKENIZER_DASH_CHARS.has('\u2014'));
    assert.ok(PS_TOKENIZER_DASH_CHARS.has('\u2015'));
  });

  it('isPowerShellParameter uses elementType as ground truth', () => {
    assert.ok(isPowerShellParameter('\u2013Path', 'Parameter'));
    assert.ok(!isPowerShellParameter('-Path', 'StringConstant'));
  });

  it('isPowerShellParameter falls back to dash-char sniffing', () => {
    assert.ok(isPowerShellParameter('\u2014Path'));
    assert.ok(isPowerShellParameter('-Path'));
    assert.ok(!isPowerShellParameter('Path'));
    assert.ok(!isPowerShellParameter(''));
  });

  it('commandHasArgAbbreviation matches unambiguous prefixes', () => {
    const enc = transformCommandAst({
      type: 'CommandAst',
      text: 'pwsh -enc abc',
      commandElements: [
        { type: 'StringConstantExpressionAst', text: 'pwsh', value: 'pwsh' },
        { type: 'CommandParameterAst', text: '-enc' },
        { type: 'StringConstantExpressionAst', text: 'abc', value: 'abc' },
      ],
    });
    assert.ok(commandHasArgAbbreviation(enc, '-EncodedCommand', '-en'));
    assert.ok(!commandHasArgAbbreviation(enc, '-EncodedCommand', '-enco'));
  });

  it('commandHasArgAbbreviation strips colon values and backtick escapes', () => {
    const colon = transformCommandAst({
      type: 'CommandAst',
      text: 'pwsh -en:abc',
      commandElements: [
        { type: 'StringConstantExpressionAst', text: 'pwsh', value: 'pwsh' },
        { type: 'CommandParameterAst', text: '-en:abc' },
      ],
    });
    assert.ok(commandHasArgAbbreviation(colon, '-EncodedCommand', '-en'));

    const tick = transformCommandAst({
      type: 'CommandAst',
      text: 'cmd -Mem`ber',
      commandElements: [
        { type: 'StringConstantExpressionAst', text: 'cmd', value: 'cmd' },
        { type: 'CommandParameterAst', text: '-Mem`ber' },
      ],
    });
    assert.ok(commandHasArgAbbreviation(tick, '-MemberName', '-mem'));
  });
});

describe('deriveSecurityFlags', () => {
  it('flags subexpressions, scriptblocks, members, expandable strings', () => {
    const parsed = transformRawOutput(
      minimalRaw({
        statements: [
          {
            type: 'PipelineAst',
            text: 'x',
            elements: [
              {
                type: 'CommandAst',
                text: 'x',
                commandElements: [
                  { type: 'StringConstantExpressionAst', text: 'x', value: 'x' },
                  { type: 'SubExpressionAst', text: '$(1)' },
                  { type: 'ScriptBlockExpressionAst', text: '{ rm / }' },
                  { type: 'InvokeMemberExpressionAst', text: '$o.M()' },
                  { type: 'ExpandableStringExpressionAst', text: '"$(x)"' },
                ],
              },
            ],
          },
        ],
      }),
    );
    const flags = deriveSecurityFlags(parsed);
    assert.ok(flags.hasSubExpressions);
    assert.ok(flags.hasScriptBlocks);
    assert.ok(flags.hasMemberInvocations);
    assert.ok(flags.hasExpandableStrings);
  });

  it('uses the securityPatterns belt-and-suspenders cross-check', () => {
    const parsed = transformRawOutput(
      minimalRaw({
        statements: [
          {
            type: 'AssignmentStatementAst',
            text: '$x = $o.Method()',
            securityPatterns: {
              hasMemberInvocations: true,
              hasSubExpressions: true,
              hasExpandableStrings: true,
              hasScriptBlocks: true,
            },
          },
        ],
      }),
    );
    const flags = deriveSecurityFlags(parsed);
    assert.ok(flags.hasAssignments);
    assert.ok(flags.hasMemberInvocations);
    assert.ok(flags.hasSubExpressions);
    assert.ok(flags.hasExpandableStrings);
    assert.ok(flags.hasScriptBlocks);
  });

  it('flags splatting and stop-parsing', () => {
    const parsed = transformRawOutput(
      minimalRaw({
        statements: [pipelineStmt('x')],
        variables: [{ path: 'args', isSplatted: true }],
        hasStopParsing: true,
      }),
    );
    const flags = deriveSecurityFlags(parsed);
    assert.ok(flags.hasSplatting);
    assert.ok(flags.hasStopParsing);
  });

  it('checkElements is a no-op when elementTypes are absent', () => {
    const parsed = transformRawOutput(
      minimalRaw({
        statements: [
          { type: 'UnknownStatementAst', text: 'weird', nestedCommands: [] },
        ],
      }),
    );
    const flags = deriveSecurityFlags(parsed);
    assert.ok(!flags.hasSubExpressions);
  });
});

describe('transformRawOutput — top-level fields', () => {
  it('surfaces typeLiterals, using statements and #Requires', () => {
    const parsed = transformRawOutput(
      minimalRaw({
        statements: [],
        typeLiterals: ['int'],
        hasUsingStatements: true,
        hasScriptRequirements: true,
      }),
    );
    assert.deepEqual(parsed.typeLiterals, ['int']);
    assert.equal(parsed.hasUsingStatements, true);
    assert.equal(parsed.hasScriptRequirements, true);
  });
  it('tolerates PS 5.1 single-object unwrapping (ensureArray)', () => {
    // ConvertTo-Json may emit a single statement as an object, not an array.
    const raw = {
      valid: true,
      errors: [],
      statements: pipelineStmt('Get-Process') as unknown,
      variables: null as unknown,
      hasStopParsing: false,
      originalCommand: 'Get-Process',
    } as unknown as RawParsedOutput;
    const parsed = transformRawOutput(raw);
    assert.equal(parsed.statements.length, 1);
    assert.deepEqual(parsed.variables, []);
  });
});

describe('length gate', () => {
  it('derives a positive Windows byte budget from PARSE_SCRIPT_BODY', () => {
    assert.ok(WINDOWS_MAX_COMMAND_LENGTH > 0);
    assert.ok(PARSE_SCRIPT_BODY.length > 1000);
  });
  it('MAX_COMMAND_LENGTH matches the platform gate', () => {
    const expected =
      process.platform === 'win32' ? WINDOWS_MAX_COMMAND_LENGTH : 4500;
    assert.equal(MAX_COMMAND_LENGTH, expected);
  });
});

describe('buildParseScript / toUtf16LeBase64', () => {
  it('embeds the base64 UTF-8 command and the parse body', () => {
    const script = buildParseScript('Get-Process');
    const b64 = Buffer.from('Get-Process', 'utf8').toString('base64');
    assert.ok(script.startsWith(`$EncodedCommand = '${b64}'`));
    assert.ok(script.includes('ParseInput'));
  });
  it('encodes UTF-16LE base64 round-trippable by PowerShell', () => {
    const enc = toUtf16LeBase64('hi');
    assert.equal(Buffer.from(enc, 'base64').toString('utf16le'), 'hi');
  });
});

describe('getParseTimeoutMs', () => {
  afterEach(() => {
    delete process.env.AMI_PWSH_PARSE_TIMEOUT_MS;
  });
  it('defaults to 5000ms', () => {
    assert.equal(getParseTimeoutMs(), 5000);
  });
  it('honours a valid positive override', () => {
    process.env.AMI_PWSH_PARSE_TIMEOUT_MS = '1234';
    assert.equal(getParseTimeoutMs(), 1234);
  });
  it('ignores non-positive / non-numeric overrides', () => {
    process.env.AMI_PWSH_PARSE_TIMEOUT_MS = '0';
    assert.equal(getParseTimeoutMs(), 5000);
    process.env.AMI_PWSH_PARSE_TIMEOUT_MS = 'nope';
    assert.equal(getParseTimeoutMs(), 5000);
  });
});

describe('parsePowerShellCommand — spawn pipeline (injected runtime)', () => {
  afterEach(() => {
    __resetPwshRuntimeForTest();
  });

  it('parses a real raw-JSON payload end to end', async () => {
    __setPwshRuntimeForTest({
      resolvePwshPath: () => '/usr/bin/pwsh',
      spawn: async () =>
        okSpawn(minimalRaw({ statements: [pipelineStmt('Get-Process')] })),
    });
    const parsed = await parsePowerShellCommand('Get-Process A');
    assert.equal(parsed.valid, true);
    assert.deepEqual(getAllCommandNames(parsed), ['get-process']);
  });

  it('passes the encoded script to the spawn runtime', async () => {
    let seenArgs: string[] = [];
    __setPwshRuntimeForTest({
      resolvePwshPath: () => '/usr/bin/pwsh',
      spawn: async (_p, args) => {
        seenArgs = args;
        return okSpawn(minimalRaw({ statements: [] }));
      },
    });
    await parsePowerShellCommand('Write-Output hi');
    assert.ok(seenArgs.includes('-EncodedCommand'));
    assert.ok(seenArgs.includes('-NonInteractive'));
  });

  it('returns NoPowerShell when no pwsh is found', async () => {
    __setPwshRuntimeForTest({ resolvePwshPath: () => null });
    const parsed = await parsePowerShellCommand('ls');
    assert.equal(parsed.valid, false);
    assert.equal(parsed.errors[0]!.errorId, 'NoPowerShell');
  });

  it('CommandTooLong uses a UTF-8 byte budget (CJK over-length)', async () => {
    __setPwshRuntimeForTest({ resolvePwshPath: () => '/usr/bin/pwsh' });
    // 2000 CJK chars = 6000 UTF-8 bytes > 4500 (Unix) and > Windows budget.
    const big = '\u4e2d'.repeat(2000);
    const parsed = await parsePowerShellCommand(big);
    assert.equal(parsed.valid, false);
    assert.equal(parsed.errors[0]!.errorId, 'CommandTooLong');
  });

  it('retries once on timeout then reports PwshTimeout, and does not cache it', async () => {
    let calls = 0;
    __setPwshRuntimeForTest({
      resolvePwshPath: () => '/usr/bin/pwsh',
      spawn: async () => {
        calls++;
        return {
          stdout: '',
          stderr: '',
          timedOut: true,
          failed: true,
          exitCode: null,
        };
      },
    });
    const first = await parsePowerShellCommand('timeout-cmd');
    assert.equal(first.errors[0]!.errorId, 'PwshTimeout');
    assert.equal(calls, 2, 'should retry once (2 attempts)');
    // Transient failure must be evicted → a second call re-spawns.
    await parsePowerShellCommand('timeout-cmd');
    assert.equal(calls, 4);
  });

  it('reports PwshError on non-zero exit', async () => {
    __setPwshRuntimeForTest({
      resolvePwshPath: () => '/usr/bin/pwsh',
      spawn: async () => ({
        stdout: '',
        stderr: 'boom',
        timedOut: false,
        failed: true,
        exitCode: 2,
      }),
    });
    const parsed = await parsePowerShellCommand('bad-exit');
    assert.equal(parsed.errors[0]!.errorId, 'PwshError');
  });

  it('reports EmptyOutput on blank stdout', async () => {
    __setPwshRuntimeForTest({
      resolvePwshPath: () => '/usr/bin/pwsh',
      spawn: async () => ({
        stdout: '   ',
        stderr: '',
        timedOut: false,
        failed: false,
        exitCode: 0,
      }),
    });
    const parsed = await parsePowerShellCommand('empty-out');
    assert.equal(parsed.errors[0]!.errorId, 'EmptyOutput');
  });

  it('reports InvalidJson on unparseable stdout', async () => {
    __setPwshRuntimeForTest({
      resolvePwshPath: () => '/usr/bin/pwsh',
      spawn: async () => ({
        stdout: 'not json {',
        stderr: '',
        timedOut: false,
        failed: false,
        exitCode: 0,
      }),
    });
    const parsed = await parsePowerShellCommand('bad-json');
    assert.equal(parsed.errors[0]!.errorId, 'InvalidJson');
  });

  it('reports PwshSpawnError when the spawn throws', async () => {
    __setPwshRuntimeForTest({
      resolvePwshPath: () => '/usr/bin/pwsh',
      spawn: async () => {
        throw new Error('ENOENT');
      },
    });
    const parsed = await parsePowerShellCommand('spawn-throw');
    assert.equal(parsed.errors[0]!.errorId, 'PwshSpawnError');
    assert.ok(parsed.errors[0]!.message.includes('ENOENT'));
  });

  it('memoizes a successful parse (single spawn for repeated calls)', async () => {
    let calls = 0;
    __setPwshRuntimeForTest({
      resolvePwshPath: () => '/usr/bin/pwsh',
      spawn: async () => {
        calls++;
        return okSpawn(minimalRaw({ statements: [pipelineStmt('ls')] }));
      },
    });
    await parsePowerShellCommand('ls -la');
    await parsePowerShellCommand('ls -la');
    assert.equal(calls, 1, 'successful result should be cached');
  });
});

describe('memoizeWithLRU cache surface', () => {
  it('exposes delete/has/size/clear and evicts LRU entries', () => {
    let n = 0;
    const m = memoizeWithLRU<number>(() => n++, 2);
    m('a');
    m('b');
    assert.equal(m.cache.size(), 2);
    assert.ok(m.cache.has('a'));
    // Access 'a' to make it most-recent, then insert 'c' → 'b' evicted.
    m('a');
    m('c');
    assert.ok(m.cache.has('a'));
    assert.ok(!m.cache.has('b'));
    assert.ok(m.cache.delete('a'));
    assert.ok(!m.cache.has('a'));
    m.cache.clear();
    assert.equal(m.cache.size(), 0);
  });
});
