import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { powershellCommandIsSafe } from '../src/tools/powershell-security';
import type {
  ParsedPowerShellCommand,
  ParsedCommandElement,
  ParsedStatement,
  CommandElementChild,
  CommandElementType,
} from '../src/tools/powershell-parser';

// ---------------------------------------------------------------------------
// U111 (w82) — AST structural defense-in-depth for Start-Process -Verb RunAs.
//
// Parity gap (Claude Code PowerShellTool/powershellSecurity.ts checkStartProcess,
// PR #23554): the AST validator inspects the parsed `children[]` of a colon-bound
// `-Verb:<value>` parameter and asks when the bound value, with quotes/backticks/
// whitespace stripped, normalizes to `runas`. This catches elevation requests
// whose value is obscured by quoting/padding the textual regex cannot anticipate
// (e.g. `-Verb:" RunAs "`). Before this layer, powershellCommandIsSafe delegated
// Start-Process entirely to the textual regex detector (detectStartProcessElevation),
// which tokenizes on whitespace and so MISSES a quoted value containing spaces —
// returning passthrough. These fixtures are hand-built ASTs (no pwsh runtime).
// ---------------------------------------------------------------------------

function cmd(
  name: string,
  args: string[] = [],
  opts: Partial<ParsedCommandElement> = {},
): ParsedCommandElement {
  return {
    name,
    nameType: 'cmdlet',
    elementType: 'CommandAst',
    args,
    text: [name, ...args].join(' '),
    elementTypes: [
      'StringConstant',
      ...args.map<CommandElementType>((a) =>
        a.startsWith('-') ? 'Parameter' : 'StringConstant',
      ),
    ],
    ...opts,
  };
}

function stmt(commands: ParsedCommandElement[]): ParsedStatement {
  return {
    statementType: 'PipelineAst',
    commands,
    redirections: [],
    text: commands.map((c) => c.text).join(' | '),
  };
}

function parsed(
  statements: ParsedStatement[],
  originalCommand: string,
): ParsedPowerShellCommand {
  return {
    valid: true,
    errors: [],
    statements,
    variables: [],
    hasStopParsing: false,
    originalCommand,
  };
}

// A colon-bound `-Verb:<value>` command element whose bound value lives in the
// parser's `children[]` (CommandParameterAst.Argument). `args[i]` is the full
// extent text; `children[i]` is the structural [{type,text}] of the value.
function startProcessColonVerb(
  verbArgExtent: string,
  childText: string,
): ParsedPowerShellCommand {
  const args = ['notepad', verbArgExtent];
  const children: (CommandElementChild[] | undefined)[] = [
    undefined, // the `notepad` positional has no colon-bound child
    [{ type: 'StringConstant', text: childText }],
  ];
  return parsed(
    [stmt([cmd('Start-Process', args, { children })])],
    `Start-Process ${args.join(' ')}`,
  );
}

describe('powershellCommandIsSafe — Start-Process -Verb RunAs structural children[] layer', () => {
  it('asks on -Verb:" RunAs " where whitespace/quote padding evades the textual regex', () => {
    const r = powershellCommandIsSafe(
      'Start-Process notepad -Verb:" RunAs "',
      startProcessColonVerb('-Verb:" RunAs "', '" RunAs "'),
    );
    assert.equal(r.behavior, 'ask');
    assert.match(r.message ?? '', /elevated|RunAs/i);
  });

  it('asks on a backtick-obscured colon-bound value -V`erb:`r`u`n`a`s', () => {
    const r = powershellCommandIsSafe(
      'Start-Process notepad -V`erb:`r`u`n`a`s',
      startProcessColonVerb('-V`erb:`r`u`n`a`s', '`r`u`n`a`s'),
    );
    assert.equal(r.behavior, 'ask');
    assert.match(r.message ?? '', /elevated|RunAs/i);
  });

  it('asks for the saps / start aliases too', () => {
    for (const alias of ['saps', 'start']) {
      const children: (CommandElementChild[] | undefined)[] = [
        undefined,
        [{ type: 'StringConstant', text: '" RunAs "' }],
      ];
      const r = powershellCommandIsSafe(
        `${alias} notepad -Verb:" RunAs "`,
        parsed(
          [stmt([cmd(alias, ['notepad', '-Verb:" RunAs "'], { children })])],
          `${alias} notepad -Verb:" RunAs "`,
        ),
      );
      assert.equal(r.behavior, 'ask', `expected ask for alias ${alias}`);
    }
  });

  it('does NOT ask on a benign colon-bound verb (-Verb:Open)', () => {
    const r = powershellCommandIsSafe(
      'Start-Process notepad -Verb:Open',
      startProcessColonVerb('-Verb:Open', 'Open'),
    );
    assert.equal(r.behavior, 'passthrough');
  });

  it('ignores a colon-bound value on a non-Verb parameter', () => {
    const children: (CommandElementChild[] | undefined)[] = [
      undefined,
      [{ type: 'StringConstant', text: '" RunAs "' }],
    ];
    const r = powershellCommandIsSafe(
      'Start-Process notepad -WindowStyle:" RunAs "',
      parsed(
        [stmt([cmd('Start-Process', ['notepad', '-WindowStyle:" RunAs "'], { children })])],
        'Start-Process notepad -WindowStyle:" RunAs "',
      ),
    );
    assert.equal(r.behavior, 'passthrough');
  });

  it('does not misfire when children are absent (fail-open structural layer)', () => {
    const r = powershellCommandIsSafe(
      'Start-Process notepad -Verb:Open',
      parsed(
        [stmt([cmd('Start-Process', ['notepad', '-Verb:Open'])])],
        'Start-Process notepad -Verb:Open',
      ),
    );
    assert.equal(r.behavior, 'passthrough');
  });
});
