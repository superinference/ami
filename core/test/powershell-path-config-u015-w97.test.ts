import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  extractPowerShellCommandPaths,
  CMDLET_PATH_CONFIG,
  powershellCommandIsSafe,
} from '../src/tools/powershell-security';
import type {
  ParsedPowerShellCommand,
  ParsedCommandElement,
  ParsedStatement,
  CommandElementType,
} from '../src/tools/powershell-parser';

// ---------------------------------------------------------------------------
// U015 (w97) — PowerShell per-cmdlet path-parameter configuration + extraction
// with the three fine-grained knobs (leafOnlyPathParams / positionalSkip /
// optionalWrite) and defaults (none / 0 / false).
//
// Parity gaps (Claude Code PowerShellTool/pathValidation.ts CMDLET_PATH_CONFIG +
// extractPathsFromCommand):
//   - Per-cmdlet path-param data structure with switch/value/path classification
//     and an unknown-param → force-ask security model.
//   - leafOnlyPathParams: New-Item -Name is resolved relative to -Path; only a
//     simple leaf is extracted, any traversal/separator forces an ask.
//   - positionalSkip: Invoke-WebRequest positional-0 is a -Uri URL, not a path.
//   - optionalWrite: iwr/irm without -OutFile write to the pipeline, not disk,
//     so the "write with no target → ask" rule is skipped.
//
// Fixtures are hand-built ASTs (no pwsh runtime), matching the existing
// powershell-security AST test harness.
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

function parsed(statements: ParsedStatement[]): ParsedPowerShellCommand {
  return {
    valid: true,
    errors: [],
    statements,
    variables: [],
    hasStopParsing: false,
    originalCommand: statements.map((s) => s.text).join('; '),
  };
}

describe('extractPowerShellCommandPaths — base extraction', () => {
  it('returns empty read result for a cmdlet with no path config', () => {
    const r = extractPowerShellCommandPaths(cmd('Get-Process'));
    assert.deepEqual(r, {
      paths: [],
      operationType: 'read',
      hasUnvalidatablePathArg: false,
      optionalWrite: false,
    });
  });

  it('extracts a named -Path value and classifies Set-Content as write', () => {
    const r = extractPowerShellCommandPaths(
      cmd('Set-Content', ['-Path', 'a.txt', '-Value', 'hi']),
    );
    assert.deepEqual(r.paths, ['a.txt']);
    assert.equal(r.operationType, 'write');
    assert.equal(r.hasUnvalidatablePathArg, false);
    assert.equal(r.optionalWrite, false);
  });

  it('extracts a positional path for a read cmdlet', () => {
    const r = extractPowerShellCommandPaths(cmd('Get-Content', ['a.txt']));
    assert.deepEqual(r.paths, ['a.txt']);
    assert.equal(r.operationType, 'read');
  });

  it('consumes a known value param without treating its value as a path', () => {
    const r = extractPowerShellCommandPaths(
      cmd('Get-Content', ['-Path', 'a.txt', '-Encoding', 'UTF8']),
    );
    assert.deepEqual(r.paths, ['a.txt']);
    assert.equal(r.hasUnvalidatablePathArg, false);
  });

  it('honors PowerShell unambiguous-prefix matching (-Lit → -LiteralPath)', () => {
    const r = extractPowerShellCommandPaths(
      cmd('Get-Content', ['-Lit', 'a.txt']),
    );
    assert.deepEqual(r.paths, ['a.txt']);
  });

  it('extracts a colon-bound path value (-Path:a.txt)', () => {
    const r = extractPowerShellCommandPaths(
      cmd('Set-Content', ['-Path:a.txt', '-Value', 'hi']),
    );
    assert.deepEqual(r.paths, ['a.txt']);
    assert.equal(r.hasUnvalidatablePathArg, false);
  });

  it('resolves an alias to its canonical config (rm → remove-item)', () => {
    const r = extractPowerShellCommandPaths(cmd('rm', ['a.txt']));
    assert.equal(r.operationType, 'write');
    assert.ok(r.paths.includes('a.txt'));
  });
});

describe('extractPowerShellCommandPaths — fail-safe unknown-param model', () => {
  it('flags an unknown -Param as unvalidatable (force ask)', () => {
    const r = extractPowerShellCommandPaths(
      cmd('Set-Content', ['-Path', 'a.txt', '-Bogus', 'x']),
    );
    assert.equal(r.hasUnvalidatablePathArg, true);
  });

  it('flags a complex colon value ($env expansion) as unvalidatable', () => {
    const r = extractPowerShellCommandPaths(
      cmd('Set-Content', ['-Path:$env:SECRET', '-Value', 'hi']),
    );
    assert.equal(r.hasUnvalidatablePathArg, true);
  });

  it('flags a non-static (Variable) path element type as unvalidatable', () => {
    const c = cmd('Set-Content', ['-Path', '$target', '-Value', 'hi']);
    // Mark the -Path value ($target) as a Variable element type.
    c.elementTypes = ['StringConstant', 'Parameter', 'Variable', 'Parameter', 'StringConstant'];
    const r = extractPowerShellCommandPaths(c);
    assert.equal(r.hasUnvalidatablePathArg, true);
  });
});

describe('extractPowerShellCommandPaths — leafOnlyPathParams knob', () => {
  it('extracts a simple leaf filename for New-Item -Name', () => {
    const r = extractPowerShellCommandPaths(
      cmd('New-Item', ['-Path', 'dir', '-Name', 'foo.txt', '-ItemType', 'File']),
    );
    assert.ok(r.paths.includes('dir'));
    assert.ok(r.paths.includes('foo.txt'));
    assert.equal(r.hasUnvalidatablePathArg, false);
  });

  it('flags a non-leaf -Name value (traversal) as unvalidatable', () => {
    const r = extractPowerShellCommandPaths(
      cmd('New-Item', ['-Path', 'dir', '-Name', '../escape']),
    );
    assert.equal(r.hasUnvalidatablePathArg, true);
  });

  it('flags a -Name value containing a path separator as unvalidatable', () => {
    const r = extractPowerShellCommandPaths(
      cmd('New-Item', ['-Name', 'sub/child.txt']),
    );
    assert.equal(r.hasUnvalidatablePathArg, true);
  });
});

describe('extractPowerShellCommandPaths — positionalSkip knob', () => {
  it('skips the leading -Uri positional for Invoke-WebRequest', () => {
    const r = extractPowerShellCommandPaths(
      cmd('Invoke-WebRequest', ['http://example.com', '-OutFile', 'out.txt']),
    );
    assert.deepEqual(r.paths, ['out.txt']);
    assert.equal(r.hasUnvalidatablePathArg, false);
    assert.equal(r.optionalWrite, true);
  });

  it('does not misread the URL positional as a filesystem path (default skip 0 differs)', () => {
    // For a cmdlet WITHOUT positionalSkip, positional-0 would be a path; the knob
    // is what prevents iwr's URL from being extracted.
    const withSkip = extractPowerShellCommandPaths(
      cmd('Invoke-RestMethod', ['https://api.example.com']),
    );
    assert.deepEqual(withSkip.paths, []);
  });
});

describe('extractPowerShellCommandPaths — optionalWrite knob', () => {
  it('reports optionalWrite true for bare iwr (pipeline-only, no disk write)', () => {
    const r = extractPowerShellCommandPaths(
      cmd('Invoke-WebRequest', ['http://example.com']),
    );
    assert.deepEqual(r.paths, []);
    assert.equal(r.operationType, 'write');
    assert.equal(r.optionalWrite, true);
  });

  it('reports optionalWrite false for a cmdlet that always writes (Set-Content)', () => {
    const r = extractPowerShellCommandPaths(cmd('Set-Content', ['-Path', 'a.txt', '-Value', 'hi']));
    assert.equal(r.optionalWrite, false);
  });
});

describe('CMDLET_PATH_CONFIG — knob defaults', () => {
  it('omits the three knobs by default (undefined) so defaults apply', () => {
    const sc = CMDLET_PATH_CONFIG['set-content']!;
    assert.equal(sc.leafOnlyPathParams, undefined);
    assert.equal(sc.positionalSkip, undefined);
    assert.equal(sc.optionalWrite, undefined);
  });

  it('declares the knobs explicitly where the reference does', () => {
    assert.deepEqual(CMDLET_PATH_CONFIG['new-item']!.leafOnlyPathParams, ['-name']);
    assert.equal(CMDLET_PATH_CONFIG['invoke-webrequest']!.positionalSkip, 1);
    assert.equal(CMDLET_PATH_CONFIG['invoke-webrequest']!.optionalWrite, true);
  });
});

describe('powershellCommandIsSafe — path-constraint integration', () => {
  it('passes a write cmdlet with a determinable target path', () => {
    const r = powershellCommandIsSafe(
      'Set-Content -Path a.txt -Value hi',
      parsed([stmt([cmd('Set-Content', ['-Path', 'a.txt', '-Value', 'hi'])])]),
    );
    assert.equal(r.behavior, 'passthrough');
  });

  it('asks when a write cmdlet carries an unknown parameter (fail-safe)', () => {
    const r = powershellCommandIsSafe(
      'Set-Content -Path a.txt -Bogus x -Value hi',
      parsed([stmt([cmd('Set-Content', ['-Path', 'a.txt', '-Bogus', 'x', '-Value', 'hi'])])]),
    );
    assert.equal(r.behavior, 'ask');
    assert.match(r.message ?? '', /cannot be statically validated/i);
  });

  it('asks when a write cmdlet has no determinable target path', () => {
    const r = powershellCommandIsSafe(
      'Out-File',
      parsed([stmt([cmd('Out-File')])]),
    );
    assert.equal(r.behavior, 'ask');
    assert.match(r.message ?? '', /no target path/i);
  });

  it('does NOT ask for bare iwr (optionalWrite — pipeline-only, no target needed)', () => {
    const r = powershellCommandIsSafe(
      'Invoke-WebRequest http://example.com',
      parsed([stmt([cmd('Invoke-WebRequest', ['http://example.com'])])]),
    );
    assert.equal(r.behavior, 'passthrough');
  });

  it('asks when New-Item -Name escapes with traversal', () => {
    const r = powershellCommandIsSafe(
      'New-Item -Path dir -Name ../escape',
      parsed([stmt([cmd('New-Item', ['-Path', 'dir', '-Name', '../escape'])])]),
    );
    assert.equal(r.behavior, 'ask');
  });

  it('passes a plain read cmdlet (no path config side effects)', () => {
    const r = powershellCommandIsSafe(
      'Get-Content a.txt',
      parsed([stmt([cmd('Get-Content', ['a.txt'])])]),
    );
    assert.equal(r.behavior, 'passthrough');
  });
});
