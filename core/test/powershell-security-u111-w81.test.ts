import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { powershellCommandIsSafe } from '../src/tools/powershell-security';
import type { ParsedPowerShellCommand } from '../src/tools/powershell-parser';

// ---------------------------------------------------------------------------
// U111 (w81) — Diagnostic error IDs + human-readable reasons surfaced at the
// fail-safe parse-failure boundary of powershellCommandIsSafe.
//
// Parity gap: the PowerShell parser already produces distinct structured
// diagnostics ({ errorId, message } — CommandTooLong / EmptyOutput / InvalidJson
// / PwshError / a syntax ErrorId, …), but powershellCommandIsSafe previously
// discarded them and emitted a single generic "Could not parse …" reason. Claude
// Code surfaces the actionable per-error message in the degraded-to-ask reason
// (powershellPermissions.ts: "…cannot be parsed: <message>"). This suite locks
// the structured diagnostic into the ask reason so the degradation surface is
// actionable, while keeping the fail-safe `ask` behavior.
// ---------------------------------------------------------------------------

function invalid(
  command: string,
  errors: { message: string; errorId: string }[],
): ParsedPowerShellCommand {
  return {
    valid: false,
    errors,
    statements: [],
    variables: [],
    hasStopParsing: false,
    originalCommand: command,
  };
}

describe('powershellCommandIsSafe — diagnostic error IDs at the parse-failure boundary', () => {
  it('surfaces the CommandTooLong errorId + human-readable message in the ask reason', () => {
    const r = powershellCommandIsSafe(
      'x'.repeat(10_000),
      invalid('x'.repeat(10_000), [
        {
          errorId: 'CommandTooLong',
          message:
            'Command too long for parsing (10000 bytes). Maximum supported length is 4500 bytes.',
        },
      ]),
    );
    assert.equal(r.behavior, 'ask');
    // Still a fail-safe "could not parse" ask …
    assert.match(r.message ?? '', /could not parse/i);
    // … but now carries the structured errorId AND its actionable message.
    assert.match(r.message ?? '', /CommandTooLong/);
    assert.match(r.message ?? '', /Maximum supported length is 4500 bytes/);
  });

  it('surfaces a syntax errorId + message (e.g. UnexpectedToken)', () => {
    const r = powershellCommandIsSafe(
      'garbage )(',
      invalid('garbage )(', [
        { errorId: 'UnexpectedToken', message: "Unexpected token ')('" },
      ]),
    );
    assert.equal(r.behavior, 'ask');
    assert.match(r.message ?? '', /UnexpectedToken/);
    assert.match(r.message ?? '', /Unexpected token/);
  });

  it('surfaces each distinct degradation errorId', () => {
    for (const errorId of [
      'EmptyOutput',
      'InvalidJson',
      'PwshError',
      'PwshTimeout',
      'NoPowerShell',
    ]) {
      const r = powershellCommandIsSafe(
        'Get-Thing',
        invalid('Get-Thing', [{ errorId, message: `reason for ${errorId}` }]),
      );
      assert.equal(r.behavior, 'ask');
      assert.match(r.message ?? '', new RegExp(errorId));
      assert.match(r.message ?? '', new RegExp(`reason for ${errorId}`));
    }
  });

  it('falls back to the generic reason when the parse failed with no error detail', () => {
    const r = powershellCommandIsSafe('weird', invalid('weird', []));
    assert.equal(r.behavior, 'ask');
    assert.match(r.message ?? '', /could not parse/i);
    // No trailing "(errorId: message)" parenthetical when there is no diagnostic.
    assert.doesNotMatch(r.message ?? '', /\(.*:.*\)/);
  });
});
