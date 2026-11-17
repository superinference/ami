import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  PermissionManager,
  validatePermissionRule,
  mcpInfoFromString,
  bashCommandArgLeaksValue,
  SAFE_ENV_VARS,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// U003/w88 — MCP permission-rule parsing + validation (gaps 115 / 116)
//
// Claude's permissionValidation.ts detects MCP rules via mcpInfoFromString and
// enforces that an MCP rule carries NO parenthesized pattern — server-level,
// wildcard, and tool-level forms are the only valid shapes, each surfaced with
// an actionable example. AMI previously performed none of these checks.
// ---------------------------------------------------------------------------
describe('mcpInfoFromString', () => {
  it('parses a server-level MCP name', () => {
    assert.deepEqual(mcpInfoFromString('mcp__github'), {
      serverName: 'github',
      toolName: undefined,
    });
  });

  it('parses a tool-level MCP name', () => {
    assert.deepEqual(mcpInfoFromString('mcp__github__create_issue'), {
      serverName: 'github',
      toolName: 'create_issue',
    });
  });

  it('preserves double underscores in the tool name segment', () => {
    assert.deepEqual(mcpInfoFromString('mcp__srv__a__b'), {
      serverName: 'srv',
      toolName: 'a__b',
    });
  });

  it('returns null for a non-MCP tool name', () => {
    assert.equal(mcpInfoFromString('bash'), null);
    assert.equal(mcpInfoFromString('Bash'), null);
  });

  it('returns null when the server segment is missing', () => {
    assert.equal(mcpInfoFromString('mcp__'), null);
    assert.equal(mcpInfoFromString('mcp'), null);
  });
});

describe('validatePermissionRule — MCP rules', () => {
  it('accepts a bare server-level MCP rule', () => {
    assert.deepEqual(validatePermissionRule('mcp__github'), { valid: true });
  });

  it('accepts a wildcard MCP rule', () => {
    assert.deepEqual(validatePermissionRule('mcp__github__*'), { valid: true });
  });

  it('accepts a tool-level MCP rule', () => {
    assert.deepEqual(
      validatePermissionRule('mcp__github__create_issue'),
      { valid: true },
    );
  });

  it('rejects an MCP rule that carries a parenthesized pattern', () => {
    const r = validatePermissionRule('mcp__github(create_issue)');
    assert.equal(r.valid, false);
    assert.match(r.error!, /MCP rules do not support patterns/);
    assert.ok(r.examples!.includes('mcp__github'));
    assert.ok(r.examples!.includes('mcp__github__*'));
  });

  it('rejects an MCP rule whose parens normalize away to a wildcard', () => {
    // "mcp__github(*)" parses to a pattern-less tool name, but the raw string
    // still carries parentheses — Claude checks the raw '(' count for exactly
    // this case so the wildcard-in-parens form is still rejected.
    const r = validatePermissionRule('mcp__github(*)');
    assert.equal(r.valid, false);
    assert.match(r.error!, /MCP rules do not support patterns/);
  });

  it('suggests the tool-level form when a tool segment is present', () => {
    const r = validatePermissionRule('mcp__github__create_issue(x)');
    assert.equal(r.valid, false);
    assert.ok(r.examples!.includes('mcp__github__create_issue'));
  });

  it('still rejects mismatched parentheses before MCP parsing', () => {
    const r = validatePermissionRule('mcp__github(create');
    assert.equal(r.valid, false);
    assert.equal(r.error, 'Mismatched parentheses');
  });
});

// ---------------------------------------------------------------------------
// U003/w88 — argLeaksValue gate (gap 52)
//
// A user allow rule asserts the COMMAND is safe, not that arbitrary variable /
// command-substitution expansion THROUGH it is safe. `echo $ANTHROPIC_API_KEY`
// exfiltrates a secret and `cp $(curl …) .` runs arbitrary code — neither is
// covered by Bash(echo:*) / Bash(cp:*). Mirrors Claude's argLeaksValue
// (PowerShell readOnlyValidation.ts) ported to bash expansion syntax.
// ---------------------------------------------------------------------------
describe('bashCommandArgLeaksValue', () => {
  it('flags a non-safe environment-variable expansion in an argument', () => {
    assert.equal(bashCommandArgLeaksValue('echo $ANTHROPIC_API_KEY'), true);
    assert.equal(bashCommandArgLeaksValue('echo ${SECRET}'), true);
    assert.equal(bashCommandArgLeaksValue('cat $HOME/.netrc'), true);
  });

  it('flags a PowerShell-style $env: expansion in an argument', () => {
    assert.equal(bashCommandArgLeaksValue('echo $env:ANTHROPIC_API_KEY'), true);
  });

  it('does NOT flag a command substitution (handled by deny-decomposition, not this gate)', () => {
    // AMI intentionally keeps a benign `$(…)` under an allow rule and relies on
    // the deny/ask substitution-decomposition to elevate a destructive inner.
    assert.equal(bashCommandArgLeaksValue('echo $(whoami)'), false);
    assert.equal(bashCommandArgLeaksValue('echo `whoami`'), false);
  });

  it('does NOT flag a vetted SAFE_ENV_VARS expansion', () => {
    assert.ok(SAFE_ENV_VARS.has('NODE_ENV'));
    assert.equal(bashCommandArgLeaksValue('echo $NODE_ENV'), false);
    assert.equal(bashCommandArgLeaksValue('echo $LANG'), false);
  });

  it('does NOT flag a plain command with literal arguments', () => {
    assert.equal(bashCommandArgLeaksValue('echo hello world'), false);
    assert.equal(bashCommandArgLeaksValue('ls -la /tmp'), false);
  });

  it('does NOT flag arithmetic expansion $((…))', () => {
    assert.equal(bashCommandArgLeaksValue('echo $((1 + 2))'), false);
  });

  it('does NOT flag positional / special params', () => {
    assert.equal(bashCommandArgLeaksValue('echo $1 $@ $?'), false);
  });

  it('treats single-quoted spans as inert (no expansion in bash)', () => {
    assert.equal(bashCommandArgLeaksValue("echo '$SECRET'"), false);
    // Double quotes DO expand, so the secret still leaks.
    assert.equal(bashCommandArgLeaksValue('echo "$SECRET"'), true);
  });

  it('inspects every segment of a compound command', () => {
    assert.equal(bashCommandArgLeaksValue('ls && echo $SECRET'), true);
  });
});

describe('PermissionManager.check — argLeaksValue gate', () => {
  it('an allow rule does NOT auto-approve a command that expands a secret', async () => {
    const pm = new PermissionManager('ask');
    pm.addRule({ tool: 'bash', pattern: 'echo *', action: 'allow' });
    // literal args: the allow rule applies
    assert.equal(await pm.check('bash', { command: 'echo hello' }), 'allow');
    // secret expansion: the allow rule must NOT silently auto-approve
    assert.equal(
      await pm.check('bash', { command: 'echo $ANTHROPIC_API_KEY' }),
      'ask',
    );
    // a vetted safe env var is still auto-approved
    assert.equal(await pm.check('bash', { command: 'echo $NODE_ENV' }), 'allow');
    // a benign command substitution stays allowed (AMI design: deny-decomposition
    // handles destructive inners; this gate only catches variable leaks)
    assert.equal(await pm.check('bash', { command: 'echo $(date)' }), 'allow');
  });

  it('an explicit deny still wins over the arg-leak gate', async () => {
    const pm = new PermissionManager('ask');
    pm.addRule({ tool: 'bash', pattern: 'echo *', action: 'deny' });
    assert.equal(
      await pm.check('bash', { command: 'echo $ANTHROPIC_API_KEY' }),
      'deny',
    );
  });

  it('attributes the ask verdict to an argLeak safety check', async () => {
    const pm = new PermissionManager('ask');
    pm.addRule({ tool: 'bash', pattern: 'echo *', action: 'allow' });
    const res = await pm.checkWithReason('bash', {
      command: 'echo $ANTHROPIC_API_KEY',
    });
    assert.equal(res.decision, 'ask');
    assert.equal(res.reason?.type, 'safetyCheck');
    assert.equal(
      (res.reason as { check: string }).check,
      'argLeak',
    );
  });
});
