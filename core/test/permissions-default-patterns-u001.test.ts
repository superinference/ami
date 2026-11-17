/**
 * U001 — getDefaultPermissionPatterns(): the serializable snapshot of the
 * built-in classifier patterns surfaced by `/permissions defaults|effective`.
 * Parity: Claude Code's `auto-mode defaults` dump.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { getDefaultPermissionPatterns } from '../src/permissions';

describe('getDefaultPermissionPatterns', () => {
  it('returns the three canonical pattern groups, serializable + sorted', () => {
    const p = getDefaultPermissionPatterns();
    // Auto-allow safe commands: non-empty, string[], sorted.
    assert.ok(Array.isArray(p.autoAllowBashCommands) && p.autoAllowBashCommands.length > 0);
    assert.ok(p.autoAllowBashCommands.every(c => typeof c === 'string'));
    assert.deepEqual(p.autoAllowBashCommands, [...p.autoAllowBashCommands].sort());
    assert.ok(p.autoAllowBashCommands.includes('ls'));

    // Hardline-deny: {pattern(string), description(string)} rows, including rm-on-root.
    assert.ok(Array.isArray(p.hardlineDenied) && p.hardlineDenied.length > 0);
    assert.ok(p.hardlineDenied.every(h => typeof h.pattern === 'string' && typeof h.description === 'string'));
    assert.ok(p.hardlineDenied.some(h => /filesystem root/i.test(h.description)));

    // Destructive commands + SQL patterns.
    assert.ok(p.destructiveCommands.includes('rm'));
    assert.deepEqual(p.destructiveCommands, [...p.destructiveCommands].sort());
    assert.ok(Array.isArray(p.destructiveSqlPatterns) && p.destructiveSqlPatterns.some(s => /DROP/i.test(s)));
  });

  it('is fully JSON-serializable (no RegExp/Set leakage)', () => {
    const p = getDefaultPermissionPatterns();
    const round = JSON.parse(JSON.stringify(p));
    assert.deepEqual(round, p);
  });
});
