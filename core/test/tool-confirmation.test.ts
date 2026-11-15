import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';
import {
  ToolConfirmationService,
  DEFAULT_MAX_APPROVALS,
} from '../src/tool-confirmation';

describe('ToolConfirmationService', () => {
  it('isApproved returns false before any approval', () => {
    const svc = new ToolConfirmationService();
    assert.equal(svc.isApproved('bash', { command: 'ls' }), false);
  });

  it('session-scoped approval is recognized', () => {
    const svc = new ToolConfirmationService();
    svc.approve('bash', { command: 'ls' }, 'session');
    assert.equal(svc.isApproved('bash', { command: 'ls' }), true);
  });

  it('workspace-scoped approval is recognized', () => {
    const svc = new ToolConfirmationService();
    svc.approve('file_write', null, 'workspace');
    assert.equal(svc.isApproved('file_write', { file_path: '/tmp/x' }), true);
  });

  it('tool-wide approval covers all params', () => {
    const svc = new ToolConfirmationService();
    svc.approve('bash', null, 'session');
    assert.equal(svc.isApproved('bash', { command: 'rm -rf /' }), true);
    assert.equal(svc.isApproved('bash', { command: 'echo hi' }), true);
  });

  it('param-specific approval does not cover different params', () => {
    const svc = new ToolConfirmationService();
    svc.approve('bash', { command: 'ls' }, 'session');
    assert.equal(svc.isApproved('bash', { command: 'rm -rf /' }), false);
  });

  it('different tool names are independent', () => {
    const svc = new ToolConfirmationService();
    svc.approve('bash', null, 'session');
    assert.equal(svc.isApproved('file_write', null), false);
  });

  it('clearSession removes once and session approvals but keeps workspace', () => {
    const svc = new ToolConfirmationService();
    svc.approve('bash', null, 'session');
    svc.approve('file_write', null, 'workspace');
    svc.clearSession();
    assert.equal(svc.isApproved('bash', null), false);
    assert.equal(svc.isApproved('file_write', null), true);
  });

  it('params order does not affect hash', () => {
    const svc = new ToolConfirmationService();
    svc.approve('bash', { a: '1', b: '2' }, 'session');
    assert.equal(svc.isApproved('bash', { b: '2', a: '1' }), true);
  });

  it('once-scoped approval is recognized by isApproved', () => {
    const svc = new ToolConfirmationService();
    svc.approve('bash', { command: 'ls' }, 'once');
    assert.equal(svc.isApproved('bash', { command: 'ls' }), true);
  });

  it('once-scoped approval is cleared by clearSession', () => {
    const svc = new ToolConfirmationService();
    svc.approve('bash', { command: 'ls' }, 'once');
    svc.clearSession();
    assert.equal(svc.isApproved('bash', { command: 'ls' }), false);
  });

  it('tool-wide once approval is consumed on first matching call', () => {
    const svc = new ToolConfirmationService();
    svc.approve('bash', null, 'once');
    assert.equal(svc.isApproved('bash', { command: 'ls' }), true);
    // consumed — the next call is no longer approved
    assert.equal(svc.isApproved('bash', { command: 'ls' }), false);
  });
});

describe('ToolConfirmationService — deny records with precedence', () => {
  it('narrow deny overrides a broad tool-wide allow', () => {
    const svc = new ToolConfirmationService();
    svc.approve('bash', null, 'session'); // allow all bash
    svc.deny('bash', { command: 'rm -rf /' }, 'session'); // but deny this one
    assert.equal(svc.isApproved('bash', { command: 'rm -rf /' }), false);
    assert.equal(svc.isApproved('bash', { command: 'echo hi' }), true);
  });

  it('tool-wide deny blocks every param', () => {
    const svc = new ToolConfirmationService();
    svc.approve('bash', { command: 'ls' }, 'session');
    svc.deny('bash', null, 'session');
    assert.equal(svc.isApproved('bash', { command: 'ls' }), false);
    assert.equal(svc.isApproved('bash', { command: 'pwd' }), false);
  });

  it('deny-once is consumed after a single block', () => {
    const svc = new ToolConfirmationService();
    svc.approve('bash', null, 'session');
    svc.deny('bash', { command: 'ls' }, 'once');
    assert.equal(svc.isApproved('bash', { command: 'ls' }), false); // blocked once
    assert.equal(svc.isApproved('bash', { command: 'ls' }), true); // deny consumed, allow wins
  });

  it('clearSession removes session denies but keeps workspace denies', () => {
    const svc = new ToolConfirmationService();
    svc.approve('bash', null, 'workspace');
    svc.deny('bash', { command: 'ls' }, 'session');
    svc.deny('bash', { command: 'pwd' }, 'workspace');
    svc.clearSession();
    assert.equal(svc.isApproved('bash', { command: 'ls' }), true); // session deny gone
    assert.equal(svc.isApproved('bash', { command: 'pwd' }), false); // workspace deny kept
  });
});

describe('ToolConfirmationService — scoped (content-based) approvals', () => {
  it('prefix pattern approves a command family without approving the whole tool', () => {
    const svc = new ToolConfirmationService();
    svc.approvePattern('bash', { param: 'command', mode: 'prefix', value: 'npm ' }, 'session');
    assert.equal(svc.isApproved('bash', { command: 'npm test' }), true);
    assert.equal(svc.isApproved('bash', { command: 'npm run build' }), true);
    assert.equal(svc.isApproved('bash', { command: 'rm -rf /' }), false);
  });

  it('wildcard pattern matches via the canonical glob matcher', () => {
    const svc = new ToolConfirmationService();
    svc.approvePattern('bash', { param: 'command', mode: 'wildcard', value: 'git *' }, 'workspace');
    assert.equal(svc.isApproved('bash', { command: 'git status' }), true);
    assert.equal(svc.isApproved('bash', { command: 'npm test' }), false);
  });

  it('exact pattern matches only the literal value', () => {
    const svc = new ToolConfirmationService();
    svc.approvePattern('bash', { param: 'command', mode: 'exact', value: 'ls' }, 'session');
    assert.equal(svc.isApproved('bash', { command: 'ls' }), true);
    assert.equal(svc.isApproved('bash', { command: 'ls -la' }), false);
  });

  it('pattern does not match when params are null or the field is not a string', () => {
    const svc = new ToolConfirmationService();
    svc.approvePattern('bash', { param: 'command', mode: 'prefix', value: 'npm' }, 'session');
    assert.equal(svc.isApproved('bash', null), false);
    assert.equal(svc.isApproved('bash', { command: 123 as unknown as string }), false);
    assert.equal(svc.isApproved('bash', { other: 'npm test' }), false);
  });

  it('pattern deny overrides a tool-wide allow for the matching family', () => {
    const svc = new ToolConfirmationService();
    svc.approve('bash', null, 'session');
    svc.approvePattern('bash', { param: 'command', mode: 'prefix', value: 'rm ' }, 'session', 'deny');
    assert.equal(svc.isApproved('bash', { command: 'rm -rf /' }), false);
    assert.equal(svc.isApproved('bash', { command: 'ls' }), true);
  });

  it('identical pattern approvals are de-duplicated', () => {
    const svc = new ToolConfirmationService();
    svc.approvePattern('bash', { param: 'command', mode: 'prefix', value: 'npm ' }, 'session');
    svc.approvePattern('bash', { param: 'command', mode: 'prefix', value: 'npm ' }, 'session');
    assert.equal(svc.size(), 1);
  });
});

describe('ToolConfirmationService — TTL / expiry', () => {
  it('honors a fresh approval but expires an old one under maxAgeMs', async () => {
    const svc = new ToolConfirmationService();
    svc.approve('bash', { command: 'ls' }, 'session');
    assert.equal(svc.isApproved('bash', { command: 'ls' }, { maxAgeMs: 60_000 }), true);
    await sleep(12);
    assert.equal(svc.isApproved('bash', { command: 'ls' }, { maxAgeMs: 1 }), false);
    // expired record was evicted
    assert.equal(svc.size(), 0);
  });

  it('expires a stale deny so the underlying allow is honored again', async () => {
    const svc = new ToolConfirmationService();
    svc.deny('bash', { command: 'ls' }, 'session');
    await sleep(25);
    svc.approve('bash', null, 'session'); // fresh tool-wide allow
    // maxAgeMs expires the ~25ms-old deny but keeps the just-created allow
    assert.equal(svc.isApproved('bash', { command: 'ls' }, { maxAgeMs: 10 }), true);
  });

  it('expires a stale once-allow before it can be consumed', async () => {
    const svc = new ToolConfirmationService();
    svc.approve('bash', { command: 'ls' }, 'once');
    await sleep(12);
    assert.equal(svc.isApproved('bash', { command: 'ls' }, { maxAgeMs: 1 }), false);
    assert.equal(svc.size(), 0);
  });
});

describe('ToolConfirmationService — revoke / bounded cache', () => {
  it('revoke removes an abandoned once approval and reports the count', () => {
    const svc = new ToolConfirmationService();
    svc.approve('bash', { command: 'ls' }, 'once');
    assert.equal(svc.revoke('bash', { command: 'ls' }), 1);
    assert.equal(svc.isApproved('bash', { command: 'ls' }), false);
    assert.equal(svc.revoke('bash', { command: 'ls' }), 0); // nothing left
  });

  it('revoke can be limited to a single scope', () => {
    const svc = new ToolConfirmationService();
    svc.approve('bash', { command: 'ls' }, 'session');
    svc.approve('bash', { command: 'ls' }, 'workspace');
    assert.equal(svc.revoke('bash', { command: 'ls' }, 'session'), 1);
    assert.equal(svc.isApproved('bash', { command: 'ls' }), true); // workspace kept
  });

  it('revoke removes both allow and deny records for a param set', () => {
    const svc = new ToolConfirmationService();
    svc.deny('bash', { command: 'ls' }, 'session');
    svc.approve('bash', { command: 'ls' }, 'workspace');
    assert.equal(svc.revoke('bash', { command: 'ls' }), 2);
  });

  it('caps the cache size with oldest-first eviction', () => {
    const svc = new ToolConfirmationService(2);
    svc.approve('bash', { command: 'a' }, 'session');
    svc.approve('bash', { command: 'b' }, 'session');
    svc.approve('bash', { command: 'c' }, 'session');
    assert.equal(svc.size(), 2);
    assert.equal(svc.isApproved('bash', { command: 'a' }), false); // oldest evicted
    assert.equal(svc.isApproved('bash', { command: 'c' }), true);
  });

  it('a non-positive maxApprovals falls back to the default cap', () => {
    const svc = new ToolConfirmationService(0);
    assert.ok(DEFAULT_MAX_APPROVALS > 1);
    for (let i = 0; i < 5; i++) svc.approve('bash', { command: `c${i}` }, 'session');
    assert.equal(svc.size(), 5); // not capped at 0
  });
});
