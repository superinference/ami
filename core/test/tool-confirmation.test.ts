import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { ToolConfirmationService } from '../src/tool-confirmation';

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
});
