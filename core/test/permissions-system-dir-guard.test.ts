import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { commandTargetsSystemDir } from '../src/tools/bash-security';
import { PermissionManager } from '../src/permissions';

// ---------------------------------------------------------------------------
// commandTargetsSystemDir — unit tests
// ---------------------------------------------------------------------------

describe('commandTargetsSystemDir', () => {
  it('detects cp to /etc', () => {
    assert.ok(commandTargetsSystemDir('cp file /etc/passwd'));
  });

  it('detects mv to /usr/bin', () => {
    assert.ok(commandTargetsSystemDir('mv file /usr/bin/something'));
  });

  it('detects tee to /etc/cron.d', () => {
    assert.ok(commandTargetsSystemDir('tee /etc/cron.d/job'));
  });

  it('detects dd writing to /dev/sda', () => {
    assert.ok(commandTargetsSystemDir('dd if=/dev/zero of=/dev/sda bs=512 count=1'));
  });

  it('detects ln -s into /usr/local/bin', () => {
    assert.ok(commandTargetsSystemDir('ln -s /tmp/malicious /usr/local/bin/real'));
  });

  it('detects install to /usr/share', () => {
    assert.ok(commandTargetsSystemDir('install -m 755 binary /usr/share/bin/tool'));
  });

  it('detects rm in /etc', () => {
    assert.ok(commandTargetsSystemDir('rm /etc/hosts'));
  });

  it('detects rm -rf in /usr', () => {
    assert.ok(commandTargetsSystemDir('rm -rf /usr/local/share'));
  });

  it('detects cp to /boot', () => {
    assert.ok(commandTargetsSystemDir('cp vmlinuz /boot/vmlinuz-new'));
  });

  it('detects redirect to /etc file', () => {
    assert.ok(commandTargetsSystemDir('echo "bad" > /etc/resolv.conf'));
  });

  it('does NOT flag reading from system dirs', () => {
    assert.ok(!commandTargetsSystemDir('cat /etc/hosts'));
  });

  it('does NOT flag ls in system dirs', () => {
    assert.ok(!commandTargetsSystemDir('ls /usr/bin'));
  });

  it('does NOT flag commands targeting user directories', () => {
    assert.ok(!commandTargetsSystemDir('cp file /home/user/backup'));
  });

  it('does NOT flag commands targeting /tmp', () => {
    assert.ok(!commandTargetsSystemDir('cp file /tmp/test'));
  });

  it('does NOT flag commands with no write intent', () => {
    assert.ok(!commandTargetsSystemDir('grep -r pattern /etc/'));
  });

  it('does NOT flag commands with no paths', () => {
    assert.ok(!commandTargetsSystemDir('echo hello'));
  });
});

// ---------------------------------------------------------------------------
// PermissionManager — system dir guard integration
// ---------------------------------------------------------------------------

describe('PermissionManager system dir guard (ask mode)', () => {
  it('returns ask for cp to /etc even with wildcard allow rule', async () => {
    const pm = new PermissionManager('ask', [
      { tool: '*', action: 'allow' },
    ]);
    const result = await pm.check('bash', { command: 'cp file /etc/passwd' });
    assert.equal(result, 'ask');
  });

  it('returns ask for mv to /usr/bin even with wildcard allow', async () => {
    const pm = new PermissionManager('ask', [
      { tool: '*', action: 'allow' },
    ]);
    const result = await pm.check('bash', { command: 'mv file /usr/bin/something' });
    assert.equal(result, 'ask');
  });

  it('returns ask for tee to /etc/cron.d', async () => {
    const pm = new PermissionManager('ask', [
      { tool: 'bash', action: 'allow' },
    ]);
    const result = await pm.check('bash', { command: 'tee /etc/cron.d/job' });
    assert.equal(result, 'ask');
  });

  it('returns ask for ln -s into /usr/local/bin', async () => {
    const pm = new PermissionManager('ask', [
      { tool: '*', action: 'allow' },
    ]);
    const result = await pm.check('bash', { command: 'ln -s /tmp/mal /usr/local/bin/real' });
    assert.equal(result, 'ask');
  });

  it('allows reading from system dirs (cat /etc/hosts is safe)', async () => {
    const pm = new PermissionManager('ask', [
      { tool: '*', action: 'allow' },
    ]);
    const result = await pm.check('bash', { command: 'cat /etc/hosts' });
    assert.equal(result, 'allow');
  });

  it('allows commands to user directories', async () => {
    const pm = new PermissionManager('ask', [
      { tool: '*', action: 'allow' },
    ]);
    const result = await pm.check('bash', { command: 'cp file /home/user/backup' });
    assert.equal(result, 'allow');
  });
});

describe('PermissionManager system dir guard (acceptEdits mode)', () => {
  it('returns ask for cp to /etc in acceptEdits mode', async () => {
    const pm = new PermissionManager('acceptEdits', [
      { tool: '*', action: 'allow' },
    ]);
    const result = await pm.check('bash', { command: 'cp file /etc/passwd' });
    assert.equal(result, 'ask');
  });

  it('returns deny for rm -rf /usr in acceptEdits mode (hardline)', async () => {
    const pm = new PermissionManager('acceptEdits', [
      { tool: '*', action: 'allow' },
    ]);
    // rm -rf to system dirs is caught by hardline detector → deny
    const result = await pm.check('bash', { command: 'rm -rf /usr/local/share' });
    assert.equal(result, 'deny');
  });

  it('returns ask for cp to /usr in acceptEdits mode (non-hardline write)', async () => {
    const pm = new PermissionManager('acceptEdits', [
      { tool: '*', action: 'allow' },
    ]);
    const result = await pm.check('bash', { command: 'cp file /usr/local/share/app' });
    assert.equal(result, 'ask');
  });

  it('allows safe bash commands in acceptEdits mode', async () => {
    const pm = new PermissionManager('acceptEdits');
    const result = await pm.check('bash', { command: 'ls /tmp' });
    assert.equal(result, 'allow');
  });
});

describe('PermissionManager system dir guard — hardline precedence', () => {
  it('hardline denial still takes precedence over system dir guard', async () => {
    const pm = new PermissionManager('ask', [
      { tool: '*', action: 'allow' },
    ]);
    // rm -rf / is hardline-blocked, system dir guard is irrelevant
    const result = await pm.check('bash', { command: 'rm -rf /' });
    assert.equal(result, 'deny');
  });

  it('auto-allow mode bypasses system dir guard (as designed)', async () => {
    const pm = new PermissionManager('auto-allow');
    const result = await pm.check('bash', { command: 'cp file /etc/passwd' });
    assert.equal(result, 'allow');
  });
});
