import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { detectPersistenceCommands, validateBashSecurity } from '../src/tools/bash-security';

// detectPersistenceCommands already blocks cron/at/systemctl/schtasks. The
// non-systemd init managers (update-rc.d / chkconfig / rc-update), systemd
// transient units (systemd-run) and user-service lingering (loginctl
// enable-linger) are the same boot/persistence vector and are now also blocked.
describe('detectPersistenceCommands — init-service & systemd persistence (U015)', () => {
  const blocked = [
    'update-rc.d evil defaults',
    'chkconfig evil on',
    'rc-update add evil default',
    'systemd-run --user --unit=evil cmd',
    'systemd-run --on-calendar=daily /evil',
    'loginctl enable-linger user',
  ];
  for (const cmd of blocked) {
    it(`blocks: ${cmd}`, () => {
      assert.equal(detectPersistenceCommands(cmd).blocked, true);
      assert.equal(validateBashSecurity(cmd).checkId, 42); // PERSISTENCE_COMMANDS
    });
  }

  const allowed = [
    'update-rc.d -f evil remove',
    'chkconfig --list',
    'chkconfig evil off',
    'rc-update del evil',
    'rc-update show',
    'loginctl disable-linger user',
    'loginctl show-user user',
  ];
  for (const cmd of allowed) {
    it(`allows (cleanup/read-only): ${cmd}`, () => {
      assert.equal(detectPersistenceCommands(cmd).blocked, false);
    });
  }

  it('does not regress the systemctl-enable guard', () => {
    assert.equal(detectPersistenceCommands('systemctl enable evil').blocked, true);
  });
});
