import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { detectPrivilegeEscalation, validateBashSecurity } from '../src/tools/bash-security';

// detectPrivilegeEscalation previously blocked ONLY `sudo`, letting modern/
// alternative privilege-escalation front-ends through. Claude Code's
// privilege-escalation prefix denylist (BashTool/bashPermissions.ts) lists
// sudo + doas + pkexec; run0 (systemd's sudo successor) and su are the same
// root-escalation vector and are additionally blocked (superset hardening).
describe('detectPrivilegeEscalation — sudo alternatives (U015)', () => {
  for (const tool of ['doas', 'pkexec', 'run0', 'su']) {
    it(`blocks ${tool}`, () => {
      const r = detectPrivilegeEscalation(`${tool} whoami`);
      assert.equal(r.blocked, true);
      assert.ok(r.reason!.includes(tool));
    });
  }

  it('still blocks sudo (no regression)', () => {
    const r = detectPrivilegeEscalation('sudo apt-get install pkg');
    assert.equal(r.blocked, true);
    assert.ok(r.reason!.includes('sudo'));
  });

  it('blocks an escalation front-end after a separator', () => {
    assert.equal(detectPrivilegeEscalation('cd /tmp && doas rm x').blocked, true);
    assert.equal(detectPrivilegeEscalation('( pkexec sh )').blocked, true);
    assert.equal(detectPrivilegeEscalation('su - root').blocked, true);
  });

  it('does not false-positive on words that merely contain the tokens', () => {
    assert.equal(detectPrivilegeEscalation('cat issue').blocked, false); // "su" in issue
    assert.equal(detectPrivilegeEscalation('echo pseudonym').blocked, false); // "su" in pseudonym
    assert.equal(detectPrivilegeEscalation('ls dnsutils').blocked, false);
    assert.equal(detectPrivilegeEscalation('cp a.su b.su').blocked, false); // "su" before '.'
  });

  it('hard-blocks the escalation through the full validator (checkId 45)', () => {
    const r = validateBashSecurity('doas cat /etc/shadow');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, 45); // CHECK_IDS.PRIVILEGE_ESCALATION
  });
});
