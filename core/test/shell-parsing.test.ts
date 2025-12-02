import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { PermissionManager } from '../src/permissions';

describe('Shell command parsing and classification', () => {
  const pm = new PermissionManager('ask');

  describe('basic commands', () => {
    it('classifies ls as safe', () => {
      assert.equal(pm.classifyBashCommand('ls -la'), 'safe');
    });
    it('classifies cat as safe', () => {
      assert.equal(pm.classifyBashCommand('cat file.txt'), 'safe');
    });
    it('classifies git status as safe', () => {
      assert.equal(pm.classifyBashCommand('git status'), 'safe');
    });
    it('classifies rm as destructive', () => {
      assert.equal(pm.classifyBashCommand('rm file.txt'), 'destructive');
    });
    it('classifies rm -rf as destructive', () => {
      assert.equal(pm.classifyBashCommand('rm -rf dir/'), 'destructive');
    });
  });

  describe('command wrappers', () => {
    it('detects sudo rm as destructive', () => {
      assert.equal(pm.classifyBashCommand('sudo rm important.txt'), 'destructive');
    });
    it('detects env rm as destructive', () => {
      assert.equal(pm.classifyBashCommand('env rm -rf /tmp/test'), 'destructive');
    });
    it('detects sudo as unsafe', () => {
      assert.equal(pm.classifyBashCommand('sudo apt update'), 'unsafe');
    });
  });

  describe('bash -c wrapper', () => {
    it('detects bash -c "rm file" as destructive', () => {
      assert.equal(pm.classifyBashCommand('bash -c "rm file.txt"'), 'destructive');
    });
    it('detects sh -c "rm -rf /" as destructive', () => {
      assert.equal(pm.classifyBashCommand('sh -c "rm -rf /"'), 'destructive');
    });
  });

  describe('pipes and chains', () => {
    it('detects rm in pipe chain', () => {
      assert.equal(pm.classifyBashCommand('ls | xargs rm'), 'destructive');
    });
    it('detects rm after &&', () => {
      assert.equal(pm.classifyBashCommand('echo hello && rm file.txt'), 'destructive');
    });
    it('classifies safe pipe chain as safe', () => {
      assert.equal(pm.classifyBashCommand('cat file | grep pattern | sort'), 'safe');
    });
  });

  describe('command substitution', () => {
    it('detects rm in $(...)', () => {
      assert.equal(pm.classifyBashCommand('echo $(rm important.txt)'), 'destructive');
    });
    it('detects rm in backticks', () => {
      assert.equal(pm.classifyBashCommand('echo `rm file.txt`'), 'destructive');
    });
  });

  describe('redirects to system paths', () => {
    it('detects redirect to /etc/', () => {
      assert.equal(pm.classifyBashCommand('echo pwned > /etc/passwd'), 'destructive');
    });
    it('detects redirect to /usr/', () => {
      assert.equal(pm.classifyBashCommand('cat x > /usr/bin/malware'), 'destructive');
    });
  });

  describe('env var prefix skip', () => {
    it('skips env vars before command', () => {
      assert.equal(pm.classifyBashCommand('VAR=value ls'), 'safe');
    });
    it('skips multiple env vars', () => {
      assert.equal(pm.classifyBashCommand('A=1 B=2 git status'), 'safe');
    });
  });

  describe('SQL injection detection', () => {
    it('detects DROP TABLE', () => {
      assert.equal(pm.classifyBashCommand('psql -c "DROP TABLE users"'), 'destructive');
    });
    it('detects DELETE FROM', () => {
      assert.equal(pm.classifyBashCommand('mysql -e "DELETE FROM orders"'), 'destructive');
    });
  });

  describe('path safety', () => {
    it('blocks system directories', () => {
      assert.ok(!pm.isPathAllowed('/etc/passwd', '/home/user/project'));
    });
    it('blocks /usr paths', () => {
      assert.ok(!pm.isPathAllowed('/usr/bin/node', '/home/user/project'));
    });
    it('allows paths within cwd', () => {
      assert.ok(pm.isPathAllowed('/home/user/project/src/file.ts', '/home/user/project'));
    });
    it('allows relative paths within cwd', () => {
      assert.ok(pm.isPathAllowed('src/file.ts', '/home/user/project'));
    });
  });
});
