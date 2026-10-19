import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  isCredentialPath,
  commandTargetsCredentialPath,
  detectCredentialPathWrite,
} from '../src/tools/bash-security';

describe('isCredentialPath', () => {
  const blocked = [
    ['~/.ssh/id_rsa', '.ssh/id_rsa'],
    ['~/.ssh/authorized_keys', '.ssh/authorized_keys'],
    ['~/.aws/credentials', '.aws/credentials'],
    ['~/.aws/config', '.aws/config'],
    ['gcloud creds', '.config/gcloud/credentials.json'],
    ['~/.gnupg/secring.gpg', '.gnupg/secring.gpg'],
    ['~/.docker/config.json', '.docker/config.json'],
    ['~/.kube/config', '.kube/config'],
    ['~/.netrc', '.netrc'],
    ['~/_netrc', '_netrc'],
    ['~/.git-credentials', '.git-credentials'],
    ['~/.config/git/credentials', '.config/git/credentials'],
    ['~/.config/gh/hosts.yml', '.config/gh/hosts.yml'],
    ['~/.config/glab-cli/config.yml', '.config/glab-cli/config.yml'],
    ['~/.npmrc', '.npmrc'],
    ['~/.claude/.credentials.json', '.claude/.credentials.json'],
    ['/etc/shadow', '/etc/shadow'],
    ['/etc/passwd', '/etc/passwd'],
    ['/etc/sudoers', '/etc/sudoers'],
  ];
  for (const [label, p] of blocked) {
    it(`blocks ${label}`, () => assert.ok(isCredentialPath(p)));
  }

  const allowed = [
    ['normal file', 'src/main.ts'],
    ['non-credential dir', '.config/something/file.txt'],
    ['package.json', 'package.json'],
    ['readme', 'README.md'],
    ['.env file', '.env'],
    ['.aws dir itself', '.aws'],
  ];
  for (const [label, p] of allowed) {
    it(`allows ${label}`, () => assert.ok(!isCredentialPath(p)));
  }
});

describe('commandTargetsCredentialPath', () => {
  it('detects cat ~/.ssh/id_rsa', () => {
    assert.ok(commandTargetsCredentialPath('cat ~/.ssh/id_rsa'));
  });
  it('detects head ~/.aws/credentials', () => {
    assert.ok(commandTargetsCredentialPath('head ~/.aws/credentials'));
  });
  it('detects less ~/.config/gh/hosts.yml', () => {
    assert.ok(commandTargetsCredentialPath('less ~/.config/gh/hosts.yml'));
  });
  it('detects grep in ~/.npmrc', () => {
    assert.ok(commandTargetsCredentialPath('grep token ~/.npmrc'));
  });
  it('detects cat ~/.netrc', () => {
    assert.ok(commandTargetsCredentialPath('cat ~/.netrc'));
  });
  it('detects cat ~/_netrc', () => {
    assert.ok(commandTargetsCredentialPath('cat ~/_netrc'));
  });
  it('detects cat ~/.config/git/credentials', () => {
    assert.ok(commandTargetsCredentialPath('cat ~/.config/git/credentials'));
  });
  it('detects cat ~/.claude/.credentials.json', () => {
    assert.ok(commandTargetsCredentialPath('cat ~/.claude/.credentials.json'));
  });
  it('detects cat ~/.config/glab-cli/config.yml', () => {
    assert.ok(commandTargetsCredentialPath('cat ~/.config/glab-cli/config.yml'));
  });
  it('detects grep -f ~/.ssh/id_rsa (flag takes file path)', () => {
    assert.ok(commandTargetsCredentialPath('grep -f ~/.ssh/id_rsa pattern .'));
  });
  it('detects grep --file=~/.aws/credentials', () => {
    assert.ok(commandTargetsCredentialPath('grep --file=~/.aws/credentials pattern .'));
  });
  it('detects sort -o ~/.npmrc (writes via flag)', () => {
    assert.ok(commandTargetsCredentialPath('sort -o ~/.npmrc input.txt'));
  });
  it('allows cat README.md', () => {
    assert.ok(!commandTargetsCredentialPath('cat README.md'));
  });
  it('allows ls -la', () => {
    assert.ok(!commandTargetsCredentialPath('ls -la'));
  });
  it('allows grep --color=auto (non-path option value)', () => {
    assert.ok(!commandTargetsCredentialPath('grep --color=auto pattern file.txt'));
  });
});

describe('detectCredentialPathWrite', () => {
  it('blocks cp to ~/.ssh/', () => {
    const r = detectCredentialPathWrite('cp key.pem ~/.ssh/id_rsa');
    assert.ok(r.blocked);
    assert.ok(r.description);
  });
  it('blocks redirect to ~/.aws/credentials', () => {
    const r = detectCredentialPathWrite('echo "[default]" > ~/.aws/credentials');
    assert.ok(r.blocked);
  });
  it('blocks tee to ~/.npmrc', () => {
    const r = detectCredentialPathWrite('echo "//registry:_authToken=x" | tee ~/.npmrc');
    assert.ok(r.blocked);
  });
  it('blocks mv to ~/.config/gh/', () => {
    const r = detectCredentialPathWrite('mv hosts.yml ~/.config/gh/hosts.yml');
    assert.ok(r.blocked);
  });
  it('blocks cp to ~/.config/git/credentials', () => {
    const r = detectCredentialPathWrite('cp /tmp/creds ~/.config/git/credentials');
    assert.ok(r.blocked);
  });
  it('blocks redirect to ~/.claude/.credentials.json', () => {
    const r = detectCredentialPathWrite('echo "{}" > ~/.claude/.credentials.json');
    assert.ok(r.blocked);
  });
  it('allows cp to a normal path', () => {
    const r = detectCredentialPathWrite('cp file.txt /tmp/backup.txt');
    assert.ok(!r.blocked);
  });
  it('allows echo to stdout', () => {
    const r = detectCredentialPathWrite('echo hello');
    assert.ok(!r.blocked);
  });
});
