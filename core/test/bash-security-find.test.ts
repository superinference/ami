import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { detectDangerousFindArgs } from '../src/tools/bash-security';

describe('detectDangerousFindArgs', () => {
  // --- Safe commands (should return false) ---

  it('returns false for find with -name only', () => {
    assert.equal(detectDangerousFindArgs('find . -name "*.ts"'), false);
  });

  it('returns false for find with -type and -print', () => {
    assert.equal(detectDangerousFindArgs('find . -type f -print'), false);
  });

  it('returns false for find with -maxdepth and -name', () => {
    assert.equal(detectDangerousFindArgs('find /tmp -maxdepth 2 -name "*.log"'), false);
  });

  it('returns false for find with no action args', () => {
    assert.equal(detectDangerousFindArgs('find .'), false);
  });

  it('returns false for find with -print0', () => {
    assert.equal(detectDangerousFindArgs('find . -name "*.txt" -print0'), false);
  });

  it('returns false for find with -ls', () => {
    assert.equal(detectDangerousFindArgs('find . -type d -ls'), false);
  });

  it('returns false for non-find commands', () => {
    assert.equal(detectDangerousFindArgs('ls -la'), false);
  });

  it('returns false for empty string', () => {
    assert.equal(detectDangerousFindArgs(''), false);
  });

  it('returns false for commands mentioning "find" in a path', () => {
    assert.equal(detectDangerousFindArgs('cat /usr/share/findutils/README'), false);
  });

  // --- Dangerous commands (should return true) ---

  it('detects find -exec rm -rf', () => {
    assert.equal(detectDangerousFindArgs('find / -exec rm -rf {} \\;'), true);
  });

  it('detects find -exec with cat (any -exec is dangerous)', () => {
    assert.equal(detectDangerousFindArgs('find . -exec cat {} \\;'), true);
  });

  it('detects find -execdir', () => {
    assert.equal(detectDangerousFindArgs('find . -execdir chmod 777 {} +'), true);
  });

  it('detects find -delete', () => {
    assert.equal(detectDangerousFindArgs('find . -delete'), true);
  });

  it('detects find -ok (interactive exec)', () => {
    assert.equal(detectDangerousFindArgs('find . -ok rm {} \\;'), true);
  });

  it('detects find -name combined with -delete', () => {
    assert.equal(detectDangerousFindArgs('find . -name "*.log" -delete'), true);
  });

  it('detects find with complex args and -exec', () => {
    assert.equal(detectDangerousFindArgs('find /var/log -type f -mtime +30 -exec rm {} \\;'), true);
  });

  it('detects find -exec with + terminator', () => {
    assert.equal(detectDangerousFindArgs('find . -name "*.o" -exec rm {} +'), true);
  });

  it('detects find with -exec in a pipeline', () => {
    assert.equal(detectDangerousFindArgs('find . -type f | xargs -exec grep foo'), false);
    assert.equal(detectDangerousFindArgs('find . -type f -exec grep foo {} \\;'), true);
  });

  it('detects -delete at end of complex find', () => {
    assert.equal(detectDangerousFindArgs('find /tmp -maxdepth 1 -type f -name "*.tmp" -mtime +7 -delete'), true);
  });

  it('detects find -execdir with full path command', () => {
    assert.equal(detectDangerousFindArgs('find . -execdir /bin/rm {} \\;'), true);
  });
});
