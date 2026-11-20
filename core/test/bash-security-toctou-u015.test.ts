import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  detectArchiveExtractionWithGit,
  detectSymlinkCreateCompound,
} from '../src/tools/bash-security.ts';

// ===========================================================================
// UNIT U015 — compound-command TOCTOU detectors in bash-security.ts
//
// Parity with the reference's GIT_SAFETY_ARCHIVE_EXTRACTORS + hasSymlinkCreate
// gates (PowerShellTool/gitSafety.ts, powershellPermissions.ts). Both detectors
// are pure string classifiers; they return { escalate } so the permission layer
// can force an `ask` (even under a matching allow rule) for a shape whose
// runtime effect the auto-allow paths cannot see.
// ===========================================================================

describe('detectArchiveExtractionWithGit', () => {
  it('escalates when an archive extractor is chained with git', () => {
    for (const cmd of [
      'tar -xf payload.tar && git status',
      'bsdtar -xf payload.tar && git status',
      'unzip pkg.zip && git log',
      '7z x a.7z; git diff',
      '7za x a.7z; git diff',
      'gzip -d a.gz && git status',
      'gunzip a.gz && git status',
      'git fetch && tar -xf late.tar', // order-independent (opaque contents)
    ]) {
      const r = detectArchiveExtractionWithGit(cmd);
      assert.equal(r.escalate, true, `expected escalate for: ${cmd}`);
      assert.match(r.reason!, /archive/);
    }
  });

  it('does not escalate without BOTH an extractor and git', () => {
    for (const cmd of [
      'tar -cf out.tar dir',        // archive but no git
      'tar -xf payload.tar',        // extract but no git
      'git status',                 // git but no archive
      'echo hi && git status',      // no archive
      'ls && cat file',             // neither
    ]) {
      assert.equal(detectArchiveExtractionWithGit(cmd).escalate, false, `unexpected escalate for: ${cmd}`);
    }
  });

  it('does not treat an archive-tool NAME used as an argument as an extractor', () => {
    // `git` appears but the only "tar" is an argument to echo, not a base command.
    assert.equal(detectArchiveExtractionWithGit('echo tar && git status').escalate, false);
  });
});

describe('detectSymlinkCreateCompound', () => {
  it('escalates when a compound creates a filesystem link', () => {
    for (const cmd of [
      'ln -s /etc/passwd link && cat link',
      'ln target hardlink && echo done',
      'echo start; ln -sf a b',
      'mkdir d && ln -s ../secret d/x',
    ]) {
      const r = detectSymlinkCreateCompound(cmd);
      assert.equal(r.escalate, true, `expected escalate for: ${cmd}`);
      assert.match(r.reason!, /link/);
    }
  });

  it('does not escalate a standalone link creation (no later statement)', () => {
    assert.equal(detectSymlinkCreateCompound('ln -s a b').escalate, false);
    assert.equal(detectSymlinkCreateCompound('ln target hardlink').escalate, false);
  });

  it('does not escalate a compound without a link creator', () => {
    assert.equal(detectSymlinkCreateCompound('cat a && cat b').escalate, false);
    assert.equal(detectSymlinkCreateCompound('echo ln && cat file').escalate, false);
  });
});
