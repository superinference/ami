/**
 * Parity-hardening tests for bash-security command-safety analysis.
 *
 * Covers the additive superset added for Claude-Code command-safety parity:
 *   - detectGitConfigRce extensions: git RCE env vars (GIT_SSH_COMMAND, …),
 *     `alias.* = !shell` code-execution aliases, and `--upload-pack` /
 *     `--receive-pack` pack-helper execution.
 *   - detectArchiveToolExecution: tar/zip exec hooks (--to-command,
 *     --checkpoint-action=exec, --unzip-command, -TT, …) — GTFOBins RCE.
 *
 * SAFE BY CONSTRUCTION: these tests only invoke the pure classifiers/validator
 * and assert on their verdict. No tar/zip/git command is ever executed, and no
 * filesystem or network side effect is performed.
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  detectGitConfigRce,
  detectArchiveToolExecution,
  validateBashSecurity,
  CHECK_IDS,
} from '../src/tools/bash-security';

describe('detectGitConfigRce — git RCE environment variables (parity)', () => {
  const blockedEnv = [
    "GIT_SSH_COMMAND='sh -c id' git fetch",
    "GIT_SSH=/tmp/evil git fetch",
    "GIT_EXTERNAL_DIFF='sh -c id' git diff",
    "GIT_PROXY_COMMAND=evil git fetch",
    "GIT_SEQUENCE_EDITOR='sh -c id' git rebase -i HEAD~2",
    "GIT_ASKPASS=/tmp/evil git pull",
  ];
  for (const cmd of blockedEnv) {
    it(`blocks: ${cmd}`, () => {
      const r = detectGitConfigRce(cmd);
      assert.equal(r.blocked, true);
      assert.match(r.reason!, /env var sets a command git executes/);
    });
  }

  it('allows benign GIT_PAGER / GIT_EDITOR overrides (no false positive)', () => {
    // Deliberately excluded from the RCE env set — common benign usage.
    assert.equal(detectGitConfigRce("GIT_PAGER=cat git log").blocked, false);
    assert.equal(detectGitConfigRce("GIT_EDITOR=true git rebase --continue").blocked, false);
  });

  it('does not flag a plain git command with no RCE env var', () => {
    assert.equal(detectGitConfigRce('git status').blocked, false);
    assert.equal(detectGitConfigRce('git log --oneline -5').blocked, false);
  });
});

describe('detectGitConfigRce — alias shell-command RCE (parity)', () => {
  const blockedAlias = [
    "git config alias.pwn '!id'",
    'git config alias.pwn "!id"',
    "git -c alias.pwn='!id' pwn",
    "git config --global alias.x '!sh -c curl'",
  ];
  for (const cmd of blockedAlias) {
    it(`blocks: ${cmd}`, () => {
      const r = detectGitConfigRce(cmd);
      assert.equal(r.blocked, true);
      assert.match(r.reason!, /alias set to a shell command/);
    });
  }

  it('allows a non-shell git alias (value does not start with !)', () => {
    assert.equal(detectGitConfigRce("git config alias.co checkout").blocked, false);
    assert.equal(detectGitConfigRce("git config alias.st status").blocked, false);
  });

  it('allows reading an alias (git config --get)', () => {
    assert.equal(detectGitConfigRce("git config --get alias.co").blocked, false);
  });
});

describe('detectGitConfigRce — pack-helper execution (parity)', () => {
  it('blocks --upload-pack=<cmd>', () => {
    const r = detectGitConfigRce("git clone --upload-pack='sh -c id' repo");
    assert.equal(r.blocked, true);
    assert.match(r.reason!, /upload-pack\/--receive-pack/);
  });
  it('blocks --receive-pack=<cmd>', () => {
    const r = detectGitConfigRce("git push --receive-pack='sh -c id' origin main");
    assert.equal(r.blocked, true);
  });
  it('does not flag an ordinary clone/push (no false positive)', () => {
    assert.equal(detectGitConfigRce("git clone https://example.com/repo.git").blocked, false);
    assert.equal(detectGitConfigRce("git push -u origin main").blocked, false);
  });
});

describe('detectArchiveToolExecution — tar exec hooks (parity)', () => {
  const blockedTar = [
    ["tar -cf /tmp/a.tar --to-command='sh -c id' .", /to-command/],
    ['tar -c --checkpoint=1 --checkpoint-action=exec=sh .', /checkpoint-action=exec/],
    ['tar -c --checkpoint-action exec=evil .', /checkpoint-action=exec/],
    ["tar --rmt-command='sh -c id' -cf a.tar .", /rmt-command/],
    ["tar -c --info-script='sh -c id' -f a.tar .", /info-script/],
    ["tar -c -F 'sh -c id' -f a.tar .", /info-script/],
  ] as const;
  for (const [cmd, re] of blockedTar) {
    it(`blocks: ${cmd}`, () => {
      const r = detectArchiveToolExecution(cmd);
      assert.equal(r.blocked, true);
      assert.match(r.reason!, re);
    });
  }

  it('allows ordinary tar archiving/extraction (no false positive)', () => {
    assert.equal(detectArchiveToolExecution('tar -czf a.tar.gz dir').blocked, false);
    assert.equal(detectArchiveToolExecution('tar -xf a.tar').blocked, false);
    // -I/--use-compress-program is deliberately allowed (common custom compressor).
    assert.equal(detectArchiveToolExecution('tar -I zstd -cf a.tar.zst dir').blocked, false);
    // Non-exec checkpoint actions are inert and allowed.
    assert.equal(detectArchiveToolExecution('tar -c --checkpoint-action=dot -f a.tar .').blocked, false);
  });
});

describe('detectArchiveToolExecution — zip exec hooks (parity)', () => {
  it('blocks zip --unzip-command', () => {
    const r = detectArchiveToolExecution("zip -r a.zip . --unzip-command='sh -c id'");
    assert.equal(r.blocked, true);
    assert.match(r.reason!, /unzip-command/);
  });
  it('blocks zip -TT / --test-command', () => {
    assert.equal(detectArchiveToolExecution("zip -r a.zip . -TT 'sh -c id'").blocked, true);
    assert.equal(detectArchiveToolExecution("zip -r a.zip . --test-command='sh -c id'").blocked, true);
  });
  it('allows ordinary zip / does not misfire on unzip (no false positive)', () => {
    assert.equal(detectArchiveToolExecution('zip -r a.zip dir').blocked, false);
    assert.equal(detectArchiveToolExecution('unzip a.zip -d out').blocked, false);
    assert.equal(detectArchiveToolExecution('zip a.zip file.txt').blocked, false);
  });
});

describe('validateBashSecurity — end-to-end wiring (parity)', () => {
  it('rejects tar --to-command through the top-level validator', () => {
    const r = validateBashSecurity("tar -cf a.tar --to-command='sh -c id' .");
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.ARCHIVE_TOOL_EXECUTION);
  });

  it('rejects zip --unzip-command through the top-level validator', () => {
    const r = validateBashSecurity("zip -r a.zip . --unzip-command='sh -c id'");
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.ARCHIVE_TOOL_EXECUTION);
  });

  it('rejects GIT_SSH_COMMAND env-var RCE through the top-level validator', () => {
    const r = validateBashSecurity("GIT_SSH_COMMAND='sh -c id' git fetch");
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.GIT_CONFIG_RCE);
  });

  it('rejects git alias shell-command RCE through the top-level validator', () => {
    const r = validateBashSecurity("git config alias.pwn '!id'");
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.GIT_CONFIG_RCE);
  });

  it('rejects --upload-pack RCE through the top-level validator', () => {
    const r = validateBashSecurity("git clone --upload-pack='sh -c id' repo");
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.GIT_CONFIG_RCE);
  });

  it('keeps ordinary archive + git commands safe (no regression)', () => {
    assert.equal(validateBashSecurity('tar -czf a.tar.gz dir').safe, true);
    assert.equal(validateBashSecurity('git status').safe, true);
    assert.equal(validateBashSecurity('git push -u origin main').safe, true);
  });
});
