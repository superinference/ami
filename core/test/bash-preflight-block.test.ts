/**
 * bash validateInput / bashPreflightBlock: a command that execute() hard-blocks is refused
 * BEFORE the permission prompt, with the same message — so the user is never asked to
 * approve something that will be refused anyway.
 *
 * SAFETY: execute() is only ever called with commands that are harmless even if a guard
 * regressed (writes into a fresh temp dir, git in a non-repo temp dir). Destructive
 * commands (nuclear list, self-kill) are checked through the PURE preflight only.
 */
import { describe, it, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { bashTool, bashPreflightBlock } from '../src/tools/bash';
import type { ToolContext } from '../src/types';

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-preflight-'));
after(() => fs.rmSync(dir, { recursive: true, force: true }));
const ctx = (): ToolContext => ({ cwd: dir } as ToolContext);

describe('bashPreflightBlock matches execute() refusals exactly (harmless corpus)', () => {
  const blocked = [
    `echo hello > ${path.join(dir, 'out.txt')}`,
    `cat < ${path.join(dir, 'in.txt')}`,
    'git commit -m "x"',
    'git commit --amend -m "x"',
    'echo $(whoami)',
  ];
  for (const cmd of blocked) {
    it(`refuses before execution with the same message: ${cmd}`, async () => {
      const pre = bashPreflightBlock(cmd, ctx());
      assert.ok(pre, 'preflight blocks it');
      const exec = await bashTool.execute({ command: cmd }, ctx());
      assert.equal(exec.isError, true);
      assert.equal(pre, exec.output, 'same message as execute()');
      const v = await bashTool.validateInput!({ command: cmd }, ctx());
      assert.deepEqual(v, { result: false, message: pre!.replace(/^Error: /, ''), errorCode: 1 });
      assert.equal(fs.existsSync(path.join(dir, 'out.txt')), false, 'nothing was written');
    });
  }
});

describe('bashPreflightBlock (pure) covers destructive commands without running them', () => {
  for (const cmd of ['rm -rf /', 'kill -9 $$', 'export LD_PRELOAD=/tmp/x.so']) {
    it(`blocks: ${cmd}`, () => {
      assert.match(bashPreflightBlock(cmd, ctx()) ?? '', /^Error: /);
    });
  }
});

describe('validateInput lets ordinary commands through to the permission flow', () => {
  for (const cmd of ['ls -la', 'git status', 'npm test', `touch ${path.join(dir, 'ok.txt')}`]) {
    it(`allows: ${cmd}`, async () => {
      assert.equal(bashPreflightBlock(cmd, ctx()), null);
      assert.deepEqual(await bashTool.validateInput!({ command: cmd }, ctx()), { result: true });
    });
  }
  it('an empty / non-string command is left to schema validation', () => {
    assert.equal(bashPreflightBlock('', ctx()), null);
    assert.equal(bashPreflightBlock(undefined, ctx()), null);
  });
});
