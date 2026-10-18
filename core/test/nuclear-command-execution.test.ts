/**
 * Tests that nuclear commands are blocked at the bash tool execute() level —
 * proving the command never reaches child_process.spawn / execCommand.
 *
 * These are integration tests that call bashTool.execute() directly,
 * verifying both the error response AND that no file system damage occurs.
 */
import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { bashTool } from '../src/tools/bash';
import type { ToolContext, ToolResult } from '../src/types';

let tmpDir: string;

function makeContext(overrides: Partial<ToolContext> = {}): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    readFile: async (p: string) => fs.readFileSync(p, 'utf-8'),
    detachedMode: false,
    ...overrides,
  } as unknown as ToolContext;
}

before(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-nuclear-exec-'));
  fs.writeFileSync(path.join(tmpDir, 'canary.txt'), 'I must survive');
});

after(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

describe('nuclear commands blocked at bash execute() — command never runs', () => {

  // -----------------------------------------------------------------------
  // Core assertion: blocked + canary file survives + output has BLOCKED
  // -----------------------------------------------------------------------

  async function assertNuclearBlocked(command: string, label: string): Promise<void> {
    const ctx = makeContext();
    const result: ToolResult = await bashTool.execute({ command }, ctx);

    // 1. Must return isError: true
    assert.equal(result.isError, true, `${label}: must return isError: true`);

    // 2. Output must contain BLOCKED
    const output = typeof result.output === 'string' ? result.output : JSON.stringify(result.output);
    assert.ok(output.includes('BLOCKED'), `${label}: output must contain BLOCKED, got: ${output.slice(0, 200)}`);

    // 3. Canary file must still exist — proves no execution happened
    const canaryExists = fs.existsSync(path.join(tmpDir, 'canary.txt'));
    assert.ok(canaryExists, `${label}: canary.txt must survive — if missing, the command executed!`);
    const content = fs.readFileSync(path.join(tmpDir, 'canary.txt'), 'utf-8');
    assert.equal(content, 'I must survive', `${label}: canary content must be intact`);
  }

  // -----------------------------------------------------------------------
  // rm -rf / — the exact incident
  // -----------------------------------------------------------------------

  it('rm -rf / is blocked, never executes', async () => {
    await assertNuclearBlocked('rm -rf /', 'rm -rf /');
  });

  it('rm -rf /* is blocked, never executes', async () => {
    await assertNuclearBlocked('rm -rf /*', 'rm -rf /*');
  });

  it('rm -rf /home is blocked, never executes', async () => {
    await assertNuclearBlocked('rm -rf /home', 'rm -rf /home');
  });

  it('rm -rf /etc is blocked, never executes', async () => {
    await assertNuclearBlocked('rm -rf /etc', 'rm -rf /etc');
  });

  it('rm -rf ~ is blocked, never executes', async () => {
    await assertNuclearBlocked('rm -rf ~', 'rm -rf ~');
  });

  it('rm -rf $HOME is blocked, never executes', async () => {
    await assertNuclearBlocked('rm -rf $HOME', 'rm -rf $HOME');
  });

  // -----------------------------------------------------------------------
  // Evasion: prefixes that strip to rm -rf /
  // -----------------------------------------------------------------------

  it('sudo rm -rf / is blocked, never executes', async () => {
    await assertNuclearBlocked('sudo rm -rf /', 'sudo rm -rf /');
  });

  it('/usr/bin/rm -rf / is blocked, never executes', async () => {
    await assertNuclearBlocked('/usr/bin/rm -rf /', '/usr/bin/rm -rf /');
  });

  it('env rm -rf / is blocked, never executes', async () => {
    await assertNuclearBlocked('env rm -rf /', 'env rm -rf /');
  });

  it('command rm -rf / is blocked, never executes', async () => {
    await assertNuclearBlocked('command rm -rf /', 'command rm -rf /');
  });

  // -----------------------------------------------------------------------
  // Compound: nuclear hidden after safe command
  // -----------------------------------------------------------------------

  it('echo hi; rm -rf / — nuclear after safe is still blocked', async () => {
    await assertNuclearBlocked('echo hi; rm -rf /', 'compound ;');
  });

  it('true && rm -rf / — nuclear after && is still blocked', async () => {
    await assertNuclearBlocked('true && rm -rf /', 'compound &&');
  });

  it('false || rm -rf / — nuclear after || is still blocked', async () => {
    await assertNuclearBlocked('false || rm -rf /', 'compound ||');
  });

  // -----------------------------------------------------------------------
  // Disk wipe
  // -----------------------------------------------------------------------

  it('mkfs.ext4 /dev/sda1 is blocked, never executes', async () => {
    await assertNuclearBlocked('mkfs.ext4 /dev/sda1', 'mkfs');
  });

  it('dd if=/dev/zero of=/dev/sda is blocked, never executes', async () => {
    await assertNuclearBlocked('dd if=/dev/zero of=/dev/sda', 'dd wipe');
  });

  it('wipefs -a /dev/sda is blocked, never executes', async () => {
    await assertNuclearBlocked('wipefs -a /dev/sda', 'wipefs');
  });

  it('shred /dev/sda is blocked, never executes', async () => {
    await assertNuclearBlocked('shred /dev/sda', 'shred');
  });

  // -----------------------------------------------------------------------
  // Fork bomb
  // -----------------------------------------------------------------------

  it(':(){ :|:& };: is blocked, never executes', async () => {
    await assertNuclearBlocked(':(){ :|:& };:', 'fork bomb');
  });

  // -----------------------------------------------------------------------
  // Separate flags
  // -----------------------------------------------------------------------

  it('rm -r -f / (separate flags) is blocked', async () => {
    await assertNuclearBlocked('rm -r -f /', 'rm -r -f /');
  });

  it('rm --recursive --force / (long flags) is blocked', async () => {
    await assertNuclearBlocked('rm --recursive --force /', 'rm --recursive --force /');
  });

  // -----------------------------------------------------------------------
  // Quoted targets
  // -----------------------------------------------------------------------

  it('rm -rf "/" (double-quoted) is blocked', async () => {
    await assertNuclearBlocked('rm -rf "/"', 'rm -rf "/"');
  });

  it("rm -rf '/' (single-quoted) is blocked", async () => {
    await assertNuclearBlocked("rm -rf '/'", "rm -rf '/'");
  });

  // -----------------------------------------------------------------------
  // Background mode: nuclear check runs BEFORE processManager.spawn()
  // -----------------------------------------------------------------------

  it('rm -rf / with run_in_background=true is still blocked', async () => {
    const ctx = makeContext();
    const result: ToolResult = await bashTool.execute(
      { command: 'rm -rf /', run_in_background: true },
      ctx,
    );
    assert.equal(result.isError, true, 'background nuclear must be blocked');
    const output = typeof result.output === 'string' ? result.output : '';
    assert.ok(output.includes('BLOCKED'), 'background output must contain BLOCKED');
    assert.ok(fs.existsSync(path.join(tmpDir, 'canary.txt')), 'canary must survive background attempt');
  });

  // -----------------------------------------------------------------------
  // Safe commands: prove they AREN'T blocked
  // -----------------------------------------------------------------------

  it('echo hello executes normally (not blocked)', async () => {
    const ctx = makeContext();
    const result: ToolResult = await bashTool.execute({ command: 'echo hello' }, ctx);
    assert.equal(result.isError, false, 'safe command must not be blocked');
    const output = typeof result.output === 'string' ? result.output : '';
    assert.ok(output.includes('hello'), 'echo output must contain hello');
  });

  it('rm -rf inside a safe subdirectory is not blocked', async () => {
    const subdir = path.join(tmpDir, 'deleteme');
    fs.mkdirSync(subdir);
    fs.writeFileSync(path.join(subdir, 'a.txt'), 'x');
    const ctx = makeContext();
    const result: ToolResult = await bashTool.execute({ command: `rm -rf ${subdir}` }, ctx);
    assert.equal(result.isError, false, 'rm -rf on a specific subdir must be allowed');
    assert.ok(!fs.existsSync(subdir), 'subdir should be deleted');
    // But canary must survive
    assert.ok(fs.existsSync(path.join(tmpDir, 'canary.txt')), 'canary must survive targeted rm');
  });
});
