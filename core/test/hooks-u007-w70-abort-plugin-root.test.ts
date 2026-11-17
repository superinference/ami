/**
 * U007 / w70 — two Claude-Code parity features in the hook layer:
 *
 * 1. Abort-signal awareness during Stop / SubagentStop hook execution. A user
 *    interrupt (Ctrl-C / turn abort) mid-chain must STOP the remaining hook chain
 *    immediately (Claude Code's stopHooks abort check). Before the fix executeStop
 *    / executeSubagentStop ran every registered hook regardless of ctx.abortSignal.
 *    RED discriminator: with an already-aborted signal the later hooks still ran.
 *
 * 2. Plugin/skill command-hook base-directory resolution: a plugin-contributed
 *    command hook gets CLAUDE_PLUGIN_ROOT (+ AMI_PLUGIN_ROOT alias) in its child
 *    env and has `${CLAUDE_PLUGIN_ROOT}` substituted in its command, so it can
 *    reference files co-located with its bundle regardless of the session cwd
 *    (Claude Code's CLAUDE_PLUGIN_ROOT substitution + registerSkillHooks skillRoot).
 *    RED discriminator: before the fix the placeholder was passed through verbatim
 *    and no CLAUDE_PLUGIN_ROOT env was set.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  HookManager,
  substitutePluginRoot,
  type HookContext,
  type SubagentContext,
} from '../src/hooks';

describe('substitutePluginRoot (pure helper)', () => {
  it('replaces ${CLAUDE_PLUGIN_ROOT} and the ${AMI_PLUGIN_ROOT} alias', () => {
    assert.equal(
      substitutePluginRoot('${CLAUDE_PLUGIN_ROOT}/scripts/check.sh', '/opt/p'),
      '/opt/p/scripts/check.sh',
    );
    assert.equal(
      substitutePluginRoot('${AMI_PLUGIN_ROOT}/x', '/opt/p'),
      '/opt/p/x',
    );
  });

  it('replaces the bare $CLAUDE_PLUGIN_ROOT form (word-bounded)', () => {
    assert.equal(substitutePluginRoot('$CLAUDE_PLUGIN_ROOT/a', '/r'), '/r/a');
    // word boundary: $CLAUDE_PLUGIN_ROOTX is NOT the var and stays untouched.
    assert.equal(substitutePluginRoot('$CLAUDE_PLUGIN_ROOTX', '/r'), '$CLAUDE_PLUGIN_ROOTX');
  });

  it('returns the input unchanged when pluginRoot is empty/undefined', () => {
    assert.equal(substitutePluginRoot('${CLAUDE_PLUGIN_ROOT}/a', undefined), '${CLAUDE_PLUGIN_ROOT}/a');
    assert.equal(substitutePluginRoot('${CLAUDE_PLUGIN_ROOT}/a', ''), '${CLAUDE_PLUGIN_ROOT}/a');
    assert.equal(substitutePluginRoot('', '/r'), '');
  });
});

describe('Stop-hook abort-signal awareness (U007/w70)', () => {
  it('runs NO stop hooks when ctx.abortSignal is already aborted', async () => {
    const hm = new HookManager();
    let ran = 0;
    hm.onStop(async () => { ran++; });
    const ac = new AbortController();
    ac.abort();
    const ctx: HookContext = { messages: [], turnCount: 0, abortSignal: ac.signal };
    const result = await hm.executeStop(ctx);
    // RED without the fix: ran === 1 (the hook executed despite the abort).
    assert.equal(ran, 0, 'no stop hook should run under an already-aborted signal');
    assert.deepEqual(result, { blockingErrors: [], preventContinuation: false, stopReason: undefined });
  });

  it('stops the chain mid-flight when a hook triggers the abort', async () => {
    const hm = new HookManager();
    const ac = new AbortController();
    const order: string[] = [];
    hm.onStop(async () => { order.push('h1'); ac.abort(); });
    hm.onStop(async () => { order.push('h2'); });
    const ctx: HookContext = { messages: [], turnCount: 0, abortSignal: ac.signal };
    await hm.executeStop(ctx);
    // RED without the fix: order === ['h1','h2'] (h2 ran after the abort).
    assert.deepEqual(order, ['h1'], 'the second stop hook must not run after an abort');
  });

  it('runs all hooks when the signal is never aborted (regression lock)', async () => {
    const hm = new HookManager();
    let ran = 0;
    hm.onStop(async () => { ran++; });
    hm.onStop(async () => { ran++; });
    const ac = new AbortController();
    await hm.executeStop({ messages: [], turnCount: 0, abortSignal: ac.signal });
    assert.equal(ran, 2);
  });
});

describe('SubagentStop-hook abort-signal awareness (U007/w70)', () => {
  it('runs NO subagentStop hooks when ctx.abortSignal is aborted', async () => {
    const hm = new HookManager();
    let ran = 0;
    hm.onSubagentStop(async () => { ran++; });
    const ac = new AbortController();
    ac.abort();
    const ctx: SubagentContext = {
      parentSessionId: 'p', subagentSessionId: 's', prompt: '', abortSignal: ac.signal,
    };
    await hm.executeSubagentStop(ctx);
    assert.equal(ran, 0);
  });

  it('runs subagentStop hooks when not aborted (regression lock)', async () => {
    const hm = new HookManager();
    let ran = 0;
    hm.onSubagentStop(async () => { ran++; });
    await hm.executeSubagentStop({ parentSessionId: 'p', subagentSessionId: 's', prompt: '' });
    assert.equal(ran, 1);
  });
});

describe('Plugin command-hook CLAUDE_PLUGIN_ROOT substitution + env (U007/w70)', () => {
  it('injects CLAUDE_PLUGIN_ROOT env and substitutes ${CLAUDE_PLUGIN_ROOT} in the command', async () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-pluginroot-'));
    const bundleRoot = path.join(cwd, '.ami', 'plugins', 'myplugin');
    fs.mkdirSync(path.join(bundleRoot, '.ami-plugin'), { recursive: true });
    fs.writeFileSync(
      path.join(bundleRoot, '.ami-plugin', 'plugin.json'),
      JSON.stringify({ name: 'myplugin', version: '1.0.0' }),
    );
    // The command both READS the env var ($CLAUDE_PLUGIN_ROOT, shell-expanded) and
    // writes to a path built from the ${CLAUDE_PLUGIN_ROOT} placeholder (substituted
    // by AMI before exec). If either path is broken, out.txt is absent or wrong.
    fs.writeFileSync(
      path.join(bundleRoot, 'hooks.json'),
      JSON.stringify({
        hooks: [
          { event: 'preToolUse', hook: { type: 'command', command: 'printf "%s" "$CLAUDE_PLUGIN_ROOT" > ${CLAUDE_PLUGIN_ROOT}/out.txt' } },
        ],
      }),
    );

    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(cwd);

    const decision = await hm.executePreToolUse({
      messages: [], turnCount: 0, toolName: 'bash', toolInput: { command: 'ls' },
    });
    assert.equal(decision.action, 'allow');

    const outPath = path.join(bundleRoot, 'out.txt');
    // RED without the fix: the file is written to a literal '${CLAUDE_PLUGIN_ROOT}'
    // directory (nonexistent → shell error, no file) OR $CLAUDE_PLUGIN_ROOT is empty.
    assert.ok(fs.existsSync(outPath), 'the ${CLAUDE_PLUGIN_ROOT} path must resolve to the bundle root');
    assert.equal(fs.readFileSync(outPath, 'utf-8').trim(), bundleRoot, 'CLAUDE_PLUGIN_ROOT env must equal the bundle root');

    fs.rmSync(cwd, { recursive: true, force: true });
  });
});

describe('Notification hook receives notification_type on stdin (U007/w70)', () => {
  it('serializes the parameterized notificationType into the hook stdin payload', async () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-notif-'));
    fs.mkdirSync(path.join(cwd, '.ami'), { recursive: true });
    const payload = path.join(cwd, 'notif.json');
    fs.writeFileSync(
      path.join(cwd, '.ami', 'hooks.json'),
      JSON.stringify({
        hooks: [
          { event: 'notification', hook: { type: 'command', command: `cat > ${payload}` } },
        ],
      }),
    );
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(cwd);

    await hm.executeNotification({ message: 'Approaching rate limit', notificationType: 'rate_limit_warning', title: 'Heads up' });

    const parsed = JSON.parse(fs.readFileSync(payload, 'utf-8'));
    assert.equal(parsed.event, 'notification');
    assert.equal(parsed.message, 'Approaching rate limit');
    // RED without the fix: notification_type was never serialized (the arm only
    // passed message + title), so a notification hook could not scope on type.
    assert.equal(parsed.notification_type, 'rate_limit_warning');

    fs.rmSync(cwd, { recursive: true, force: true });
  });
});
