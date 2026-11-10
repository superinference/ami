import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { HookManager } from '../src/index';
import type { HookConfigEntry } from '../src/hooks';

// ---------------------------------------------------------------------------
// Coverage target: core/core/src/hooks.ts
//
// This file drives the *reachable* behaviour that surrounds three defensive
// guards reported uncovered by the ESM coverage harness (compiled-.mjs lines
// 220-221, 223-224, 378):
//
//   (A) runCommandHookAsync: the `if (stdinData) { try { write; end } }` happy
//       path — line 215-218 — which pipes the JSON event payload to the hook
//       process over stdin. The sibling `catch { proc.stdin?.end() }` (write
//       throws) and `else { proc.stdin?.end() }` (empty stdin) arms are
//       defensive: through HookManager's public API the event payload is ALWAYS
//       a non-empty `JSON.stringify(...)` string, and writing a valid utf-8
//       string to a freshly-spawned pipe never throws synchronously. These
//       tests prove the payload is delivered and is always non-empty, which is
//       the observable contract those guards protect.
//
//   (B) loadFromFile: the `try { discoverPlugins(...) } catch { /* optional */ }`
//       block. The happy path (plugin bundles contributing a hooks.json) is
//       exercised here with a real on-disk bundle; the catch arm only fires if
//       the plugins module itself fails to load, which does not happen in a
//       normal checkout.
// ---------------------------------------------------------------------------

const tmpDirs: string[] = [];
function mkTmp(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'cov100-hooks-'));
  tmpDirs.push(d);
  return d;
}

/** Write a project-scoped .superinference/hooks.json and return its cwd. */
function writeHooks(entries: HookConfigEntry[]): string {
  const cwd = mkTmp();
  const dir = path.join(cwd, '.superinference');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'hooks.json'), JSON.stringify({ hooks: entries }));
  return cwd;
}

after(() => {
  for (const d of tmpDirs) {
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
});

function makePreToolCtx(toolName = 'bash') {
  return { messages: [], turnCount: 1, toolName, toolInput: { command: 'ls' } };
}

describe('hooks.ts — runCommandHookAsync stdin delivery (guards A)', () => {
  it('pipes the non-empty JSON event payload to a command hook over stdin', async () => {
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.setSessionId('sess-A1');
    // The hook reads its entire stdin and re-emits it as additionalContext via
    // the SYSTEM: prefix. This proves the `if (stdinData) { try { write } }`
    // path actually delivered the bytes.
    const cwd = writeHooks([
      { event: 'preToolUse', hook: { type: 'command', command: `printf 'SYSTEM:'; cat` } },
    ]);
    hm.loadFromFile(cwd);

    const decision = await hm.executePreToolUse(makePreToolCtx('bash'));
    assert.equal(decision.action, 'allow');
    // The payload that was written to stdin round-trips back as context.
    assert.ok(decision.additionalContext, 'hook should have returned stdin echo');
    const payload = JSON.parse(decision.additionalContext as string);
    assert.equal(payload.event, 'preToolUse');
    assert.equal(payload.toolName, 'bash');
    assert.deepEqual(payload.toolInput, { command: 'ls' });
    // Critical for the `else`/`catch` defensive arms: the stdin payload is a
    // real, non-empty JSON object — never the empty string.
    assert.ok((decision.additionalContext as string).length > 10);
  });

  it('delivers a non-empty payload even for a zero-field event (stop)', async () => {
    // `stop` carries the least data of any event. If anything could ever make
    // stdin empty (triggering the `else` arm), this is where it would show — but
    // JSON.stringify({ event: 'stop' }) is still non-empty, so the hook sees it.
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    const outFile = path.join(mkTmp(), 'stdin.txt');
    const cwd = writeHooks([
      { event: 'stop', hook: { type: 'command', command: `cat > ${outFile}` } },
    ]);
    hm.loadFromFile(cwd);

    await hm.executeStop({ messages: [], turnCount: 0 });
    const written = fs.readFileSync(outFile, 'utf-8');
    assert.ok(written.length > 0, 'stop hook must still receive a non-empty stdin payload');
    assert.deepEqual(JSON.parse(written), { event: 'stop' });
  });
});

describe('hooks.ts — command hook exit codes around runCommandHookAsync', () => {
  it('exit 2 (blocking) from a real command hook denies the tool (reason from stderr)', async () => {
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    const cwd = writeHooks([
      { event: 'preToolUse', hook: { type: 'command', command: `echo 'nope' 1>&2; exit 2` } },
    ]);
    hm.loadFromFile(cwd);
    const decision = await hm.executePreToolUse(makePreToolCtx());
    assert.equal(decision.action, 'deny');
    assert.match(String(decision.reason), /nope/);
  });

  // Red-green for the Claude Code exit-code contract: only exit 2 blocks; any
  // OTHER non-zero exit is a non-blocking error (surface stderr, allow the tool).
  it('other non-zero exit (e.g. 3) is NON-blocking — tool is NOT denied', async () => {
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    const cwd = writeHooks([
      { event: 'preToolUse', hook: { type: 'command', command: `echo 'warn' 1>&2; exit 3` } },
    ]);
    hm.loadFromFile(cwd);
    const decision = await hm.executePreToolUse(makePreToolCtx());
    assert.notEqual(decision.action, 'deny');
  });

  it('exit code 2 with asyncRewake emits a hookRewake event', async () => {
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    const rewakes: unknown[] = [];
    hm.on('hookRewake', (e) => rewakes.push(e));
    const cwd = writeHooks([
      { event: 'preToolUse', hook: { type: 'command', command: `echo 'bg done'; exit 2`, asyncRewake: true } },
    ]);
    hm.loadFromFile(cwd);
    const decision = await hm.executePreToolUse(makePreToolCtx());
    // exit code != 0 => DENY, and exit code === 2 with asyncRewake => rewake event
    assert.equal(decision.action, 'deny');
    assert.equal(rewakes.length, 1);
    assert.match(JSON.stringify(rewakes[0]), /bg done/);
  });
});

describe('hooks.ts — loadFromFile plugins discovery (guards B)', () => {
  it('contributes hooks from a real plugin bundle hooks.json', async () => {
    const cwd = mkTmp();
    // Build a project-scoped plugin bundle that contributes a hooks.json.
    const bundleRoot = path.join(cwd, '.superinference', 'plugins', 'my-bundle');
    fs.mkdirSync(path.join(bundleRoot, '.superinference-plugin'), { recursive: true });
    fs.writeFileSync(
      path.join(bundleRoot, '.superinference-plugin', 'plugin.json'),
      JSON.stringify({ name: 'my-bundle', version: '1.0.0' }),
    );
    const markerDir = mkTmp();
    const marker = path.join(markerDir, 'ran.txt');
    fs.writeFileSync(
      path.join(bundleRoot, 'hooks.json'),
      JSON.stringify({ hooks: [{ event: 'stop', hook: { type: 'command', command: `echo ran > ${marker}` } }] }),
    );

    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(cwd); // exercises the discoverPlugins try-body + bundle.hooksFile push

    assert.equal(hm.getSummary().stop, 1, 'plugin-contributed stop hook should be registered');
    await hm.executeStop({ messages: [], turnCount: 0 });
    assert.ok(fs.existsSync(marker), 'plugin hook command should have executed');
  });

  it('loadFromFile does not throw when the plugins dir is absent (catch is defensive)', () => {
    const cwd = mkTmp(); // no .superinference/plugins tree at all
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    // discoverPlugins returns [] for a missing dir; the surrounding try/catch
    // must let loadFromFile complete without error.
    assert.doesNotThrow(() => hm.loadFromFile(cwd));
    assert.equal(hm.getSummary().stop, 0);
  });
});
