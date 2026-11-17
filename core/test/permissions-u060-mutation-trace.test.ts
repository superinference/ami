import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { PermissionManager } from '../src/permissions';
import { fsHooks, __resetLoggerForTest } from '../src/logger';

// ---------------------------------------------------------------------------
// U060 — Human-readable debug trace of every permission mutation.
//
// The permission layer previously mutated state silently: addRule/removeRule/
// setMode/registerAllowedRoot/removeAllowedRoot/saveRules emitted no trace. This
// suite proves each mutation now routes ONE descriptive line through the core
// logger (AMI's introspection surface, mirroring Claude Code's logForDebugging
// calls in applyPermissionUpdate/persistPermissionUpdate). It is also the RED
// guard: with the traceMutation() calls reverted, every assertion below fails.
//
// Capture is ESM-safe — debug mode is forced on (so the logger's test-env gate
// does not suppress writes) and the mockable `fsHooks.appendFileSync` seam
// records the composed line, without touching the shared on-disk engine.log.
// ---------------------------------------------------------------------------

/** Run `fn` with debug logging forced on and the on-disk append captured. */
function capture(fn: () => void): string[] {
  const lines: string[] = [];
  const prevDebug = process.env.AMI_DEBUG;
  const prevComponents = process.env.AMI_DEBUG_COMPONENTS;
  process.env.AMI_DEBUG = '1';
  delete process.env.AMI_DEBUG_COMPONENTS; // no component filter → all lines pass
  __resetLoggerForTest();
  mock.method(fsHooks, 'mkdirSync', () => undefined as unknown as string);
  mock.method(fsHooks, 'statSync', () => { throw new Error('ENOENT'); });
  mock.method(fsHooks, 'appendFileSync', (_f: fs.PathOrFileDescriptor, data: unknown) => {
    lines.push(String(data));
  });
  try {
    fn();
  } finally {
    mock.restoreAll();
    if (prevDebug === undefined) delete process.env.AMI_DEBUG; else process.env.AMI_DEBUG = prevDebug;
    if (prevComponents === undefined) delete process.env.AMI_DEBUG_COMPONENTS; else process.env.AMI_DEBUG_COMPONENTS = prevComponents;
  }
  return lines;
}

/** True when some captured line contains every one of `needles`. */
function hasLine(lines: string[], ...needles: string[]): boolean {
  return lines.some((l) => needles.every((n) => l.includes(n)));
}

afterEach(() => {
  mock.restoreAll();
  __resetLoggerForTest();
});

describe('U060 permission mutation debug trace', () => {
  it('traces a mode change via setMode', () => {
    const pm = new PermissionManager('ask');
    const lines = capture(() => pm.setMode('plan'));
    assert.ok(hasLine(lines, '[permissions]', "Setting mode to 'plan'", "was 'ask'"), lines.join('|'));
  });

  it('traces a refused mode change under managed bypass lockdown', () => {
    const pm = new PermissionManager('ask');
    pm.setDisableBypass(true);
    const lines = capture(() => {
      const ok = pm.setMode('bypassPermissions');
      assert.equal(ok, false);
    });
    assert.ok(hasLine(lines, '[permissions]', "Refused mode change to 'bypassPermissions'", 'managed policy'), lines.join('|'));
  });

  it('traces an added rule, including its human-readable description', () => {
    const pm = new PermissionManager();
    const lines = capture(() => pm.addRule({ tool: 'bash', pattern: 'git status', action: 'allow' }));
    assert.ok(hasLine(lines, '[permissions]', 'Adding allow rule', 'Allow bash', 'git status'), lines.join('|'));
  });

  it('traces a removed rule with the removed count', () => {
    const pm = new PermissionManager();
    pm.addRule({ tool: 'bash', pattern: 'rm -rf', action: 'deny' });
    const lines = capture(() => {
      const n = pm.removeRule((r) => r.tool === 'bash');
      assert.equal(n, 1);
    });
    assert.ok(hasLine(lines, '[permissions]', 'Removing 1 rule(s)'), lines.join('|'));
  });

  it('does NOT trace removeRule when nothing matched', () => {
    const pm = new PermissionManager();
    pm.addRule({ tool: 'bash', pattern: 'git status', action: 'allow' });
    const lines = capture(() => {
      const n = pm.removeRule((r) => r.tool === 'web_fetch');
      assert.equal(n, 0);
    });
    assert.ok(!hasLine(lines, 'Removing'), lines.join('|'));
  });

  it('traces a granted read directory via registerAllowedRoot', () => {
    const pm = new PermissionManager();
    const dir = path.resolve(os.tmpdir(), 'u060-grant-dir');
    const lines = capture(() => pm.registerAllowedRoot(dir));
    assert.ok(hasLine(lines, '[permissions]', 'Adding read directory', path.normalize(dir)), lines.join('|'));
  });

  it('does NOT trace an idempotent re-grant of the same read directory', () => {
    const dir = path.resolve(os.tmpdir(), 'u060-grant-dir-2');
    const pm = new PermissionManager();
    pm.registerAllowedRoot(dir); // first grant (outside capture)
    const lines = capture(() => pm.registerAllowedRoot(dir)); // re-grant → no widening
    assert.ok(!hasLine(lines, 'Adding read directory'), lines.join('|'));
  });

  it('traces a revoked read directory via removeAllowedRoot', () => {
    const dir = path.resolve(os.tmpdir(), 'u060-revoke-dir');
    const pm = new PermissionManager();
    pm.registerAllowedRoot(dir);
    const lines = capture(() => {
      const ok = pm.removeAllowedRoot(dir);
      assert.equal(ok, true);
    });
    assert.ok(hasLine(lines, '[permissions]', 'Removing read directory', path.normalize(dir)), lines.join('|'));
  });

  it('traces each mutation applied through the canonical applyUpdate path', () => {
    const pm = new PermissionManager();
    const lines = capture(() => {
      pm.applyUpdate({ type: 'setMode', mode: 'acceptEdits' });
      pm.applyUpdate({ type: 'addRules', behavior: 'deny', destination: 'session', rules: [{ toolName: 'bash', ruleContent: 'curl *' }] });
    });
    // applyUpdate delegates to setMode/addRule, so the low-level mutator traces
    // cover the canonical path with no duplicate logging site.
    assert.ok(hasLine(lines, "Setting mode to 'acceptEdits'"), lines.join('|'));
    assert.ok(hasLine(lines, 'Adding deny rule', 'Deny bash', 'curl *'), lines.join('|'));
  });

  it('traces persistence of rules to disk via saveRules', () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'u060-save-'));
    try {
      const pm = new PermissionManager();
      pm.addRule({ tool: 'bash', pattern: 'ls', action: 'allow', source: 'projectSettings' });
      const lines = capture(() => {
        const res = pm.saveRules(cwd);
        assert.equal(res.ok, true);
      });
      assert.ok(hasLine(lines, '[permissions]', 'Persisting', 'permissions.json'), lines.join('|'));
    } finally {
      fs.rmSync(cwd, { recursive: true, force: true });
    }
  });
});
