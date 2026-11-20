import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execSync } from 'node:child_process';
import { PermissionManager } from '../src/permissions';

// ---------------------------------------------------------------------------
// U003 / w119 — permissions.ts parity additions:
//  1. MCP server-level rule matching (bare `mcp__server` ⇒ every server tool)
//  2. detectShadowedRules: tool-wide allow rules are not "shadowed"
//  3. docker logs/inspect typed safeFlags (unknown flag ⇒ not auto-allowed)
//  4. special-file (FIFO/socket/device) guard before realpath in resolveForCheck
//  5. isWithinWorkspace: realpath-resolve cwd/roots (symlinked-PWD parity) +
//     macOS /private lexical fold
// Parity: the reference's utils/permissions/*, utils/shell/readOnlyCommandValidation.ts.
// ---------------------------------------------------------------------------

// --- 1. MCP server-level matching (i=60) --------------------------------------

test('MCP: bare `mcp__server` allow rule matches every tool from that server', async () => {
  const pm = new PermissionManager('ask', [{ tool: 'mcp__gh', action: 'allow' }]);
  // The whole point: a server-only rule grants every tool of that server.
  assert.equal(await pm.check('mcp__gh__list_prs', {}), 'allow');
  assert.equal(await pm.check('mcp__gh__view_pr', {}), 'allow');
  // A different server is NOT covered — falls through to the ask default.
  assert.equal(await pm.check('mcp__other__thing', {}), 'ask');
});

test('MCP: `mcp__server__*` wildcard still matches; a specific rule stays specific', async () => {
  const wild = new PermissionManager('ask', [{ tool: 'mcp__gh__*', action: 'allow' }]);
  assert.equal(await wild.check('mcp__gh__list_prs', {}), 'allow');

  const specific = new PermissionManager('ask', [{ tool: 'mcp__gh__view', action: 'allow' }]);
  assert.equal(await specific.check('mcp__gh__view', {}), 'allow');
  // A sibling tool on the same server must NOT match a tool-specific rule.
  assert.equal(await specific.check('mcp__gh__other', {}), 'ask');
});

test('MCP: a bare-server rule does not match a non-MCP builtin tool', async () => {
  // mcpInfoFromString('file_read') is null ⇒ the bare-server rule never matches a
  // builtin tool, so a sibling deny rule governs it unaffected.
  const denyAll = new PermissionManager('ask', [
    { tool: 'mcp__gh', action: 'allow' },
    { tool: 'file_read', action: 'deny' },
  ]);
  assert.equal(await denyAll.check('file_read', { file_path: 'x.ts' }), 'deny');
});

// --- 2. detectShadowedRules excludes tool-wide allow (i=71 / i=72) -------------

test('detectShadowedRules: a TOOL-WIDE allow under a tool-wide deny is NOT shadowed', () => {
  const pm = new PermissionManager('ask', [
    { tool: 'bash', action: 'allow' }, // tool-wide (no pattern) — merely conflicts
    { tool: 'bash', action: 'deny' },
  ]);
  assert.deepEqual(pm.detectShadowedRules(), []);
});

test('detectShadowedRules: a `*`-pattern allow is tool-wide and NOT shadowed', () => {
  const pm = new PermissionManager('ask', [
    { tool: 'bash', pattern: '*', action: 'allow' },
    { tool: 'bash', action: 'ask' },
  ]);
  assert.deepEqual(pm.detectShadowedRules(), []);
});

test('detectShadowedRules: a SPECIFIC allow under a tool-wide deny IS still shadowed', () => {
  const pm = new PermissionManager('ask', [
    { tool: 'bash', pattern: 'ls *', action: 'allow', source: 'localSettings' },
    { tool: 'bash', action: 'deny', source: 'projectSettings' },
  ]);
  const shadows = pm.detectShadowedRules();
  assert.equal(shadows.length, 1);
  assert.equal(shadows[0]!.shadowType, 'deny');
});

// --- 3. docker logs/inspect typed safeFlags (i=82) ----------------------------

test('docker: read-only subcommands with KNOWN flags stay safe', () => {
  const pm = new PermissionManager();
  assert.equal(pm.classifyBashCommand('docker logs -f c'), 'safe');
  assert.equal(pm.classifyBashCommand('docker logs --tail 50 c'), 'safe');
  assert.equal(pm.classifyBashCommand('docker logs --since 1h --until 2h c'), 'safe');
  assert.equal(pm.classifyBashCommand('docker inspect --format {{.Id}} c'), 'safe');
  assert.equal(pm.classifyBashCommand('docker inspect -s c'), 'safe');
  // No flags / positional-only is unchanged.
  assert.equal(pm.classifyBashCommand('docker inspect c'), 'safe');
  assert.equal(pm.classifyBashCommand('docker logs c'), 'safe');
});

test('docker: an UNKNOWN flag on logs/inspect is no longer auto-allowed', () => {
  const pm = new PermissionManager();
  assert.equal(pm.classifyBashCommand('docker inspect --evil c'), 'unsafe');
  assert.equal(pm.classifyBashCommand('docker logs --exec sh c'), 'unsafe');
});

test('docker: subcommands without flag constraints keep name-only auto-allow', () => {
  const pm = new PermissionManager();
  assert.equal(pm.classifyBashCommand('docker ps -a'), 'safe');
  assert.equal(pm.classifyBashCommand('docker stats --no-stream'), 'safe');
  assert.equal(pm.classifyBashCommand('docker images --digests'), 'safe');
});

// --- 4. special-file guard before realpath (i=41) -----------------------------

test('special-file guard: a FIFO path is validated without a realpath hang', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-fifo-'));
  const fifo = path.join(dir, 'pipe');
  try {
    execSync(`mkfifo ${fifo}`);
  } catch {
    // mkfifo unavailable on this platform — nothing to exercise.
    fs.rmSync(dir, { recursive: true, force: true });
    return;
  }
  try {
    // Must return promptly (the guard skips realpathSync on the FIFO) and classify
    // the in-workspace FIFO as allowed.
    assert.equal(pm_isPathAllowed(fifo, dir), true);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

function pm_isPathAllowed(p: string, cwd: string): boolean {
  return new PermissionManager().isPathAllowed(p, cwd);
}

// --- 5. isWithinWorkspace: symlinked cwd + realpath fold (i=49 / i=62 / i=64) --

test('containment: a file under a SYMLINKED launch cwd is inside the workspace', () => {
  const real = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-real-'));
  const link = path.join(os.tmpdir(), `ami-link-${process.pid}-${Date.now()}`);
  fs.writeFileSync(path.join(real, 'x.txt'), 'hi');
  fs.symlinkSync(real, link);
  try {
    const pm = new PermissionManager();
    // cwd is the SYMLINK; the file realpath-resolves to the REAL dir. Before the
    // realpath-cwd fold this produced a false denial (resolved under real/, cwd=link).
    assert.equal(pm.isPathAllowed(path.join(link, 'x.txt'), link), true);
    // classifyReadAccess agrees it is an in-workspace allow (not a prompt).
    assert.equal(pm.classifyReadAccess(path.join(link, 'x.txt'), link), 'allow');
  } finally {
    fs.rmSync(link, { force: true });
    fs.rmSync(real, { recursive: true, force: true });
  }
});

test('containment: a sibling of the real cwd is still OUTSIDE the workspace', () => {
  const real = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-real2-'));
  const sibling = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-sib-'));
  fs.writeFileSync(path.join(sibling, 's.txt'), 'x');
  const link = path.join(os.tmpdir(), `ami-link2-${process.pid}-${Date.now()}`);
  fs.symlinkSync(real, link);
  try {
    const pm = new PermissionManager();
    // The realpath fold must not widen containment beyond the real tree.
    assert.equal(pm.isPathAllowed(path.join(sibling, 's.txt'), link), false);
  } finally {
    fs.rmSync(link, { force: true });
    fs.rmSync(real, { recursive: true, force: true });
    fs.rmSync(sibling, { recursive: true, force: true });
  }
});
