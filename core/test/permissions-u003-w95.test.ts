import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  PermissionManager,
  detectPathExpansionHazard,
  pathExpansionHazardReason,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// U003 / w95 — G10: path-shape unvalidatable gates (UNC / credential-leak +
// backtick command-substitution) extend the existing TOCTOU path-hazard
// detector so these obscured path shapes are never silently auto-allowed and
// are forced to a manual-approval prompt (parity: the reference's pathValidation UNC
// + escape gating). Mirrors pathValidation.ts L1022-L1159.
// ---------------------------------------------------------------------------

test('G10: a // UNC / network-share path is flagged network-path', () => {
  assert.equal(detectPathExpansionHazard('//server/share/secret'), 'network-path');
});

test('G10: a \\\\ Windows UNC path is flagged network-path', () => {
  assert.equal(detectPathExpansionHazard('\\\\server\\share\\x'), 'network-path');
});

test('G10: a WebDAV credential-leak marker (@SSL@ / DavWWWRoot) is flagged network-path', () => {
  assert.equal(detectPathExpansionHazard('//host@SSL@/DavWWWRoot/file'), 'network-path');
  assert.equal(detectPathExpansionHazard('/mnt/DavWWWRoot/secret'), 'network-path');
});

test('G10: a backtick command-substitution in a path is flagged command-substitution', () => {
  assert.equal(detectPathExpansionHazard('/project/`whoami`/x'), 'command-substitution');
});

test('G10: an inert normal absolute path is not flagged', () => {
  assert.equal(detectPathExpansionHazard('/project/src/index.ts'), null);
  assert.equal(detectPathExpansionHazard('./relative/file.txt'), null);
});

test('G10: existing $-expansion and tilde hazards still detected (no regression)', () => {
  assert.equal(detectPathExpansionHazard('$HOME/.ssh/id_rsa'), 'shell-expansion');
  assert.equal(detectPathExpansionHazard('~user/file'), 'tilde-variant');
});

test('G10: network-path and command-substitution carry distinct user-facing reasons', () => {
  const net = pathExpansionHazardReason('network-path');
  const cmd = pathExpansionHazardReason('command-substitution');
  assert.ok(net.length > 0 && cmd.length > 0);
  assert.notEqual(net, cmd);
  assert.match(net, /network|UNC|remote/i);
  assert.match(cmd, /substitution|backtick/i);
});

test('G10: isPathAllowed rejects, classifyReadAccess prompts for a UNC path', () => {
  const pm = new PermissionManager('ask');
  const cwd = process.cwd();
  assert.equal(pm.isPathAllowed('//server/share/x', cwd), false);
  assert.equal(pm.classifyReadAccess('//server/share/x', cwd), 'prompt');
  // The hazard reason is surfaced so the UI can explain WHY it was gated.
  assert.match(pm.pathExpansionHazardReason('//server/share/x') ?? '', /network|UNC|remote/i);
});

// ---------------------------------------------------------------------------
// U003 / w95 — G18: strip dangerous code-exec `allow` rules that originate from
// an UNTRUSTED (shared/committable) settings tier (projectSettings / command),
// recording them for reporting. A policy/personal-tier rule is NOT stripped.
// Mirrors the reference's ToolPermissionContext.strippedDangerousRules.
// ---------------------------------------------------------------------------

function withTempProject(rules: unknown[], fn: (cwd: string) => void): void {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-perm-w95-'));
  try {
    fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(dir, '.ami', 'permissions.json'), JSON.stringify({ rules }));
    fn(dir);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

test('G18: loadRules strips a project-sourced dangerous code-exec allow rule', () => {
  withTempProject(
    [
      { tool: 'bash', pattern: 'python:*', action: 'allow' }, // dangerous code-exec prefix
      { tool: 'bash', pattern: 'ls', action: 'allow' },       // safe — kept
      { tool: 'bash', pattern: 'python manage.py migrate', action: 'allow' }, // narrow — kept
    ],
    (cwd) => {
      const pm = new PermissionManager('ask');
      const warnings = pm.loadRules(cwd);
      const patterns = pm.getRules().map(r => r.pattern);
      assert.ok(!patterns.includes('python:*'), 'dangerous prefix must be stripped');
      assert.ok(patterns.includes('ls'), 'safe rule kept');
      assert.ok(patterns.includes('python manage.py migrate'), 'narrow rule kept');
      const stripped = pm.getStrippedDangerousRules();
      assert.equal(stripped.length, 1);
      assert.equal(stripped[0]!.pattern, 'python:*');
      assert.equal(stripped[0]!.source, 'projectSettings');
      assert.ok(warnings.some(w => /strip|dangerous|code execution/i.test(w)),
        'a warning must report the stripped rule');
    },
  );
});

test('G18: a dangerous code-exec allow from a PERSONAL/trusted source is NOT stripped', () => {
  // Constructor rules carry no source → treated as personal/trusted → kept.
  const pm = new PermissionManager('ask', [
    { tool: 'bash', pattern: 'python:*', action: 'allow' },
  ]);
  assert.equal(pm.getRules().length, 1);
  assert.equal(pm.getStrippedDangerousRules().length, 0);
});

test('G18: a policy (managed) dangerous allow is trusted and NOT stripped', () => {
  withTempProject(
    [{ tool: 'bash', pattern: 'python:*', action: 'allow', source: 'policySettings' }],
    (cwd) => {
      const pm = new PermissionManager('ask');
      pm.loadRules(cwd);
      assert.ok(pm.getRules().some(r => r.pattern === 'python:*'),
        'enterprise-managed rule is trusted and kept');
      assert.equal(pm.getStrippedDangerousRules().length, 0);
    },
  );
});
