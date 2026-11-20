import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  RISK_LEVELS,
  riskLevelRank,
  classifyPermissionRisk,
  PermissionManager,
  type RiskLevel,
  type PermissionExplanation,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// U003 (w78): deterministic permission risk explainer (RiskLevel /
// PermissionExplanation) + its wiring into the recent-denials log.
//
// Parity: the reference's PermissionExplanation / RiskLevel
// (utils/permissions/permissionExplainer.ts) — Claude generates it with an LLM
// side-query; AMI computes it DETERMINISTICALLY from the canonical permission
// classifiers, so there is no model round-trip and the output is stable.
// ---------------------------------------------------------------------------

test('RISK_LEVELS is ordered LOW < MEDIUM < HIGH and riskLevelRank reflects it', () => {
  assert.deepEqual([...RISK_LEVELS], ['LOW', 'MEDIUM', 'HIGH']);
  assert.equal(riskLevelRank('LOW'), 1);
  assert.equal(riskLevelRank('MEDIUM'), 2);
  assert.equal(riskLevelRank('HIGH'), 3);
  assert.ok(riskLevelRank('LOW') < riskLevelRank('MEDIUM'));
  assert.ok(riskLevelRank('MEDIUM') < riskLevelRank('HIGH'));
});

function shape(e: PermissionExplanation): void {
  assert.ok(RISK_LEVELS.includes(e.riskLevel));
  assert.ok(e.explanation.length > 0);
  assert.ok(e.reasoning.startsWith('I '));
  assert.ok(e.risk.length > 0);
}

test('destructive bash command is classified HIGH risk', () => {
  const e = classifyPermissionRisk('bash', { command: 'rm -rf build' });
  shape(e);
  assert.equal(e.riskLevel, 'HIGH');
  assert.match(e.explanation, /rm -rf build/);
});

test('hardline-blocked bash command is HIGH risk', () => {
  const e = classifyPermissionRisk('bash', { command: 'rm -rf /' });
  assert.equal(e.riskLevel, 'HIGH');
});

test('read-only bash command is LOW risk', () => {
  const e = classifyPermissionRisk('bash', { command: 'ls -la' });
  shape(e);
  assert.equal(e.riskLevel, 'LOW');
});

test('ordinary mutating bash command is MEDIUM risk', () => {
  const e = classifyPermissionRisk('bash', { command: 'git push' });
  assert.equal(e.riskLevel, 'MEDIUM');
});

test('empty bash command is LOW risk with a no-effect explanation', () => {
  const e = classifyPermissionRisk('bash', { command: '   ' });
  assert.equal(e.riskLevel, 'LOW');
  assert.match(e.explanation, /empty/i);
});

test('bash input accepts a raw string command', () => {
  const e = classifyPermissionRisk('Bash', 'rm -rf node_modules');
  assert.equal(e.riskLevel, 'HIGH');
});

test('read-only tools are LOW, mutating tools MEDIUM', () => {
  assert.equal(classifyPermissionRisk('grep', { pattern: 'x' }).riskLevel, 'LOW');
  assert.equal(classifyPermissionRisk('Read', { file_path: '/a' }).riskLevel, 'LOW');
  assert.equal(classifyPermissionRisk('file_edit', { file_path: '/a' }).riskLevel, 'MEDIUM');
  assert.equal(classifyPermissionRisk('Edit', { file_path: '/a' }).riskLevel, 'MEDIUM');
});

test('web_fetch is MEDIUM (network) and unknown tools default to MEDIUM, never LOW', () => {
  assert.equal(classifyPermissionRisk('web_fetch', { url: 'http://x' }).riskLevel, 'MEDIUM');
  const unknown = classifyPermissionRisk('some_custom_tool', {});
  assert.equal(unknown.riskLevel, 'MEDIUM');
  assert.match(unknown.explanation, /some_custom_tool/);
});

test('tool-name aliases are normalized (NotebookEdit -> notebook_edit)', () => {
  assert.equal(classifyPermissionRisk('NotebookEdit', { notebook_path: '/n' }).riskLevel, 'MEDIUM');
});

// --- wiring: recorded denials carry a deterministic risk level -------------

test('recordDenial attaches a deterministic riskLevel for command denials', () => {
  const pm = new PermissionManager('ask');
  pm.recordDenial('rm -rf dist', 'hardline', 'blocked');
  pm.recordDenial('ls', 'rule');
  const denials = pm.getRecentDenials();
  assert.equal(denials.length, 2);
  // newest-first
  assert.equal(denials[0].display, 'ls');
  assert.equal(denials[0].riskLevel, 'LOW');
  assert.equal(denials[1].display, 'rm -rf dist');
  assert.equal(denials[1].riskLevel, 'HIGH');
});

test('a mode denial (prose display) carries no riskLevel', () => {
  const pm = new PermissionManager('ask');
  pm.recordDenial('plan mode forbids edits', 'mode');
  const [d] = pm.getRecentDenials();
  assert.equal(d.riskLevel, undefined);
});

test('classifyPermissionRisk is pure — repeated calls are identical', () => {
  const a = classifyPermissionRisk('bash', { command: 'rm -rf x' });
  const b = classifyPermissionRisk('bash', { command: 'rm -rf x' });
  assert.deepEqual(a, b);
});

const _typecheck: RiskLevel = 'HIGH';
void _typecheck;
