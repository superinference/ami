// U023 (w75): per-skill / per-agent frontmatter `hooks` field — the canonical
// parser (parseFrontmatterHooks), the SkillDefinition/AgentDefinition schema +
// safe-property + validation wiring, and the invocation-scoped
// HookManager.registerConfigEntries lifecycle API. Parity with Claude Code's
// PromptCommand/agent `hooks` + registerFrontmatterHooks.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  parseFrontmatterHooks,
  HookManager,
  type HookConfigEntry,
} from '../src/hooks';
import {
  SAFE_SKILL_PROPERTIES,
  skillHasOnlySafeProperties,
  validateAgentDefinition,
  type SkillDefinition,
  type AgentDefinition,
} from '../src/skills';

// ---------------------------------------------------------------------------
// parseFrontmatterHooks — flat-array form
// ---------------------------------------------------------------------------

test('parseFrontmatterHooks keeps valid flat-array entries and translates the matcher', () => {
  const out = parseFrontmatterHooks([
    { event: 'preToolUse', matcher: 'Bash', hook: { type: 'command', command: 'echo hi' } },
    { event: ' postToolUse ', hook: { type: 'command', command: 'echo bye' } },
  ]);
  assert.equal(out.length, 2);
  // event is trimmed; Claude-Code tool-name matcher 'Bash' → AMI 'bash'
  assert.equal(out[0].event, 'preToolUse');
  assert.equal(out[0].matcher, 'bash');
  assert.deepEqual(out[0].hook, { type: 'command', command: 'echo hi' });
  // trimmed event, no matcher carried when absent
  assert.equal(out[1].event, 'postToolUse');
  assert.equal(out[1].matcher, undefined);
});

test('parseFrontmatterHooks drops malformed flat-array entries', () => {
  const out = parseFrontmatterHooks([
    null,
    'nope',
    ['nested'],
    { event: '', hook: { type: 'command', command: 'x' } }, // blank event
    { event: 'preToolUse' },                                  // missing hook
    { event: 'preToolUse', hook: 'str' },                     // non-object hook
    { event: 'preToolUse', hook: ['arr'] },                   // array hook
    { event: 'stop', hook: { type: 'command', command: 'ok' } }, // the one valid entry
  ]);
  assert.equal(out.length, 1);
  assert.equal(out[0].event, 'stop');
});

// ---------------------------------------------------------------------------
// parseFrontmatterHooks — Claude Code map-keyed form + junk
// ---------------------------------------------------------------------------

test('parseFrontmatterHooks accepts the Claude Code map-keyed shape', () => {
  const out = parseFrontmatterHooks({
    PreToolUse: [{ matcher: 'Edit|Write', hooks: [{ type: 'command', command: 'guard' }] }],
  });
  assert.equal(out.length, 1);
  assert.equal(out[0].event, 'preToolUse');          // lower-camel-cased
  assert.equal(out[0].matcher, 'file_edit|file_edit'); // tool names translated
  assert.deepEqual(out[0].hook, { type: 'command', command: 'guard' });
});

test('parseFrontmatterHooks parses a JSON-string block (the SKILL.md authoring form)', () => {
  // How a frontmatter `hooks:` block is actually authored (JSON string, like mcp-servers).
  const out = parseFrontmatterHooks(
    '[{"event":"preToolUse","matcher":"Bash","hook":{"type":"command","command":"guard"}}]',
  );
  assert.equal(out.length, 1);
  assert.equal(out[0].event, 'preToolUse');
  assert.equal(out[0].matcher, 'bash');
  // A JSON-string in the Claude map-keyed shape works too.
  const mapped = parseFrontmatterHooks('{"PostToolUse":[{"hooks":[{"type":"command","command":"x"}]}]}');
  assert.equal(mapped.length, 1);
  assert.equal(mapped[0].event, 'postToolUse');
});

test('parseFrontmatterHooks returns [] for absent / malformed / scalar values', () => {
  assert.deepEqual(parseFrontmatterHooks(undefined), []);
  assert.deepEqual(parseFrontmatterHooks(null), []);
  assert.deepEqual(parseFrontmatterHooks(''), []);
  assert.deepEqual(parseFrontmatterHooks('not json {['), []); // malformed JSON string
  assert.deepEqual(parseFrontmatterHooks('42'), []);          // JSON scalar, not array/map
  assert.deepEqual(parseFrontmatterHooks(42), []);
});

// ---------------------------------------------------------------------------
// Schema wiring: SkillDefinition / AgentDefinition carry hooks; safe property;
// validation surfaces a malformed hooks block.
// ---------------------------------------------------------------------------

test('hooks is a recognized SAFE skill property (no permission prompt for a scoped-config skill)', () => {
  assert.ok(SAFE_SKILL_PROPERTIES.has('hooks'));
  const skill: Record<string, unknown> = {
    name: 'demo',
    description: 'd',
    hooks: [{ event: 'preToolUse', hook: { type: 'command', command: 'x' } }],
  };
  assert.equal(skillHasOnlySafeProperties(skill), true);
});

test('SkillDefinition / AgentDefinition accept a hooks field', () => {
  const hooks: HookConfigEntry[] = [{ event: 'preToolUse', hook: { type: 'command', command: 'x' } }];
  const skill: SkillDefinition = { name: 'a-skill', description: 'd', content: 'body', filePath: '<t>', hooks };
  const agent: AgentDefinition = { name: 'an-agent', description: 'd', systemPrompt: 'p', filePath: '<t>', hooks };
  assert.equal(skill.hooks?.length, 1);
  assert.equal(agent.hooks?.length, 1);
});

test('validateAgentDefinition warns on a non-array hooks value, accepts an array', () => {
  const bad = validateAgentDefinition({ name: 'agent-x', systemPrompt: 'p', hooks: 'nope' as unknown as HookConfigEntry[] });
  assert.ok(bad.warnings.some((w) => w.toLowerCase().includes('hooks')));
  const ok = validateAgentDefinition({
    name: 'agent-x',
    systemPrompt: 'p',
    hooks: [{ event: 'preToolUse', hook: { type: 'command', command: 'x' } }],
  });
  assert.ok(!ok.warnings.some((w) => w.toLowerCase().includes('hooks')));
});

// ---------------------------------------------------------------------------
// HookManager.registerConfigEntries — invocation-scoped registration + gating
// ---------------------------------------------------------------------------

test('registerConfigEntries installs a frontmatter hook (visible in the audit view)', () => {
  const hm = new HookManager();
  hm.registerConfigEntries(
    [{ event: 'preToolUse', matcher: 'bash', hook: { type: 'command', command: 'echo "DENY: blocked"' } }],
    process.cwd(),
  );
  const rows = hm.getHookAuditView();
  const row = rows.find((r) => r.event === 'preToolUse');
  assert.ok(row, 'preToolUse hook should be registered');
  assert.equal(row!.type, 'command');
});

test('registerConfigEntries hook actually fires and can DENY a matching tool call', async () => {
  const hm = new HookManager();
  hm.registerConfigEntries(
    [{ event: 'preToolUse', matcher: 'bash', hook: { type: 'command', command: 'echo "DENY: blocked by test hook"' } }],
    process.cwd(),
  );
  const decision = await hm.executePreToolUse({
    toolName: 'bash',
    toolInput: { command: 'ls' },
    messages: [],
    turnCount: 0,
  });
  assert.equal(decision.action, 'deny');

  // A NON-matching tool is allowed (matcher scopes the hook to bash).
  const allow = await hm.executePreToolUse({
    toolName: 'file_read',
    toolInput: { path: 'x' },
    messages: [],
    turnCount: 0,
  });
  assert.equal(allow.action, 'allow');
});

test('registerConfigEntries respects disableAllHooks (no registration)', () => {
  const hm = new HookManager();
  hm.setDisableAllHooks(true, true);
  hm.registerConfigEntries(
    [{ event: 'preToolUse', hook: { type: 'command', command: 'echo x' } }],
    process.cwd(),
  );
  assert.equal(hm.getHookAuditView().length, 0);
});

test('registerConfigEntries skips author hooks under allowManagedHooksOnly', () => {
  const hm = new HookManager();
  hm.setAllowManagedHooksOnly(true);
  hm.registerConfigEntries(
    [{ event: 'preToolUse', hook: { type: 'command', command: 'echo x' } }],
    process.cwd(),
  );
  assert.equal(hm.getHookAuditView().length, 0);
});

test('registerConfigEntries ignores empty + structurally-invalid entries', () => {
  const hm = new HookManager();
  hm.registerConfigEntries([], process.cwd());
  assert.equal(hm.getHookAuditView().length, 0);
  hm.registerConfigEntries(
    [
      { event: 'preToolUse' } as unknown as HookConfigEntry, // missing hook
      { hook: { type: 'command', command: 'x' } } as unknown as HookConfigEntry, // missing event
    ],
    process.cwd(),
  );
  assert.equal(hm.getHookAuditView().length, 0);
});
