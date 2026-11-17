// U023 / w82 — Agent `baseDir` provenance + frontmatter-hook plugin-root resolution.
//
// Closes the "source / baseDir provenance fields on agent definitions" gap (the
// baseDir half): an AgentDefinition now carries a `baseDir` bundle/base root, which
// `agentPluginRoot()` resolves (falling back to the agent's on-disk directory),
// mirroring the skill `skillRoot` / `skillPluginRoot` pair. `withFrontmatterHookPluginRoot`
// then stamps that root onto a skill's / agent's invocation-scoped command hooks so a
// `${CLAUDE_PLUGIN_ROOT}`-referencing hook resolves its bundled asset path when the
// sub-engine runs it — a capability agent/skill frontmatter hooks previously lacked
// (only plugin-FILE hooks carried a plugin root).
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { agentPluginRoot, type AgentDefinition } from '../src/skills';
import { withFrontmatterHookPluginRoot, HookManager, type HookConfigEntry } from '../src/hooks';

function agent(partial: Partial<AgentDefinition>): AgentDefinition {
  return { name: 'a', description: 'd', systemPrompt: 'body', filePath: '<builtin>', ...partial };
}

describe('agentPluginRoot (U023/w82)', () => {
  it('prefers an explicit baseDir (trimmed) over the filePath directory', () => {
    const a = agent({ baseDir: '  /plugins/my-bundle  ', filePath: '/elsewhere/a.md' });
    assert.equal(agentPluginRoot(a), '/plugins/my-bundle');
  });

  it('falls back to the directory of the on-disk filePath when no baseDir', () => {
    const a = agent({ filePath: '/home/u/.ami/agents/reviewer.md' });
    assert.equal(agentPluginRoot(a), '/home/u/.ami/agents');
  });

  it('falls back to filePath when baseDir is blank/whitespace', () => {
    const a = agent({ baseDir: '   ', filePath: '/x/y/a.md' });
    assert.equal(agentPluginRoot(a), '/x/y');
  });

  it('returns undefined for a <builtin> sentinel, a bare filename, or no filePath', () => {
    assert.equal(agentPluginRoot(agent({ filePath: '<builtin>' })), undefined);
    assert.equal(agentPluginRoot(agent({ filePath: 'reviewer.md' })), undefined);
    assert.equal(agentPluginRoot(agent({ filePath: '' })), undefined);
  });
});

describe('withFrontmatterHookPluginRoot (U023/w82)', () => {
  const cmd = (command: string, extra: Record<string, unknown> = {}): HookConfigEntry => ({
    event: 'preToolUse',
    hook: { type: 'command', command, ...extra },
  });

  it('stamps the root onto command hooks that lack one, without mutating the input', () => {
    const input: HookConfigEntry[] = [cmd('${CLAUDE_PLUGIN_ROOT}/x.sh')];
    const out = withFrontmatterHookPluginRoot(input, '/bundle')!;
    assert.equal((out[0].hook as { pluginRoot?: string }).pluginRoot, '/bundle');
    // original untouched (a new array + cloned hook object)
    assert.equal((input[0].hook as { pluginRoot?: string }).pluginRoot, undefined);
    assert.notEqual(out, input);
    assert.notEqual(out[0], input[0]);
  });

  it('leaves an already-declared pluginRoot untouched', () => {
    const out = withFrontmatterHookPluginRoot([cmd('x', { pluginRoot: '/explicit' })], '/bundle')!;
    assert.equal((out[0].hook as { pluginRoot?: string }).pluginRoot, '/explicit');
  });

  it('passes non-command hooks through unchanged', () => {
    const promptHook: HookConfigEntry = { event: 'preToolUse', hook: { type: 'prompt', prompt: 'hi' } };
    const out = withFrontmatterHookPluginRoot([promptHook], '/bundle')!;
    assert.equal(out[0], promptHook);
    assert.ok(!('pluginRoot' in (out[0].hook as object)));
  });

  it('returns the input unchanged for a blank/undefined root or an empty list', () => {
    const input = [cmd('x')];
    assert.equal(withFrontmatterHookPluginRoot(input, '   '), input);
    assert.equal(withFrontmatterHookPluginRoot(input, undefined), input);
    assert.equal(withFrontmatterHookPluginRoot([], '/bundle').length, 0);
    assert.equal(withFrontmatterHookPluginRoot(undefined, '/bundle'), undefined);
  });
});

describe('E2E: agent frontmatter command hook resolves ${CLAUDE_PLUGIN_ROOT} (U023/w82)', () => {
  it('a baseDir-stamped agent hook writes to the resolved bundle root', async () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u023-agentroot-'));
    const bundleRoot = path.join(cwd, '.ami', 'plugins', 'teamkit');
    fs.mkdirSync(bundleRoot, { recursive: true });

    // An agent whose frontmatter declares a command hook referencing the bundle root
    // for both the write PATH (${...} placeholder) and the env ($CLAUDE_PLUGIN_ROOT).
    const agentDef = agent({
      name: 'reviewer',
      baseDir: bundleRoot,
      filePath: path.join(bundleRoot, 'agents', 'reviewer.md'),
      hooks: [
        {
          event: 'preToolUse',
          hook: { type: 'command', command: 'printf "%s" "$CLAUDE_PLUGIN_ROOT" > ${CLAUDE_PLUGIN_ROOT}/out.txt' },
        },
      ],
    });

    // The exact chain task.ts runs on spawn.
    const stamped = withFrontmatterHookPluginRoot(agentDef.hooks, agentPluginRoot(agentDef))!;
    assert.equal((stamped[0].hook as { pluginRoot?: string }).pluginRoot, bundleRoot);

    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.registerConfigEntries(stamped, cwd, { isAgent: true, sourceName: "agent 'reviewer'" });

    const decision = await hm.executePreToolUse({
      messages: [], turnCount: 0, toolName: 'bash', toolInput: { command: 'ls' },
    });
    assert.equal(decision.action, 'allow');

    const outPath = path.join(bundleRoot, 'out.txt');
    // RED without the stamping: the hook carries no pluginRoot, so ${CLAUDE_PLUGIN_ROOT}
    // stays literal (nonexistent dir → no file) and $CLAUDE_PLUGIN_ROOT is empty.
    assert.ok(fs.existsSync(outPath), '${CLAUDE_PLUGIN_ROOT} must resolve to the agent baseDir');
    assert.equal(fs.readFileSync(outPath, 'utf-8').trim(), bundleRoot, 'CLAUDE_PLUGIN_ROOT env must equal the baseDir');

    fs.rmSync(cwd, { recursive: true, force: true });
  });
});
