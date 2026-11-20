import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  sanitizeAgentTypeForPath,
  getAgentMemoryDir,
  getAgentMemoryEntrypoint,
  buildAgentMemoryPrompt,
  AGENT_REMOTE_MEMORY_DIR_ENV,
} from '../src/memory';
import { resolveConfigDir, projectSlug, canonicalGitRoot } from '../src/reference-journal';

/**
 * U016 — per-agent persistent memory primitives (parity: the reference
 * AgentTool/agentMemory.ts: sanitizeAgentTypeForPath / getAgentMemoryDir /
 * getAgentMemoryEntrypoint / getMemoryScopeDisplay / loadAgentMemoryPrompt).
 *
 * Covers: agent-type path sanitization (colon→dash + traversal safety), the
 * three storage scopes (user/project/local), the AMI_REMOTE_MEMORY_DIR redirect
 * for local scope with per-project namespacing, the MEMORY.md entrypoint path,
 * the scope-display labels, and the "Persistent Agent Memory" prompt builder
 * (empty-affordance, dir-seeding, scope note, entrypoint truncation).
 */

let tmpDir: string;
let configDir: string;
let env: NodeJS.ProcessEnv;

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-agentmem-'));
  configDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-agentmem-cfg-'));
  // Isolated env: pin the config dir so user-scope resolves into a temp location,
  // and ensure the remote-memory redirect is unset unless a test opts in.
  env = { SUPERINFERENCE_DATA_DIR: configDir };
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
  fs.rmSync(configDir, { recursive: true, force: true });
});

describe('sanitizeAgentTypeForPath', () => {
  it('maps colons (plugin-namespaced types) to dashes', () => {
    assert.equal(sanitizeAgentTypeForPath('my-plugin:my-agent'), 'my-plugin-my-agent');
    assert.equal(sanitizeAgentTypeForPath('a:b:c'), 'a-b-c');
  });

  it('leaves a plain agent type unchanged', () => {
    assert.equal(sanitizeAgentTypeForPath('reviewer'), 'reviewer');
  });

  it('neutralizes path separators and traversal runs (safety)', () => {
    assert.equal(sanitizeAgentTypeForPath('a/b'), 'a-b');
    assert.equal(sanitizeAgentTypeForPath('a\\b'), 'a-b');
    assert.equal(sanitizeAgentTypeForPath('..'), '-');
    // No separator or '..' traversal token survives — the result is always a
    // single safe path component that cannot escape its parent dir.
    for (const probe of ['../../etc', 'x/../y', 'a\\..\\b']) {
      const out = sanitizeAgentTypeForPath(probe);
      assert.ok(!out.includes('..'), out);
      assert.ok(!out.includes('/') && !out.includes('\\'), out);
    }
  });
});

describe('getAgentMemoryDir — scopes', () => {
  it('user scope resolves under the global config dir (cross-project)', () => {
    const dir = getAgentMemoryDir('reviewer', 'user', tmpDir, env);
    assert.equal(dir, path.join(resolveConfigDir(env), 'agent-memory', 'reviewer'));
  });

  it('project scope resolves under <git-root>/.ami/agent-memory (committed)', () => {
    const dir = getAgentMemoryDir('reviewer', 'project', tmpDir, env);
    assert.equal(dir, path.join(canonicalGitRoot(tmpDir), '.ami', 'agent-memory', 'reviewer'));
  });

  it('local scope resolves under <git-root>/.ami/agent-memory-local (not committed)', () => {
    const dir = getAgentMemoryDir('reviewer', 'local', tmpDir, env);
    assert.equal(dir, path.join(canonicalGitRoot(tmpDir), '.ami', 'agent-memory-local', 'reviewer'));
  });

  it('namespaces by the sanitized agent type', () => {
    const dir = getAgentMemoryDir('plug:agent', 'project', tmpDir, env);
    assert.ok(dir.endsWith(path.join('agent-memory', 'plug-agent')), dir);
  });
});

describe('getAgentMemoryDir — AMI_REMOTE_MEMORY_DIR redirect (local scope only)', () => {
  it('redirects local scope to the mount with per-project namespacing', () => {
    const mount = path.join(tmpDir, 'mount');
    const redirectEnv = { ...env, [AGENT_REMOTE_MEMORY_DIR_ENV]: mount };
    const dir = getAgentMemoryDir('reviewer', 'local', tmpDir, redirectEnv);
    assert.equal(
      dir,
      path.join(mount, 'projects', projectSlug(canonicalGitRoot(tmpDir)), 'agent-memory-local', 'reviewer'),
    );
  });

  it('does NOT redirect user or project scope', () => {
    const mount = path.join(tmpDir, 'mount');
    const redirectEnv = { ...env, [AGENT_REMOTE_MEMORY_DIR_ENV]: mount };
    assert.ok(!getAgentMemoryDir('r', 'user', tmpDir, redirectEnv).startsWith(mount));
    assert.ok(!getAgentMemoryDir('r', 'project', tmpDir, redirectEnv).startsWith(mount));
  });

  it('a blank redirect value falls back to the in-repo local dir', () => {
    const redirectEnv = { ...env, [AGENT_REMOTE_MEMORY_DIR_ENV]: '   ' };
    const dir = getAgentMemoryDir('reviewer', 'local', tmpDir, redirectEnv);
    assert.equal(dir, path.join(canonicalGitRoot(tmpDir), '.ami', 'agent-memory-local', 'reviewer'));
  });
});

describe('getAgentMemoryEntrypoint', () => {
  it('appends MEMORY.md to the scope dir', () => {
    const ep = getAgentMemoryEntrypoint('reviewer', 'project', tmpDir, env);
    assert.equal(ep, path.join(getAgentMemoryDir('reviewer', 'project', tmpDir, env), 'MEMORY.md'));
  });
});

describe('buildAgentMemoryPrompt', () => {
  it('seeds the agent-memory directory and shows the empty affordance', () => {
    const prompt = buildAgentMemoryPrompt('reviewer', 'project', tmpDir, env);
    const dir = getAgentMemoryDir('reviewer', 'project', tmpDir, env);
    assert.ok(fs.existsSync(dir), 'agent-memory dir should be created on build');
    assert.ok(prompt.includes('# Persistent Agent Memory'), prompt);
    assert.ok(prompt.includes('`reviewer` agent'), prompt);
    assert.ok(prompt.includes('## MEMORY.md'), prompt);
    assert.ok(prompt.includes('currently empty'), prompt);
  });

  it('carries the scope-specific note', () => {
    assert.match(buildAgentMemoryPrompt('r', 'user', tmpDir, env), /user-scope and applies across every project/);
    assert.match(buildAgentMemoryPrompt('r', 'project', tmpDir, env), /shared with your team via version control/);
    assert.match(buildAgentMemoryPrompt('r', 'local', tmpDir, env), /NOT checked into version control/);
  });

  it('includes the existing MEMORY.md entrypoint content when present', () => {
    const dir = getAgentMemoryDir('reviewer', 'project', tmpDir, env);
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'MEMORY.md'), '- [Role](role.md) — the user prefers terse answers\n');
    const prompt = buildAgentMemoryPrompt('reviewer', 'project', tmpDir, env);
    assert.ok(prompt.includes('the user prefers terse answers'), prompt);
    assert.ok(!prompt.includes('currently empty'), prompt);
  });

  it('size-caps an oversized entrypoint (truncateEntrypointContent)', () => {
    const dir = getAgentMemoryDir('reviewer', 'project', tmpDir, env);
    fs.mkdirSync(dir, { recursive: true });
    // Far more than MAX_ENTRYPOINT_LINES (200) lines → must be truncated.
    const huge = Array.from({ length: 500 }, (_, i) => `- line ${i}`).join('\n');
    fs.writeFileSync(path.join(dir, 'MEMORY.md'), huge);
    const prompt = buildAgentMemoryPrompt('reviewer', 'project', tmpDir, env);
    assert.ok(!prompt.includes('line 499'), 'tail lines must be dropped by the entrypoint cap');
  });

  it('reuses the canonical save/type guidance (no duplicate copy)', () => {
    const prompt = buildAgentMemoryPrompt('reviewer', 'project', tmpDir, env);
    // These headings come from the shared guidance constants used by the global
    // auto-memory prompt — proving the agent prompt consolidates, not duplicates.
    assert.ok(prompt.includes('### What NOT to save'), prompt);
    assert.ok(prompt.includes('two step') || prompt.includes('two-step') || prompt.includes('two steps'), prompt);
  });
});
