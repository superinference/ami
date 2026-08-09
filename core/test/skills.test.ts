import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { SkillManager } from '../src/skills';

let tmpDir: string;

function createTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'si-skills-test-'));
}

function cleanupTmpDir(dir: string): void {
  try {
    fs.rmSync(dir, { recursive: true, force: true });
  } catch {
    // Best-effort cleanup
  }
}

describe('SkillManager', () => {
  beforeEach(() => {
    tmpDir = createTmpDir();
  });

  afterEach(() => {
    cleanupTmpDir(tmpDir);
  });

  // --- Built-in skills ---

  describe('built-in skills', () => {
    it('lists built-in skills when no project skills exist', () => {
      const mgr = new SkillManager(tmpDir);
      const skills = mgr.listSkills();

      assert.ok(skills.length >= 4, `Expected at least 4 built-in skills, got ${skills.length}`);
      const names = skills.map(s => s.name);
      assert.ok(names.includes('code-review'));
      assert.ok(names.includes('explain'));
      assert.ok(names.includes('refactor'));
      assert.ok(names.includes('test-gen'));
    });

    it('returns built-in skills by name', () => {
      const mgr = new SkillManager(tmpDir);
      const skill = mgr.getSkill('code-review');

      assert.ok(skill !== null);
      assert.equal(skill!.name, 'code-review');
      assert.equal(skill!.filePath, '<builtin>');
      assert.ok(skill!.content.includes('Bugs and logic errors'));
    });

    it('returns null for non-existent skill', () => {
      const mgr = new SkillManager(tmpDir);
      assert.equal(mgr.getSkill('nonexistent'), null);
    });
  });

  // --- Loading skills from directory ---

  describe('loadSkillsDir', () => {
    it('loads a SKILL.md from a subdirectory', () => {
      const skillDir = path.join(tmpDir, '.superinference', 'skills', 'my-lint');
      fs.mkdirSync(skillDir, { recursive: true });
      fs.writeFileSync(
        path.join(skillDir, 'SKILL.md'),
        `---
name: my-lint
description: Lint code with custom rules
when-to-use: When user asks to lint
paths: ["*.ts", "*.js"]
---

Check for:
1. Unused variables
2. Missing types
`,
      );

      const mgr = new SkillManager(tmpDir);
      const skill = mgr.getSkill('my-lint');

      assert.ok(skill !== null);
      assert.equal(skill!.name, 'my-lint');
      assert.equal(skill!.description, 'Lint code with custom rules');
      assert.equal(skill!.whenToUse, 'When user asks to lint');
      assert.deepEqual(skill!.paths, ['*.ts', '*.js']);
      assert.ok(skill!.content.includes('Unused variables'));
    });

    it('loads a direct .md file in skills directory', () => {
      const skillDir = path.join(tmpDir, '.superinference', 'skills');
      fs.mkdirSync(skillDir, { recursive: true });
      fs.writeFileSync(
        path.join(skillDir, 'docs.md'),
        `---
name: docs
description: Generate documentation
---

Write clear JSDoc comments.
`,
      );

      const mgr = new SkillManager(tmpDir);
      const skill = mgr.getSkill('docs');

      assert.ok(skill !== null);
      assert.equal(skill!.name, 'docs');
      assert.equal(skill!.description, 'Generate documentation');
    });

    it('project skills override built-in skills with the same name', () => {
      const skillDir = path.join(tmpDir, '.superinference', 'skills');
      fs.mkdirSync(skillDir, { recursive: true });
      fs.writeFileSync(
        path.join(skillDir, 'code-review.md'),
        `---
name: code-review
description: Custom code review
---

Custom review instructions.
`,
      );

      const mgr = new SkillManager(tmpDir);
      const skill = mgr.getSkill('code-review');

      assert.ok(skill !== null);
      assert.equal(skill!.description, 'Custom code review');
      assert.ok(skill!.content.includes('Custom review instructions'));
    });

    it('skips skill files with empty body', () => {
      const skillDir = path.join(tmpDir, '.superinference', 'skills');
      fs.mkdirSync(skillDir, { recursive: true });
      fs.writeFileSync(
        path.join(skillDir, 'empty.md'),
        `---
name: empty-skill
description: Empty skill
---
`,
      );

      const mgr = new SkillManager(tmpDir);
      assert.equal(mgr.getSkill('empty-skill'), null);
    });
  });

  // --- Parse SKILL.md with frontmatter ---

  describe('skill file parsing', () => {
    it('parses user-invocable: false', () => {
      const skillDir = path.join(tmpDir, '.superinference', 'skills');
      fs.mkdirSync(skillDir, { recursive: true });
      fs.writeFileSync(
        path.join(skillDir, 'internal.md'),
        `---
name: internal-helper
description: Internal helper, not user-invocable
user-invocable: false
---

Internal instructions.
`,
      );

      const mgr = new SkillManager(tmpDir);
      const skill = mgr.getSkill('internal-helper');
      assert.ok(skill !== null);
      assert.equal(skill!.userInvocable, false);

      // Should NOT appear in listSkills
      const listed = mgr.listSkills();
      assert.ok(!listed.some(s => s.name === 'internal-helper'));
    });

    it('parses model override', () => {
      const skillDir = path.join(tmpDir, '.superinference', 'skills');
      fs.mkdirSync(skillDir, { recursive: true });
      fs.writeFileSync(
        path.join(skillDir, 'fast.md'),
        `---
name: fast-skill
description: Fast skill
model: gemini-2.0-flash
---

Fast response instructions.
`,
      );

      const mgr = new SkillManager(tmpDir);
      const skill = mgr.getSkill('fast-skill');
      assert.ok(skill !== null);
      assert.equal(skill!.model, 'gemini-2.0-flash');
    });

    it('handles file without frontmatter', () => {
      const skillDir = path.join(tmpDir, '.superinference', 'skills', 'raw');
      fs.mkdirSync(skillDir, { recursive: true });
      fs.writeFileSync(
        path.join(skillDir, 'SKILL.md'),
        'Just plain instructions without frontmatter.',
      );

      const mgr = new SkillManager(tmpDir);
      // Name derived from parent directory name
      const skill = mgr.getSkill('raw');
      assert.ok(skill !== null);
      assert.equal(skill!.name, 'raw');
      assert.ok(skill!.content.includes('Just plain instructions'));
    });
  });

  // --- Variable substitution ---

  describe('variable substitution', () => {
    it('substitutes ${CWD} in skill content', () => {
      const skillDir = path.join(tmpDir, '.superinference', 'skills');
      fs.mkdirSync(skillDir, { recursive: true });
      fs.writeFileSync(
        path.join(skillDir, 'paths.md'),
        `---
name: paths
description: Skill with variables
---

Working dir: \${CWD}
Skill dir: \${SKILL_DIR}
Model: \${MODEL}
Date: \${DATE}
`,
      );

      const mgr = new SkillManager(tmpDir);
      const content = mgr.getSkillContent('paths');

      assert.ok(content !== null);
      assert.ok(content!.includes(`Working dir: ${tmpDir}`));
      assert.ok(content!.includes(`Skill dir: ${skillDir}`));
      assert.ok(content!.includes('Model: default'));
      // Date should be YYYY-MM-DD
      assert.ok(/Date: \d{4}-\d{2}-\d{2}/.test(content!));
    });

    it('substitutes model when skill has model override', () => {
      const skillDir = path.join(tmpDir, '.superinference', 'skills');
      fs.mkdirSync(skillDir, { recursive: true });
      fs.writeFileSync(
        path.join(skillDir, 'modeled.md'),
        `---
name: modeled
description: Test
model: gpt-4o
---

Using model: \${MODEL}
`,
      );

      const mgr = new SkillManager(tmpDir);
      const content = mgr.getSkillContent('modeled');

      assert.ok(content !== null);
      assert.ok(content!.includes('Using model: gpt-4o'));
    });

    it('applies user-supplied args', () => {
      const mgr = new SkillManager(tmpDir);
      // Built-in 'code-review' won't have custom vars, so create one
      const skillDir = path.join(tmpDir, '.superinference', 'skills');
      fs.mkdirSync(skillDir, { recursive: true });
      fs.writeFileSync(
        path.join(skillDir, 'custom.md'),
        `---
name: custom
description: Custom skill
---

Review file: \${FILE}
In language: \${LANG}
`,
      );

      const mgr2 = new SkillManager(tmpDir);
      const content = mgr2.getSkillContent('custom', { FILE: 'app.ts', LANG: 'TypeScript' });

      assert.ok(content !== null);
      assert.ok(content!.includes('Review file: app.ts'));
      assert.ok(content!.includes('In language: TypeScript'));
    });

    it('returns null for non-existent skill', () => {
      const mgr = new SkillManager(tmpDir);
      assert.equal(mgr.getSkillContent('does-not-exist'), null);
    });
  });

  // --- Skill discovery by name ---

  describe('getSkill', () => {
    it('finds skill by exact name', () => {
      const mgr = new SkillManager(tmpDir);
      assert.ok(mgr.getSkill('explain') !== null);
    });

    it('returns null for unknown name', () => {
      const mgr = new SkillManager(tmpDir);
      assert.equal(mgr.getSkill('unknown-xyz'), null);
    });
  });

  // --- Path-based matching ---

  describe('findMatchingSkills', () => {
    it('matches skills by file extension patterns', () => {
      const skillDir = path.join(tmpDir, '.superinference', 'skills');
      fs.mkdirSync(skillDir, { recursive: true });
      fs.writeFileSync(
        path.join(skillDir, 'ts-lint.md'),
        `---
name: ts-lint
description: TypeScript linter
paths: ["*.ts", "*.tsx"]
---

Lint TypeScript files.
`,
      );
      fs.writeFileSync(
        path.join(skillDir, 'py-lint.md'),
        `---
name: py-lint
description: Python linter
paths: ["*.py"]
---

Lint Python files.
`,
      );

      const mgr = new SkillManager(tmpDir);

      const tsMatches = mgr.findMatchingSkills(['src/app.ts']);
      assert.ok(tsMatches.some(s => s.name === 'ts-lint'));
      assert.ok(!tsMatches.some(s => s.name === 'py-lint'));

      const pyMatches = mgr.findMatchingSkills(['scripts/main.py']);
      assert.ok(pyMatches.some(s => s.name === 'py-lint'));
      assert.ok(!pyMatches.some(s => s.name === 'ts-lint'));
    });

    it('returns empty when no patterns match', () => {
      const mgr = new SkillManager(tmpDir);
      const matches = mgr.findMatchingSkills(['image.png']);
      // Built-in skills don't have paths, so nothing should match
      assert.equal(matches.length, 0);
    });

    it('handles skills without paths', () => {
      const mgr = new SkillManager(tmpDir);
      // Built-ins have no paths — they should not appear in path-based matches
      const matches = mgr.findMatchingSkills(['anything.ts']);
      assert.equal(matches.length, 0);
    });
  });

  // --- AGENTS.md loading ---

  describe('AGENTS.md', () => {
    it('loads AGENTS.md from workspace root', () => {
      fs.writeFileSync(
        path.join(tmpDir, 'AGENTS.md'),
        '# Project Agents\n\nAll agents should follow the project coding standards.',
      );

      const mgr = new SkillManager(tmpDir);
      const content = mgr.loadAgentsFile();

      assert.ok(content.includes('Project Agents'));
      assert.ok(content.includes('coding standards'));
    });

    it('returns empty string when AGENTS.md does not exist', () => {
      const mgr = new SkillManager(tmpDir);
      assert.equal(mgr.loadAgentsFile(), '');
    });

    it('includes AGENTS.md in skill context', () => {
      fs.writeFileSync(
        path.join(tmpDir, 'AGENTS.md'),
        '# Agent Rules\n\nBe concise.',
      );

      const mgr = new SkillManager(tmpDir);
      const context = mgr.getSkillContext();

      assert.ok(context.includes('## AGENTS.md'));
      assert.ok(context.includes('Be concise.'));
    });
  });

  // --- Agent definition parsing ---

  describe('agent definitions', () => {
    it('loads agent from .superinference/agents/', () => {
      const agentDir = path.join(tmpDir, '.superinference', 'agents');
      fs.mkdirSync(agentDir, { recursive: true });
      fs.writeFileSync(
        path.join(agentDir, 'test-writer.md'),
        `---
name: test-writer
description: Specialized agent for writing tests
model: gemini-2.0-flash
max-turns: 10
tools: [file_read, file_write, file_edit, bash, grep, glob]
---

You are a test writing specialist. Given a source file, generate comprehensive tests.
`,
      );

      const mgr = new SkillManager(tmpDir);
      const agent = mgr.getAgent('test-writer');

      assert.ok(agent !== null);
      assert.equal(agent!.name, 'test-writer');
      assert.equal(agent!.description, 'Specialized agent for writing tests');
      assert.equal(agent!.model, 'gemini-2.0-flash');
      assert.equal(agent!.maxTurns, 10);
      assert.deepEqual(agent!.tools, ['file_read', 'file_write', 'file_edit', 'bash', 'grep', 'glob']);
      assert.ok(agent!.systemPrompt.includes('test writing specialist'));
    });

    it('lists all agents', () => {
      const agentDir = path.join(tmpDir, '.superinference', 'agents');
      fs.mkdirSync(agentDir, { recursive: true });
      fs.writeFileSync(
        path.join(agentDir, 'agent-a.md'),
        `---
name: agent-a
description: Agent A
---

Agent A instructions.
`,
      );
      fs.writeFileSync(
        path.join(agentDir, 'agent-b.md'),
        `---
name: agent-b
description: Agent B
---

Agent B instructions.
`,
      );

      const mgr = new SkillManager(tmpDir);
      const agents = mgr.listAgents();

      assert.equal(agents.length, 2);
      const names = agents.map(a => a.name);
      assert.ok(names.includes('agent-a'));
      assert.ok(names.includes('agent-b'));
    });

    it('returns null for non-existent agent', () => {
      const mgr = new SkillManager(tmpDir);
      assert.equal(mgr.getAgent('no-such-agent'), null);
    });

    it('handles agent without optional fields', () => {
      const agentDir = path.join(tmpDir, '.superinference', 'agents');
      fs.mkdirSync(agentDir, { recursive: true });
      fs.writeFileSync(
        path.join(agentDir, 'minimal.md'),
        `---
name: minimal-agent
description: A minimal agent
---

Do the work.
`,
      );

      const mgr = new SkillManager(tmpDir);
      const agent = mgr.getAgent('minimal-agent');

      assert.ok(agent !== null);
      assert.equal(agent!.model, undefined);
      assert.equal(agent!.maxTurns, undefined);
      assert.equal(agent!.tools, undefined);
      assert.equal(agent!.disallowedTools, undefined);
    });

    it('parses disallowed-tools', () => {
      const agentDir = path.join(tmpDir, '.superinference', 'agents');
      fs.mkdirSync(agentDir, { recursive: true });
      fs.writeFileSync(
        path.join(agentDir, 'restricted.md'),
        `---
name: restricted
description: Restricted agent
disallowed-tools: [bash, file_write]
---

Restricted agent instructions.
`,
      );

      const mgr = new SkillManager(tmpDir);
      const agent = mgr.getAgent('restricted');

      assert.ok(agent !== null);
      assert.deepEqual(agent!.disallowedTools, ['bash', 'file_write']);
    });

    it('skips agent files with empty body', () => {
      const agentDir = path.join(tmpDir, '.superinference', 'agents');
      fs.mkdirSync(agentDir, { recursive: true });
      fs.writeFileSync(
        path.join(agentDir, 'empty.md'),
        `---
name: empty-agent
description: Empty
---
`,
      );

      const mgr = new SkillManager(tmpDir);
      assert.equal(mgr.getAgent('empty-agent'), null);
    });
  });

  // --- getSkillContext formatting ---

  describe('getSkillContext', () => {
    it('includes skills section with built-in skills', () => {
      const mgr = new SkillManager(tmpDir);
      const context = mgr.getSkillContext();

      assert.ok(context.includes('## Available Skills'));
      assert.ok(context.includes('/code-review'));
      assert.ok(context.includes('/explain'));
    });

    it('includes agents section when agents are defined', () => {
      const agentDir = path.join(tmpDir, '.superinference', 'agents');
      fs.mkdirSync(agentDir, { recursive: true });
      fs.writeFileSync(
        path.join(agentDir, 'builder.md'),
        `---
name: builder
description: Build agent
model: claude-3.5-sonnet
---

Build things.
`,
      );

      const mgr = new SkillManager(tmpDir);
      const context = mgr.getSkillContext();

      assert.ok(context.includes('## Available Agents'));
      assert.ok(context.includes('builder'));
      assert.ok(context.includes('claude-3.5-sonnet'));
    });

    it('includes AGENTS.md, skills, and agents in combined context', () => {
      // Set up AGENTS.md
      fs.writeFileSync(path.join(tmpDir, 'AGENTS.md'), 'Follow these rules.');

      // Set up a project skill
      const skillDir = path.join(tmpDir, '.superinference', 'skills');
      fs.mkdirSync(skillDir, { recursive: true });
      fs.writeFileSync(
        path.join(skillDir, 'deploy.md'),
        `---
name: deploy
description: Deploy the app
---

Run deploy script.
`,
      );

      // Set up an agent
      const agentDir = path.join(tmpDir, '.superinference', 'agents');
      fs.mkdirSync(agentDir, { recursive: true });
      fs.writeFileSync(
        path.join(agentDir, 'qa.md'),
        `---
name: qa
description: QA agent
---

Test everything.
`,
      );

      const mgr = new SkillManager(tmpDir);
      const context = mgr.getSkillContext();

      assert.ok(context.includes('## AGENTS.md'));
      assert.ok(context.includes('Follow these rules.'));
      assert.ok(context.includes('## Available Skills'));
      assert.ok(context.includes('/deploy'));
      assert.ok(context.includes('## Available Agents'));
      assert.ok(context.includes('qa'));
    });

    it('returns empty string when nothing is defined and no built-ins match', () => {
      // Built-in skills always exist, so context should not be empty
      const mgr = new SkillManager(tmpDir);
      const context = mgr.getSkillContext();
      // Should at least have built-in skills
      assert.ok(context.length > 0);
    });

    it('shows whenToUse hint for skills that have it', () => {
      const skillDir = path.join(tmpDir, '.superinference', 'skills');
      fs.mkdirSync(skillDir, { recursive: true });
      fs.writeFileSync(
        path.join(skillDir, 'security.md'),
        `---
name: security
description: Security audit
when-to-use: When reviewing auth or crypto code
---

Audit for security issues.
`,
      );

      const mgr = new SkillManager(tmpDir);
      const context = mgr.getSkillContext();

      assert.ok(context.includes('When: When reviewing auth or crypto code'));
    });
  });
});

// ---------------------------------------------------------------------------
// substituteVars (lines 155-164) — variable substitution in skill content
// ---------------------------------------------------------------------------
describe('SkillManager — variable substitution', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-skills-vars-'));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('substitutes ${CWD}, ${DATE}, ${MODEL}, ${SKILL_DIR}', () => {
    const skillDir = path.join(tmpDir, '.superinference', 'skills');
    fs.mkdirSync(skillDir, { recursive: true });
    fs.writeFileSync(
      path.join(skillDir, 'vartest.md'),
      `---
name: vartest
description: Tests variable substitution
model: claude-sonnet-4
---

CWD is \${CWD}
Date is \${DATE}
Model is \${MODEL}
SkillDir is \${SKILL_DIR}
`,
    );

    const mgr = new SkillManager(tmpDir);
    const content = mgr.getSkillContent('vartest');

    assert.ok(content);
    assert.ok(content!.includes(`CWD is ${tmpDir}`), 'Should substitute CWD');
    assert.ok(!content!.includes('${CWD}'), 'Should not have raw ${CWD}');
    assert.ok(!content!.includes('${DATE}'), 'Should not have raw ${DATE}');
    assert.ok(content!.includes('claude-sonnet-4'), 'Should substitute MODEL');
  });
});

// ---------------------------------------------------------------------------
// getSkillContent with args — $ARGUMENTS and positional params (lines 267-269, 285-288)
// ---------------------------------------------------------------------------
describe('SkillManager — getSkillContent with args', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-skills-args-'));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('substitutes $ARGUMENTS and positional $0, $1, $2', () => {
    const skillDir = path.join(tmpDir, '.superinference', 'skills');
    fs.mkdirSync(skillDir, { recursive: true });
    fs.writeFileSync(
      path.join(skillDir, 'argskill.md'),
      `---
name: argskill
description: Tests argument substitution
---

Full args: $ARGUMENTS
Also: \${ARGUMENTS}
First: $0
Second: $1
Third: $2
`,
    );

    const mgr = new SkillManager(tmpDir);
    const content = mgr.getSkillContent('argskill', { ARGUMENTS: 'hello world' });

    assert.ok(content);
    assert.ok(content!.includes('Full args: hello world'));
    assert.ok(content!.includes('Also: hello world'));
    assert.ok(content!.includes('First: hello world')); // $0 = full args
    assert.ok(content!.includes('Second: hello'));       // $1 = first token
    assert.ok(content!.includes('Third: world'));        // $2 = second token
  });

  it('substitutes custom variables via args', () => {
    const skillDir = path.join(tmpDir, '.superinference', 'skills');
    fs.mkdirSync(skillDir, { recursive: true });
    fs.writeFileSync(
      path.join(skillDir, 'customvar.md'),
      `---
name: customvar
description: Tests custom variables
---

Target: \${TARGET}
`,
    );

    const mgr = new SkillManager(tmpDir);
    const content = mgr.getSkillContent('customvar', { TARGET: '/src/main.ts' });

    assert.ok(content);
    assert.ok(content!.includes('Target: /src/main.ts'));
  });

  it('returns null for nonexistent skill', () => {
    const mgr = new SkillManager(tmpDir);
    const content = mgr.getSkillContent('nonexistent');
    assert.equal(content, null);
  });
});

// ---------------------------------------------------------------------------
// loadAgentsFile (lines 276-286) — AGENTS.md loading
// ---------------------------------------------------------------------------
describe('SkillManager — loadAgentsFile', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-skills-agents-'));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('returns empty string when AGENTS.md does not exist', () => {
    const mgr = new SkillManager(tmpDir);
    const result = mgr.loadAgentsFile();
    assert.equal(result, '');
  });

  it('returns empty string when AGENTS.md is a directory', () => {
    fs.mkdirSync(path.join(tmpDir, 'AGENTS.md'));
    const mgr = new SkillManager(tmpDir);
    const result = mgr.loadAgentsFile();
    assert.equal(result, '');
  });
});

describe('SkillManager — direct .md name fallback', () => {
  afterEach(() => { cleanupTmpDir(tmpDir); });

  it('uses filename (not parent dir) as fallback name for direct .md files', () => {
    tmpDir = createTmpDir();
    const skillsDir = path.join(tmpDir, '.superinference', 'skills');
    fs.mkdirSync(skillsDir, { recursive: true });
    fs.writeFileSync(path.join(skillsDir, 'my-skill.md'), '---\ndescription: test\n---\nSkill body here.');
    fs.writeFileSync(path.join(skillsDir, 'other-skill.md'), '---\ndescription: test2\n---\nOther body here.');
    const mgr = new SkillManager(tmpDir);
    const s1 = mgr.getSkill('my-skill');
    const s2 = mgr.getSkill('other-skill');
    assert.ok(s1, 'my-skill should be loaded');
    assert.ok(s2, 'other-skill should be loaded');
    assert.notEqual(s1!.name, s2!.name, 'skills should have different names');
  });
});
