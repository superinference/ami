/**
 * Tests for skills frontmatter parsing:
 * - argumentHint from 'argument-hint' field
 * - context from 'context' field ('inline' | 'fork')
 *
 * SkillManager(cwd) auto-loads from <cwd>/.superinference/skills/
 * expecting SKILL.md files inside subdirectories.
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { SkillManager } from '../src/skills';

let tmpDir: string;
let skillDir: string;

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-skills-'));
  skillDir = path.join(tmpDir, '.superinference', 'skills');
  fs.mkdirSync(skillDir, { recursive: true });
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

function writeSkill(name: string, frontmatter: string, body: string) {
  const dir = path.join(skillDir, name);
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'SKILL.md'), `---\n${frontmatter}\n---\n${body}`);
}

describe('SkillManager — argumentHint parsing', () => {
  it('parses argument-hint from frontmatter', () => {
    writeSkill('test-skill', 'name: test-skill\ndescription: A test\nargument-hint: <file-path>', 'Do something with the file.');
    const mgr = new SkillManager(tmpDir);
    const skill = mgr.getSkill('test-skill');
    assert.ok(skill, 'skill should be found');
    assert.equal(skill!.argumentHint, '<file-path>');
  });

  it('returns undefined when argument-hint is absent', () => {
    writeSkill('no-hint', 'name: no-hint\ndescription: No hint', 'Just a skill.');
    const mgr = new SkillManager(tmpDir);
    const skill = mgr.getSkill('no-hint');
    assert.ok(skill);
    assert.equal(skill!.argumentHint, undefined);
  });
});

describe('SkillManager — context parsing', () => {
  it('parses context: fork from frontmatter', () => {
    writeSkill('fork-skill', 'name: fork-skill\ndescription: Forked\ncontext: fork', 'Run in a fork.');
    const mgr = new SkillManager(tmpDir);
    const skill = mgr.getSkill('fork-skill');
    assert.ok(skill);
    assert.equal(skill!.context, 'fork');
  });

  it('parses context: inline from frontmatter', () => {
    writeSkill('inline-skill', 'name: inline-skill\ndescription: Inline\ncontext: inline', 'Run inline.');
    const mgr = new SkillManager(tmpDir);
    const skill = mgr.getSkill('inline-skill');
    assert.ok(skill);
    assert.equal(skill!.context, 'inline');
  });

  it('returns undefined for unrecognized context values', () => {
    writeSkill('bad-ctx', 'name: bad-ctx\ndescription: Bad\ncontext: invalid', 'Hmm.');
    const mgr = new SkillManager(tmpDir);
    const skill = mgr.getSkill('bad-ctx');
    assert.ok(skill);
    assert.equal(skill!.context, undefined);
  });

  it('returns undefined when context is absent', () => {
    writeSkill('no-ctx', 'name: no-ctx\ndescription: None', 'No context.');
    const mgr = new SkillManager(tmpDir);
    const skill = mgr.getSkill('no-ctx');
    assert.ok(skill);
    assert.equal(skill!.context, undefined);
  });
});
