import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { loadCommands, isCommandModelListable } from '../src/plugins';

// ---------------------------------------------------------------------------
// U028 w112 — plugins.ts parity building blocks:
//   - CommandDefinition.hasUserSpecifiedDescription: records whether the
//     description was author-specified (TOML/frontmatter `description`) vs
//     auto-derived from the prompt/body first line (Claude parity:
//     Command.hasUserSpecifiedDescription).
//   - isCommandModelListable: a plugin/MCP command is advertised to the model
//     only when it carries an explicit description OR a whenToUse trigger
//     (Claude parity: getSkillToolCommands' `hasUserSpecifiedDescription ||
//     whenToUse` gate).
// ---------------------------------------------------------------------------

let dir: string;
beforeEach(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-u028-w112-')); });
afterEach(() => { try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* best effort */ } });

describe('hasUserSpecifiedDescription — explicit vs derived description', () => {
  it('TOML: marks true when description is author-specified', () => {
    fs.writeFileSync(path.join(dir, 'deploy.toml'), 'description = "Deploy the app"\nprompt = "Run the deploy"\n');
    const [cmd] = loadCommands(dir);
    assert.equal(cmd.description, 'Deploy the app');
    assert.equal(cmd.hasUserSpecifiedDescription, true);
  });

  it('TOML: omits the flag and derives the description when none is declared', () => {
    fs.writeFileSync(path.join(dir, 'min.toml'), 'prompt = "First line of the prompt"\n');
    const [cmd] = loadCommands(dir);
    assert.equal(cmd.description, 'First line of the prompt'); // derived from the prompt
    assert.equal(cmd.hasUserSpecifiedDescription, undefined);
  });

  it('markdown: marks true from an explicit frontmatter description', () => {
    fs.writeFileSync(path.join(dir, 'review.md'), '---\ndescription: Review the PR\n---\nReview the changes carefully.\n');
    const [cmd] = loadCommands(dir);
    assert.equal(cmd.description, 'Review the PR');
    assert.equal(cmd.hasUserSpecifiedDescription, true);
  });

  it('markdown: omits the flag when the description is derived from the body', () => {
    fs.writeFileSync(path.join(dir, 'bare.md'), '# Fix the bug\n\nActually do the fix.\n');
    const [cmd] = loadCommands(dir);
    assert.equal(cmd.description, 'Fix the bug'); // derived from the first body line (header stripped)
    assert.equal(cmd.hasUserSpecifiedDescription, undefined);
  });

  it('commandsMetadata description override marks the flag true', () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'si-u028-meta-'));
    try {
      fs.writeFileSync(path.join(root, 'ship.toml'), 'prompt = "Ship it"\n');
      const cmds = loadCommands(root, undefined, undefined, {
        root,
        entries: { ship: { source: 'ship.toml', description: 'Author-specified override' } },
      });
      const cmd = cmds.find(c => c.name === 'ship')!;
      assert.equal(cmd.description, 'Author-specified override');
      assert.equal(cmd.hasUserSpecifiedDescription, true);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});

describe('isCommandModelListable — plugin/MCP model-listing gate', () => {
  it('lists a command with an explicit description', () => {
    assert.equal(isCommandModelListable({ hasUserSpecifiedDescription: true }), true);
  });

  it('lists a command with a whenToUse trigger even without an explicit description', () => {
    assert.equal(isCommandModelListable({ whenToUse: 'when deploying' }), true);
  });

  it('withholds a command whose description was only auto-derived', () => {
    assert.equal(isCommandModelListable({ hasUserSpecifiedDescription: undefined, whenToUse: undefined }), false);
    assert.equal(isCommandModelListable({ hasUserSpecifiedDescription: false }), false);
  });

  it('treats a blank whenToUse as absent', () => {
    assert.equal(isCommandModelListable({ whenToUse: '   ' }), false);
  });
});
