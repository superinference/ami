import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { PersonaManager } from '../src/personas';

describe('PersonaManager', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-personas-'));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('loads built-in personas', () => {
    const pm = new PersonaManager(tmpDir);
    const list = pm.list();
    assert.ok(list.length >= 4);
    assert.ok(list.some(p => p.name === 'code'));
    assert.ok(list.some(p => p.name === 'pentest'));
    assert.ok(list.some(p => p.name === 'sre'));
    assert.ok(list.some(p => p.name === 'research'));
  });

  it('defaults to code persona', () => {
    const pm = new PersonaManager(tmpDir);
    assert.equal(pm.getActive().name, 'code');
  });

  it('accepts initial persona', () => {
    const pm = new PersonaManager(tmpDir, 'pentest');
    assert.equal(pm.getActive().name, 'pentest');
  });

  it('switches persona', () => {
    const pm = new PersonaManager(tmpDir);
    const result = pm.switchTo('sre');
    assert.ok(result);
    assert.equal(result!.name, 'sre');
    assert.equal(pm.getActive().name, 'sre');
  });

  it('returns null for unknown persona', () => {
    const pm = new PersonaManager(tmpDir);
    assert.equal(pm.switchTo('nonexistent'), null);
  });

  it('pentest has auto-allow patterns', () => {
    const pm = new PersonaManager(tmpDir, 'pentest');
    const patterns = pm.getAutoAllowPatterns();
    assert.ok(patterns.length > 0);
    assert.ok(patterns.some(p => p.includes('nmap')));
    assert.ok(patterns.some(p => p.includes('curl')));
  });

  it('sre has auto-allow patterns for kubectl', () => {
    const pm = new PersonaManager(tmpDir, 'sre');
    const patterns = pm.getAutoAllowPatterns();
    assert.ok(patterns.some(p => p.includes('kubectl')));
    assert.ok(patterns.some(p => p.includes('docker')));
  });

  it('pentest defaults to high thinking', () => {
    const pm = new PersonaManager(tmpDir, 'pentest');
    assert.equal(pm.getDefaultThinkingLevel(), 'high');
  });

  it('code defaults to medium thinking', () => {
    const pm = new PersonaManager(tmpDir, 'code');
    assert.equal(pm.getDefaultThinkingLevel(), 'medium');
  });

  it('returns system prompt overlay', () => {
    const pm = new PersonaManager(tmpDir, 'pentest');
    const overlay = pm.getSystemPromptOverlay();
    assert.ok(overlay.includes('penetration testing'));
    assert.ok(overlay.includes('vulnerability'));
  });

  it('loads custom persona from project dir', () => {
    const personaDir = path.join(tmpDir, '.superinference', 'personas');
    fs.mkdirSync(personaDir, { recursive: true });
    fs.writeFileSync(path.join(personaDir, 'custom.md'), `---
name: custom
description: My custom persona
thinking: high
---

You are a custom assistant for my specific needs.`);

    const pm = new PersonaManager(tmpDir);
    const list = pm.list();
    assert.ok(list.some(p => p.name === 'custom'));

    const custom = pm.switchTo('custom');
    assert.ok(custom);
    assert.equal(custom!.name, 'custom');
    assert.ok(pm.getSystemPromptOverlay().includes('custom assistant'));
  });

  it('custom persona uses filename when name field is missing', () => {
    const personaDir = path.join(tmpDir, '.superinference', 'personas');
    fs.mkdirSync(personaDir, { recursive: true });
    fs.writeFileSync(path.join(personaDir, 'noname.md'), `---
description: A persona without a name field
---

Just a body.`);

    const pm = new PersonaManager(tmpDir);
    assert.ok(pm.list().some(p => p.name === 'noname'));
  });

  it('custom persona with auto-allow patterns', () => {
    const personaDir = path.join(tmpDir, '.superinference', 'personas');
    fs.mkdirSync(personaDir, { recursive: true });
    fs.writeFileSync(path.join(personaDir, 'autoallow.md'), `---
name: autoallow
description: Custom with auto-allow
auto-allow: pytest*, mypy*, ruff*
thinking: low
---

Body text.`);

    const pm = new PersonaManager(tmpDir);
    pm.switchTo('autoallow');
    const patterns = pm.getAutoAllowPatterns();
    assert.ok(patterns.includes('pytest*'));
    assert.ok(patterns.includes('mypy*'));
    assert.ok(patterns.includes('ruff*'));
    assert.equal(pm.getDefaultThinkingLevel(), 'low');
  });

  it('skips files without frontmatter', () => {
    const personaDir = path.join(tmpDir, '.superinference', 'personas');
    fs.mkdirSync(personaDir, { recursive: true });
    fs.writeFileSync(path.join(personaDir, 'nofm.md'), `No frontmatter here, just a plain markdown file.`);

    const pm = new PersonaManager(tmpDir);
    assert.ok(!pm.list().some(p => p.name === 'nofm'));
  });

  it('skips files with unclosed frontmatter', () => {
    const personaDir = path.join(tmpDir, '.superinference', 'personas');
    fs.mkdirSync(personaDir, { recursive: true });
    fs.writeFileSync(path.join(personaDir, 'broken.md'), `---
name: broken
description: Missing closing dashes`);

    const pm = new PersonaManager(tmpDir);
    assert.ok(!pm.list().some(p => p.name === 'broken'));
  });

  it('frontmatter lines without colons are skipped', () => {
    const personaDir = path.join(tmpDir, '.superinference', 'personas');
    fs.mkdirSync(personaDir, { recursive: true });
    fs.writeFileSync(path.join(personaDir, 'badlines.md'), `---
name: badlines
this line has no colon
description: Has a description
---

Body.`);

    const pm = new PersonaManager(tmpDir);
    const persona = pm.switchTo('badlines');
    assert.ok(persona);
    assert.equal(persona!.description, 'Has a description');
  });

  it('code persona has no auto-allow patterns', () => {
    const pm = new PersonaManager(tmpDir, 'code');
    const patterns = pm.getAutoAllowPatterns();
    assert.deepEqual(patterns, []);
  });

  it('research persona has auto-allow patterns for python and latex', () => {
    const pm = new PersonaManager(tmpDir, 'research');
    const patterns = pm.getAutoAllowPatterns();
    assert.ok(patterns.some(p => p.includes('python')));
    assert.ok(patterns.some(p => p.includes('pdflatex')));
  });

  it('research defaults to high thinking', () => {
    const pm = new PersonaManager(tmpDir, 'research');
    assert.equal(pm.getDefaultThinkingLevel(), 'high');
  });

  it('custom persona without thinking level defaults to medium', () => {
    const personaDir = path.join(tmpDir, '.superinference', 'personas');
    fs.mkdirSync(personaDir, { recursive: true });
    fs.writeFileSync(path.join(personaDir, 'nothink.md'), `---
name: nothink
description: No thinking level
---

Body.`);

    const pm = new PersonaManager(tmpDir);
    pm.switchTo('nothink');
    assert.equal(pm.getDefaultThinkingLevel(), 'medium');
  });

  it('custom persona description defaults to name', () => {
    const personaDir = path.join(tmpDir, '.superinference', 'personas');
    fs.mkdirSync(personaDir, { recursive: true });
    fs.writeFileSync(path.join(personaDir, 'nodesc.md'), `---
name: nodesc
---

Body.`);

    const pm = new PersonaManager(tmpDir);
    const persona = pm.switchTo('nodesc');
    assert.ok(persona);
    assert.equal(persona!.description, 'nodesc');
  });

  it('custom persona overrides built-in with same name', () => {
    const personaDir = path.join(tmpDir, '.superinference', 'personas');
    fs.mkdirSync(personaDir, { recursive: true });
    fs.writeFileSync(path.join(personaDir, 'code.md'), `---
name: code
description: Custom code persona
thinking: high
---

Custom code assistant.`);

    const pm = new PersonaManager(tmpDir);
    const code = pm.switchTo('code');
    assert.ok(code);
    assert.equal(code!.description, 'Custom code persona');
    assert.ok(pm.getSystemPromptOverlay().includes('Custom code'));
  });

  it('handles non-existent persona directories gracefully', () => {
    const pm = new PersonaManager('/tmp/nonexistent-dir-' + Date.now());
    assert.ok(pm.list().length >= 4);
  });

  it('unknown initial persona falls back to code', () => {
    const pm = new PersonaManager(tmpDir, 'nonexistent');
    assert.equal(pm.getActive().name, 'code');
  });
});
