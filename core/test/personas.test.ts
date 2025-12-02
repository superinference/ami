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
});
