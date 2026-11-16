import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { MemoryManager, isAutoMemoryEnabled } from '../src/memory';

// ---------------------------------------------------------------------------
// Shared temp-dir harness. The auto-memory store is redirected to an isolated
// temp dir via AMI_MEMORY_DIR so getMemoryContext reads only test fixtures and
// never the real ~/.ami store.
// ---------------------------------------------------------------------------

let cwdDir: string;
let memDir: string;
const savedMemEnv = process.env.AMI_MEMORY_DIR;
const savedDisableEnv = process.env.AMI_DISABLE_AUTO_MEMORY;

function mkTmp(prefix: string): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

function writeMemoryFile(name: string, body: string): void {
  fs.mkdirSync(memDir, { recursive: true });
  fs.writeFileSync(
    path.join(memDir, `${name}.md`),
    `---\nname: ${name}\ndescription: ${name} desc\ntype: project\n---\n\n${body}\n`,
    'utf-8',
  );
}

beforeEach(() => {
  cwdDir = mkTmp('si-automem-cwd-');
  memDir = mkTmp('si-automem-store-');
  process.env.AMI_MEMORY_DIR = memDir;
  delete process.env.AMI_DISABLE_AUTO_MEMORY;
});

afterEach(() => {
  try { fs.rmSync(cwdDir, { recursive: true, force: true }); } catch { /* ignore */ }
  try { fs.rmSync(memDir, { recursive: true, force: true }); } catch { /* ignore */ }
  if (savedMemEnv === undefined) delete process.env.AMI_MEMORY_DIR;
  else process.env.AMI_MEMORY_DIR = savedMemEnv;
  if (savedDisableEnv === undefined) delete process.env.AMI_DISABLE_AUTO_MEMORY;
  else process.env.AMI_DISABLE_AUTO_MEMORY = savedDisableEnv;
});

// ---------------------------------------------------------------------------
// #30 — isAutoMemoryEnabled toggle (env var + settings + default)
// ---------------------------------------------------------------------------

describe('isAutoMemoryEnabled — priority chain', () => {
  it('defaults to enabled when nothing is set', () => {
    assert.equal(isAutoMemoryEnabled({}), true);
  });

  it('a truthy AMI_DISABLE_AUTO_MEMORY disables it', () => {
    assert.equal(isAutoMemoryEnabled({ AMI_DISABLE_AUTO_MEMORY: '1' }), false);
    assert.equal(isAutoMemoryEnabled({ AMI_DISABLE_AUTO_MEMORY: 'true' }), false);
    assert.equal(isAutoMemoryEnabled({ AMI_DISABLE_AUTO_MEMORY: 'yes' }), false);
  });

  it('an explicitly-falsy AMI_DISABLE_AUTO_MEMORY force-enables it, overriding a settings opt-out', () => {
    assert.equal(isAutoMemoryEnabled({ AMI_DISABLE_AUTO_MEMORY: '0' }), true);
    assert.equal(isAutoMemoryEnabled({ AMI_DISABLE_AUTO_MEMORY: 'false' }), true);
    // settings says OFF, but the explicit falsy env wins
    assert.equal(isAutoMemoryEnabled({ AMI_DISABLE_AUTO_MEMORY: 'off' }, false), true);
  });

  it('a blank/whitespace env var defers to settings/default', () => {
    assert.equal(isAutoMemoryEnabled({ AMI_DISABLE_AUTO_MEMORY: '   ' }), true);
    assert.equal(isAutoMemoryEnabled({ AMI_DISABLE_AUTO_MEMORY: '' }, false), false);
  });

  it('honors the settings opt-out when the env var is unset', () => {
    assert.equal(isAutoMemoryEnabled({}, false), false);
    assert.equal(isAutoMemoryEnabled({}, true), true);
  });
});

// ---------------------------------------------------------------------------
// #30 — getMemoryContext is gated by the auto-memory toggle
// ---------------------------------------------------------------------------

describe('getMemoryContext — auto-memory gate', () => {
  it('includes stored memories + guidance when enabled (default)', () => {
    writeMemoryFile('alpha', 'alpha body content');
    const ctx = new MemoryManager(cwdDir).getMemoryContext();
    assert.match(ctx, /## Stored Memories/);
    assert.match(ctx, /## Memory Guidelines/);
  });

  it('suppresses all memory sections when AMI_DISABLE_AUTO_MEMORY is truthy', () => {
    writeMemoryFile('alpha', 'alpha body content');
    process.env.AMI_DISABLE_AUTO_MEMORY = '1';
    const ctx = new MemoryManager(cwdDir).getMemoryContext();
    assert.doesNotMatch(ctx, /## Stored Memories/);
    assert.doesNotMatch(ctx, /## Memory Guidelines/);
    assert.doesNotMatch(ctx, /alpha body content/);
  });

  it('still loads project INSTRUCTION files when auto-memory is disabled', () => {
    fs.writeFileSync(path.join(cwdDir, 'AGENTS.md'), '# Project rules\nAlways run tests.', 'utf-8');
    writeMemoryFile('alpha', 'alpha body content');
    process.env.AMI_DISABLE_AUTO_MEMORY = '1';
    const ctx = new MemoryManager(cwdDir).getMemoryContext();
    assert.match(ctx, /## Project Instructions/);
    assert.match(ctx, /Always run tests\./);
    // but the memory system is gone
    assert.doesNotMatch(ctx, /## Stored Memories/);
  });

  it('the opts.autoMemoryEnabled setting gates even when the env var is unset', () => {
    writeMemoryFile('alpha', 'alpha body content');
    const ctx = new MemoryManager(cwdDir).getMemoryContext({ autoMemoryEnabled: false });
    assert.doesNotMatch(ctx, /## Stored Memories/);
  });
});

// ---------------------------------------------------------------------------
// #17 — "Searching past context" prompt section
// ---------------------------------------------------------------------------

describe('buildSearchingPastContextSection + getMemoryContext wiring', () => {
  it('builds a section with runnable grep commands for the memory dir and session JSONL', () => {
    const mgr = new MemoryManager(cwdDir);
    const section = mgr.buildSearchingPastContextSection();
    assert.match(section, /### Searching past context/);
    assert.ok(section.includes(mgr.getMemoryDir()), 'must cite the live memory dir');
    assert.match(section, /--include="\*\.md"/);
    assert.match(section, /--include="\*\.jsonl"/);
    assert.match(section, /narrow search terms/);
  });

  it('is injected into the memory guidance block', () => {
    writeMemoryFile('alpha', 'alpha body content');
    const ctx = new MemoryManager(cwdDir).getMemoryContext();
    assert.match(ctx, /### Searching past context/);
    assert.match(ctx, /--include="\*\.jsonl"/);
  });
});

// ---------------------------------------------------------------------------
// #19 — Two-step MEMORY.md index save guidance
// ---------------------------------------------------------------------------

describe('two-step save guidance', () => {
  it('teaches the MEMORY.md index pointer pattern in getMemoryContext', () => {
    writeMemoryFile('alpha', 'alpha body content');
    const ctx = new MemoryManager(cwdDir).getMemoryContext();
    assert.match(ctx, /### How to save a memory \(two steps\)/);
    assert.match(ctx, /\*\*Step 1\*\*/);
    assert.match(ctx, /\*\*Step 2\*\*/);
    assert.match(ctx, /MEMORY\.md/);
    assert.match(ctx, /one-line hook/);
    assert.match(ctx, /Never write memory content directly/);
  });
});
