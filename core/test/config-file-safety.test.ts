import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  safeWriteConfig,
  cleanupStaleConfigTmp,
  validateConfigJson,
  CONFIG_WRITE_TIMEOUT_MS,
} from '../src/config';

describe('CONFIG_WRITE_TIMEOUT_MS', () => {
  it('is 5000', () => {
    assert.equal(CONFIG_WRITE_TIMEOUT_MS, 5000);
  });
});

describe('validateConfigJson', () => {
  it('accepts a valid JSON object', () => {
    const result = validateConfigJson('{"key": "value"}');
    assert.equal(result.valid, true);
    assert.equal(result.error, undefined);
  });

  it('accepts an empty JSON object', () => {
    assert.equal(validateConfigJson('{}').valid, true);
  });

  it('rejects invalid JSON', () => {
    const result = validateConfigJson('{bad json');
    assert.equal(result.valid, false);
    assert.ok(result.error!.includes('Invalid JSON'));
  });

  it('rejects JSON arrays', () => {
    const result = validateConfigJson('[1, 2, 3]');
    assert.equal(result.valid, false);
    assert.ok(result.error!.includes('array'));
  });

  it('rejects JSON null', () => {
    const result = validateConfigJson('null');
    assert.equal(result.valid, false);
    assert.ok(result.error!.includes('null'));
  });

  it('rejects JSON strings', () => {
    const result = validateConfigJson('"hello"');
    assert.equal(result.valid, false);
    assert.ok(result.error!.includes('string'));
  });

  it('rejects JSON numbers', () => {
    const result = validateConfigJson('42');
    assert.equal(result.valid, false);
  });

  it('rejects empty string', () => {
    const result = validateConfigJson('');
    assert.equal(result.valid, false);
  });
});

describe('safeWriteConfig', () => {
  const tmpDirs: string[] = [];
  function makeTmp(): string {
    const d = fs.mkdtempSync(path.join(os.tmpdir(), 'cfg-safe-'));
    tmpDirs.push(d);
    return d;
  }

  after(() => {
    for (const d of tmpDirs) {
      try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* */ }
    }
  });

  it('writes a valid JSON file', () => {
    const dir = makeTmp();
    const fp = path.join(dir, 'config.json');
    safeWriteConfig(fp, { model: 'opus' });
    const content = fs.readFileSync(fp, 'utf-8');
    const parsed = JSON.parse(content);
    assert.equal(parsed.model, 'opus');
  });

  it('produces formatted JSON (2-space indent)', () => {
    const dir = makeTmp();
    const fp = path.join(dir, 'config.json');
    safeWriteConfig(fp, { a: 1 });
    const content = fs.readFileSync(fp, 'utf-8');
    assert.ok(content.includes('  "a"'));
  });

  it('cleans up .tmp file on success', () => {
    const dir = makeTmp();
    const fp = path.join(dir, 'config.json');
    safeWriteConfig(fp, { ok: true });
    assert.equal(fs.existsSync(fp + '.tmp'), false);
  });

  it('creates intermediate directories', () => {
    const dir = makeTmp();
    const fp = path.join(dir, 'sub', 'dir', 'config.json');
    safeWriteConfig(fp, { nested: true });
    const parsed = JSON.parse(fs.readFileSync(fp, 'utf-8'));
    assert.equal(parsed.nested, true);
  });

  it('overwrites an existing file', () => {
    const dir = makeTmp();
    const fp = path.join(dir, 'config.json');
    safeWriteConfig(fp, { version: 1 });
    safeWriteConfig(fp, { version: 2 });
    const parsed = JSON.parse(fs.readFileSync(fp, 'utf-8'));
    assert.equal(parsed.version, 2);
  });
});

describe('cleanupStaleConfigTmp', () => {
  const tmpDirs: string[] = [];
  function makeTmp(): string {
    const d = fs.mkdtempSync(path.join(os.tmpdir(), 'cfg-cleanup-'));
    tmpDirs.push(d);
    return d;
  }

  after(() => {
    for (const d of tmpDirs) {
      try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* */ }
    }
  });

  it('returns false when no .tmp file exists', () => {
    const dir = makeTmp();
    const fp = path.join(dir, 'config.json');
    assert.equal(cleanupStaleConfigTmp(fp), false);
  });

  it('recovers a valid .tmp file by renaming it', () => {
    const dir = makeTmp();
    const fp = path.join(dir, 'config.json');
    const tmpPath = fp + '.tmp';
    fs.writeFileSync(tmpPath, '{"recovered": true}', 'utf-8');
    const result = cleanupStaleConfigTmp(fp);
    assert.equal(result, true);
    assert.equal(fs.existsSync(tmpPath), false);
    const parsed = JSON.parse(fs.readFileSync(fp, 'utf-8'));
    assert.equal(parsed.recovered, true);
  });

  it('deletes a corrupt .tmp file', () => {
    const dir = makeTmp();
    const fp = path.join(dir, 'config.json');
    const tmpPath = fp + '.tmp';
    fs.writeFileSync(tmpPath, '{corrupt data!!!', 'utf-8');
    const result = cleanupStaleConfigTmp(fp);
    assert.equal(result, true);
    assert.equal(fs.existsSync(tmpPath), false);
    assert.equal(fs.existsSync(fp), false);
  });

  it('does not overwrite existing config with corrupt .tmp', () => {
    const dir = makeTmp();
    const fp = path.join(dir, 'config.json');
    fs.writeFileSync(fp, '{"original": true}', 'utf-8');
    const tmpPath = fp + '.tmp';
    fs.writeFileSync(tmpPath, 'not json', 'utf-8');
    cleanupStaleConfigTmp(fp);
    const parsed = JSON.parse(fs.readFileSync(fp, 'utf-8'));
    assert.equal(parsed.original, true);
  });

  it('replaces existing config with valid .tmp', () => {
    const dir = makeTmp();
    const fp = path.join(dir, 'config.json');
    fs.writeFileSync(fp, '{"old": true}', 'utf-8');
    const tmpPath = fp + '.tmp';
    fs.writeFileSync(tmpPath, '{"new": true}', 'utf-8');
    cleanupStaleConfigTmp(fp);
    const parsed = JSON.parse(fs.readFileSync(fp, 'utf-8'));
    assert.equal(parsed.new, true);
  });
});
