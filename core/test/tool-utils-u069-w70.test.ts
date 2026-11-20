import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { execFileSync } from 'child_process';

import {
  resolveFilePath,
  resolveSearchPath,
  safeReadFile,
  safeLstat,
  scanForSecrets,
  redactSecrets,
} from '../src/tools/tool-utils';
import { fileReadTool } from '../src/tools/file-read';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// U069 (w70) — tool-utils.ts the reference parity additions:
//  [8]/[17] Special-file (FIFO/socket/device) lstat pre-guard in resolveFilePath,
//           BEFORE realpathSync/readFileSync (which can block on a FIFO → DoS).
//  [16]     UNC path blocking in resolveSearchPath (grep/glob), not only reads.
//  [14]     Containment errors name the current working directory as context.
//  [4]      Full-content secret scan (no per-line 500-char truncation).
//  [20]     Curated-provider secret rules (Azure AD, Twilio, Databricks,
//           HashiCorp TF, Pulumi, Postman, Grafana Cloud).
// ---------------------------------------------------------------------------

describe('U069 [8/17] special-file pre-guard in resolveFilePath', () => {
  let dir: string;
  beforeEach(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'u069w70-fifo-')); });
  afterEach(() => { fs.rmSync(dir, { recursive: true, force: true }); });

  it('refuses a FIFO inside the workspace instead of realpath/read-hanging', () => {
    const fifo = path.join(dir, 'pipe');
    execFileSync('mkfifo', [fifo]);
    const { error } = resolveFilePath(fifo, dir);
    assert.ok(error, 'a FIFO must be refused by resolveFilePath');
    assert.match(error!.output, /special file \(FIFO\/socket\/device\)/);
  });

  it('safeReadFile on a FIFO returns an error quickly (does not hang)', () => {
    const fifo = path.join(dir, 'pipe2');
    execFileSync('mkfifo', [fifo]);
    const start = Date.now();
    const res = safeReadFile(fifo, dir);
    const elapsed = Date.now() - start;
    assert.ok(res.error, 'reading a FIFO must error, not hang');
    assert.ok(elapsed < 1000, `safeReadFile on FIFO took ${elapsed}ms — should be immediate`);
  });

  it('still resolves an ordinary regular file inside the workspace', () => {
    const file = path.join(dir, 'normal.txt');
    fs.writeFileSync(file, 'hello');
    const { resolved, error } = resolveFilePath(file, dir);
    assert.equal(error, undefined);
    assert.equal(resolved, file);
  });

  it('still allows a not-yet-existing create target (lstat ENOENT is skipped)', () => {
    const target = path.join(dir, 'new-file.txt');
    const { error } = resolveFilePath(target, dir);
    assert.equal(error, undefined, error?.output);
  });
});

describe('U069 [16] UNC blocking in resolveSearchPath', () => {
  it('rejects a POSIX-form UNC path', () => {
    const { error } = resolveSearchPath('//server/share/x', process.cwd());
    assert.ok(error);
    assert.match(error!, /UNC paths are not allowed/);
  });

  it('rejects a Windows-form UNC path', () => {
    const { error } = resolveSearchPath('\\\\server\\share', process.cwd());
    assert.ok(error);
    assert.match(error!, /UNC paths are not allowed/);
  });

  it('does not treat an ordinary single-slash absolute subpath as UNC', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'u069w70-unc-'));
    try {
      const sub = path.join(dir, 'src');
      fs.mkdirSync(sub);
      const { resolved, error } = resolveSearchPath(sub, dir);
      assert.equal(error, undefined, error);
      assert.equal(resolved, sub);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe('U069 [14] containment errors name the cwd', () => {
  let dir: string;
  beforeEach(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'u069w70-cwd-')); });
  afterEach(() => { fs.rmSync(dir, { recursive: true, force: true }); });

  it('resolveFilePath names the cwd on an out-of-workspace path', () => {
    const { error } = resolveFilePath(path.join(dir, '..', 'escape.txt'), dir);
    assert.ok(error);
    assert.ok(error!.output.includes(`(cwd: ${dir})`), error!.output);
  });

  it('resolveSearchPath names the cwd on an out-of-workspace path', () => {
    const { error } = resolveSearchPath(path.join(dir, '..', 'escape'), dir);
    assert.ok(error);
    assert.ok(error!.includes(`(cwd: ${dir})`), error);
  });

  it('safeLstat names the cwd on an out-of-workspace path', () => {
    const { error } = safeLstat(path.join(dir, '..', 'escape.txt'), dir);
    assert.ok(error);
    assert.ok(error!.output.includes(`(cwd: ${dir})`), error!.output);
  });
});

describe('U069 [4] full-content secret scan (no per-line cap)', () => {
  it('detects a secret embedded far past column 500 on one line', () => {
    const line = 'export CONFIG=' + 'x'.repeat(800) + ' sk-abcdefghijklmnopqrstuvwxyz1234567890abcd';
    const found = scanForSecrets(line);
    assert.ok(found.length > 0, 'secret past column 500 must be detected');
  });

  it('does not hang on a 100KB non-matching single line', () => {
    const start = Date.now();
    const found = scanForSecrets('a'.repeat(100_000));
    const elapsed = Date.now() - start;
    assert.equal(found.length, 0);
    assert.ok(elapsed < 1000, `took ${elapsed}ms`);
  });
});

describe('U069 [20] curated-provider secret rules (scan + redact)', () => {
  const cases: Array<{ ruleId: string; label: string; sample: string }> = [
    {
      ruleId: 'azure-ad-client-secret',
      label: 'Azure AD Client Secret',
      // leading '(' and trailing ')' satisfy the boundary groups.
      sample: 'cfg (abc8Q~' + 'a'.repeat(32) + ')',
    },
    {
      ruleId: 'twilio-api-key',
      label: 'Twilio API Key',
      // Prefix split from the body so the synthetic token is never a contiguous
      // literal in source (keeps secret-scanning push-protection off a test
      // fixture); the runtime string is byte-identical to what the rule matches.
      sample: 'key SK' + '0123456789abcdef0123456789abcdef',
    },
    {
      ruleId: 'databricks-api-token',
      label: 'Databricks API Token',
      sample: 'tok dapi' + '0123456789abcdef0123456789abcdef',
    },
    {
      ruleId: 'hashicorp-tf-api-token',
      label: 'HashiCorp TF API Token',
      sample: 'tf abcdefghij1234.atlasv1.' + 'a'.repeat(64),
    },
    {
      ruleId: 'pulumi-api-token',
      label: 'Pulumi API Token',
      sample: 'pul pul-' + '0123456789abcdef0123456789abcdef01234567',
    },
    {
      ruleId: 'postman-api-token',
      label: 'Postman API Token',
      sample: 'pm PMAK-' + '0123456789abcdef01234567' + '-0123456789abcdef0123456789abcdef01',
    },
    {
      ruleId: 'grafana-cloud-api-token',
      label: 'Grafana Cloud API Token',
      sample: 'gf glc_' + 'A'.repeat(40),
    },
  ];

  for (const c of cases) {
    it(`detects ${c.ruleId} and never surfaces the secret bytes`, () => {
      const matches = scanForSecrets(c.sample);
      const hit = matches.find(m => m.ruleId === c.ruleId);
      assert.ok(hit, `${c.ruleId} rule should fire on its sample`);
      assert.equal(hit!.label, c.label);
      for (const m of matches) {
        assert.ok(!JSON.stringify(m).includes(c.sample.split(' ').pop()!),
          `${c.ruleId} result must not echo the matched secret bytes`);
      }
    });

    it(`redacts ${c.ruleId} while preserving surrounding text`, () => {
      const token = c.sample.split(' ').pop()!.replace(/[()]/g, '');
      const out = redactSecrets(c.sample);
      assert.ok(!out.includes(token), `${c.ruleId} token must be redacted: ${out}`);
      assert.ok(out.includes('[REDACTED-SECRET]'));
    });
  }

  it('does not false-positive on ordinary prose', () => {
    const matches = scanForSecrets('The SKYSCRAPER was tall and the pulse quickened.');
    assert.ok(!matches.some(m => ['twilio-api-key', 'pulumi-api-token'].includes(m.ruleId)));
  });
});

describe('U069 [8/17] e2e — file_read tool refuses a FIFO (does not hang)', () => {
  let ws: string;
  beforeEach(() => { ws = fs.mkdtempSync(path.join(os.tmpdir(), 'u069w70-e2e-')); });
  afterEach(() => { fs.rmSync(ws, { recursive: true, force: true }); });

  it('returns an isError result immediately for a FIFO target', async () => {
    const fifo = path.join(ws, 'pipe');
    execFileSync('mkfifo', [fifo]);
    const ctx: ToolContext = { cwd: ws, abortSignal: new AbortController().signal };
    const start = Date.now();
    const res = await fileReadTool.execute({ file_path: fifo }, ctx);
    const elapsed = Date.now() - start;
    assert.equal(res.isError, true, res.output);
    assert.match(res.output, /special file \(FIFO\/socket\/device\)/);
    assert.ok(elapsed < 2000, `file_read on a FIFO took ${elapsed}ms — must not hang`);
  });
});
