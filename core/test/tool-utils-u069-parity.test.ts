import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  scanForSecrets,
  redactSecrets,
  isBlockedPath,
  isSensitivePath,
  validateFileWritePath,
} from '../src/tools/tool-utils';
import { _pathHooks } from '../src/utils/path';

// ---------------------------------------------------------------------------
// U069 — tool-utils.ts Claude Code parity additions:
//  - GCP / Vertex AI service-account email detection + redaction.
//  - Device-file blocklist completion (/dev/full, /dev/tty, /dev/console).
//  - Glob-metacharacter block on NEW write/create targets (base-dir bypass).
//  - Case-insensitive sensitive-path detection on case-insensitive filesystems.
// ---------------------------------------------------------------------------

describe('GCP/Vertex AI service-account identity (scan + redact)', () => {
  const email = 'my-runner@my-project-123.iam.gserviceaccount.com';

  it('scanForSecrets detects the identity without surfacing it', () => {
    const matches = scanForSecrets(`service_account = ${email}`);
    const hit = matches.find(m => m.ruleId === 'gcp-service-account');
    assert.ok(hit, 'gcp-service-account rule should fire');
    assert.equal(hit!.label, 'GCP Service Account');
    // The matched secret bytes must never be returned.
    for (const m of matches) {
      assert.ok(!JSON.stringify(m).includes(email));
    }
  });

  it('redactSecrets removes the identity, preserving surrounding text', () => {
    const out = redactSecrets(`account: ${email};`);
    assert.ok(!out.includes(email), 'email must be redacted');
    assert.ok(out.includes('[REDACTED-SECRET]'));
    assert.ok(out.startsWith('account: '));
    assert.ok(out.trimEnd().endsWith(';'));
  });

  it('does not fire on an ordinary email address', () => {
    const matches = scanForSecrets('contact me at alice@example.com');
    assert.ok(!matches.some(m => m.ruleId === 'gcp-service-account'));
  });
});

describe('device-file blocklist completion', () => {
  for (const dev of ['/dev/full', '/dev/tty', '/dev/console']) {
    it(`blocks ${dev}`, () => {
      assert.equal(isBlockedPath(dev), true);
    });
  }

  it('still allows a normal file path', () => {
    assert.equal(isBlockedPath('/home/user/notes.txt'), false);
  });
});

describe('glob-metacharacter block on write/create', () => {
  let dir: string;
  beforeEach(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'u069-glob-')); });
  afterEach(() => { fs.rmSync(dir, { recursive: true, force: true }); });

  it('rejects a NON-existent target containing glob metacharacters', () => {
    const res = validateFileWritePath(path.join(dir, 'out', '*.ts'));
    assert.equal(res.valid, false);
    assert.match(res.error ?? '', /glob metacharacters/);
  });

  it('rejects brace and question-mark create targets', () => {
    assert.equal(validateFileWritePath(path.join(dir, '{a,b}.ts')).valid, false);
    assert.equal(validateFileWritePath(path.join(dir, 'x?.ts')).valid, false);
  });

  it('ALLOWS editing an EXISTING literal file whose name contains brackets', () => {
    // SUPERSET over Claude: a real Next.js-style [id].tsx dynamic route is a
    // literal existing file — no base-dir bypass — so it must stay editable.
    const existing = path.join(dir, '[id].tsx');
    fs.writeFileSync(existing, 'export default 1;');
    const res = validateFileWritePath(existing);
    assert.equal(res.valid, true, res.error);
  });

  it('allows a normal non-existent create target', () => {
    assert.equal(validateFileWritePath(path.join(dir, 'new-file.ts')).valid, true);
  });
});

describe('case-insensitive sensitive-path detection', () => {
  const realPlatform = _pathHooks.platform;
  afterEach(() => { _pathHooks.platform = realPlatform; });

  it('folds case on case-insensitive filesystems (darwin/win32)', () => {
    _pathHooks.platform = 'darwin';
    assert.equal(isSensitivePath('/ETC/shadow'), true);
    assert.equal(isSensitivePath('/Etc/Passwd'), true);
  });

  it('folds a mixed-case home sensitive dir on case-insensitive FS', () => {
    const home = fs.mkdtempSync(path.join(os.tmpdir(), 'u069-home-'));
    const prevHome = process.env.HOME;
    const prevProfile = process.env.USERPROFILE;
    process.env.HOME = home;
    delete process.env.USERPROFILE;
    try {
      _pathHooks.platform = 'darwin';
      assert.equal(isSensitivePath(path.join(home, '.SSH', 'id_rsa')), true);
    } finally {
      if (prevHome === undefined) delete process.env.HOME; else process.env.HOME = prevHome;
      if (prevProfile !== undefined) process.env.USERPROFILE = prevProfile;
      fs.rmSync(home, { recursive: true, force: true });
    }
  });

  it('does NOT fold on case-sensitive Linux (exact match still required)', () => {
    _pathHooks.platform = 'linux';
    assert.equal(isSensitivePath('/ETC/shadow'), false);
    assert.equal(isSensitivePath('/etc/shadow'), true);
  });
});
