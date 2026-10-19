import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  validateRequiredString,
  resolveSearchPath,
  resolveFilePath,
  validatePatternAndPath,
  extractQuery,
  buildToolDescriptionContext,
  renderToolDescription,
  detectLineEnding,
  normalizeToLf,
  convertToLineEnding,
  scanForSecrets,
  scanAddedSecrets,
  redactSecrets,
  isNonUtfEncoding,
  detectEncoding,
  decodeBuffer,
  encodeText,
  validateFilePath,
} from '../src/tools/tool-utils';

// ---------------------------------------------------------------------------
// validateRequiredString
// ---------------------------------------------------------------------------

describe('validateRequiredString', () => {
  it('returns null for valid string', () => {
    assert.equal(validateRequiredString('hello', 'field'), null);
  });

  it('returns error for empty string', () => {
    const result = validateRequiredString('', 'field');
    assert.ok(result?.isError);
    assert.ok(result?.output.includes('field'));
  });

  it('returns error for whitespace-only string', () => {
    const result = validateRequiredString('   ', 'test');
    assert.ok(result?.isError);
  });

  it('returns error for null', () => {
    const result = validateRequiredString(null, 'x');
    assert.ok(result?.isError);
  });

  it('returns error for undefined', () => {
    const result = validateRequiredString(undefined, 'x');
    assert.ok(result?.isError);
  });

  it('rejects false-y non-strings like 0', () => {
    const result = validateRequiredString(0 as unknown, 'num');
    assert.ok(result?.isError);
  });
});

// ---------------------------------------------------------------------------
// resolveSearchPath
// ---------------------------------------------------------------------------

describe('resolveSearchPath', () => {
  it('returns cwd when no path given', () => {
    const result = resolveSearchPath(undefined, '/home/user');
    assert.equal(result.resolved, '/home/user');
    assert.equal(result.error, undefined);
  });

  it('resolves relative path against cwd', () => {
    const result = resolveSearchPath('src', '/home/user/project');
    assert.equal(result.resolved, path.resolve('/home/user/project', 'src'));
    assert.equal(result.error, undefined);
  });

  it('returns error for path outside workspace', () => {
    const result = resolveSearchPath('/tmp/dir', '/home');
    assert.ok(result.error);
    assert.ok(result.error.includes('outside the workspace'));
  });

  it('allows cwd-relative subpaths', () => {
    const result = resolveSearchPath('subdir', '/home/user/project');
    assert.equal(result.resolved, path.resolve('/home/user/project', 'subdir'));
    assert.equal(result.error, undefined);
  });

  it('accepts an absolute path that equals cwd', () => {
    const r = resolveSearchPath('/home/user', '/home/user');
    assert.equal(r.error, undefined);
    assert.equal(r.resolved, '/home/user');
  });

  it('accepts an absolute subpath of cwd', () => {
    const r = resolveSearchPath('/home/user/sub', '/home/user');
    assert.equal(r.error, undefined);
    assert.equal(r.resolved, '/home/user/sub');
  });
});

// ---------------------------------------------------------------------------
// resolveFilePath — symlink escape protection
// ---------------------------------------------------------------------------

describe('resolveFilePath', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-resolve-fp-'));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('allows files inside workspace', () => {
    fs.writeFileSync(path.join(tmpDir, 'ok.txt'), 'data');
    const { resolved, error } = resolveFilePath('ok.txt', tmpDir);
    assert.equal(error, undefined);
    assert.ok(resolved.endsWith('ok.txt'));
  });

  it('rejects symlinks pointing outside workspace', () => {
    const outside = fs.mkdtempSync(path.join(os.tmpdir(), 'si-outside-'));
    fs.writeFileSync(path.join(outside, 'secret.txt'), 'secret');
    fs.symlinkSync(path.join(outside, 'secret.txt'), path.join(tmpDir, 'link.txt'));
    const { error } = resolveFilePath('link.txt', tmpDir);
    assert.ok(error, 'should block symlink escaping workspace');
    assert.ok(error!.output.includes('symlink'));
    fs.rmSync(outside, { recursive: true, force: true });
  });

  it('allows symlinks within workspace', () => {
    fs.writeFileSync(path.join(tmpDir, 'real.txt'), 'data');
    fs.symlinkSync(path.join(tmpDir, 'real.txt'), path.join(tmpDir, 'link.txt'));
    const { error } = resolveFilePath('link.txt', tmpDir);
    assert.equal(error, undefined);
  });

  it('resolves an absolute path inside the workspace', () => {
    const target = path.join(tmpDir, 'src', 'index.ts');
    const r = resolveFilePath(target, tmpDir);
    assert.equal(r.resolved, target);
    assert.equal(r.error, undefined);
  });

  it('resolves a relative path inside the workspace', () => {
    const r = resolveFilePath('src/index.ts', tmpDir);
    assert.equal(r.resolved, path.resolve(tmpDir, 'src/index.ts'));
    assert.equal(r.error, undefined);
  });

  it('returns error for a path outside the workspace', () => {
    const r = resolveFilePath('/etc/passwd', tmpDir);
    assert.ok(r.error);
    assert.ok(r.error!.isError);
    assert.ok(
      r.error!.output.includes('outside the workspace') || r.error!.output.includes('sensitive system path'),
      `Expected workspace or sensitive path error, got: ${r.error!.output}`,
    );
  });

  it('allows the workspace root itself', () => {
    const r = resolveFilePath(tmpDir, tmpDir);
    assert.equal(r.error, undefined);
  });

  it('rejects parent traversal outside the workspace', () => {
    const r = resolveFilePath('../../../etc/passwd', tmpDir);
    assert.ok(r.error);
    assert.ok(r.error!.isError);
  });
});

// ---------------------------------------------------------------------------
// validatePatternAndPath / extractQuery
// ---------------------------------------------------------------------------

describe('validatePatternAndPath', () => {
  it('returns pattern and resolved path for valid inputs', () => {
    const cwd = '/home/user/project';
    const result = validatePatternAndPath('*.ts', 'src', cwd);
    assert.ok(!result.error);
    assert.equal(result.pattern, '*.ts');
    assert.equal(result.resolved, path.resolve(cwd, 'src'));
  });

  it('returns error for empty pattern', () => {
    const result = validatePatternAndPath('', undefined, '/tmp');
    assert.ok(result.error);
    assert.ok(result.error!.isError);
    assert.ok(result.error!.output.includes('pattern'));
  });

  it('returns error for null pattern', () => {
    const result = validatePatternAndPath(null, undefined, '/tmp');
    assert.ok(result.error);
    assert.ok(result.error!.isError);
  });

  it('returns error for path outside workspace', () => {
    const result = validatePatternAndPath('*.ts', '/etc', '/home/user/project');
    assert.ok(result.error);
    assert.ok(result.error!.isError);
    assert.ok(result.error!.output.includes('outside the workspace'));
  });

  it('uses cwd when no search path is provided', () => {
    const cwd = '/home/user/project';
    const result = validatePatternAndPath('*.ts', undefined, cwd);
    assert.ok(!result.error);
    assert.equal(result.resolved, cwd);
  });
});

describe('extractQuery', () => {
  it('extracts a valid query string', () => {
    const result = extractQuery({ query: 'hello world' });
    assert.ok(!result.error);
    assert.equal(result.query, 'hello world');
  });

  it('returns error for empty query', () => {
    const result = extractQuery({ query: '' });
    assert.ok(result.error);
    assert.ok(result.error!.isError);
    assert.ok(result.error!.output.includes('query'));
  });

  it('returns error for missing query', () => {
    const result = extractQuery({});
    assert.ok(result.error);
    assert.ok(result.error!.isError);
  });

  it('returns error for whitespace-only query', () => {
    const result = extractQuery({ query: '   ' });
    assert.ok(result.error);
    assert.ok(result.error!.isError);
  });

  it('returns error for null query', () => {
    const result = extractQuery({ query: null });
    assert.ok(result.error);
  });

  it('returns error for undefined query', () => {
    const result = extractQuery({ query: undefined });
    assert.ok(result.error);
  });
});

// ---------------------------------------------------------------------------
// buildToolDescriptionContext
// ---------------------------------------------------------------------------

describe('buildToolDescriptionContext', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-tool-desc-'));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('sets cwd and projectName', () => {
    const ctx = buildToolDescriptionContext(tmpDir);
    assert.equal(ctx.cwd, tmpDir);
    assert.equal(ctx.projectName, path.basename(tmpDir));
  });

  it('detects .git directory', () => {
    fs.mkdirSync(path.join(tmpDir, '.git'));
    const ctx = buildToolDescriptionContext(tmpDir);
    assert.equal(ctx.hasGit, true);
  });

  it('detects missing .git', () => {
    const ctx = buildToolDescriptionContext(tmpDir);
    assert.equal(ctx.hasGit, false);
  });

  it('detects package.json', () => {
    fs.writeFileSync(path.join(tmpDir, 'package.json'), '{}');
    const ctx = buildToolDescriptionContext(tmpDir);
    assert.equal(ctx.hasPackageJson, true);
  });

  it('detects missing package.json', () => {
    const ctx = buildToolDescriptionContext(tmpDir);
    assert.equal(ctx.hasPackageJson, false);
  });
});

// ---------------------------------------------------------------------------
// renderToolDescription
// ---------------------------------------------------------------------------

describe('renderToolDescription', () => {
  it('replaces {{cwd}} placeholder', () => {
    const result = renderToolDescription('Working in {{cwd}}', {
      cwd: '/home/user/project',
    });
    assert.equal(result, 'Working in /home/user/project');
  });

  it('replaces {{projectName}} placeholder', () => {
    const result = renderToolDescription('Project: {{projectName}}', {
      cwd: '/tmp',
      projectName: 'my-app',
    });
    assert.equal(result, 'Project: my-app');
  });

  it('includes hasGit block when git is present', () => {
    const result = renderToolDescription(
      'Run commands.{{#hasGit}} Use git for version control.{{/hasGit}}',
      { cwd: '/tmp', hasGit: true },
    );
    assert.ok(result.includes('Use git'));
  });

  it('removes hasGit block when git is absent', () => {
    const result = renderToolDescription(
      'Run commands.{{#hasGit}} Use git for version control.{{/hasGit}}',
      { cwd: '/tmp', hasGit: false },
    );
    assert.ok(!result.includes('Use git'));
  });

  it('includes hasPackageJson block when present', () => {
    const result = renderToolDescription(
      '{{#hasPackageJson}}npm available{{/hasPackageJson}}',
      { cwd: '/tmp', hasPackageJson: true },
    );
    assert.equal(result, 'npm available');
  });

  it('removes hasPackageJson block when absent', () => {
    const result = renderToolDescription(
      '{{#hasPackageJson}}npm available{{/hasPackageJson}}',
      { cwd: '/tmp', hasPackageJson: false },
    );
    assert.equal(result, '');
  });

  it('handles multiple replacements', () => {
    const result = renderToolDescription(
      '{{cwd}} {{cwd}} {{projectName}}',
      { cwd: '/a', projectName: 'b' },
    );
    assert.equal(result, '/a /a b');
  });

  it('handles empty projectName', () => {
    const result = renderToolDescription('{{projectName}}', { cwd: '/tmp' });
    assert.equal(result, '');
  });

  it('leaves templates with no placeholders unchanged', () => {
    assert.equal(renderToolDescription('plain text', { cwd: '/tmp' }), 'plain text');
  });

  it('handles adjacent conditional blocks', () => {
    const tmpl = '{{#hasGit}}git{{/hasGit}} {{#hasPackageJson}}npm{{/hasPackageJson}}';
    const result = renderToolDescription(tmpl, { cwd: '/tmp', hasGit: true, hasPackageJson: false });
    assert.equal(result, 'git ');
  });
});

// ---------------------------------------------------------------------------
// CRLF helpers
// ---------------------------------------------------------------------------

describe('detectLineEnding', () => {
  it('detects CRLF when majority', () => {
    assert.equal(detectLineEnding('a\r\nb\r\nc\n'), '\r\n');
  });

  it('detects LF when majority', () => {
    assert.equal(detectLineEnding('a\nb\nc\r\n'), '\n');
  });

  it('defaults to LF on empty string', () => {
    assert.equal(detectLineEnding(''), '\n');
  });

  it('returns LF for LF-only content', () => {
    assert.equal(detectLineEnding('line1\nline2\n'), '\n');
  });

  it('returns CRLF for CRLF-only content', () => {
    assert.equal(detectLineEnding('line1\r\nline2\r\n'), '\r\n');
  });

  it('returns LF when CRLF and LF counts are equal', () => {
    assert.equal(detectLineEnding('a\r\nb\nc'), '\n');
  });
});

describe('normalizeToLf', () => {
  it('converts CRLF to LF', () => {
    assert.equal(normalizeToLf('a\r\nb\r\n'), 'a\nb\n');
  });

  it('leaves LF unchanged', () => {
    assert.equal(normalizeToLf('a\nb\n'), 'a\nb\n');
  });

  it('handles text with no line endings', () => {
    assert.equal(normalizeToLf('no newlines'), 'no newlines');
  });

  it('handles mixed endings', () => {
    assert.equal(normalizeToLf('a\r\nb\nc\r\n'), 'a\nb\nc\n');
  });
});

describe('convertToLineEnding', () => {
  it('converts to CRLF', () => {
    assert.equal(convertToLineEnding('a\nb\n', '\r\n'), 'a\r\nb\r\n');
  });

  it('keeps LF when ending is LF', () => {
    assert.equal(convertToLineEnding('a\nb\n', '\n'), 'a\nb\n');
  });

  it('normalizes mixed input before converting', () => {
    assert.equal(convertToLineEnding('a\r\nb\nc\r\n', '\r\n'), 'a\r\nb\r\nc\r\n');
  });

  it('handles empty string', () => {
    assert.equal(convertToLineEnding('', '\r\n'), '');
    assert.equal(convertToLineEnding('', '\n'), '');
  });

  it('handles text with no line endings', () => {
    assert.equal(convertToLineEnding('no newlines', '\r\n'), 'no newlines');
  });
});

// ---------------------------------------------------------------------------
// scanForSecrets
// ---------------------------------------------------------------------------

describe('scanForSecrets', () => {
  it('detects OpenAI API keys', () => {
    const found = scanForSecrets('key: sk-abcdefghijklmnopqrstuvwxyz1234567890abcdef');
    assert.ok(found.length > 0);
  });

  it('detects Anthropic API keys', () => {
    const found = scanForSecrets('ANTHROPIC_KEY=sk-ant-abcdefghijklmnopqrstuvwxyz1234567890');
    assert.ok(found.length > 0);
  });

  it('detects GitHub tokens', () => {
    const found = scanForSecrets('token=ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij');
    assert.ok(found.length > 0);
  });

  it('detects AWS access key IDs', () => {
    const found = scanForSecrets('aws_key=AKIAIOSFODNN7EXAMPLE');
    assert.ok(found.length > 0);
  });

  it('detects Google API keys', () => {
    const found = scanForSecrets('google=AIzaSyA1234567890abcdefghijklmnopqrstuv');
    assert.ok(found.length > 0);
  });

  it('detects Groq API keys', () => {
    const found = scanForSecrets('GROQ_KEY=gsk_abcdefghijklmnopqrstuvwx');
    assert.ok(found.length > 0);
  });

  it('detects GitLab personal access tokens (glpat-)', () => {
    const found = scanForSecrets('GITLAB_TOKEN=glpat-abcdefghijklmnopqrstuvwx');
    assert.ok(found.length > 0);
  });

  it('detects GitLab runner tokens (glrt-)', () => {
    const found = scanForSecrets('RUNNER_TOKEN=glrt-abcdefghijklmnopqrstuvwx');
    assert.ok(found.length > 0);
  });

  it('detects GitLab OAuth tokens (gloas-)', () => {
    const found = scanForSecrets('token=gloas-abcdefghijklmnopqrstuvwx');
    assert.ok(found.length > 0);
  });

  it('detects GitLab agent tokens (glagent-)', () => {
    const found = scanForSecrets('token=glagent-abcdefghijklmnopqrstuvwx');
    assert.ok(found.length > 0);
  });

  it('detects GitLab deploy tokens (gldt-)', () => {
    const found = scanForSecrets('DEPLOY_TOKEN=gldt-abcdefghijklmnopqrstuvwx');
    assert.ok(found.length > 0);
  });

  it('detects GitLab SCIM/OAuth tokens (glsoat-)', () => {
    const found = scanForSecrets('SCIM_TOKEN=glsoat-abcdefghijklmnopqrstuvwx');
    assert.ok(found.length > 0);
  });

  it('detects GitLab CI job tokens (glcbt-)', () => {
    const found = scanForSecrets('CI_JOB_TOKEN=glcbt-abcdefghijklmnopqrstuvwx');
    assert.ok(found.length > 0);
  });

  it('detects GitLab feed tokens (glft-)', () => {
    const found = scanForSecrets('FEED_TOKEN=glft-abcdefghijklmnopqrstuvwx');
    assert.ok(found.length > 0);
  });

  it('detects GitLab feature flags client tokens (glffct-)', () => {
    const found = scanForSecrets('FF_TOKEN=glffct-abcdefghijklmnopqrstuvwx');
    assert.ok(found.length > 0);
  });

  it('detects GitLab incoming mail tokens (glimt-)', () => {
    const found = scanForSecrets('MAIL_TOKEN=glimt-abcdefghijklmnopqrstuvwx');
    assert.ok(found.length > 0);
  });

  it('detects GitLab pipeline trigger tokens (glptt-)', () => {
    const found = scanForSecrets('TRIGGER_TOKEN=glptt-abcdefghijklmnopqrstuvwx');
    assert.ok(found.length > 0);
  });

  it('detects Slack bot tokens (xoxb-)', () => {
    const found = scanForSecrets('SLACK_TOKEN=xoxb-1234567890-abcdefghij');
    assert.ok(found.length > 0);
  });

  it('detects Slack user tokens (xoxp-)', () => {
    const found = scanForSecrets('token=xoxp-1234567890-abcdefghij');
    assert.ok(found.length > 0);
  });

  it('detects JWT tokens (eyJ...)', () => {
    const found = scanForSecrets('auth=eyJhbGciOiJIUzI1NiIsInR5.eyJzdWIiOiIxMjM0NTY3ODkw');
    assert.ok(found.length > 0);
  });

  it('detects npm tokens (npm_)', () => {
    const found = scanForSecrets('NPM_TOKEN=npm_abcdefghijklmnopqrstuvwxyz1234567890ab');
    assert.ok(found.length > 0);
  });

  it('detects PyPI tokens (pypi-)', () => {
    const found = scanForSecrets('PYPI_TOKEN=pypi-' + 'a'.repeat(50));
    assert.ok(found.length > 0);
  });

  it('detects GitHub OAuth tokens (gho_)', () => {
    const found = scanForSecrets('token=gho_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij1234');
    assert.ok(found.length > 0);
  });

  it('detects GitHub server tokens (ghs_)', () => {
    const found = scanForSecrets('token=ghs_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij1234');
    assert.ok(found.length > 0);
  });

  it('detects password assignments', () => {
    const found = scanForSecrets('password = "my-secret-password-123"');
    assert.ok(found.length > 0);
  });

  it('detects token assignments', () => {
    const found = scanForSecrets("token: 'a-long-secret-token-value'");
    assert.ok(found.length > 0);
  });

  it('returns empty array for clean content', () => {
    const found = scanForSecrets('const x = 42;\nconsole.log("hello world");');
    assert.equal(found.length, 0);
  });

  it('ignores a password constant that was already in the file', () => {
    const before = 'package cli\n\nconst (\n    Password            = "placeholder"\n)\n\nUsage: "Optional retry attempts."\n';
    const after = before.replace(
      'Usage: "Optional retry attempts."',
      'Usage: "Optional retry attempts. Must be greater than 0."',
    );
    assert.deepEqual(scanAddedSecrets(before, after), []);
  });

  it('returns nothing when the edit only deletes lines', () => {
    assert.deepEqual(scanAddedSecrets('keep\npassword = "my-secret-password-123"\n', 'keep\n'), []);
  });

  it('flags a password assignment the edit introduces', () => {
    const before = 'package cli\n\nvar x = 1\n';
    const after = 'package cli\n\nvar x = 1\npassword = "my-secret-password-123"\n';
    const found = scanAddedSecrets(before, after);
    assert.ok(found.length > 0);
  });

  it('truncates matched secrets for safety', () => {
    const found = scanForSecrets('sk-abcdefghijklmnopqrstuvwxyz1234567890abcdef');
    for (const f of found) {
      assert.ok(f.endsWith('...'));
      assert.ok(f.length <= 25);
    }
  });
});

// ---------------------------------------------------------------------------
// isNonUtfEncoding
// ---------------------------------------------------------------------------

describe('isNonUtfEncoding', () => {
  it('returns false for valid UTF-8 text', () => {
    const buf = Buffer.from('Hello, world! 日本語テスト', 'utf-8');
    assert.equal(isNonUtfEncoding(buf), false);
  });

  it('returns false for ASCII-only text', () => {
    const buf = Buffer.from('plain ascii text 1234');
    assert.equal(isNonUtfEncoding(buf), false);
  });

  it('returns false for UTF-8-BOM text', () => {
    const buf = Buffer.concat([Buffer.from([0xEF, 0xBB, 0xBF]), Buffer.from('hello', 'utf-8')]);
    assert.equal(isNonUtfEncoding(buf), false);
  });

  it('returns false for UTF-16LE text', () => {
    const buf = Buffer.concat([Buffer.from([0xFF, 0xFE]), Buffer.from('hello', 'utf16le')]);
    assert.equal(isNonUtfEncoding(buf), false);
  });

  it('returns false for UTF-16BE text', () => {
    const buf = Buffer.concat([Buffer.from([0xFE, 0xFF]), Buffer.from('hello', 'utf16le')]);
    assert.equal(isNonUtfEncoding(buf), false);
  });

  it('returns true for Shift-JIS-like invalid UTF-8 sequences', () => {
    // Shift-JIS encoded text has many high bytes that don't form valid UTF-8
    const shiftJisBytes = Buffer.from([
      0x82, 0xB1, 0x82, 0xCC, 0x83, 0x65, 0x83, 0x4C, // "このテキ" in Shift-JIS
      0x83, 0x58, 0x83, 0x67, 0x82, 0xCD, // "ストは"
      0x53, 0x68, 0x69, 0x66, 0x74, 0x2D, 0x4A, 0x49, 0x53, // "Shift-JIS"
    ]);
    assert.equal(isNonUtfEncoding(shiftJisBytes), true);
  });

  it('returns true for Windows-1252-like invalid UTF-8 sequences', () => {
    // Windows-1252 has bytes 0x80-0x9F that are invalid UTF-8 lead bytes
    const win1252Bytes = Buffer.from([
      0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, // "Hello "
      0x93, 0x77, 0x6F, 0x72, 0x6C, 0x64, 0x94, // "world" with smart quotes
      0x20, 0x80, 0x31, 0x30, 0x30, // " €100"
    ]);
    assert.equal(isNonUtfEncoding(win1252Bytes), true);
  });

  it('returns false for empty buffer', () => {
    assert.equal(isNonUtfEncoding(Buffer.alloc(0)), false);
  });

  it('returns false for buffer with few isolated non-UTF-8 bytes', () => {
    // 2 invalid bytes is below the threshold of 3
    const buf = Buffer.from([0x48, 0x65, 0x6C, 0x80, 0x6C, 0x6F, 0x80]);
    assert.equal(isNonUtfEncoding(buf), false);
  });
});

// ---------------------------------------------------------------------------
// detectEncoding / decodeBuffer / encodeText round-trip
// ---------------------------------------------------------------------------

describe('encoding round-trip', () => {
  it('round-trips UTF-8', () => {
    const text = 'Hello, 世界!';
    const buf = encodeText(text, 'utf-8');
    const { text: decoded, encoding } = decodeBuffer(buf);
    assert.equal(decoded, text);
    assert.equal(encoding, 'utf-8');
  });

  it('round-trips UTF-8-BOM', () => {
    const text = 'BOM test';
    const buf = encodeText(text, 'utf-8-bom');
    assert.deepEqual(buf.subarray(0, 3), Buffer.from([0xEF, 0xBB, 0xBF]));
    const { text: decoded, encoding } = decodeBuffer(buf);
    assert.equal(decoded, text);
    assert.equal(encoding, 'utf-8-bom');
  });

  it('round-trips UTF-16LE', () => {
    const text = 'UTF-16LE test';
    const buf = encodeText(text, 'utf-16le');
    assert.deepEqual(buf.subarray(0, 2), Buffer.from([0xFF, 0xFE]));
    const { text: decoded, encoding } = decodeBuffer(buf);
    assert.equal(decoded, text);
    assert.equal(encoding, 'utf-16le');
  });

  it('round-trips UTF-16BE', () => {
    const text = 'UTF-16BE test';
    const buf = encodeText(text, 'utf-16be');
    assert.deepEqual(buf.subarray(0, 2), Buffer.from([0xFE, 0xFF]));
    const { text: decoded, encoding } = decodeBuffer(buf);
    assert.equal(decoded, text);
    assert.equal(encoding, 'utf-16be');
  });
});

// ---------------------------------------------------------------------------
// scanForSecrets — freeze protection on long unbroken text
// ---------------------------------------------------------------------------
describe('scanForSecrets – freeze protection', () => {
  it('does not hang on a 100KB unbroken alphanumeric line', () => {
    const longLine = 'a'.repeat(100_000);
    const start = Date.now();
    const found = scanForSecrets(longLine);
    const elapsed = Date.now() - start;
    assert.equal(found.length, 0);
    assert.ok(elapsed < 1000, `scanForSecrets took ${elapsed}ms on long line — should be < 1s`);
  });

  it('still detects secrets within the first 500 chars of a line', () => {
    const secret = 'sk-abcdefghijklmnopqrstuvwxyz1234567890abcdef';
    const line = secret + 'x'.repeat(10_000);
    const found = scanForSecrets(line);
    assert.ok(found.length > 0, 'should detect secret at start of long line');
  });

  it('does not detect secrets past the line scan limit', () => {
    const padding = 'x'.repeat(600);
    const secret = 'sk-abcdefghijklmnopqrstuvwxyz1234567890abcdef';
    const line = padding + secret;
    const found = scanForSecrets(line);
    assert.equal(found.length, 0, 'secret past 500-char limit should not be scanned');
  });
});

// ---------------------------------------------------------------------------
// redactSecrets — freeze protection on long unbroken text
// ---------------------------------------------------------------------------
describe('redactSecrets – freeze protection', () => {
  it('does not hang on a 200KB unbroken base64 blob', () => {
    const base64Chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=';
    let blob = '';
    for (let i = 0; i < 200_000; i++) blob += base64Chars[i % base64Chars.length];
    const start = Date.now();
    redactSecrets(blob);
    const elapsed = Date.now() - start;
    assert.ok(elapsed < 2000, `redactSecrets took ${elapsed}ms on 200KB blob — should be < 2s`);
  });

  it('still redacts secrets within the per-line limit', () => {
    const line = 'token: sk-ant-abcdefghijklmnopqrstuvwxyz1234567890 end';
    const result = redactSecrets(line);
    assert.ok(result.includes('[REDACTED-SECRET]'), 'should redact short-line secret');
    assert.ok(!result.includes('sk-ant-abcdefghijklmnopqrstuvwxyz1234567890'));
  });
});

// ---------------------------------------------------------------------------
// redactSecrets — GitLab token families
// ---------------------------------------------------------------------------
describe('redactSecrets – GitLab tokens', () => {
  it('redacts GitLab personal access tokens (glpat-)', () => {
    const result = redactSecrets('export GITLAB_TOKEN=glpat-abcdefghijklmnopqrstuvwx');
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('glpat-abcdefghijklmnopqrstuvwx'));
  });

  it('redacts GitLab runner tokens (glrt-)', () => {
    const result = redactSecrets('RUNNER_TOKEN=glrt-abcdefghijklmnopqrstuvwx');
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('glrt-abcdefghijklmnopqrstuvwx'));
  });

  it('redacts GitLab OAuth tokens (gloas-)', () => {
    const result = redactSecrets('Authorization: gloas-abcdefghijklmnopqrstuvwx');
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('gloas-abcdefghijklmnopqrstuvwx'));
  });

  it('redacts GitLab agent tokens (glagent-)', () => {
    const result = redactSecrets('config: glagent-abcdefghijklmnopqrstuvwx');
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('glagent-abcdefghijklmnopqrstuvwx'));
  });

  it('redacts GitLab deploy tokens (gldt-)', () => {
    const result = redactSecrets('deploy_token=gldt-abcdefghijklmnopqrstuvwx');
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('gldt-abcdefghijklmnopqrstuvwx'));
  });

  it('redacts GitLab SCIM/OAuth tokens (glsoat-)', () => {
    const result = redactSecrets('scim_key: glsoat-abcdefghijklmnopqrstuvwx');
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('glsoat-abcdefghijklmnopqrstuvwx'));
  });

  it('redacts GitLab CI job tokens (glcbt-)', () => {
    const result = redactSecrets('CI_JOB_TOKEN=glcbt-abcdefghijklmnopqrstuvwx');
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('glcbt-abcdefghijklmnopqrstuvwx'));
  });

  it('redacts GitLab feed tokens (glft-)', () => {
    const result = redactSecrets('feed=glft-abcdefghijklmnopqrstuvwx');
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('glft-abcdefghijklmnopqrstuvwx'));
  });

  it('redacts GitLab feature flags client tokens (glffct-)', () => {
    const result = redactSecrets('ff_client_token=glffct-abcdefghijklmnopqrstuvwx');
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('glffct-abcdefghijklmnopqrstuvwx'));
  });

  it('redacts GitLab incoming mail tokens (glimt-)', () => {
    const result = redactSecrets('mail_token: glimt-abcdefghijklmnopqrstuvwx');
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('glimt-abcdefghijklmnopqrstuvwx'));
  });

  it('redacts GitLab pipeline trigger tokens (glptt-)', () => {
    const result = redactSecrets('trigger=glptt-abcdefghijklmnopqrstuvwx');
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('glptt-abcdefghijklmnopqrstuvwx'));
  });

  it('does not false-positive on "gl" followed by non-token text', () => {
    const text = 'global variable gladiator glorious';
    const result = redactSecrets(text);
    assert.equal(result, text);
  });
});

// ---------------------------------------------------------------------------
// redactSecrets — URL passwords with special characters
// ---------------------------------------------------------------------------
describe('redactSecrets – URL password special chars', () => {
  it('redacts password containing @ symbol', () => {
    const url = 'https://user:p@ssword@host.com/path';
    const result = redactSecrets(url);
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('p@ssword'));
    assert.ok(result.includes('@host.com'));
  });

  it('redacts URL-encoded password with %40 (@)', () => {
    const url = 'https://admin:p%40ss+w0rd@db.example.com:5432/mydb';
    const result = redactSecrets(url);
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('p%40ss+w0rd'));
  });

  it('redacts database connection string password', () => {
    const url = 'postgres://user:Pa$$w0rd!@host:5432/db';
    const result = redactSecrets(url);
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('Pa$$w0rd!'));
  });

  it('redacts mongodb connection string password', () => {
    const url = 'mongodb://admin:s3cr3t@cluster.mongodb.net/db';
    const result = redactSecrets(url);
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('s3cr3t'));
  });

  it('does not redact short passwords (< 4 chars)', () => {
    const url = 'https://user:abc@host.com/path';
    const result = redactSecrets(url);
    assert.equal(result, url);
  });
});

// ---------------------------------------------------------------------------
// validateFilePath — NUL byte rejection
// ---------------------------------------------------------------------------

describe('validateFilePath', () => {
  it('returns null for a normal file path', () => {
    assert.equal(validateFilePath('/home/user/file.txt'), null);
  });

  it('returns null for an empty string', () => {
    assert.equal(validateFilePath(''), null);
  });

  it('returns error for path containing NUL byte', () => {
    const result = validateFilePath('/etc/passwd\x00.txt');
    assert.ok(result, 'should return a ToolResult');
    assert.equal(result!.isError, true);
    assert.ok(result!.output.includes('null byte'));
  });

  it('returns error for path with NUL at the start', () => {
    const result = validateFilePath('\x00/etc/passwd');
    assert.ok(result);
    assert.equal(result!.isError, true);
  });

  it('returns error for path with NUL in the middle', () => {
    const result = validateFilePath('/home/user\x00/../../etc/shadow');
    assert.ok(result);
    assert.equal(result!.isError, true);
  });

  it('returns error for path that is only a NUL byte', () => {
    const result = validateFilePath('\x00');
    assert.ok(result);
    assert.equal(result!.isError, true);
  });

  it('returns null for paths with other special characters', () => {
    assert.equal(validateFilePath('/home/user/file with spaces.txt'), null);
    assert.equal(validateFilePath('/home/user/file-name_v2.tar.gz'), null);
    assert.equal(validateFilePath('/home/user/日本語ファイル.txt'), null);
  });
});

// ---------------------------------------------------------------------------
// redactSecrets — unquoted key=value assignments (Gap #1)
// ---------------------------------------------------------------------------

describe('redactSecrets — unquoted key=value (Gap #1)', () => {
  it('redacts unquoted PASSWORD=value', () => {
    const result = redactSecrets('PASSWORD=mysecret123');
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('mysecret123'));
  });

  it('redacts export API_KEY=value', () => {
    const result = redactSecrets('export API_KEY=abc123456789');
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('abc123456789'));
  });

  it('redacts TOKEN=value', () => {
    const result = redactSecrets('TOKEN=abcdefghij');
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('abcdefghij'));
  });

  it('redacts SECRET=value', () => {
    const result = redactSecrets('SECRET=myverysecretval');
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('myverysecretval'));
  });

  it('redacts GITHUB_TOKEN=value', () => {
    const result = redactSecrets('GITHUB_TOKEN=ghnotapattern');
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('ghnotapattern'));
  });

  it('redacts DATABASE_URL=value', () => {
    const result = redactSecrets('DATABASE_URL=postgres://user:pass@host/db');
    assert.ok(result.includes('[REDACTED'));
    assert.ok(!result.includes('user:pass'));
  });

  it('does NOT redact non-sensitive keys like PATH', () => {
    const result = redactSecrets('PATH=/usr/bin');
    assert.equal(result, 'PATH=/usr/bin');
  });

  it('does NOT redact short values (< 6 chars)', () => {
    const result = redactSecrets('TOKEN=abc');
    assert.equal(result, 'TOKEN=abc');
  });

  it('handles multi-line with mixed redactable keys', () => {
    const input = 'HOME=/home/user\nAPI_KEY=supersecretkey123\nPATH=/usr/bin';
    const result = redactSecrets(input);
    assert.ok(!result.includes('supersecretkey123'));
    assert.ok(result.includes('HOME=/home/user'));
    assert.ok(result.includes('PATH=/usr/bin'));
  });
});

// ---------------------------------------------------------------------------
// redactSecrets — Bearer/Basic percent-encoding and positional (Gap #13, #14)
// ---------------------------------------------------------------------------

describe('redactSecrets — Bearer/Basic edge cases (Gap #13, #14)', () => {
  it('redacts percent-encoded Bearer%20token', () => {
    const result = redactSecrets('Authorization: Bearer%20abc123456789012345678901234567890');
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('abc123456789012345678901234567890'));
  });

  it('redacts "Bearer was <token>" in error messages', () => {
    const result = redactSecrets('error: Bearer was abc123456789012345678901234567890');
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('abc123456789012345678901234567890'));
  });

  it('redacts "Bearer token <value>" pattern', () => {
    const result = redactSecrets('The Bearer token abc123456789012345678901234567890 expired');
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('abc123456789012345678901234567890'));
  });

  it('still redacts standard Bearer header', () => {
    const result = redactSecrets('Authorization: Bearer abc123456789012345678901234567890');
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('abc123456789012345678901234567890'));
  });

  it('redacts Basic auth with percent encoding', () => {
    const result = redactSecrets('Basic%20dXNlcjpwYXNzd29yZHNlY3JldA==');
    assert.ok(result.includes('[REDACTED-SECRET]'));
  });
});

// ---------------------------------------------------------------------------
// redactSecrets — invisible chars in key names (Gap #15)
// ---------------------------------------------------------------------------

describe('redactSecrets — invisible chars in keys (Gap #15)', () => {
  it('redacts key with zero-width space in name', () => {
    const result = redactSecrets('API​_KEY=secretvalue123');
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('secretvalue123'));
  });

  it('redacts key with soft hyphen in name', () => {
    const result = redactSecrets('PASS­WORD=secretvalue123');
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('secretvalue123'));
  });

  it('redacts key with bidi override in name', () => {
    const result = redactSecrets('TOKEN‪=secretvalue123');
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('secretvalue123'));
  });
});

describe('redactSecrets — new token patterns', () => {
  it('redacts Slack bot tokens', () => {
    const result = redactSecrets('SLACK_TOKEN=xoxb-1234567890-abcdefghij');
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('xoxb-'));
  });

  it('redacts Slack user tokens', () => {
    const result = redactSecrets('token=xoxp-1234567890-abcdefghij');
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('xoxp-'));
  });

  it('redacts JWT tokens', () => {
    const result = redactSecrets('auth: eyJhbGciOiJIUzI1NiIsInR5cCI6.eyJzdWIiOiIxMjM0NTY3ODkwIiw.SflKxwRJSMeKKF2QT4fwpM');
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('eyJhbGci'));
  });

  it('redacts npm tokens', () => {
    const result = redactSecrets('NPM_TOKEN=npm_abcdefghijklmnopqrstuvwxyz1234567890ab');
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('npm_abcdef'));
  });

  it('redacts PyPI tokens', () => {
    const result = redactSecrets('PYPI_TOKEN=pypi-' + 'a'.repeat(50));
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('pypi-'));
  });

  it('redacts GitHub OAuth tokens (gho_)', () => {
    const result = redactSecrets('token=gho_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij1234');
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('gho_'));
  });

  it('redacts GitHub server tokens (ghs_)', () => {
    const result = redactSecrets('token=ghs_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij1234');
    assert.ok(result.includes('[REDACTED-SECRET]'));
    assert.ok(!result.includes('ghs_'));
  });
});
