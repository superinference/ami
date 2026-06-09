import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  validateRequiredString,
  resolveSearchPath,
  buildToolDescriptionContext,
  renderToolDescription,
  detectLineEnding,
  normalizeToLf,
  convertToLineEnding,
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
});

describe('normalizeToLf', () => {
  it('converts CRLF to LF', () => {
    assert.equal(normalizeToLf('a\r\nb\r\n'), 'a\nb\n');
  });

  it('leaves LF unchanged', () => {
    assert.equal(normalizeToLf('a\nb\n'), 'a\nb\n');
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
});
