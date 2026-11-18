import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  PermissionManager,
  validatePermissionRule,
  isFilePatternTool,
  isBashPrefixTool,
  toolPatternClass,
  TOOL_PATTERN_CLASS,
  TOOL_PATTERN_CLASSES,
  permissionModeSchema,
  externalPermissionModeSchema,
  normalizePermissionMode,
  isExternalPermissionMode,
  PERMISSION_MODES,
  EXTERNAL_PERMISSION_MODES,
} from '../src/permissions';

// ===========================================================================
// U003 w121 — core/core/src/permissions.ts parity gaps
// ===========================================================================

// ---------------------------------------------------------------------------
// Gap: Disable xargs auto-allow on Windows (data-to-code UNC bridge).
// Parity: Claude's getCommandAllowlist drops `xargs` on win32
// (tools/BashTool/readOnlyValidation.ts L1203-1209). The hostile UNC path lives
// in the PIPED DATA, not the command string, so the UNC guard cannot see it —
// the only safe answer is to never auto-allow an xargs pipeline on Windows.
// ---------------------------------------------------------------------------
describe('xargs auto-allow disabled on Windows', () => {
  const realPlatform = process.platform;
  afterEach(() => {
    Object.defineProperty(process, 'platform', { value: realPlatform });
  });

  const safeXargsPipeline = 'cat list.txt | xargs cat';

  it('classifies an xargs read-only pipeline as safe on a POSIX platform', () => {
    Object.defineProperty(process, 'platform', { value: 'linux' });
    const pm = new PermissionManager({ cwd: process.cwd() });
    assert.equal(pm.classifyBashCommand(safeXargsPipeline), 'safe');
  });

  it('refuses the SAME xargs pipeline on win32 (never auto-allowed)', () => {
    Object.defineProperty(process, 'platform', { value: 'win32' });
    const pm = new PermissionManager({ cwd: process.cwd() });
    // The discriminating assertion: on win32 it must NOT classify as 'safe'
    // (before the fix it did, because isXargsSafe ignored the platform).
    assert.notEqual(pm.classifyBashCommand(safeXargsPipeline), 'safe');
  });
});

// ---------------------------------------------------------------------------
// Gap: Tool pattern-class table (filePatternTools vs bashPrefixTools) +
// isFilePatternTool/isBashPrefixTool helpers. Parity: Claude's
// toolValidationConfig (utils/settings/toolValidationConfig.ts L9-38).
// ---------------------------------------------------------------------------
describe('tool pattern classification', () => {
  it('classifies file-glob tools as "file" (canonical + Claude-spelled)', () => {
    for (const t of ['file_read', 'file_edit', 'glob', 'notebook_edit']) {
      assert.equal(toolPatternClass(t), 'file', t);
      assert.equal(isFilePatternTool(t), true, t);
      assert.equal(isBashPrefixTool(t), false, t);
    }
    // Alias resolution: Claude names map through normalizeToolName first.
    assert.equal(isFilePatternTool('Edit'), true);
    assert.equal(isFilePatternTool('Read'), true);
    assert.equal(isFilePatternTool('Write'), true); // Write → file_edit
    assert.equal(isFilePatternTool('Glob'), true);
    assert.equal(isFilePatternTool('NotebookEdit'), true);
  });

  it('classifies bash as a "bash" prefix tool (canonical + alias)', () => {
    assert.equal(toolPatternClass('bash'), 'bash');
    assert.equal(isBashPrefixTool('bash'), true);
    assert.equal(isBashPrefixTool('Bash'), true);
    assert.equal(isFilePatternTool('bash'), false);
  });

  it('returns undefined for tools that use custom content validators', () => {
    for (const t of ['web_fetch', 'web_search', 'grep', 'mcp__srv__tool']) {
      assert.equal(toolPatternClass(t), undefined, t);
      assert.equal(isFilePatternTool(t), false, t);
      assert.equal(isBashPrefixTool(t), false, t);
    }
  });

  it('the TOOL_PATTERN_CLASS map is consistent with the class list', () => {
    for (const cls of Object.values(TOOL_PATTERN_CLASS)) {
      assert.ok((TOOL_PATTERN_CLASSES as readonly string[]).includes(cls));
    }
    assert.deepEqual([...TOOL_PATTERN_CLASSES], ['file', 'bash']);
  });
});

// ---------------------------------------------------------------------------
// Gap: File-pattern-tool rule validation — reject ':*' (Bash-only) in a file
// pattern, and flag a non-boundary wildcard. Parity: Claude's
// permissionValidation file-tool branch (utils/settings/permissionValidation.ts
// L199-236).
// ---------------------------------------------------------------------------
describe('validatePermissionRule — file-pattern tools', () => {
  it('rejects the Bash ":*" prefix form in a file pattern with glob guidance', () => {
    const r = validatePermissionRule('file_edit(npm:*)');
    assert.equal(r.valid, false);
    assert.match(r.error ?? '', /":\*" syntax is only for Bash/);
    assert.ok(r.examples?.some(e => e.includes('**')));
  });

  it('rejects a wildcard that is not at a path boundary', () => {
    const r = validatePermissionRule('file_edit(src/foo*bar)');
    assert.equal(r.valid, false);
    assert.match(r.error ?? '', /Wildcard placement might be incorrect/);
  });

  it('accepts boundary wildcards and recursive globs', () => {
    for (const rule of [
      'file_edit(*.ts)',
      'file_read(src/**)',
      'glob(**/*.test.ts)',
      'notebook_edit(*.ipynb)',
      'file_edit(src/*)',
      'file_edit(src/foo.ts)',
    ]) {
      assert.equal(validatePermissionRule(rule).valid, true, rule);
    }
  });

  it('still rejects the empty and misplaced ":*" forms for bash via the shared helper', () => {
    assert.equal(validatePermissionRule('bash(:*)').valid, false);
    assert.equal(validatePermissionRule('bash(npm:* run)').valid, false);
    assert.equal(validatePermissionRule('bash(npm:*)').valid, true);
  });
});

// ---------------------------------------------------------------------------
// Gap: Zod enum schema for permission-mode validation (permissionModeSchema /
// externalPermissionModeSchema). Parity: Claude's
// utils/permissions/PermissionMode.ts schemas. The schema must back the existing
// normalize/guard so they can never disagree on the valid set.
// ---------------------------------------------------------------------------
describe('permission-mode zod schemas', () => {
  it('permissionModeSchema accepts exactly the canonical modes', () => {
    for (const m of PERMISSION_MODES) {
      assert.equal(permissionModeSchema.parse(m), m);
    }
    assert.equal(permissionModeSchema.safeParse('nonsense').success, false);
    assert.equal(permissionModeSchema.safeParse('default').success, false); // alias, not canonical
  });

  it('externalPermissionModeSchema accepts exactly the external modes', () => {
    for (const m of EXTERNAL_PERMISSION_MODES) {
      assert.equal(externalPermissionModeSchema.parse(m), m);
    }
    assert.equal(externalPermissionModeSchema.safeParse('ask').success, false); // internal-only
  });

  it('normalizePermissionMode agrees with the schema on the canonical arm', () => {
    for (const m of PERMISSION_MODES) {
      assert.equal(normalizePermissionMode(m), m);
    }
    // Aliases still resolve (schema only governs the canonical fast-path).
    assert.equal(normalizePermissionMode('default'), 'ask');
    assert.equal(normalizePermissionMode('nope'), null);
  });

  it('isExternalPermissionMode is backed by the external schema', () => {
    assert.equal(isExternalPermissionMode('plan'), true);
    assert.equal(isExternalPermissionMode('acceptEdits'), true);
    assert.equal(isExternalPermissionMode('auto-allow'), false);
    assert.equal(isExternalPermissionMode('ask'), false);
  });
});
