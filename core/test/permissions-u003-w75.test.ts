import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  resolveFileOperationType,
  resolveConfigFolderScope,
  buildPermissionSuggestions,
  type FileOperationType,
  type ConfigFolderScope,
  type PermissionUpdate,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// U003 (w75): operation-aware (read/write/create) + config-folder-scope permission
// affordances. Mirrors Claude's FileOperationType + isInClaudeFolder /
// isInGlobalClaudeFolder (components/permissions/FilePermissionDialog/
// permissionOptions.tsx): a mutating edit inside the agent's OWN config folder
// offers a broader, session-scoped "allow editing its own settings this session"
// grant instead of only a single-file allow.
// ---------------------------------------------------------------------------

let tmp: string;
before(() => {
  tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'perm-u003-w75-'));
});
after(() => {
  try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ }
});

// ---- resolveFileOperationType ---------------------------------------------

test('resolveFileOperationType: read-shaped tools (canonical + aliases) are read', () => {
  for (const t of ['file_read', 'glob', 'grep', 'list_dir', 'Read', 'Grep', 'Glob', 'LS']) {
    assert.equal(
      resolveFileOperationType(t, { file_path: '/repo/a.ts' }),
      'read' satisfies FileOperationType,
      `${t} should be read`,
    );
  }
});

test('resolveFileOperationType: mutating tool on an EXISTING file is write', () => {
  const f = path.join(tmp, 'exists.ts');
  fs.writeFileSync(f, 'x');
  assert.equal(resolveFileOperationType('file_edit', { file_path: f }), 'write');
  assert.equal(resolveFileOperationType('Edit', { file_path: f }), 'write');
});

test('resolveFileOperationType: mutating tool on a MISSING file is create', () => {
  const f = path.join(tmp, 'brand-new.ts');
  assert.equal(resolveFileOperationType('file_edit', { file_path: f }), 'create');
  assert.equal(resolveFileOperationType('Write', { path: f }), 'create');
});

test('resolveFileOperationType: non-file tool / no path → undefined', () => {
  assert.equal(resolveFileOperationType('bash', { command: 'ls' }), undefined);
  assert.equal(resolveFileOperationType('file_edit', {}), undefined);
  assert.equal(resolveFileOperationType('file_edit', { file_path: '' }), undefined);
});

// ---- resolveConfigFolderScope ---------------------------------------------

test('resolveConfigFolderScope: project .ami / .ami-plugin / .claude → project', () => {
  for (const name of ['.ami', '.ami-plugin', '.claude']) {
    assert.equal(
      resolveConfigFolderScope(path.join(tmp, name, 'settings.json'), tmp),
      'project-config-folder' satisfies ConfigFolderScope,
      `${name} should be project scope`,
    );
  }
});

test('resolveConfigFolderScope: global ~/.ami etc → global', () => {
  const home = os.homedir();
  assert.equal(
    resolveConfigFolderScope(path.join(home, '.ami', 'settings.json'), tmp),
    'global-config-folder',
  );
});

test('resolveConfigFolderScope: project wins when cwd is under $HOME (overlap)', () => {
  const home = os.homedir();
  // A project folder nested in HOME: its own .ami must resolve as PROJECT, not global.
  const projectCwd = path.join(home, '.ami', 'projects', 'demo');
  assert.equal(
    resolveConfigFolderScope(path.join(projectCwd, '.ami', 'settings.json'), projectCwd),
    'project-config-folder',
  );
});

test('resolveConfigFolderScope: a path outside every config folder → null', () => {
  assert.equal(resolveConfigFolderScope(path.join(tmp, 'src', 'a.ts'), tmp), null);
  assert.equal(resolveConfigFolderScope('', tmp), null);
  // .vscode / .git are config-ish but NOT AMI's own settings → not self-grantable.
  assert.equal(resolveConfigFolderScope(path.join(tmp, '.vscode', 'tasks.json'), tmp), null);
  assert.equal(resolveConfigFolderScope(path.join(tmp, '.git', 'config'), tmp), null);
});

// ---- buildPermissionSuggestions: config-folder grant -----------------------

test('buildPermissionSuggestions: config-folder edit offers a session folder grant + the specific-file grant', () => {
  const prevCwd = process.cwd();
  process.chdir(tmp);
  try {
    const target = path.join(tmp, '.ami', 'settings.json');
    const sugg = buildPermissionSuggestions('file_edit', { file_path: target });
    // w96/G9: folder grant + file grant + a switch-to-acceptEdits affordance.
    assert.equal(sugg.length, 3, 'folder grant + file grant + setMode acceptEdits');
    const [folderGrant, fileGrant, setMode] = sugg as [PermissionUpdate, PermissionUpdate, PermissionUpdate];
    assert.equal(folderGrant.type, 'addRules');
    assert.equal(folderGrant.destination, 'session', 'folder grant must be session-only (never persisted)');
    const expectedRoot = `${path.resolve(tmp, '.ami')}${path.sep}*`;
    assert.deepEqual(
      (folderGrant as { rules: { toolName: string; ruleContent?: string }[] }).rules,
      [{ toolName: 'file_edit', ruleContent: expectedRoot }],
    );
    assert.deepEqual(
      (fileGrant as { rules: { toolName: string; ruleContent?: string }[] }).rules,
      [{ toolName: 'file_edit', ruleContent: target }],
    );
    assert.equal(setMode.type, 'setMode');
    assert.equal((setMode as Extract<PermissionUpdate, { type: 'setMode' }>).mode, 'acceptEdits');
  } finally {
    process.chdir(prevCwd);
  }
});

test('buildPermissionSuggestions: ordinary (non-config) edit still yields a single specific-file grant', () => {
  const prevCwd = process.cwd();
  process.chdir(tmp);
  try {
    const target = path.join(tmp, 'src', 'a.ts');
    const sugg = buildPermissionSuggestions('file_edit', { file_path: target });
    // w96/G9: specific-file grant + the switch-to-acceptEdits affordance (no
    // config-folder grant for an ordinary path).
    assert.equal(sugg.length, 2, 'file grant + setMode acceptEdits, no config-folder grant');
    const fileGrant = sugg.find(s => s.type === 'addRules') as Extract<PermissionUpdate, { type: 'addRules' }>;
    assert.deepEqual(
      fileGrant.rules,
      [{ toolName: 'file_edit', ruleContent: target }],
    );
    assert.ok(sugg.some(s => s.type === 'setMode'), 'offers switch-to-acceptEdits');
  } finally {
    process.chdir(prevCwd);
  }
});

test('buildPermissionSuggestions: a READ of a config-folder file widens the dir, no folder grant', () => {
  const prevCwd = process.cwd();
  process.chdir(tmp);
  try {
    const target = path.join(tmp, '.ami', 'settings.json');
    const sugg = buildPermissionSuggestions('file_read', { file_path: target });
    // A read widens the dir (addDirectories) and additionally offers a persistable
    // file_read Read rule (/**) — but NO config-folder session grant, which is a
    // write-only affordance (w87/U003).
    assert.equal(sugg.length, 2);
    assert.equal(sugg[0]!.type, 'addDirectories');
    const rule = sugg[1]!;
    assert.ok(rule.type === 'addRules');
    assert.equal(rule.rules[0]!.toolName, 'file_read');
    assert.ok(rule.rules[0]!.ruleContent?.endsWith('/**'));
  } finally {
    process.chdir(prevCwd);
  }
});
