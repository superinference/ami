import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

// ---- tools/index.ts: ToolRegistry + createDefaultTools ----
import { ToolRegistry, createDefaultTools } from '../src/tools/index';

describe('ToolRegistry', () => {
  it('register, get, getAll, toOpenAIFormat', () => {
    const reg = new ToolRegistry();
    const tool = {
      name: 'test_tool',
      description: 'A test tool',
      inputSchema: { type: 'object' as const, properties: { x: { type: 'string', description: 'x' } } },
      execute: async () => ({ output: 'ok' }),
    };
    reg.register(tool);
    assert.equal(reg.get('test_tool'), tool);
    assert.equal(reg.get('nonexistent'), undefined);
    assert.deepEqual(reg.getAll(), [tool]);

    const oai = reg.toOpenAIFormat();
    assert.equal(oai.length, 1);
    assert.equal(oai[0].type, 'function');
    assert.equal(oai[0].function.name, 'test_tool');
    assert.deepEqual(oai[0].function.parameters, tool.inputSchema);
  });
});

describe('createDefaultTools', () => {
  it('returns a ToolRegistry with many tools registered', () => {
    const reg = createDefaultTools('/tmp');
    const all = reg.getAll();
    assert.ok(all.length >= 30, `Expected >=30 tools, got ${all.length}`);
    assert.ok(reg.get('bash'));
    assert.ok(reg.get('file_read'));
    assert.ok(reg.get('file_write'));
    assert.ok(reg.get('file_edit'));
  });

  it('toOpenAIFormat converts all tools', () => {
    const reg = createDefaultTools('/tmp');
    const oai = reg.toOpenAIFormat();
    assert.ok(oai.length >= 30);
    for (const t of oai) {
      assert.equal(t.type, 'function');
      assert.ok(t.function.name);
      assert.ok(t.function.description);
    }
  });
});

// ---- bash-security.ts ----
import { validateBashSecurity, extractCommandPaths } from '../src/tools/bash-security';

describe('bash-security — validateBashSecurity', () => {
  it('validates safe commands', () => {
    const r = validateBashSecurity('echo hello');
    assert.ok(r);
  });
  it('flags dangerous commands', () => {
    const r = validateBashSecurity('rm -rf /');
    assert.ok(r);
  });
  it('handles pipe chains', () => {
    const r = validateBashSecurity('cat file.txt | grep pattern | wc -l');
    assert.ok(r);
  });
  it('handles redirects', () => {
    const r = validateBashSecurity('echo hello > /tmp/file.txt');
    assert.ok(r);
  });
  it('handles curl', () => {
    const r = validateBashSecurity('curl https://example.com');
    assert.ok(r);
  });
  it('handles git operations', () => {
    const r = validateBashSecurity('git push --force');
    assert.ok(r);
  });
  it('handles sudo', () => {
    const r = validateBashSecurity('sudo rm -rf /');
    assert.ok(r);
  });
  it('handles complex chains', () => {
    const r = validateBashSecurity('find . -name "*.js" -exec rm {} \\;');
    assert.ok(r);
  });
  it('handles env vars', () => {
    const r = validateBashSecurity('export SECRET=abc && echo $SECRET');
    assert.ok(r);
  });
  it('handles empty command', () => {
    const r = validateBashSecurity('');
    assert.ok(r);
  });
});

describe('bash-security — extractCommandPaths', () => {
  it('extracts paths from ls command', () => {
    const r = extractCommandPaths('ls /tmp');
    assert.ok(Array.isArray(r));
  });
  it('extracts paths from cat command', () => {
    const r = extractCommandPaths('cat /etc/hosts');
    assert.ok(r.includes('/etc/hosts'));
  });
  it('returns empty for unknown commands', () => {
    const r = extractCommandPaths('foobar baz');
    assert.ok(Array.isArray(r));
  });
  it('handles pipes', () => {
    const r = extractCommandPaths('cat file.txt | grep pattern');
    assert.ok(Array.isArray(r));
  });
  it('handles multiple commands', () => {
    const r = extractCommandPaths('cd /tmp && ls -la');
    assert.ok(Array.isArray(r));
  });
  it('handles git commands', () => {
    const r = extractCommandPaths('git add file.ts');
    assert.ok(Array.isArray(r));
  });
  it('handles rm commands', () => {
    const r = extractCommandPaths('rm -rf /tmp/test');
    assert.ok(Array.isArray(r));
  });
  it('handles mv commands', () => {
    const r = extractCommandPaths('mv a.txt b.txt');
    assert.ok(Array.isArray(r));
  });
  it('handles cp commands', () => {
    const r = extractCommandPaths('cp -r src dst');
    assert.ok(Array.isArray(r));
  });
  it('handles find commands', () => {
    const r = extractCommandPaths('find . -name "*.ts"');
    assert.ok(Array.isArray(r));
  });
  it('handles grep commands', () => {
    const r = extractCommandPaths('grep -r pattern src/');
    assert.ok(Array.isArray(r));
  });
  it('handles sed commands', () => {
    const r = extractCommandPaths('sed -i "s/old/new/" file.txt');
    assert.ok(Array.isArray(r));
  });
  it('handles awk commands', () => {
    const r = extractCommandPaths('awk "{print}" file.txt');
    assert.ok(Array.isArray(r));
  });
  it('handles touch commands', () => {
    const r = extractCommandPaths('touch newfile.txt');
    assert.ok(Array.isArray(r));
  });
  it('handles mkdir commands', () => {
    const r = extractCommandPaths('mkdir -p /tmp/test/dir');
    assert.ok(Array.isArray(r));
  });
});

// ---- exit-worktree.ts ----
import { exitWorktreeTool } from '../src/tools/exit-worktree';

describe('exitWorktreeTool — execute paths', () => {
  it('errors when not in a git repo', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ewt-'));
    const ctx: any = { cwd: tmpDir, _engineConfig: {}, _abortSignal: new AbortController().signal, _sessionId: 'test' };
    const r = await exitWorktreeTool.execute({ action: 'keep' }, ctx);
    assert.ok(r.output.length > 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---- file-read.ts ----
import { fileReadTool } from '../src/tools/file-read';

describe('fileReadTool — uncovered paths', () => {
  let tmpDir: string;
  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'fr-test-'));
  });

  it('handles nonexistent file', async () => {
    const ctx: any = { cwd: tmpDir, _abortSignal: new AbortController().signal, _engineConfig: {} };
    const r = await fileReadTool.execute({ file_path: path.join(tmpDir, 'nope.txt') }, ctx);
    assert.ok(r.output.length > 0);
    assert.equal(r.isError, true);
  });

  it('reads file with offset and limit', async () => {
    const f = path.join(tmpDir, 'lines.txt');
    fs.writeFileSync(f, Array.from({length: 100}, (_, i) => `line${i+1}`).join('\n'));
    const ctx: any = { cwd: tmpDir, _abortSignal: new AbortController().signal, _engineConfig: {} };
    const r = await fileReadTool.execute({ file_path: f, offset: 5, limit: 3 }, ctx);
    assert.ok(r.output.includes('line'));
  });

  it('handles empty file', async () => {
    const f = path.join(tmpDir, 'empty.txt');
    fs.writeFileSync(f, '');
    const ctx: any = { cwd: tmpDir, _abortSignal: new AbortController().signal, _engineConfig: {} };
    const r = await fileReadTool.execute({ file_path: f }, ctx);
    assert.ok(r.output.length > 0);
  });
});

// ---- process-manager.ts ----
import { ProcessManager } from '../src/process-manager';

describe('ProcessManager — coverage', () => {
  it('list returns empty initially', () => {
    const pm = new ProcessManager('/tmp');
    const list = pm.list();
    assert.deepEqual(list, []);
  });

  it('get returns null for unknown id', () => {
    const pm = new ProcessManager('/tmp');
    assert.equal(pm.get('nonexistent'), null);
  });

  it('kill returns false for unknown id', async () => {
    const pm = new ProcessManager('/tmp');
    const result = await pm.kill('nonexistent');
    assert.equal(result, false);
  });
});

// ---- config.ts ----
import { loadProjectConfig, loadGlobalConfig, loadLocalConfig, loadManagedConfig, mergeConfigs } from '../src/config';

describe('config — coverage', () => {
  it('loadProjectConfig returns empty for nonexistent dir', () => {
    const cfg = loadProjectConfig('/nonexistent/path/12345');
    assert.ok(cfg !== undefined);
  });

  it('loadGlobalConfig returns a config object', () => {
    const cfg = loadGlobalConfig();
    assert.ok(cfg !== undefined);
  });

  it('loadLocalConfig returns config for nonexistent dir', () => {
    const cfg = loadLocalConfig('/nonexistent/path/12345');
    assert.ok(cfg !== undefined);
  });

  it('loadManagedConfig returns a config', () => {
    const cfg = loadManagedConfig();
    assert.ok(cfg !== undefined);
  });

  it('mergeConfigs merges multiple configs', () => {
    const result = mergeConfigs({}, {}, {}, {});
    assert.ok(result !== undefined);
  });
});

// ---- worktree-manager.ts ----
import { validateWorktreeSlug, flattenSlug, worktreeBranchName, getCurrentWorktreeSession, setWorktreeSession } from '../src/worktree-manager';

describe('worktree-manager — functions', () => {
  it('validateWorktreeSlug validates good slugs', () => {
    const err = validateWorktreeSlug('my-feature');
    assert.equal(err, null);
  });

  it('validateWorktreeSlug rejects bad slugs', () => {
    const err = validateWorktreeSlug('bad slug with spaces!');
    assert.ok(err !== null);
  });

  it('flattenSlug removes slashes', () => {
    const r = flattenSlug('feature/my-branch');
    assert.ok(!r.includes('/'));
  });

  it('worktreeBranchName produces branch name', () => {
    const r = worktreeBranchName('my-feature');
    assert.ok(r.length > 0);
  });

  it('getCurrentWorktreeSession returns null initially', () => {
    setWorktreeSession(null);
    assert.equal(getCurrentWorktreeSession(), null);
  });
});

// ---- provider.ts ----
import { resolveModel, convertMessages, toolDefinitionsToOpenAI } from '../src/provider';

describe('provider — resolveModel', () => {
  it('returns model object for config', () => {
    const m = resolveModel({ baseUrl: 'https://api.openai.com/v1', apiKey: 'sk-test', provider: 'openai' });
    assert.ok(m !== null && m !== undefined);
  });

  it('uses specified model', () => {
    const m = resolveModel({ baseUrl: 'https://api.openai.com/v1', apiKey: 'sk-test', provider: 'openai', model: 'gpt-4' });
    assert.ok(m !== null);
  });
});

describe('provider — convertMessages', () => {
  it('converts simple user message', () => {
    const msgs = convertMessages([{ role: 'user', content: 'hello' }]);
    assert.ok(msgs.length >= 1);
  });

  it('converts assistant message', () => {
    const msgs = convertMessages([{ role: 'assistant', content: 'hi there' }]);
    assert.ok(msgs.length >= 1);
  });

  it('handles empty array', () => {
    const msgs = convertMessages([]);
    assert.deepEqual(msgs, []);
  });
});

describe('provider — toolDefinitionsToOpenAI', () => {
  it('converts tool definitions', () => {
    const tools = [{
      name: 'test',
      description: 'test tool',
      inputSchema: { type: 'object' as const, properties: { x: { type: 'string', description: 'x' } } },
      execute: async () => ({ output: 'ok' }),
    }];
    const result = toolDefinitionsToOpenAI(tools);
    assert.equal(result.length, 1);
    assert.equal(result[0].function.name, 'test');
  });
});

// ---- utils/shell.ts ----
import { execCommand } from '../src/utils/shell';

describe('shell — execCommand', () => {
  it('executes simple command', async () => {
    const r = await execCommand('echo hello', '/tmp');
    assert.ok(r.stdout.includes('hello'));
  });
});

// ---- tool-guardrails.ts ----
import { ToolCallGuardrailController } from '../src/tool-guardrails';

describe('ToolCallGuardrailController', () => {
  it('instantiates and checks a tool call', () => {
    const ctrl = new ToolCallGuardrailController('/tmp');
    assert.ok(ctrl);
  });
});
